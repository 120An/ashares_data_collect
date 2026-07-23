"""新闻钉钉日报任务（新闻系统 MVP 收尾 T10a）。

每日 20:40 汇总一条钉钉：当日发布(pub_time 落当日)的各 channel 文档数 + pending
补向量积压。定位是 **运营可见性**——采集/补向量各自的告警是异常信号（失败才发），
日报则是"每天一眼看健康度"的常态汇总（成功也发，进 news_daily pipeline 尾部）。

两个数据源（读别名 news）：
1. **当日发布(pub_time 落当日)的各 channel 文档数**：pub_time 落当日的文档按 channel
   聚合（terms agg），flash/cctv/marker 固定报（缺失补 0），出现的其他 channel 追加；
   总数取 track_total_hits（不被 10000 上限截断）。注：口径是"当日**发布**"而非"当日入库"
   ——今天经 verify 补采的昨日新闻（pub_time=昨天）不计入今日日报（设计如此，按 pub_time）。
2. **pending 积压**：全库 vec_status=pending 总数 + 最老一条的 fetch_time
   （距今分钟数，同 news_embed 积压口径）——补向量调度跟不上产出的监控信号。

容错基调（日报必达，但区分错因不误导运维）：
- 真·索引未建——聚合查询遇 404 / NotFoundError / 别名不存在（冷启动尚无采集），
  或响应载荷带 error（_channel_counts 抛 LookupError）→ 摘要标"索引未建"、各字段
  0/None，**仍发钉钉**（运维需要"今天索引还没建"这条信息，不能静默）；
- 其余异常——集群超时 / 查询报错等**不是**索引缺失，绝不误标"索引未建"（否则
  误导排查方向，且 query① 成功 query② 失败时自相矛盾）→ 摘要标"查询异常(类型名)"，
  仍发钉钉；
- send_dingtalk guarded——发送失败仅记日志，绝不让 job 失败（日报是通知，
  发送故障不该反噬 pipeline 成败）。
"""

from __future__ import annotations

import logging
from typing import Dict, Optional, Tuple

from opensearchpy.exceptions import NotFoundError

from data_collect.config import get_news_config
from data_collect.utils import notify
from data_collect.utils import opensearch_utils as osu
from data_collect.utils.news_common import age_minutes as _age_minutes
from data_collect.utils.news_common import now as _now
from data_collect.utils.news_common import normalize_date8 as _normalize_date8
from data_collect.utils.news_common import today as _today
from data_collect.utils.notify import send_dingtalk

logger = logging.getLogger(__name__)

# 固定上报的 channel（缺失 bucket 也报 0）：采集源 flash/cctv/announcement/stock +
# 系统标记 marker。0 行兜底可见是监控意义所在——某条采集管线整体静默挂掉时，
# 日报至少显示 0 而非无此频道。注：stock 22:00 才入库且按 pub_time 口径，
# 当日日报（20:40）恒近零，属已知时序特性（非故障信号，趋势看昨日档）。
_FIXED_CHANNELS = ("flash", "cctv", "announcement", "stock", "marker")


def _guarded_send(message: str) -> bool:
    """guarded 钉钉：发送失败仅记日志，绝不让日报 job 失败（发送故障不反噬 pipeline）。

    委托 notify.guarded_send，注入本模块级 send_dingtalk（保留其 monkeypatch 点）。
    """
    return notify.guarded_send(message, send=send_dingtalk)


def _channel_counts(client, day: str) -> Tuple[int, Dict[str, int]]:
    """当日发布(pub_time 落当日)的各 channel 文档数分布：返回 (总数, {channel: count})。

    pub_time 落 [day 00:00:00, day 23:59:59] 的文档按 channel terms 聚合；
    size=0 只取聚合与总数，track_total_hits 保证总数不被 10000 上限截断。
    响应载荷带 error（部分部署以 200 + error 回索引缺失）→ 抛 LookupError，
    与 NotFoundError 一并由 run 归入"索引未建"。
    """
    body = {
        "query": {"range": {"pub_time": {"gte": f"{day} 00:00:00",
                                         "lte": f"{day} 23:59:59"}}},
        "aggs": {"by_channel": {"terms": {"field": "channel", "size": 100}}},
        "size": 0,
        "track_total_hits": True,
    }
    resp = osu.search_raw(client, body)
    if isinstance(resp, dict) and resp.get("error"):
        raise LookupError(f"channel 聚合返回 error: {resp.get('error')!r}")
    buckets = (((resp or {}).get("aggregations") or {})
               .get("by_channel") or {}).get("buckets") or []
    counts = {b.get("key"): int(b.get("doc_count") or 0) for b in buckets if b.get("key")}
    return osu.hits_total(resp), counts


def _pending_backlog(client) -> Tuple[int, Optional[str]]:
    """pending 积压：返回 (总数, 最老 pending 的 fetch_time)。

    size=1 + fetch_time asc + track_total_hits：一次取总数与最老一条（同
    news_embed._pending_backlog 口径）。总数不被 10000 上限截断。
    """
    body = {
        "query": {"term": {"vec_status": "pending"}},
        "size": 1,
        "sort": [{"fetch_time": {"order": "asc"}}],
        "_source": ["fetch_time"],
        "track_total_hits": True,
    }
    resp = osu.search_raw(client, body)
    hits = ((resp or {}).get("hits") or {}).get("hits") or []
    oldest = (hits[0].get("_source") or {}).get("fetch_time") if hits else None
    return osu.hits_total(resp), oldest


def _channel_segment(counts: Dict[str, int]) -> str:
    """channel 计数段：固定 flash/cctv/marker（缺失补 0）+ 其他 channel（按数量降序追加）。"""
    parts = [f"{ch} {counts.get(ch, 0)}" for ch in _FIXED_CHANNELS]
    extras = {ch: n for ch, n in counts.items() if ch not in _FIXED_CHANNELS}
    for ch in sorted(extras, key=lambda c: (-extras[c], c)):
        parts.append(f"{ch} {extras[ch]}")
    return " ".join(parts)


def _format_summary(day: str, total: int, counts: Dict[str, int],
                    pending: int, oldest_minutes: Optional[int],
                    index_missing: bool = False, vec_deferred: bool = False) -> str:
    """拼装日报摘要串（发送串 == 返回串，测试锁定）。"""
    if index_missing:
        # 索引未建：各字段 0/None，仍给出完整形状供运维一眼识别
        return (f"新闻日报 {day}: 索引未建 | 总 0 | {_channel_segment({})} | "
                f"pending 0(最老 - 分钟)")
    minutes = "-" if oldest_minutes is None else str(oldest_minutes)
    # 向量禁用时 pending 是有意留存（省储存），标注清楚防误读为积压故障
    pending_seg = (f"pending {pending}(向量已禁用,留待补算)" if vec_deferred
                   else f"pending {pending}(最老 {minutes} 分钟)")
    return f"新闻日报 {day}: 总 {total} | {_channel_segment(counts)} | {pending_seg}"


# ======== Pipeline 标准接口 ========

def run(run_date: str | None = None, **kwargs) -> str:
    """每日任务：汇总当日发布(pub_time 落当日)的各 channel 文档数 + pending 积压，发钉钉日报并返回摘要串。

    `run_date` 缺省今天（"YYYYMMDD"）。查询遇真·索引未建（NotFoundError / 别名不存在 /
    响应带 error）→ 摘要标"索引未建"；其余异常（超时/查询错）→ 标"查询异常(类型名)"不
    误报索引未建；两者均仍发钉钉；send 失败不让 job 失败（均见模块 docstring）。
    """
    if run_date is None or not str(run_date).strip():
        date8 = _today().strftime("%Y%m%d")
    else:
        date8 = _normalize_date8(run_date)   # 异形 --date → 畸形 day → 误报索引未建
    day = f"{date8[:4]}-{date8[4:6]}-{date8[6:]}"

    client = osu.get_client()

    try:
        total, counts = _channel_counts(client, day)
        pending, oldest_fetch = _pending_backlog(client)
    except (NotFoundError, LookupError) as exc:
        # 真·索引未建（冷启动尚无采集 / 别名不存在 → NotFoundError；响应带 error
        # 载荷 → _channel_counts 抛 LookupError）：标"索引未建"，日报仍必达
        logger.warning(f"日报查询遇索引未建（仍发钉钉）: {exc!r}")
        summary = _format_summary(day, 0, {}, 0, None, index_missing=True)
        _guarded_send(summary)
        return summary
    except Exception as exc:
        # 其余异常（集群超时 / 查询报错等）：绝不误报"索引未建"（那会误导运维排查
        # 方向，且 query① 成功 query② 失败时自相矛盾），如实标查询异常，日报仍必达
        logger.error(f"日报查询异常（非索引未建，仍发钉钉）: {exc!r}", exc_info=True)
        summary = f"新闻日报 {day}: 查询异常({type(exc).__name__})，请查集群/日志"
        _guarded_send(summary)
        return summary

    oldest_minutes = _age_minutes(oldest_fetch, _now())
    vec_deferred = not (get_news_config().get("embedding") or {}).get("enabled", True)
    summary = _format_summary(day, total, counts, pending, oldest_minutes,
                              vec_deferred=vec_deferred)
    _guarded_send(summary)
    return summary
