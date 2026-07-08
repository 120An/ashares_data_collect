"""公司公告采集任务（巨潮官方 API，新闻子系统 ⑤，Phase 1 元数据）。

数据源单一：巨潮 `cninfo.fetch_announcements(date)` 取**当日全市场**公告原始记录
（单查询深沪京并集）。流程：记录 → 信封化（channel=announcement，_id=cninfo-{annId}，
raw_content 存原始记录 JSON）→ 归档（source=cninfo）→ OpenSearch create-only 入库
（剥 raw_*，幂等由 409→dup 保证）。

两条与行情类 job 的差异（沿用 news_cctv/news_flash 契约）：
1. **verify 自然日语义、忽略框架传入窗口**：框架按交易日推窗口（为行情设计、漏周末），
   改读 config `news.verify_days_back` 按自然日回溯逐日重采（create-only 天然补漏）；
2. **单源失败即 job 失败**：无双活，网络/限频失败 → run 上抛 → 框架 retry + 钉钉；
   run_verify 逐日隔离、failed>0 末尾 raise。

标的用**权威 secCode**（normalize_stock_code）而非 tag_stocks 标题打标——公告标题常
含保荐机构/律所等他方公司名，正则打标会误标。
"""

from __future__ import annotations

import datetime
import json
import logging
from typing import List, Tuple

from data_collect.config import get_news_config
from data_collect.utils import cninfo
from data_collect.utils import news_archive
from data_collect.utils import news_normalize as nn
from data_collect.utils import opensearch_utils as osu
from data_collect.utils import notify
from data_collect.utils.news_common import cell as _cell
from data_collect.utils.news_common import flush_spool_safe as _flush_spool_safe
from data_collect.utils.news_common import fmt_truncated as _fmt_days
from data_collect.utils.news_common import strip_raw as _strip_raw
from data_collect.utils.news_common import today as _today
from data_collect.utils.news_common import verify_dates as _verify_dates
from data_collect.utils.notify import send_dingtalk

logger = logging.getLogger(__name__)

_SOURCE = "cninfo"
_CHANNEL = "announcement"
_STATIC_BASE = "http://static.cninfo.com.cn/"

# 剥离键契约（raw_title/raw_content/time_estimated）已收敛至 news_common.RAW_ONLY_KEYS


def _record_to_envelope(rec: dict, fetch_time: str) -> dict | None:
    """单条巨潮记录 → 统一信封；缺 announcementId（无法生成幂等 _id）→ None（丢弃）。

    - `_id` = ``cninfo-{announcementId}``（make_id native 一级，源前缀防撞车）；
    - `content` = 清洗后 title（Phase1 无正文，Phase2 由 PDF body 承载）；
    - `pub_time` = announcementTime（epoch 毫秒 → 北京）；缺失回退 fetch_time + time_estimated；
    - `stocks` = [规范化 secCode]（权威码，号段推不出则空列表）；
    - `raw_content` = 完整原始记录 JSON（归档保原文，供 Phase2 回填 announcementType 等）。
    """
    ann_id = str(rec.get("announcementId") or "").strip()
    if not ann_id:
        return None

    raw_title = _cell(rec.get("announcementTitle")).strip()
    title = nn.clean_text(raw_title)
    pub_time = nn.normalize_time(rec.get("announcementTime"), source=_SOURCE)
    code = nn.normalize_stock_code(rec.get("secCode"))
    adjunct = _cell(rec.get("adjunctUrl")).strip()

    envelope = {
        "_id": nn.make_id({"source": _SOURCE, "native_id": ann_id}),
        "title": title,
        "content": title,
        "raw_title": raw_title,
        "raw_content": json.dumps(rec, ensure_ascii=False, sort_keys=True),
        "pub_time": pub_time or fetch_time,
        "fetch_time": fetch_time,
        "source": _SOURCE,
        "channel": _CHANNEL,
        "stocks": [code] if code else [],
        "vec_status": "pending",
        "pdf_status": "pending",
    }
    if adjunct:
        envelope["url"] = _STATIC_BASE + adjunct
    if pub_time is None:
        envelope["time_estimated"] = True
    return envelope


def _alert(message: str) -> bool:
    """guarded 钉钉：发送失败仅记日志，不阻塞采集（news 系统一习语）。"""
    return notify.guarded_send(message, send=send_dingtalk)


def _load_archived_ids(date8: str) -> set:
    """读当日归档 `_id` 集（append 去重用）；断写损坏降级空集 + 告警，不阻塞采集。

    降级语义同 news_cctv：create-only 入库使去重缺失无害（多出的重复行被 _id 幂等
    吸收），采集连续性优先。
    """
    try:
        return news_archive.load_ids(_SOURCE, date8)
    except (EOFError, OSError) as exc:
        logger.warning(f"公告 {date8} 归档读取失败，放弃当日归档去重继续采集: {exc!r}")
        _alert(f"公告 {date8} 归档文件读取失败（疑断写损坏：{exc!r}），已降级继续采集；"
               f"修复方式见 news_archive 模块说明")
        return set()


def _collect_day(date8: str, client=None) -> Tuple[int, int, int, int, int]:
    """采集单日核心（run/verify 共用）：取数 → 信封化（批内去重/丢缺 id）→ 归档新增 → 入库。

    返回 (有效信封数, 入库新增, 重复, 归档新增, 丢弃数)；源空返回全 0。
    date8: YYYYMMDD；内部转 ISO 传 cninfo。fetch 失败（page1）由本函数上抛，
    调用方（run 直接上抛触发框架 retry；run_verify 逐日 try 隔离）决定容错。
    """
    date_iso = f"{date8[:4]}-{date8[4:6]}-{date8[6:]}"
    records = cninfo.fetch_announcements(date_iso)
    if not records:
        return 0, 0, 0, 0, 0

    fetch_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    envelopes: List[dict] = []
    seen: set = set()
    dropped = 0
    for rec in records:
        env = _record_to_envelope(rec, fetch_time)
        if env is None:
            dropped += 1
            continue
        if env["_id"] in seen:            # 同 announcementId 批内偶发重复，保首见
            continue
        seen.add(env["_id"])
        envelopes.append(env)

    # 归档只 append 尚未归档的新信封（seDate 已按当日过滤，归档归属 date8）
    archived_seen = _load_archived_ids(date8)
    fresh = [env for env in envelopes if env["_id"] not in archived_seen]
    archived = news_archive.append(_SOURCE, date8, fresh) if fresh else 0

    # 入库写全量（与归档去重无关，幂等由 create-only 409→dup 保证），剥 raw_* 键
    if client is None:
        client = osu.get_client()
    ok, dup = osu.bulk_create(client, [_strip_raw(env) for env in envelopes])
    return len(envelopes), ok, dup, archived, dropped


# ======== Pipeline 标准接口 ========

def run(run_date: str | None = None, **kwargs) -> str:
    """每日任务：采集当日全市场公司公告（缺省今天，20:40 取当日覆盖绝大部分）。

    单源无双活：fetch page1 失败经 _collect_day 上抛 → 框架 retry + 钉钉；
    次日早晨少量补发由 run_verify 自然日回溯补齐。
    """
    if run_date is None or not str(run_date).strip():
        date8 = _today().strftime("%Y%m%d")
    else:
        # 归一化：run_job.py --date 明示支持 YYYYMMDD 或 YYYY-MM-DD；不归一化则
        # 切片拼出垃圾 seDate → 巨潮返回 0 条 → 任务静默"成功"，比报错更糟
        date8 = str(run_date).strip().replace("-", "")
        if len(date8) != 8 or not date8.isdigit():
            raise ValueError(f"run_date 需为 YYYYMMDD 或 YYYY-MM-DD，收到: {run_date!r}")

    _flush_spool_safe()

    total, ok, dup, archived, dropped = _collect_day(date8)
    if total == 0:
        return f"公告 {date8}: 源返回 0 条"
    msg = (f"公告 {date8}: 源 {total} 条, 入库新增 {ok} 重复 {dup}, "
           f"归档新增 {archived}")
    if dropped:
        msg += f", 丢弃 {dropped}(缺id)"
    return msg


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    """查漏补缺：**自然日**回溯逐日重采（create-only 幂等补漏）。

    重要——verify 自然日语义，**忽略框架传入的 start_date/end_date**：框架
    `_resolve_invocation` 按**交易日**推窗口（`minus_one_market_day`，为行情设计），
    公告含周末/节假日补发场景，交易日窗口会系统性漏。实际窗口改读 config
    `news.verify_days_back`（默认 3）按自然日回溯 `[today-N, today-1]`——**不含今天**
    （今天归 run 及其 retry；过去日补次日补发的旧日期公告 + 集群宕机缺口）。

    逐日重采（幂等：seDate 定当日 + create-only 天然去重）；单日异常隔离计失败继续，
    failed>0 末尾 raise RuntimeError(摘要) 保留框架失败通知语义。
    """
    dates = _verify_dates(get_news_config().get("verify_days_back", 3),
                          _today(), include_today=False)
    if not dates:
        return "公告verify: verify_days_back<=0，无检查窗口"

    client = osu.get_client()
    refilled = ok_total = dup_total = 0
    failed_days: List[str] = []
    for d in dates:
        try:
            total, ok, dup, archived, dropped = _collect_day(d, client=client)
            refilled += total
            ok_total += ok
            dup_total += dup
            logger.info(f"公告 {d} 补采: 源 {total} 条, 新增 {ok} 重复 {dup} 归档 {archived}")
        except Exception as exc:
            failed_days.append(d)
            logger.warning(f"公告 verify {d} 处理失败（继续后续日期）: {exc!r}")

    summary = (f"公告verify [{dates[0]}~{dates[-1]}]: 重采 {refilled} 条, "
               f"新入库 {ok_total}, 重复 {dup_total}, 失败 {len(failed_days)} 日")
    if failed_days:
        summary += f" ({_fmt_days(failed_days)})"
        raise RuntimeError(summary)   # 窗口已处理完，仍上抛保留框架失败通知语义
    return summary
