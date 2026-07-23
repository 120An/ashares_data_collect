"""个股新闻采集任务（东财 stock_news_em，新闻子系统 ⑥，Phase 1 摘要元数据）。

全市场逐票 `stock_news_em` 拉每票最新 10 条（滚动窗、硬顶）→ **内存聚合
article→stocks[]**（按 新闻链接 去重，每篇文章的标的 = 被查代码 ∪ tag_stocks(标题+
摘要)，探针实测 17% 跨票重复坐实）→ 唯一文章一次写全标的 → 归档(source=em_stock) →
create-only 入库。摘要向量复用现有 news_embed（vec_status=pending）；body_status=
pending 预留 Phase 2 全文。

滚动窗**源头不可回补**——归档是 stock 的耐久存档（同 news_flash）；run_verify 归档
重放对账，不触源。逐码 try/except 隔离（废码/网络异常跳过继续，尽力收集）。
"""

from __future__ import annotations

import datetime
import json
import logging
import time
from typing import Dict, List, Tuple

import pandas as pd

from data_collect.config import get_news_config
from data_collect.utils import news_archive
from data_collect.utils import news_normalize as nn
from data_collect.utils import notify
from data_collect.utils import opensearch_utils as osu
from data_collect.utils.news_common import cell as _cell
from data_collect.utils.news_common import flush_spool_safe as _flush_spool_safe
from data_collect.utils.news_common import fmt_truncated as _fmt_items
from data_collect.utils.news_common import now as _now
from data_collect.utils.news_common import strip_raw as _strip_raw
from data_collect.utils.news_common import today as _today
from data_collect.utils.news_common import verify_dates as _verify_dates
from data_collect.utils.notify import send_dingtalk
from data_collect.utils import source_registry

logger = logging.getLogger(__name__)

_SOURCE = "em_stock"
_CHANNEL = "stock"
# 逐票节奏（测试 patch 0；生产前全量探针校准，被限则调大）
_PACE_SECONDS = 0.3
# 重试后仍失败码占比超此阈值 → 钉钉告警（部分失败日不静默：残缺 stocks[] 的
# 文章因 create-only 无法事后补标，运维需知当晚有缺口）
_FAIL_ALERT_RATIO = 0.05
# 源必需列（缺列疑源改版，跳过该码计失败）
_REQUIRED_COLUMNS = ("新闻标题", "新闻内容", "发布时间", "文章来源", "新闻链接")


def _entry_to_envelope(url: str, entry: dict, fetch_time: str,
                       name_dict: Dict[str, str]) -> dict | None:
    """聚合项(首见行 + 被查码集) → 统一信封；缺 url（无法生成幂等 _id）→ None。

    - `_id` = sha1(url)（make_id 二级 url 策略，同 URL 跨票天然去重）；
    - `content` = 清洗后摘要（Phase1；Phase2 由 body 承载全文）；
    - `stocks` = 被查代码 ∪ tag_stocks(**清洗后** 标题+摘要)——打标必须在 clean_text
      之后：词典键是 NFKC+t2s 归一化形态（load_name_dict 对齐清洗后文本），raw 文本
      含繁体/全角时打不中，且 create-only 不可变使漏标永久化；每篇唯一文章只打一次
      （而非 sweep 逐行打，跨票重复行白打 + 5218 词条×52k 行是纯浪费）；
    - `raw_content` = 首见原始记录 JSON（含 关键词/文章来源，归档保原文）。
    """
    if not url:
        return None
    row = entry["row"]
    raw_title = _cell(row.get("新闻标题")).strip()
    raw_summary = _cell(row.get("新闻内容")).strip()
    pub_time = nn.normalize_time(row.get("发布时间"), source=_SOURCE)
    title = nn.clean_text(raw_title)
    content = nn.clean_text(raw_summary)
    tagged = nn.tag_stocks(f"{title} {content}", name_dict)

    envelope = {
        "_id": nn.make_id({"source": _SOURCE, "url": url}),
        "title": title,
        "content": content,
        "raw_title": raw_title,
        # default=str：row 来自 DataFrame.to_dict()，akshare dtype 漂移（np.int64/
        # Timestamp）时不可 JSON 序列化——55min 全量扫完后在此抛错=整批不可回补丢失
        "raw_content": json.dumps(row, ensure_ascii=False, sort_keys=True, default=str),
        "pub_time": pub_time or fetch_time,
        "fetch_time": fetch_time,
        "source": _SOURCE,
        "channel": _CHANNEL,
        "stocks": sorted(entry["stocks"] | set(tagged)),
        "url": url,
        "vec_status": "pending",
        "body_status": "pending",
    }
    if pub_time is None:
        envelope["time_estimated"] = True
    return envelope


def _fetch_stock_news(code: str) -> pd.DataFrame:
    """拉取单票东财个股新闻（最新 10 条）。lazy import：akshare 重、测试 monkeypatch 本函数。"""
    import akshare as ak
    return ak.stock_news_em(symbol=code)


def _collect_code(code: str, by_url: Dict[str, dict]) -> bool:
    """采集单票并并入聚合；返回是否成功（空结果算成功）。

    逐码隔离（取数异常/缺列 → False，不上抛）；限速集中在 finally（任何分支
    恰好 sleep 一次，新增提前 continue 不会漏掉 pace）。
    """
    try:
        try:
            df = _fetch_stock_news(code)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"个股新闻 {code} 取数失败（逐码隔离，继续）: {exc!r}")
            return False
        if df is None or len(df) == 0:
            return True
        if any(c not in df.columns for c in _REQUIRED_COLUMNS):
            logger.warning(f"个股新闻 {code} 缺列（疑源改版），跳过: 实际列 {list(df.columns)}")
            return False

        market_code = nn.normalize_stock_code(code)
        for _, row in df.iterrows():
            url = _cell(row.get("新闻链接")).strip()
            if not url:
                continue
            entry = by_url.setdefault(url, {"row": row.to_dict(), "stocks": set()})
            if market_code:
                entry["stocks"].add(market_code)          # 该票被东财关联
        return True
    finally:
        time.sleep(_PACE_SECONDS)


def _sweep(codes: List[str]) -> Tuple[Dict[str, dict], int]:
    """逐票取数并按 新闻链接 聚合被查码关联；失败码整轮结束后**重试一次**。

    返回 (by_url, 重试后仍失败数)。by_url[url] = {"row": 首见行 dict,
    "stocks": set(被查规范码)}。文本打标（tag_stocks）**不在此做**——延后到
    _entry_to_envelope 在清洗后文本上每篇一次（正确性：词典键为归一化形态；
    效率：跨票重复行不重复打标）。

    重试的意义（跨运行 stocks[] 完整性）：某票瞬时失败会使其共享文章以残缺
    stocks[] 入库，create-only 不可变 → 次日该票恢复后 409 dup 把补全标的永久
    丢弃。整轮后重试一次可吸收网络毛刺/瞬时限频；仍失败的（废码/持续故障）
    计 failed，由 run 按失败率告警。
    """
    by_url: Dict[str, dict] = {}
    failed_codes = [code for code in codes if not _collect_code(code, by_url)]
    if not failed_codes:
        return by_url, 0
    logger.info(f"个股新闻首轮失败 {len(failed_codes)} 码，整轮结束重试一次")
    still_failed = sum(1 for code in failed_codes if not _collect_code(code, by_url))
    return by_url, still_failed


def _alert(message: str) -> bool:
    """guarded 钉钉：发送失败仅记日志，不阻塞采集。"""
    return notify.guarded_send(message, send=send_dingtalk)


def _load_name_dict() -> Dict[str, str]:
    """加载股票简称词典（tag_stocks 用）；DB 不可用降级空词典 + 告警（不阻塞，同 news_flash）。"""
    try:
        return nn.load_name_dict()
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"个股新闻简称词典加载失败，本轮打标降级为仅正则代码: {exc!r}")
        _alert(f"个股新闻简称词典加载失败（{exc!r}），本轮 stocks 打标降级为仅正则代码")
        return {}


def _load_archived_ids(date8: str) -> set:
    """读某日归档 `_id` 集；断写损坏降级空集 + 告警不阻塞（同 news_flash）。"""
    try:
        return news_archive.load_ids(_SOURCE, date8)
    except (EOFError, OSError) as exc:
        logger.warning(f"个股新闻 {date8} 归档读取失败，放弃当日归档去重继续: {exc!r}")
        _alert(f"个股新闻 {date8} 归档读取失败（疑断写损坏：{exc!r}），已降级继续采集")
        return set()


def _archive_envelopes(envelopes: List[dict]) -> int:
    """按 pub_time 日分组归档新信封（滚动窗跨日），返回归档新增条数。"""
    if not envelopes:
        return 0
    by_day: Dict[str, List[dict]] = {}
    for env in envelopes:
        day8 = env["pub_time"][:10].replace("-", "")
        by_day.setdefault(day8, []).append(env)
    new_count = 0
    for day8 in sorted(by_day):
        seen = _load_archived_ids(day8)
        fresh = [env for env in by_day[day8] if env["_id"] not in seen]
        if fresh:
            news_archive.append(_SOURCE, day8, fresh)
            new_count += len(fresh)
    return new_count


def _store(envelopes: List[dict]) -> Tuple[int, int]:
    """create-only 入库（剥 raw_*），返回 (新增, 重复)。空列表不触库。"""
    if not envelopes:
        return 0, 0
    return osu.bulk_create(osu.get_client(), [_strip_raw(env) for env in envelopes])


# ======== Pipeline 标准接口 ========

def run(run_date: str | None = None, **kwargs) -> str:
    """每日任务：全市场逐票扫描个股新闻 → 聚合 → 归档 → create-only 入库。

    `run_date` 仅签名兼容——stock_news_em 是滚动窗，永远采"当前最新"（归档/入库
    日期归属由每条 pub_time 决定，窗口跨日）。全市场码取失败（DB 挂）直接上抛 →
    框架 retry；逐码采集异常隔离；全市场全失败 raise（触发框架 retry + 钉钉）。
    """
    if not source_registry.is_enabled(_SOURCE):   # 注册表 kill-switch（二期）
        return f"个股新闻: 源 {_SOURCE} 已在注册表禁用（enabled=false），跳过"
    _flush_spool_safe()
    codes = nn.load_a_share_codes()          # DB 挂则抛 → 框架 retry
    name_dict = _load_name_dict()            # 降级空 dict + 告警
    fetch_time = _now().strftime("%Y-%m-%d %H:%M:%S")   # 统一 _now（审查 S6）

    by_url, failed = _sweep(codes)
    envelopes = [env for url, entry in by_url.items()
                 if (env := _entry_to_envelope(url, entry, fetch_time, name_dict)) is not None]

    archived = _archive_envelopes(envelopes)
    ok, dup = _store(envelopes)

    summary = (f"个股新闻: 扫 {len(codes)} 票(失败 {failed}), 唯一文章 "
               f"{len(envelopes)}, 入库新增 {ok} 重复 {dup}, 归档新增 {archived}")
    if codes and failed >= len(codes):
        raise RuntimeError(summary + " — 全市场采集全部失败")
    if codes and failed / len(codes) > _FAIL_ALERT_RATIO:
        _alert(f"个股新闻失败率告警：{failed}/{len(codes)} 码重试后仍失败"
               f"（>{_FAIL_ALERT_RATIO:.0%}）；这些码的共享文章以残缺 stocks[] 入库且"
               f"无法事后补标（create-only），请排查源/网络")
    return summary


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    """查漏补缺：**归档重放对账**（stock_news_em 滚动窗无源头回补，verify 不触源）。

    **忽略框架传入的 start_date/end_date**（交易日窗口漏周末），改读 config
    `news.verify_days_back`（默认 3）按自然日回溯 `[today-N, today]`——**含今天**
    （当天归档可能含集群宕机期"已归档未入库"的条目，同 news_flash）。

    逐日重放归档 → 剥 raw_* → create-only 入库；单日异常隔离计失败继续，failed>0
    末尾 raise RuntimeError(摘要) 保留框架失败通知语义。不做聚合/去重（那是 run 的
    实时职责，重放旧窗口无此语义）。
    """
    dates = _verify_dates(get_news_config().get("verify_days_back", 3),
                          _today(), include_today=True)

    client = osu.get_client()
    replayed = ok_total = dup_total = 0
    failed: List[str] = []
    for day in dates:
        try:
            envelopes = list(news_archive.replay(_SOURCE, (day, day)))
            if not envelopes:
                continue
            ok, dup = osu.bulk_create(client, [_strip_raw(env) for env in envelopes])
            replayed += len(envelopes)
            ok_total += ok
            dup_total += dup
            logger.info(f"个股新闻verify 重放 {day}: {len(envelopes)} 条, 新 {ok} 重复 {dup}")
        except Exception as exc:  # noqa: BLE001
            failed.append(day)
            logger.warning(f"个股新闻verify 重放 {day} 失败（继续其余）: {exc!r}")

    summary = (f"个股新闻verify [{dates[0]}~{dates[-1]}]: 重放 {replayed} 条, "
               f"新入库 {ok_total}, 重复 {dup_total}, 失败 {len(failed)}")
    if failed:
        summary += f" ({_fmt_items(failed)})"
        raise RuntimeError(summary)
    return summary
