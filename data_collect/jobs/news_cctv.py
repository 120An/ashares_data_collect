"""新闻联播文字稿采集任务（新闻系统架构 §4.1）。

数据源 akshare ``news_cctv(date)``（历史下限 2016-02-03），每日一批文字稿。
流程：抓取 → 信封化（channel=cctv，三级 make_id，raw_* 保留源原文）→ NAS 归档
（news_archive，收含 raw_* 的完整信封）→ OpenSearch create-only 入库
（bulk_create 前剥除 raw_*，幂等由 409→dup 保证）。

两条关键语义（与行情类 job 的差异）：

1. **verify 为自然日语义、不使用框架传入窗口**：pipeline 框架 `_resolve_invocation`
   按 `days_back` 推算的 start/end 是**交易日**窗口（`minus_one_market_day`，为行情
   任务设计），而联播周末/节假日照常播出——交易日窗口会系统性漏掉周末。故
   `run_verify` **忽略入参**，改读 config `news.verify_days_back`（默认 3）按自然日
   回溯。

2. **空日 T+2 判定**：当天源空不立即标记（文字稿可能晚点/源迟到）；verify 重试至
   T+2 仍空 → 写 `marker-cctv-empty-{date}` 标记文档（channel=marker）终止重试并
   告警一次——仿 tick `.empty` 模式，防 verify 永久重试与误报。人工核实确有数据后
   删除该标记文档即可恢复重试。
"""

from __future__ import annotations

import datetime
import logging
import re
import time
from typing import Dict, List, Tuple

import pandas as pd

from data_collect.config import get_news_config
from data_collect.utils import news_archive
from data_collect.utils import news_normalize as nn
from data_collect.utils import opensearch_utils as osu
from data_collect.utils.news_common import cell as _cell
from data_collect.utils.news_common import flush_spool_safe as _flush_spool_safe
from data_collect.utils.news_common import fmt_truncated as _fmt_days
from data_collect.utils.news_common import today as _today
from data_collect.utils.news_common import verify_dates as _verify_dates
from data_collect.utils.notify import send_dingtalk

logger = logging.getLogger(__name__)

_BACKFILL_SLEEP_SECONDS = 1.5    # backfill 日间限速（防触发源反爬）
_AKSHARE_MIN_DATE = "20160203"   # akshare news_cctv 历史下限（源码核验）
_EMPTY_MARK_AFTER_DAYS = 2       # 空日重试至 T+N 仍空才写标记终止重试


def _fetch_cctv(date: str) -> pd.DataFrame:
    """拉取某日联播文字稿，返回列 date/title/content 的 DataFrame。

    lazy import：akshare 导入慢且依赖重，模块级 import 会拖累整个 jobs 包；
    测试 monkeypatch 本函数即可完全隔离网络。
    """
    import akshare as ak

    return ak.news_cctv(date=date)


def _load_name_dict() -> Dict[str, str]:
    """加载股票简称词典；DB 不可用降级空词典 + 钉钉告警（不阻塞采集）。

    告警与归档损坏同级：create-only 文档不可变，当晚入库文档的简称打标为空即
    **永久缺失**（事后无法补打），必须当晚可见以便人工决策（如删除当日文档重采）。
    """
    try:
        return nn.load_name_dict()
    except Exception as exc:
        logger.warning(f"简称词典加载失败，本轮股票打标降级为仅正则代码: {exc!r}")
        try:
            send_dingtalk(
                f"联播简称词典加载失败（{exc!r}），本轮 stocks 打标降级为仅正则代码；"
                f"create-only 下当晚文档简称标注永久缺失，请尽快排查 DB"
            )
        except Exception:
            logger.warning("词典降级钉钉告警发送失败", exc_info=True)
        return {}


def _pub_time_for(date8: str) -> str:
    """YYYYMMDD → "YYYY-MM-DD 19:00:00"。

    19:00 为联播固定播出时间：源不提供逐条时间，取固定值保证 pub_time 稳定，
    进而保证三级 make_id 决定论（pub_time 参与拼串，漂移即重复入库）。
    """
    return f"{date8[:4]}-{date8[4:6]}-{date8[6:]} 19:00:00"


def _row_date(value, run_date: str) -> str:
    """行内 date 列优先（akshare 返回 YYYYMMDD 串），异形/缺失回退 run_date。

    取数字字符前 8 位（兼容 "2026-07-03"、Timestamp 等形态），决定论纯函数
    （参与 _id 计算，不可有随机性）。
    """
    digits = re.sub(r"\D", "", "" if value is None else str(value))
    return digits[:8] if len(digits) >= 8 else run_date


# 信封中只进归档、不进 OpenSearch 的键（见 _collect_day 入库前剥除）
_RAW_ONLY_KEYS = ("raw_title", "raw_content")


def _build_envelopes(df: pd.DataFrame, run_date: str,
                     name_dict: Dict[str, str]) -> List[dict]:
    """DataFrame → 信封列表（架构 §5.2 信封契约；联播无 url/native_id）。

    - `_id` 用**原始 title**（clean_text 之前）计算：清洗规则日后调整不会使同一
      条目产出新 `_id` 造成重复入库；
    - `raw_title`/`raw_content` 保留源原文（仅 str+strip，不做任何清洗）：归档层
      本意是"原始归档"（先归档后清洗），clean_text 的 bug 不能永久毁掉原文——
      对快讯类源（无法回补）归档是唯一存档，此形状为各 news job 统一契约；
    - `title`/`content` 存 clean 后版本（重放可直接入库，无需重跑清洗）。
    """
    fetch_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    envelopes: List[dict] = []
    for _, row in df.iterrows():
        pub_time = _pub_time_for(_row_date(row.get("date"), run_date))
        raw_title = _cell(row.get("title")).strip()
        raw_content = _cell(row.get("content")).strip()
        title = nn.clean_text(raw_title)
        envelopes.append({
            "_id": nn.make_id(
                {"source": "cctv", "pub_time": pub_time, "title": raw_title}),
            "title": title,
            "content": nn.clean_text(raw_content),
            "raw_title": raw_title,
            "raw_content": raw_content,
            "pub_time": pub_time,
            "fetch_time": fetch_time,
            "source": "cctv",
            "channel": "cctv",
            "stocks": nn.tag_stocks(title, name_dict),
            "vec_status": "pending",
        })
    return envelopes


def _load_archived_ids(run_date: str) -> set:
    """读当日归档 `_id` 集（append 去重用）；断写损坏降级空集不阻塞采集。

    归档读故障（EOFError 断在数据区 / BadGzipFile、OSError 断在头部，见
    news_archive 异常契约）时放弃当日归档去重：create-only 入库使去重缺失无害，
    归档最多多出可被 `_id` 幂等吸收的重复行——采集连续性优先。
    """
    try:
        return news_archive.load_ids("cctv", run_date)
    except (EOFError, OSError) as exc:
        logger.warning(f"联播 {run_date} 归档读取失败，放弃当日归档去重继续采集: {exc!r}")
        try:
            send_dingtalk(
                f"联播 {run_date} 归档文件读取失败（疑断写损坏：{exc!r}），"
                f"已降级继续采集；修复方式见 news_archive 模块说明"
            )
        except Exception:
            logger.warning("归档损坏钉钉告警发送失败", exc_info=True)
        return set()


def _collect_day(run_date: str, client=None,
                 name_dict: Dict[str, str] | None = None) -> Tuple[int, int, int, int]:
    """采集单日核心：抓取 → 信封化 → 归档新增 → bulk_create（run/verify/backfill 共用）。

    返回 (源条数, 入库新增, 重复, 归档新增)；源空返回全 0——是否写空日标记由
    调用方按 T+2 规则决定，本函数不管。client/name_dict 可传入复用，缺省即时创建。
    """
    df = _fetch_cctv(run_date)
    if df is None or len(df) == 0:
        return 0, 0, 0, 0

    if name_dict is None:
        name_dict = _load_name_dict()
    envelopes = _build_envelopes(df, run_date, name_dict)

    # 归档只 append 尚未归档的新信封（append 本身不去重，见 news_archive）
    seen = _load_archived_ids(run_date)
    fresh = [env for env in envelopes if env["_id"] not in seen]
    archived = news_archive.append("cctv", run_date, fresh) if fresh else 0

    if client is None:
        client = osu.get_client()
    # 入库写全量（与归档去重无关，幂等由 create-only 409→dup 保证），但剥除 raw_*：
    # 索引 mapping 未定义该两键，不剥会被动态 mapping 长出计划外字段（原文只进归档）
    docs = [{k: v for k, v in env.items() if k not in _RAW_ONLY_KEYS}
            for env in envelopes]
    ok, dup = osu.bulk_create(client, docs)
    return len(envelopes), ok, dup, archived


# ---------- 空日标记（verify T+2） ----------

def _marker_id(date8: str) -> str:
    """空日标记文档 `_id`（固定格式，人工可按名删除以恢复重试）。"""
    return f"marker-cctv-empty-{date8}"


def _day_has_data(client, date8: str) -> bool:
    """该日是否已有联播数据（读别名 news，size=0 只取 total）。

    查询异常（含首日别名尚未建、404）视为无数据——宁可重采（幂等）不可漏采。
    """
    day = f"{date8[:4]}-{date8[4:6]}-{date8[6:]}"
    body = {
        "query": {"bool": {"filter": [
            {"term": {"channel": "cctv"}},
            {"range": {"pub_time": {"gte": f"{day} 00:00:00",
                                    "lte": f"{day} 23:59:59"}}},
        ]}},
        "size": 0,
    }
    try:
        return osu.hits_total(osu.search_raw(client, body)) > 0
    except Exception as exc:
        logger.warning(f"联播 {date8} 已有数据查询失败（按无数据处理）: {exc!r}")
        return False


def _marker_exists(client, date8: str) -> bool:
    """该日空日标记是否已写（已写即重试已终止，不再重采/重写/重告警）。"""
    body = {"query": {"ids": {"values": [_marker_id(date8)]}}, "size": 0}
    try:
        return osu.hits_total(osu.search_raw(client, body)) > 0
    except Exception as exc:
        logger.warning(f"联播 {date8} 空日标记查询失败（按不存在处理）: {exc!r}")
        return False


def _write_empty_marker(client, date8: str) -> None:
    """写空日标记文档并告警一次（create-only 天然幂等；上游 _marker_exists 已挡重入）。

    vec_status="done"：marker 非新闻正文，不需要向量，置 done 防 news_embed 把它
    当 pending 白白编码。marker 不进 NAS 归档（是运行状态，不是新闻数据）。
    """
    marker = {
        "_id": _marker_id(date8),
        "title": f"联播空日标记 {date8}",
        "content": "T+2 仍无数据，终止重试（停播/源缺失）",
        "pub_time": _pub_time_for(date8),
        "fetch_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "cctv",
        "channel": "marker",
        "stocks": [],
        "vec_status": "done",
    }
    osu.bulk_create(client, [marker])
    try:
        send_dingtalk(
            f"联播 {date8} 重试至 T+2 仍无数据，已写空日标记终止重试"
            f"（疑停播/源缺失；人工核实确有数据后删除 {_marker_id(date8)} 可恢复重试）"
        )
    except Exception:
        logger.warning("联播空日标记钉钉告警发送失败", exc_info=True)


# ======== Pipeline 标准接口 ========

def run(run_date: str | None = None, **kwargs) -> str:
    """每日任务：采集当日新闻联播文字稿（缺省今天，播出当晚文字稿即可取）。

    当天源返回空时**不写空日标记**（文字稿可能晚点），交 run_verify 按 T+2 判定。
    """
    if run_date is None or not str(run_date).strip():
        date8 = _today().strftime("%Y%m%d")
    else:
        date8 = str(run_date).strip()

    _flush_spool_safe()

    total, ok, dup, archived = _collect_day(date8)
    if total == 0:
        return f"联播 {date8}: 源返回 0 条（交 verify T+2 判定）"
    return f"联播 {date8}: 源 {total} 条, 入库新增 {ok} 重复 {dup}, 归档新增 {archived}"


def run_backfill(start_date: str, end_date: str, **kwargs) -> str:
    """补历史：逐**自然日**采集联播（akshare 下限 2016-02-03，日间限速防封）。

    单日异常只计失败继续，不中断整个 backfill（多年跨度中断代价高，失败日
    可重跑同区间幂等补齐）。
    """
    if start_date < _AKSHARE_MIN_DATE:   # YYYYMMDD 字典序即日期序
        logger.warning(
            f"start_date={start_date} 早于 akshare 联播数据下限，"
            f"已 clamp 到 {_AKSHARE_MIN_DATE}"
        )
        start_date = _AKSHARE_MIN_DATE

    _flush_spool_safe()
    client = osu.get_client()
    name_dict = _load_name_dict()   # 整个 backfill 共用一份词典

    cur = datetime.datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.datetime.strptime(end_date, "%Y%m%d").date()
    success = empty = 0
    failed_days: List[str] = []
    first = True
    while cur <= end:
        if not first:
            time.sleep(_BACKFILL_SLEEP_SECONDS)   # 日间限速
        first = False
        d = cur.strftime("%Y%m%d")
        try:
            total, ok, dup, archived = _collect_day(d, client=client, name_dict=name_dict)
            if total == 0:
                empty += 1
                logger.info(f"联播 {d}: 源无数据")
            else:
                success += 1
                logger.info(f"联播 {d}: 源 {total} 条, 新增 {ok} 重复 {dup} 归档 {archived}")
        except Exception as exc:
            failed_days.append(d)
            logger.warning(f"联播 backfill {d} 失败（继续后续日期）: {exc!r}")
        cur += datetime.timedelta(days=1)

    message = (f"联播补历史完成 ({start_date}~{end_date}): "
               f"成功 {success} 日 / 空 {empty} 日 / 失败 {len(failed_days)} 日")
    if failed_days:
        message += f" 失败日: {_fmt_days(failed_days)}"
    send_dingtalk(message)
    return message


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    """查漏补缺：**自然日**回溯补采 + T+2 空日标记。

    重要——verify 自然日语义，**忽略框架传入的 start_date/end_date**：框架
    `_resolve_invocation` 按**交易日**推算窗口（`minus_one_market_day`，为行情
    任务设计），而联播周末/节假日照常播出，交易日窗口会系统性漏掉周末。实际
    窗口改读 config `news.verify_days_back`（默认 3），按自然日回溯
    [today-N, today-1]——不含今天（当天归 run 管，源晚点交下轮判定）。

    逐日状态机：已有数据 → skip；已有空日标记 → skip（重试已终止）；重采有
    数据 → 补录；仍空且距今 ≥ T+2 → 写 marker 终止重试 + 告警一次；仍空且
    < T+2 → 留待下轮。

    逐日异常隔离：单日处理抛错只计失败继续后续日期（一天故障不挡整窗补采）；
    但 failed>0 时末尾 raise RuntimeError(摘要)——保留框架"任务失败"通知语义
    （失败日期随摘要进钉钉），且 verify 幂等、下轮/重试安全。
    """
    today = _today()   # T+2 空日判定（下方 age_days）与窗口共用同一"今天"
    dates = _verify_dates(get_news_config().get("verify_days_back", 3),
                          today, include_today=False)
    if not dates:
        return "联播verify: verify_days_back<=0，无检查窗口"

    client = osu.get_client()
    name_dict = _load_name_dict()   # 一次加载整窗共用（避免逐日重查全表）
    refilled = existing = marked = pending = 0
    failed_days: List[str] = []
    for d in dates:
        try:
            if _day_has_data(client, d):
                existing += 1
                continue
            if _marker_exists(client, d):
                existing += 1   # 已标记空日视同已处理（重试已终止）
                continue
            total, ok, dup, archived = _collect_day(d, client=client, name_dict=name_dict)
            if total > 0:
                refilled += 1
                logger.info(f"联播 {d} 补采: 源 {total} 条, 新增 {ok} 重复 {dup} 归档 {archived}")
                continue
            age_days = (today - datetime.datetime.strptime(d, "%Y%m%d").date()).days
            if age_days >= _EMPTY_MARK_AFTER_DAYS:
                _write_empty_marker(client, d)
                marked += 1
            else:
                pending += 1
                logger.info(f"联播 {d} 重采仍空（T+{age_days}），留待下轮 verify")
        except Exception as exc:
            failed_days.append(d)
            logger.warning(f"联播 verify {d} 处理失败（继续后续日期）: {exc!r}")

    summary = (f"联播verify [{dates[0]}~{dates[-1]}]: 补采 {refilled} 日, "
               f"已有 {existing} 日, 新标记 {marked} 日, 待定 {pending} 日, "
               f"失败 {len(failed_days)} 日")
    if failed_days:
        summary += f"({_fmt_days(failed_days)})"
        raise RuntimeError(summary)   # 窗口已处理完，仍上抛保留框架失败通知语义
    return summary
