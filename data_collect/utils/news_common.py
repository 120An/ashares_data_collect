"""新闻系统跨模块公共纯函数（消除 news_cctv/news_flash/news_embed/news_report 的重复 helper）。

本模块只收**行为字节一致**的小工具，各调用方以模块级别名（如 `_today = today`）导入，
既复用实现、又保留原私有名——测试 `monkeypatch.setattr(mod, "_today", ...)` 仍生效。
"""

from __future__ import annotations

import datetime
import logging

import pandas as pd

from data_collect.utils import news_archive

logger = logging.getLogger(__name__)

# fetch_time 统一格式（各采集 job 信封契约，见 news_cctv/news_flash）
_TIME_FMT = "%Y-%m-%d %H:%M:%S"


def today() -> datetime.date:
    """今天（自然日）。独立成函数便于测试 monkeypatch 固定日期。"""
    return datetime.date.today()


def now() -> datetime.datetime:
    """当前时间。独立成函数便于测试 monkeypatch 固定时钟。"""
    return datetime.datetime.now()


def cell(value) -> str:
    """DataFrame 单元格 → str；None/NaN → ""（pandas 缺失单元格是 float('nan')）。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value)


def flush_spool_safe() -> None:
    """尽力搬回 spool 积压归档；失败仅记日志，不阻塞采集（搬运是尽力而为）。"""
    try:
        news_archive.flush_spool()
    except Exception as exc:
        logger.warning(f"flush_spool 失败（不阻塞采集）: {exc!r}")


def age_minutes(fetch_time, now: datetime.datetime):
    """fetch_time 串 → 距 now 分钟数；缺失/异形返回 None（放弃年龄判定）。"""
    if not fetch_time:
        return None
    try:
        dt = datetime.datetime.strptime(str(fetch_time), _TIME_FMT)
    except ValueError:
        logger.warning(f"pending fetch_time 异形，放弃积压年龄判定: {fetch_time!r}")
        return None
    return max(0, int((now - dt).total_seconds() // 60))


def fmt_truncated(items, limit: int = 10) -> str:
    """格式化条目列表（最多 limit 个，超出以…截断）。"""
    head = ",".join(items[:limit])
    return head + ("…" if len(items) > limit else "")


# 信封中只进归档、不进 OpenSearch 的键（信封↔索引契约：索引 mapping 未定义这些键，
# 不剥会经动态 mapping 长出计划外字段）。flash/announcement/stock 共享此契约；
# cctv 的信封无 time_estimated，自带独立元组（历史契约，勿混用）。
RAW_ONLY_KEYS = ("raw_title", "raw_content", "time_estimated")


def strip_raw(envelope: dict) -> dict:
    """入库前剥除只进归档的键（RAW_ONLY_KEYS，见上）。"""
    return {k: v for k, v in envelope.items() if k not in RAW_ONLY_KEYS}


def verify_dates(days_back, today_date: datetime.date,
                 include_today: bool) -> list:
    """news 系 verify 的自然日窗口（YYYYMMDD 升序列表）。

    include_today 语义：**归档重放**类 verify（flash/stock——重放廉价，含今天补
    "已归档未入库"缺口）传 True 得 [today-N, today]；**源重采**类（cctv/
    announcement——当天归 run 管）传 False 得 [today-N, today-1]。
    days_back < 0 统一按 0 处理（原四 job 的 clamp 行为不一致，收敛后统一）。
    """
    days_back = max(0, int(days_back))
    end = -1 if include_today else 0
    return [(today_date - datetime.timedelta(days=offset)).strftime("%Y%m%d")
            for offset in range(days_back, end, -1)]
