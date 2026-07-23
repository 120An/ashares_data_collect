"""融资融券明细采集（东财 datacenter RPTA_WEB_RZRQ_GGMX，按日全市场，写 margin_daily）。

发布规律：T 日数据 T+1 早晨发布 → run(T) 采 minus_one_market_day(T)（17:30 跑当日
数据尚未出）；周 verify 按窗补缺兜底。历史多年可全量 backfill（逐日、幂等跳过已采日）。
数据发布即最终值 → 插入即幂等（PK 去重），无需按日替换。
"""
from __future__ import annotations

import logging

import pandas as pd

from data_collect.utils.date_utils import is_market_day, date_range, minus_one_market_day
from data_collect.utils.db import save_to_postgres, get_dates_with_data
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.eastmoney import datacenter_query

logger = logging.getLogger(__name__)
TABLE_NAME = "margin_daily"
_REPORT = "RPTA_WEB_RZRQ_GGMX"


def normalize_margin(raw: list) -> pd.DataFrame:
    """datacenter 行 → margin_daily 表行（关键列显式索引，缺列 KeyError fail-fast）。"""
    if not raw:
        return pd.DataFrame()
    rows = []
    for r in raw:
        rows.append({
            "trade_date": pd.to_datetime(r["DATE"]).date(),
            "stock_code": r["SCODE"],
            "name": r.get("SECNAME"),
            "market": r.get("MARKET"),
            "rzye": r.get("RZYE"), "rzmre": r.get("RZMRE"), "rzche": r.get("RZCHE"),
            "rzyezb": r.get("RZYEZB"),
            "rqye": r.get("RQYE"), "rqyl": r.get("RQYL"),
            "rqmcl": r.get("RQMCL"), "rqchl": r.get("RQCHL"),
            "rzrqye": r.get("RZRQYE"), "rzrqyecz": r.get("RZRQYECZ"),
        })
    return pd.DataFrame(rows)


def _save(df: pd.DataFrame) -> tuple[int, int]:
    if df.empty:
        return 0, 0
    df = df.astype(object).where(pd.notna(df), None)   # NaN→NULL（防 int 列 NaN 溢出坑）
    return save_to_postgres(df, pre_aligned_df=None, table_name=TABLE_NAME)


def _collect_date(date_str: str) -> tuple[int, int]:
    d = pd.to_datetime(date_str).strftime("%Y-%m-%d")
    raw = datacenter_query(_REPORT, filter_str=f"(DATE='{d}')", sort_columns="RZYE")
    return _save(normalize_margin(raw))


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} 非交易日，两融跳过。"
    target = minus_one_market_day(trade_date)     # T+1 早发布 → 采 T-1
    tried, inserted = _collect_date(target)
    return f"两融完成（数据日 {target}），写入 {tried}/{inserted} 条。"


def run_backfill(start_date: str, end_date: str, **kwargs) -> str:
    days = date_range(start_date, end_date)
    existing = get_dates_with_data(TABLE_NAME, "trade_date", start_date, end_date)
    tot_t = tot_i = 0
    for i, d in enumerate(days, 1):
        if pd.to_datetime(d).date() in existing:
            continue
        t, ins = _collect_date(d)
        tot_t += t; tot_i += ins
        logger.info(f"{i}/{len(days)} {d}: 写入 {t}/{ins}")
    return f"两融回补完成 ({start_date}~{end_date})，{len(days)} 交易日，写入 {tot_t}/{tot_i} 条。"


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    days = date_range(start_date, end_date)
    existing = get_dates_with_data(TABLE_NAME, "trade_date", start_date, end_date)
    missing = [d for d in days if pd.to_datetime(d).date() not in existing]
    if not missing:
        return f"两融检查完成，{len(days)} 交易日完整。"
    tot_t = tot_i = 0
    for d in missing:
        t, ins = _collect_date(d)
        tot_t += t; tot_i += ins
    return f"两融补缺完成，{len(missing)}/{len(days)} 天缺失，补 {tot_t}/{tot_i} 条。"
