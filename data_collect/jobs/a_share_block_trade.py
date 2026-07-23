"""大宗交易采集（东财 datacenter RPT_DATA_BLOCKTRADE，按日全市场，写 block_trade）。

T 日盘后当晚披露 → run(T) 采 T（17:30 时点若尚未出全由周 verify 补齐）。
同股同日多笔无自然主键 → 表上全字段唯一索引 block_trade_uk 幂等去重。
历史多年可全量 backfill（逐日、幂等跳过已采日）。
"""
from __future__ import annotations

import logging

import pandas as pd

from data_collect.utils.date_utils import is_market_day, date_range
from data_collect.utils.db import save_to_postgres, get_dates_with_data
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.eastmoney import datacenter_query

logger = logging.getLogger(__name__)
TABLE_NAME = "block_trade"
_REPORT = "RPT_DATA_BLOCKTRADE"


def normalize_block(raw: list) -> pd.DataFrame:
    """datacenter 行 → block_trade 表行（关键列显式索引，缺列 KeyError fail-fast）。"""
    if not raw:
        return pd.DataFrame()
    rows = []
    for r in raw:
        rows.append({
            "trade_date": pd.to_datetime(r["TRADE_DATE"]).date(),
            "stock_code": r["SECURITY_CODE"],
            "name": r.get("SECURITY_NAME_ABBR"),
            "deal_price": r.get("DEAL_PRICE"),
            "close_price": r.get("CLOSE_PRICE"),
            "premium_pct": r.get("PREMIUM_RATIO"),
            "deal_volume": r.get("DEAL_VOLUME"),
            "deal_amount": r.get("DEAL_AMT"),
            "buyer_name": r.get("BUYER_NAME"),
            "seller_name": r.get("SELLER_NAME"),
        })
    return pd.DataFrame(rows)


def _save(df: pd.DataFrame) -> tuple[int, int]:
    if df.empty:
        return 0, 0
    df = df.astype(object).where(pd.notna(df), None)
    return save_to_postgres(df, pre_aligned_df=None, table_name=TABLE_NAME)


def _collect_date(date_str: str) -> tuple[int, int]:
    d = pd.to_datetime(date_str).strftime("%Y-%m-%d")
    raw = datacenter_query(_REPORT, filter_str=f"(TRADE_DATE='{d}')",
                           sort_columns="DEAL_AMT")
    return _save(normalize_block(raw))


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} 非交易日，大宗交易跳过。"
    tried, inserted = _collect_date(trade_date)
    return f"{trade_date} 大宗交易完成，写入 {tried}/{inserted} 条。"


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
    return f"大宗回补完成 ({start_date}~{end_date})，{len(days)} 交易日，写入 {tot_t}/{tot_i} 条。"


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    days = date_range(start_date, end_date)
    existing = get_dates_with_data(TABLE_NAME, "trade_date", start_date, end_date)
    missing = [d for d in days if pd.to_datetime(d).date() not in existing]
    if not missing:
        return f"大宗检查完成，{len(days)} 交易日完整。"
    tot_t = tot_i = 0
    for d in missing:
        t, ins = _collect_date(d)
        tot_t += t; tot_i += ins
    return f"大宗补缺完成，{len(missing)}/{len(days)} 天缺失，补 {tot_t}/{tot_i} 条。"
