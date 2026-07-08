"""ETF 净值采集。run=spot(IOPV/折价/最新价)；backfill/verify=官方单位/累计净值。跨平台(akshare)。"""
from __future__ import annotations

import logging
import sys
from typing import List

import pandas as pd
from tqdm import tqdm

from data_collect.utils.akshare_utils import get_etf_spot, get_etf_nav_hist, build_nav_spot_df, build_nav_official_df
from data_collect.utils.date_utils import is_market_day
from data_collect.utils.db import get_connection, require_psycopg2
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.etf_utils import to_bare_code
from data_collect.utils.notify import send_dingtalk
from data_collect.utils.xtquant_utils import get_etf_codes

logger = logging.getLogger(__name__)
TABLE_NAME = "etf_nav"


def _upsert(df: pd.DataFrame, value_cols: List[str]) -> tuple[int, int]:
    """按 (code, trade_date) upsert，仅更新 value_cols（+updated_at）。返回 (尝试, 影响)。"""
    if df.empty:
        return 0, 0
    _, execute_values = require_psycopg2()
    cols = ["code", "trade_date"] + value_cols
    df = df[cols].where(pd.notna(df[cols]), None)
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in value_cols)
    quoted = ", ".join(f'"{c}"' for c in cols)
    sql = (
        f'INSERT INTO "{TABLE_NAME}" ({quoted}) VALUES %s '
        f'ON CONFLICT (code, trade_date) DO UPDATE SET {set_clause}, "updated_at" = NOW()'
    )
    with get_connection() as conn:
        with conn.cursor() as cur:
            # 单页执行，使 cur.rowcount 反映全部影响行（分页时只报最后一页，会误导）
            execute_values(cur, sql, rows, page_size=max(len(rows), 1))
            affected = cur.rowcount
        conn.commit()
    return len(rows), affected


def run(run_date: str, **kwargs) -> str:
    """盘后 spot 快照：全市场 IOPV/折价/最新价。"""
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} 非交易日，ETF净值(spot)跳过。"
    spot = get_etf_spot()
    df = build_nav_spot_df(spot, pd.to_datetime(trade_date).date())
    tried, aff = _upsert(df, ["close", "iopv", "discount_rate"])
    return f"{trade_date} ETF净值spot完成，{tried} 只，upsert {aff}。"


def _official_backfill(start: str, end: str, limit_stocks=None) -> tuple[int, int]:
    codes = get_etf_codes()
    if limit_stocks:
        codes = codes[:limit_stocks]
    s, e = start.replace("-", ""), end.replace("-", "")
    tot_t = tot_a = 0
    for code in tqdm(codes, desc="ETF官方净值", unit="只", file=sys.stdout):
        bare = to_bare_code(code)
        try:
            hist = get_etf_nav_hist(bare, s, e)
            df = build_nav_official_df(hist, bare)
            t, a = _upsert(df, ["unit_nav", "accum_nav", "daily_growth", "nav_date"])
            tot_t += t; tot_a += a
        except Exception as exc:
            logger.debug(f"ETF官方净值 {bare} 失败: {exc}")
    return tot_t, tot_a


def run_backfill(start_date: str, end_date: str, limit_stocks=None) -> str:
    t, a = _official_backfill(start_date, end_date, limit_stocks)
    msg = f"ETF官方净值补历史完成 ({start_date}~{end_date})，尝试 {t}，upsert {a}。"
    send_dingtalk(msg)
    return msg


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    """回补近段官方净值（T+1 滞后）。akshare 无按天查询，按范围重取幂等 upsert。"""
    t, a = _official_backfill(start_date, end_date, kwargs.get("limit_stocks"))
    return f"ETF官方净值补缺完成 ({start_date}~{end_date})，尝试 {t}，upsert {a}。"
