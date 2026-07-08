"""ETF 复权因子采集（get_divid_factors，写 etf_divid_factors）。镜像 divid_factors。"""
from __future__ import annotations

import logging
import sys
from typing import Tuple

import pandas as pd
from tqdm import tqdm

from data_collect.utils.date_utils import is_market_day
from data_collect.utils.db import save_to_postgres
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.etf_utils import to_bare_code
from data_collect.utils.notify import send_dingtalk
from data_collect.utils.retry import retry_xtquant
from data_collect.utils.xtquant_utils import require_xtdata, get_etf_codes

logger = logging.getLogger(__name__)
TABLE_NAME = "etf_divid_factors"
_FIELDS = ["interest", "stock_bonus", "stock_gift", "allot_num", "allot_price", "dr"]


@retry_xtquant
def _fetch(code: str, start="", end="") -> pd.DataFrame:
    xtdata = require_xtdata()
    raw = xtdata.get_divid_factors(code, start_time=start, end_time=end)
    if raw is None or not isinstance(raw, pd.DataFrame) or raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    df["code"] = to_bare_code(code)
    if df.index.name is None:
        df.index.name = "date"
    df = df.reset_index()
    date_col = df.columns[0]
    df["date"] = pd.to_datetime(df[date_col], errors="coerce")
    if df["date"].isna().all():
        df["date"] = pd.to_datetime(df[date_col], unit="ms", errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return df
    df["date"] = df["date"].dt.date
    lower = {c.lower(): c for c in df.columns}
    ren = {}
    for f in _FIELDS:
        for cand in (f, f.replace("_", "")):
            if cand in lower:
                ren[lower[cand]] = f
    df = df.rename(columns=ren)
    for f in _FIELDS:
        if f not in df.columns:
            df[f] = None
    return df[["code", "date", *_FIELDS]]


def _fetch_all(start="", end="", limit_stocks=None) -> pd.DataFrame:
    codes = get_etf_codes()
    if not codes:
        raise RuntimeError("未获取到 ETF 列表")
    if limit_stocks:
        codes = codes[:limit_stocks]
    chunks = []
    for code in tqdm(codes, desc="ETF复权因子", unit="只", file=sys.stdout):
        try:
            one = _fetch(code, start, end)
            if not one.empty:
                chunks.append(one)
        except Exception:
            pass
    if not chunks:
        return pd.DataFrame(columns=["code", "date", *_FIELDS])
    return pd.concat(chunks, ignore_index=True).drop_duplicates(subset=["code", "date"])


def _save(df: pd.DataFrame) -> Tuple[int, int]:
    if df.empty:
        return 0, 0
    return save_to_postgres(df, pre_aligned_df=df, table_name=TABLE_NAME)


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} 非交易日，ETF复权因子跳过。"
    df = _fetch_all(trade_date, trade_date, kwargs.get("limit_stocks"))
    t, ins = _save(df)
    return f"{trade_date} ETF复权因子完成，写入 {t}/{ins} 条。"


def run_backfill(start_date: str, end_date: str, limit_stocks=None) -> str:
    df = _fetch_all(start_date, end_date, limit_stocks)
    t, ins = _save(df)
    msg = f"ETF复权因子补历史完成 ({start_date}~{end_date})，写入 {t}/{ins} 条。"
    send_dingtalk(msg)
    return msg


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    df = _fetch_all(start_date, end_date, kwargs.get("limit_stocks"))
    t, ins = _save(df)
    return f"ETF复权因子补缺完成 ({start_date}~{end_date})，尝试 {t}，新增 {ins} 条。"
