"""指数日线采集（不复权，写 index_daily）。镜像 etf_daily，代码带点透传。"""
from __future__ import annotations

import logging
import sys
from typing import List

import pandas as pd
from tqdm import tqdm

from data_collect.utils.date_utils import is_market_day, date_range
from data_collect.utils.db import save_to_postgres, get_connection
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.notify import send_dingtalk
from data_collect.utils.xtquant_utils import require_xtdata, get_index_codes, download_history_with_retry

logger = logging.getLogger(__name__)
TABLE_NAME = "index_daily"
_BATCH = 50


def _normalize(df: pd.DataFrame, code: str, trade_date: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    if "time" in df.columns and pd.api.types.is_numeric_dtype(df["time"]):
        df["trade_date"] = (pd.to_datetime(df["time"], unit="ms") + pd.Timedelta(hours=8)).dt.date
    df = df.drop(columns=["time"], errors="ignore")
    df["index_code"] = code                      # 原样带点，如 000300.SH
    target = pd.to_datetime(trade_date).date()
    df = df[df["trade_date"] == target]
    cols = ["index_code", "trade_date", "open", "high", "low", "close", "volume", "amount"]
    return df[[c for c in cols if c in df.columns]]


def _download_save_batch(batch: List[str], trade_date: str) -> tuple[int, int]:
    xtdata = require_xtdata()
    download_history_with_retry(batch, "1d", trade_date, trade_date)
    raw = xtdata.get_market_data_ex(
        field_list=["time", "open", "high", "low", "close", "volume", "amount"],
        stock_list=batch, period="1d", start_time=trade_date, end_time=trade_date,
        count=-1, dividend_type="none", fill_data=False,
    )
    chunks = []
    for code in batch:
        d = raw.get(code) if isinstance(raw, dict) else None
        if d is None or d.empty:
            continue
        one = _normalize(d, code, trade_date)
        if not one.empty:
            chunks.append(one)
    if not chunks:
        return 0, 0
    merged = pd.concat(chunks, ignore_index=True)
    return save_to_postgres(merged, pre_aligned_df=None, table_name=TABLE_NAME)


def _missing_codes(trade_date: str, codes: list) -> list:
    with get_connection() as conn:
        existing = pd.read_sql(
            "SELECT DISTINCT index_code FROM index_daily WHERE trade_date = %s",
            conn, params=[trade_date])
    have = set(existing["index_code"])
    return [c for c in codes if c not in have]


def _collect_one_day(trade_date: str, codes: list, pbar=None) -> tuple[int, int]:
    missing = _missing_codes(trade_date, codes)
    if not missing:
        if pbar:
            pbar.update(len(codes)); pbar.set_postfix_str("已完整")
        return 0, 0
    tried = inserted = 0
    for i in range(0, len(missing), _BATCH):
        t, ins = _download_save_batch(missing[i:i + _BATCH], trade_date)
        tried += t; inserted += ins
        if pbar:
            pbar.update(len(missing[i:i + _BATCH]))
    return tried, inserted


def _get_codes(limit_stocks=None) -> list:
    codes = get_index_codes()
    if not codes:
        raise RuntimeError("未获取到指数列表（板块缓存缺失？需先跑 a_share_sector）")
    return codes[:limit_stocks] if limit_stocks else codes


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} 非交易日，指数日线跳过。"
    codes = _get_codes(kwargs.get("limit_stocks"))
    with tqdm(total=len(codes), desc=f"指数日线({trade_date})", unit="个", file=sys.stdout) as pbar:
        tried, inserted = _collect_one_day(trade_date, codes, pbar)
    return f"{trade_date} 指数日线完成，写入 {tried}/{inserted} 条。"


def run_backfill(start_date: str, end_date: str, limit_stocks=None) -> str:
    days = date_range(start_date, end_date)
    codes = _get_codes(limit_stocks)
    tot_t = tot_i = 0
    for i, d in enumerate(days, 1):
        with tqdm(total=len(codes), desc=f"{i}/{len(days)} {d}", unit="个", file=sys.stdout) as pbar:
            t, ins = _collect_one_day(d, codes, pbar)
            tot_t += t; tot_i += ins
    msg = f"指数日线补历史完成 ({start_date}~{end_date})，{len(days)} 交易日，写入 {tot_t}/{tot_i} 条。"
    send_dingtalk(msg)
    return msg


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    days = date_range(start_date, end_date)
    codes = _get_codes(kwargs.get("limit_stocks"))
    tot_t = tot_i = fixed = 0
    for i, d in enumerate(days, 1):
        missing = _missing_codes(d, codes)
        if not missing:
            continue
        fixed += 1
        with tqdm(total=len(missing), desc=f"补缺 {i}/{len(days)} {d} 缺{len(missing)}", unit="个", file=sys.stdout) as pbar:
            t, ins = _collect_one_day(d, missing, pbar)
            tot_t += t; tot_i += ins
    if fixed == 0:
        return f"指数日线检查完成，{len(days)} 交易日完整（{len(codes)}个）。"
    return f"指数日线补缺完成，{fixed}/{len(days)} 天有缺，补 {tot_t}/{tot_i} 条。"
