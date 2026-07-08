"""ETF 分钟线采集（不复权，写既有 etf_minute 分区表）。

etf_minute 为中文列、按月分区、裸6位代码；列序 日期,时间,代码,开盘,收盘,最高,最低,成交量,成交额,最新价。
必须按列名显式构造 DataFrame（不用位置对齐，避免 O/C/H/L 错位）。
"""
from __future__ import annotations

import logging
import sys

import pandas as pd
from tqdm import tqdm

from data_collect.utils.date_utils import is_market_day, date_range
from data_collect.utils.db import save_to_postgres, get_dates_with_data
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.etf_utils import to_bare_code
from data_collect.utils.notify import send_dingtalk
from data_collect.utils.xtquant_utils import require_xtdata, get_etf_codes, download_history_with_retry

logger = logging.getLogger(__name__)
TABLE_NAME = "etf_minute"
_BATCH = 50


def build_etf_minute_df(raw: pd.DataFrame, code_bare: str) -> pd.DataFrame:
    """xtquant 分钟 df -> etf_minute 中文列 schema（按列名映射，防 O/C/H/L 错位）。"""
    if raw is None or raw.empty:
        return pd.DataFrame()
    r = raw.copy()
    if "time" in r.columns and pd.api.types.is_numeric_dtype(r["time"]):
        ts = pd.to_datetime(r["time"], unit="ms") + pd.Timedelta(hours=8)
    else:
        ts = pd.to_datetime(r.get("time"), errors="coerce")
    close = pd.to_numeric(r["close"], errors="coerce")
    out = pd.DataFrame({
        "日期": ts.dt.date,
        "时间": ts.dt.time,
        "代码": code_bare,
        "开盘": pd.to_numeric(r["open"], errors="coerce"),
        "收盘": close,
        "最高": pd.to_numeric(r["high"], errors="coerce"),
        "最低": pd.to_numeric(r["low"], errors="coerce"),
        "成交量": pd.to_numeric(r["volume"], errors="coerce"),
        "成交额": pd.to_numeric(r["amount"], errors="coerce"),
        "最新价": close,
    })
    return out.dropna(subset=["日期", "时间"])


def _collect_day(trade_date: str, codes: list) -> tuple[int, int]:
    """按批下载 + 批量读取（一次 get_market_data_ex 读整批，避免逐只读）。"""
    xtdata = require_xtdata()
    chunks = []
    for i in range(0, len(codes), _BATCH):
        batch = codes[i:i + _BATCH]
        download_history_with_retry(batch, "1m", trade_date, trade_date)
        try:
            raw = xtdata.get_market_data_ex(
                field_list=["time", "open", "high", "low", "close", "volume", "amount"],
                stock_list=batch, period="1m", start_time=trade_date, end_time=trade_date,
                count=-1, dividend_type="none", fill_data=False)
        except Exception as exc:
            logger.debug(f"ETF分钟批量读取失败({batch[0]}..): {exc}")
            continue
        for code in batch:
            frame = raw.get(code) if isinstance(raw, dict) else None
            one = build_etf_minute_df(frame, to_bare_code(code))
            if not one.empty:
                chunks.append(one)
    if not chunks:
        return 0, 0
    merged = pd.concat(chunks, ignore_index=True)
    return save_to_postgres(merged, pre_aligned_df=None, table_name=TABLE_NAME)


def _get_codes(limit_stocks=None) -> list:
    codes = get_etf_codes()
    if not codes:
        raise RuntimeError("未获取到 ETF 列表")
    return codes[:limit_stocks] if limit_stocks else codes


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} 非交易日，ETF分钟线跳过。"
    codes = _get_codes(kwargs.get("limit_stocks"))
    with tqdm(total=len(codes), desc=f"ETF分钟({trade_date})", unit="只", file=sys.stdout):
        tried, inserted = _collect_day(trade_date, codes)
    return f"{trade_date} ETF分钟线完成，写入 {tried}/{inserted} 条。"


def run_backfill(start_date: str, end_date: str, limit_stocks=None) -> str:
    days = date_range(start_date, end_date)
    codes = _get_codes(limit_stocks)
    tot_t = tot_i = 0
    for i, d in enumerate(days, 1):
        t, ins = _collect_day(d, codes)
        tot_t += t; tot_i += ins
        logger.info(f"{i}/{len(days)} {d}: 写入 {t}/{ins}")
    msg = f"ETF分钟线补历史完成 ({start_date}~{end_date})，{len(days)} 交易日，写入 {tot_t}/{tot_i} 条。"
    send_dingtalk(msg)
    return msg


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    days = date_range(start_date, end_date)
    codes = _get_codes(kwargs.get("limit_stocks"))
    existing = get_dates_with_data("etf_minute", "日期", start_date, end_date)
    missing = [d for d in days if pd.to_datetime(d).date() not in existing]
    if not missing:
        return f"ETF分钟线检查完成，{len(days)} 交易日完整。"
    tot_t = tot_i = 0
    for d in missing:
        t, ins = _collect_day(d, codes)
        tot_t += t; tot_i += ins
    return f"ETF分钟线补缺完成，{len(missing)}/{len(days)} 天缺失，补 {tot_t}/{tot_i} 条。"
