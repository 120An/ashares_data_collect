"""指数分钟线采集（不复权，写 index_minute 月分区表；前向积累，无历史回补）。

复用 a_share_minute.normalize_minute_df 的稳健时间解析 + index_daily 的增量骨架
（逐批入库、按日 _missing_codes 跳过已采代码）。代码带点透传，英文列名与表按列名对齐（非位置）。
历史 1m 不可回补且拉旧窗口会挂死（spec §2.3）→ run_backfill 抛错。

抗挂死设计（2026-07-21 实测修订）：QMT 对部分指数 1m 首次 download 冷缓存慢、偶发无超时挂起，
批 download 挂起会被框架子进程超时硬杀。故**逐批立即入库**（进度即时落库、被杀不丢已采批），
并按日 `_missing_codes` 跳过已采代码 → 重试/下轮幂等推进；download 落 QMT 磁盘缓存、逐轮预热收敛。
挂起多为瞬时（同一码重试即通），配 pipeline retries + 周 verify 兜底。
"""
from __future__ import annotations

import logging
import sys
from typing import List

import pandas as pd
from tqdm import tqdm

from data_collect.jobs.a_share_minute import normalize_minute_df
from data_collect.utils.date_utils import is_market_day, date_range
from data_collect.utils.db import save_to_postgres, get_connection
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.xtquant_utils import require_xtdata, get_index_codes, download_history_with_retry

logger = logging.getLogger(__name__)
TABLE_NAME = "index_minute"
_BATCH = 50


def build_index_minute_df(raw, index_code: str) -> pd.DataFrame:
    """xtquant 分钟 df -> index_minute 英文列 schema。

    复用 normalize_minute_df 的时间解析；再把 bar_time/trade_date 转为 psycopg2 可原生适配的
    time/date 类型——**列名对齐不做类型强转**，故此处必须显式转（否则 Timestamp 写不进 TIME 列）。
    """
    base = normalize_minute_df(raw, index_code)
    if base.empty:
        return pd.DataFrame()
    return pd.DataFrame({
        "index_code": base["stock_code"],
        "bar_time": base["bar_time"].dt.time,
        "trade_date": base["bar_time"].dt.date,
        "open": base["open"],
        "high": base["high"],
        "low": base["low"],
        "close": base["close"],
        "volume": base["volume"],
        "amount": base["amount"],
    })


def _month_partition_ddl(trade_date: str) -> str:
    """生成当月分区的 CREATE TABLE IF NOT EXISTS 语句（幂等）。"""
    d = pd.to_datetime(trade_date)
    start = d.replace(day=1)
    nxt = start + pd.offsets.MonthBegin(1)
    child = f"index_minute_{start.year}_{start.month:02d}"
    return (
        f"CREATE TABLE IF NOT EXISTS {child} PARTITION OF index_minute "
        f"FOR VALUES FROM ('{start.strftime('%Y-%m-%d')}') TO ('{nxt.strftime('%Y-%m-%d')}');"
    )


def ensure_month_partition(conn, trade_date: str) -> None:
    """写入前确保当月分区存在（O(1)、幂等）。"""
    with conn.cursor() as cur:
        cur.execute(_month_partition_ddl(trade_date))
    conn.commit()


def _download_save_batch(batch: List[str], trade_date: str) -> tuple[int, int]:
    """下载 + 读取 + **逐批立即入库**（被超时硬杀时已采批不丢，抗 1m download 挂起）。"""
    xtdata = require_xtdata()
    download_history_with_retry(batch, "1m", trade_date, trade_date)
    try:
        raw = xtdata.get_market_data_ex(
            field_list=["time", "open", "high", "low", "close", "volume", "amount"],
            stock_list=batch, period="1m", start_time=trade_date, end_time=trade_date,
            count=-1, dividend_type="none", fill_data=False)
    except Exception as exc:
        logger.debug(f"指数分钟批量读取失败({batch[0]}..): {exc}")
        return 0, 0
    chunks = []
    for code in batch:
        frame = raw.get(code) if isinstance(raw, dict) else None
        one = build_index_minute_df(frame, code)
        if not one.empty:
            chunks.append(one)
    if not chunks:
        return 0, 0
    merged = pd.concat(chunks, ignore_index=True)
    return save_to_postgres(merged, pre_aligned_df=None, table_name=TABLE_NAME)


def _missing_codes(trade_date: str, codes: list) -> list:
    """按日跳过已入库代码（幂等推进：重试/下轮只采未采到的，抗挂起收敛）。"""
    with get_connection() as conn:
        existing = pd.read_sql(
            "SELECT DISTINCT index_code FROM index_minute WHERE trade_date = %s",
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
        return f"{trade_date} 非交易日，指数分钟线跳过。"
    codes = _get_codes(kwargs.get("limit_stocks"))
    with get_connection() as conn:
        ensure_month_partition(conn, trade_date)
    with tqdm(total=len(codes), desc=f"指数分钟({trade_date})", unit="个", file=sys.stdout) as pbar:
        tried, inserted = _collect_one_day(trade_date, codes, pbar)
    return f"{trade_date} 指数分钟线完成，写入 {tried}/{inserted} 条。"


def run_backfill(*args, **kwargs) -> str:
    raise NotImplementedError(
        "指数分钟线无历史回补：QMT 仅保留近端窗口，拉旧 1m 窗口会无超时挂死（见 spec §2.3）。"
        "仅支持前向积累（每日 run 采当天）。"
    )


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    days = date_range(start_date, end_date)
    codes = _get_codes(kwargs.get("limit_stocks"))
    tot_t = tot_i = fixed = 0
    for i, d in enumerate(days, 1):
        with get_connection() as conn:
            ensure_month_partition(conn, d)
        missing = _missing_codes(d, codes)
        if not missing:
            continue
        fixed += 1
        with tqdm(total=len(missing), desc=f"补缺 {i}/{len(days)} {d} 缺{len(missing)}", unit="个", file=sys.stdout) as pbar:
            t, ins = _collect_one_day(d, missing, pbar)
            tot_t += t; tot_i += ins
    if fixed == 0:
        return f"指数分钟线检查完成，{len(days)} 交易日完整（{len(codes)}个）。"
    return f"指数分钟线补缺完成，{fixed}/{len(days)} 天有缺，补 {tot_t}/{tot_i} 条。"
