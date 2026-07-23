"""个股资金流日级采集（东财 push2，写 fund_flow_daily）。

两条路径：
- run(T)：clist 全市场快照（~60 页 × 100 行，一轮 ~1.5 分钟），**按日替换**语义
  （盘中滚动收盘定格，17:30 调度拿最终值；盘中误跑被下次替换自愈）。
- run_backfill / run_verify 缺日回补：per-stock `fflow/daykline` 端点（每票一请求
  返回近 120 交易日全部日级），**仅 120 交易日窗内**——更早直接 raise（同
  index_minute 教义）。一轮全市场 ~5900 请求 × ≥1s ≈ 2h，一次补齐窗内全部缺日
  → 逐票立即入库（韧性骨架），幂等可断点续跑。
"""
from __future__ import annotations

import datetime
import logging

import pandas as pd

from data_collect.utils.date_utils import is_market_day, date_range
from data_collect.utils.db import (
    save_to_postgres, get_dates_with_data, replace_day_then_insert,
)
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.eastmoney import em_get, clist_query, beijing_now

logger = logging.getLogger(__name__)
TABLE_NAME = "fund_flow_daily"

# 全市场（沪主板/深主板/创业/科创/北证）
_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
_FIELDS = "f12,f14,f2,f3,f62,f66,f72,f78,f84,f184"
_WINDOW = 120          # daykline 端点仅保留近 120 交易日


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def normalize_clist(diff: list, trade_date: str) -> pd.DataFrame:
    """clist diff 行 → fund_flow_daily 表行（'-' 占位转 None）。"""
    if not diff:
        return pd.DataFrame()
    d = pd.to_datetime(trade_date).date()
    rows = []
    for r in diff:
        rows.append({"trade_date": d, "stock_code": r["f12"], "name": r.get("f14"),
                     "close": _f(r.get("f2")), "pct": _f(r.get("f3")),
                     "main_net": _f(r.get("f62")), "super_net": _f(r.get("f66")),
                     "large_net": _f(r.get("f72")), "mid_net": _f(r.get("f78")),
                     "small_net": _f(r.get("f84")), "main_net_pct": _f(r.get("f184"))})
    return pd.DataFrame(rows)


def parse_daykline(stock_code: str, klines: list) -> list[dict]:
    """daykline 行串 → 行 dict 列表。列序：date,main,small,mid,large,super,...（后接占比列）。"""
    rows = []
    for line in klines:
        parts = line.split(",")
        if len(parts) < 6:
            continue
        rows.append({"trade_date": pd.to_datetime(parts[0]).date(),
                     "stock_code": stock_code,
                     "main_net": _f(parts[1]), "small_net": _f(parts[2]),
                     "mid_net": _f(parts[3]), "large_net": _f(parts[4]),
                     "super_net": _f(parts[5])})
    return rows


def _fetch_daykline(stock_code: str) -> list[dict]:
    market = 1 if stock_code.startswith("6") else 0
    r = em_get("https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get",
               params={"secid": f"{market}.{stock_code}",
                       "fields1": "f1,f2,f3,f7",
                       "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,"
                                  "f61,f62,f63,f64,f65",
                       "lmt": str(_WINDOW)},
               headers={"Referer": "https://quote.eastmoney.com/",
                        "Origin": "https://quote.eastmoney.com"}, timeout=15)
    klines = ((r.json() or {}).get("data") or {}).get("klines") or []
    return parse_daykline(stock_code, klines)


def _all_codes() -> list[str]:
    """全市场代码集（来自 clist 自身，免 QMT 依赖）。"""
    return [r["f12"] for r in clist_query(fs=_FS, fields="f12", fid="f62")]


def _assert_in_window(start_date: str) -> None:
    today = datetime.date.today().strftime("%Y%m%d")
    n = len(date_range(start_date, today))
    if n > _WINDOW:
        raise ValueError(
            f"资金流历史仅保留近 {_WINDOW} 交易日（daykline 端点窗口），"
            f"start={start_date} 距今 {n} 交易日超窗，无法回补。")


def _safe_max_date(bj_now: datetime.datetime) -> datetime.date:
    """回补安全上限日：未过 15:05（收盘+缓冲）只允许补到昨天。

    insert-only 回补若写入进行中交易日的盘中半截值将**永远无法修正**（PK 去重挡住
    后续更正），故必须排除；当日终值由 run() 的 clist 按日替换路径负责。
    """
    if bj_now.time() >= datetime.time(15, 5):
        return bj_now.date()
    return bj_now.date() - datetime.timedelta(days=1)


def _sweep_window(target_dates: set) -> tuple[int, int]:
    """per-stock 全市场扫一轮，把 target_dates 内的行全部补入（逐票立即入库）。

    以服务器北京时间（beijing_now，不信本机钟——本机时区曾误设 UTC-7）裁剪掉
    未收盘交易日，防盘中半截值被 insert-only 永久锁死。
    """
    safe_max = _safe_max_date(beijing_now())
    dropped = {d for d in target_dates if d > safe_max}
    if dropped:
        logger.info(f"资金流回补跳过未收盘日 {sorted(dropped)}（由当日 run 负责）")
    target_dates = target_dates - dropped
    if not target_dates:
        return 0, 0
    codes = _all_codes()
    tot_t = tot_i = fail = 0
    for i, code in enumerate(codes, 1):
        try:
            rows = [r for r in _fetch_daykline(code) if r["trade_date"] in target_dates]
        except Exception as exc:
            fail += 1
            logger.debug(f"资金流 daykline 失败 {code}: {exc}")
            continue
        if not rows:
            continue
        df = pd.DataFrame(rows).astype(object)
        df = df.where(pd.notna(df), None)
        t, ins = save_to_postgres(df, pre_aligned_df=None, table_name=TABLE_NAME)
        tot_t += t; tot_i += ins
        if i % 500 == 0:
            logger.info(f"资金流回补进度 {i}/{len(codes)}，累计写入 {tot_i}")
    if fail:
        logger.warning(f"资金流回补 {fail} 票请求失败（周 verify 兜底）")
    return tot_t, tot_i


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} 非交易日，资金流跳过。"
    diff = clist_query(fs=_FS, fields=_FIELDS, fid="f62")
    df = normalize_clist(diff, trade_date)
    tried, inserted = replace_day_then_insert(TABLE_NAME, trade_date, df)
    return f"{trade_date} 资金流完成，{len(df)} 票，写入 {tried}/{inserted} 条。"


def run_backfill(start_date: str, end_date: str, **kwargs) -> str:
    _assert_in_window(start_date)
    days = date_range(start_date, end_date)
    existing = get_dates_with_data(TABLE_NAME, "trade_date", start_date, end_date)
    target = {pd.to_datetime(d).date() for d in days} - existing
    if not target:
        return f"资金流回补：{len(days)} 交易日已完整，无需回补。"
    tot_t, tot_i = _sweep_window(target)
    return (f"资金流回补完成 ({start_date}~{end_date})，目标 {len(target)} 天，"
            f"写入 {tot_t}/{tot_i} 条。")


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    days = date_range(start_date, end_date)
    existing = get_dates_with_data(TABLE_NAME, "trade_date", start_date, end_date)
    target = {pd.to_datetime(d).date() for d in days} - existing
    if not target:
        return f"资金流检查完成，{len(days)} 交易日完整。"
    tot_t, tot_i = _sweep_window(target)
    return f"资金流补缺完成，{len(target)}/{len(days)} 天缺失，补 {tot_t}/{tot_i} 条。"
