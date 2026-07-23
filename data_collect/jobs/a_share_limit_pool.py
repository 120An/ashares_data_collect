"""打板层采集：东财涨停/炸板/跌停/昨涨停四池 + 同花顺涨停揭秘。

数据形态为**盘中滚动、收盘定格**的快照 → run 采用**按日替换**语义（先 DELETE 当日
再插入）：盘中误跑/重跑都会被下一次运行修正，幂等自愈。调度定 17:30（收盘定格后）。

历史边界（2026-07-22 实测）：东财四池仅保留 ~15 交易日近窗（超窗返回空）→ 前向积累
为主；ths 揭秘可回补约 8 个月（~2025-12 起）。backfill 对空返回跳过不报错。
衍生指标（炸板率/连板梯队/晋级率）读时自算，不入库。
"""
from __future__ import annotations

import datetime
import logging

import pandas as pd
import requests

from data_collect.utils.date_utils import is_market_day, date_range
from data_collect.utils.db import get_connection, replace_day_then_insert
from data_collect.utils.df_utils import normalize_trade_date
from data_collect.utils.eastmoney import em_get, UA

logger = logging.getLogger(__name__)

_ZTB_UT = "7eea3edcaed734bea9cbfc24409ed989"
_POOL_TABLE = "limit_pool"
_THS_TABLE = "limit_up_reason"
_BJ_TZ = datetime.timezone(datetime.timedelta(hours=8))

# 四池端点与排序（dpt 均为 wz.ztzt；sort 各池固定）
_POOLS = {
    "zt": ("getTopicZTPool", "fbt:asc"),
    "zb": ("getTopicZBPool", "fbt:asc"),
    "dt": ("getTopicDTPool", "fund:asc"),
    "yzt": ("getYesterdayZTPool", "zs:desc"),
}

_THS_URL = "https://data.10jqka.com.cn/dataapi/limit_up/limit_up_pool"
_THS_FIELD = ("199112,10,9001,330323,330324,330325,9002,330329,"
              "133971,133970,1968584,3475914,9003,9004")


def _fmt_zt_time(t) -> datetime.time | None:
    """池时间整数 92500 → time(9,25,0)；None/0 → None。"""
    if not t:
        return None
    s = str(int(t)).zfill(6)
    return datetime.time(int(s[0:2]), int(s[2:4]), int(s[4:6]))


def _zt_stat(p: dict) -> str | None:
    tj = p.get("zttj") or {}
    if not tj:
        return None
    return f'{tj.get("days", "?")}天{tj.get("ct", "?")}板'


def normalize_pool(pool_type: str, raw: list, trade_date: str) -> pd.DataFrame:
    """东财池原始行 → limit_pool 表行（关键列显式索引，缺列 KeyError fail-fast）。"""
    if not raw:
        return pd.DataFrame()
    d = pd.to_datetime(trade_date).date()
    rows = []
    for p in raw:
        base = {"trade_date": d, "pool_type": pool_type,
                "stock_code": p["c"], "name": p["n"],
                "price": p["p"] / 1000, "pct": round(p["zdp"], 4),
                "turnover": round(p["hs"], 4) if p.get("hs") is not None else None,
                "industry": p.get("hybk", ""), "zt_stat": _zt_stat(p)}
        if pool_type == "zt":
            base.update({"amount": p["amount"], "float_cap": p["ltsz"],
                         "limit_days": p["lbc"], "first_seal": _fmt_zt_time(p["fbt"]),
                         "last_seal": _fmt_zt_time(p["lbt"]), "seal_fund": p["fund"],
                         "break_times": p["zbc"]})
        elif pool_type == "zb":
            base.update({"limit_price": p["ztp"] / 1000, "first_seal": _fmt_zt_time(p["fbt"]),
                         "break_times": p["zbc"], "amplitude": round(p["zf"], 4),
                         "speed": round(p["zs"], 4)})
        elif pool_type == "dt":
            base.update({"pe": p.get("pe"), "seal_fund": p["fund"],
                         "last_seal": _fmt_zt_time(p["lbt"]), "board_amount": p.get("fba"),
                         "dt_days": p.get("days"), "break_times": p.get("oc")})
        elif pool_type == "yzt":
            base.update({"amplitude": round(p["zf"], 4), "speed": round(p["zs"], 4),
                         "y_first_seal": _fmt_zt_time(p["yfbt"]), "y_limit_days": p["ylbc"]})
        rows.append(base)
    return pd.DataFrame(rows)


def normalize_ths(raw: list, trade_date: str) -> pd.DataFrame:
    """ths 揭秘原始行 → limit_up_reason 表行（first_time 为 unix秒→北京 TIME）。"""
    if not raw:
        return pd.DataFrame()
    d = pd.to_datetime(trade_date).date()
    rows = []
    for it in raw:
        ft = it.get("first_limit_up_time")
        first_time = (datetime.datetime.fromtimestamp(int(ft), tz=_BJ_TZ)
                      .time().replace(microsecond=0)) if ft else None
        rows.append({"trade_date": d, "stock_code": it["code"], "name": it.get("name"),
                     "price": it.get("latest"), "pct": it.get("change_rate"),
                     "reason": it.get("reason_type", ""),
                     "board_type": it.get("limit_up_type", ""),
                     "seal_rate": it.get("limit_up_suc_rate"),
                     "break_times": it.get("open_num") or 0,
                     "seal_amount": it.get("order_amount"),
                     "high_days": it.get("high_days", ""),
                     "first_time": first_time,
                     "is_again": it.get("is_again_limit")})
    return pd.DataFrame(rows)


def _fetch_pool(pool_type: str, trade_date: str) -> list:
    endpoint, sort = _POOLS[pool_type]
    r = em_get(f"https://push2ex.eastmoney.com/{endpoint}",
               params={"ut": _ZTB_UT, "dpt": "wz.ztzt", "Pageindex": 0,
                       "pagesize": 10000, "sort": sort, "date": trade_date},
               headers={"Referer": "https://quote.eastmoney.com/"}, timeout=10)
    return ((r.json() or {}).get("data") or {}).get("pool") or []


def _fetch_ths(trade_date: str) -> list:
    """ths 揭秘（分页）。limit 服务端上限 200：>200 时 status=-1 静默空
    （2026-07-22 实测，与 cls rn>50 同款坑），勿调大。历史最大涨停 ~400 → 2 页足。"""
    out, page = [], 1
    while page <= 5:
        r = requests.get(_THS_URL, params={
            "page": page, "limit": 200, "field": _THS_FIELD,
            "filter": "HS,GEM2STAR", "order_field": "330324",
            "order_type": "0", "date": trade_date},
            headers={"User-Agent": UA}, timeout=10)
        info = ((r.json() or {}).get("data") or {}).get("info") or []
        out.extend(info)
        if len(info) < 200:
            break
        page += 1
    return out


def _collect_day(trade_date: str) -> tuple[int, int]:
    """采单日：四池 + ths。返回 (池写入, ths写入)。"""
    pool_frames = []
    for pt in _POOLS:
        raw = _fetch_pool(pt, trade_date)
        one = normalize_pool(pt, raw, trade_date)
        if not one.empty:
            pool_frames.append(one)
    n_pool = 0
    if pool_frames:
        merged = pd.concat(pool_frames, ignore_index=True)
        _, n_pool = replace_day_then_insert(_POOL_TABLE, trade_date, merged)
    ths_df = normalize_ths(_fetch_ths(trade_date), trade_date)
    n_ths = 0
    if not ths_df.empty:
        _, n_ths = replace_day_then_insert(_THS_TABLE, trade_date, ths_df)
    return n_pool, n_ths


def run(run_date: str, **kwargs) -> str:
    trade_date = normalize_trade_date(run_date)
    if not is_market_day(trade_date):
        return f"{trade_date} 非交易日，打板池跳过。"
    n_pool, n_ths = _collect_day(trade_date)
    return f"{trade_date} 打板池完成：四池 {n_pool} 行，揭秘 {n_ths} 行。"


def run_backfill(start_date: str, end_date: str, **kwargs) -> str:
    """逐日回补：东财四池超近窗返回空→跳过；ths 可回补约至 2025-12（更早自然为空）。"""
    days = date_range(start_date, end_date)
    tot_pool = tot_ths = skipped = 0
    for i, d in enumerate(days, 1):
        n_pool, n_ths = _collect_day(d)
        if n_pool == 0 and n_ths == 0:
            skipped += 1
        tot_pool += n_pool; tot_ths += n_ths
        logger.info(f"{i}/{len(days)} {d}: 池 {n_pool} 揭秘 {n_ths}")
    return (f"打板池回补完成 ({start_date}~{end_date})，{len(days)} 交易日，"
            f"四池 {tot_pool} 行，揭秘 {tot_ths} 行，空/超窗 {skipped} 天。")


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    """近窗补缺：库内无当日行且源返回非空 → 补采（源空=无涨停或超窗，视为完整）。"""
    days = date_range(start_date, end_date)
    fixed = 0
    with get_connection() as conn:
        existing = pd.read_sql(
            f'SELECT DISTINCT trade_date FROM "{_POOL_TABLE}" '
            f"WHERE trade_date BETWEEN %s AND %s",
            conn, params=[pd.to_datetime(start_date).date(),
                          pd.to_datetime(end_date).date()])
    have = set(existing["trade_date"])
    for d in days:
        if pd.to_datetime(d).date() in have:
            continue
        n_pool, n_ths = _collect_day(d)
        if n_pool or n_ths:
            fixed += 1
    if fixed == 0:
        return f"打板池检查完成，{len(days)} 交易日完整。"
    return f"打板池补缺完成，补 {fixed} 天。"
