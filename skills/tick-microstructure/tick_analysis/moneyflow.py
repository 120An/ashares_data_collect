"""资金流/主动买卖/主力净额/笔均/大中小单。

方向分类：
- lee_ready：成交价 vs 上一快照买一/卖一（quote rule），中价用 tick test 兜底；
- tick：仅按价格涨跌；
- bvc：块成交分类，用标准化价格变化的正态 CDF 估买入额占比（适合 3 秒聚合）。
全部使用滞后盘口/前值，避免 look-ahead。
"""
from __future__ import annotations

import datetime as _dt
import math

import numpy as np
import pandas as pd

from .contract import validate_tick_frame
from .preprocess import to_increments

_erf = np.vectorize(math.erf, otypes=[float])
_BVC_SPAN = 40          # BVC 因果 EWMA 标准差 span
_BVC_MIN_PERIODS = 20   # σ 估计最小样本数（不足则该切片中性 frac=0.5）


def _norm_cdf(x) -> np.ndarray:
    return 0.5 * (1.0 + _erf(np.asarray(x, dtype=float) / math.sqrt(2.0)))


def _signed_increments(df: pd.DataFrame, method: str = "lee_ready") -> pd.DataFrame:
    """返回 dAmt>0 的增量帧，附 buy_amt/sell_amt/signed/signed_vol；
    非 bvc 另附 dir(±1)。"""
    df = validate_tick_frame(df, require_orderbook=True)
    inc = to_increments(df)
    inc["prev_bid1"] = inc["bid_price_1"].shift(1)
    inc["prev_ask1"] = inc["ask_price_1"].shift(1)
    inc = inc[inc["dAmt"] > 0].copy()
    if inc.empty:
        for c in ("dir", "buy_amt", "sell_amt", "signed", "signed_vol"):
            inc[c] = pd.Series(dtype=float)
        return inc

    if method == "bvc":
        dp = inc["last_price"].diff()
        # 因果 EWMA 标准差：仅用过去信息，避免全天 σ 的前视泄漏（做因子/回测时关键）
        sigma = dp.ewm(span=_BVC_SPAN, min_periods=_BVC_MIN_PERIODS).std()
        frac = pd.Series(0.5, index=inc.index)
        ok = sigma.notna() & (sigma > 0) & dp.notna()
        frac[ok] = _norm_cdf(dp[ok] / sigma[ok])
        inc["buy_amt"] = inc["dAmt"] * frac
        inc["sell_amt"] = inc["dAmt"] * (1.0 - frac)
        inc["signed"] = inc["buy_amt"] - inc["sell_amt"]
        inc["signed_vol"] = inc["dVol"] * (2.0 * frac - 1.0)
        return inc

    if method == "tick":
        chg = inc["last_price"].diff()
        dirs = np.sign(chg).replace(0, np.nan).ffill().fillna(1.0)
        dirs = dirs.where(np.isfinite(inc["last_price"]), 0.0)
    elif method == "lee_ready":
        pbid, pask = inc["prev_bid1"], inc["prev_ask1"]
        mid = (pbid + pask) / 2.0
        dirs = pd.Series(
            np.where(inc["last_price"] >= pask, 1,
                     np.where(inc["last_price"] <= pbid, -1,
                              np.where(inc["last_price"] > mid, 1, -1))),
            index=inc.index, dtype=float,
        )
        finite = (np.isfinite(inc["last_price"]) & np.isfinite(pbid) & np.isfinite(pask))
        dirs = dirs.where(finite, 0.0)
    else:
        raise ValueError(f"unknown method: {method}")

    inc["dir"] = dirs.astype(int)
    inc["buy_amt"] = np.where(inc["dir"] > 0, inc["dAmt"], 0.0)
    inc["sell_amt"] = np.where(inc["dir"] < 0, inc["dAmt"], 0.0)
    inc["signed"] = inc["dir"] * inc["dAmt"]
    inc["signed_vol"] = inc["dir"] * inc["dVol"]
    return inc


def classify_trades(df: pd.DataFrame, method: str = "bvc") -> pd.Series:
    """每切片方向 ±1（method='bvc' 时返回买入额占比 ∈[0,1]）。"""
    inc = _signed_increments(df, method)
    if method == "bvc":
        return (inc["buy_amt"] / inc["dAmt"]).rename("buy_frac")
    return inc["dir"].rename("dir")


def money_flow(df: pd.DataFrame, method: str = "bvc") -> dict:
    """全单主动买/卖/净额/净占比。"""
    inc = _signed_increments(df, method)
    buy = float(inc["buy_amt"].sum())
    sell = float(inc["sell_amt"].sum())
    tot = buy + sell
    return {"buy": buy, "sell": sell, "net": buy - sell,
            "net_ratio": (buy - sell) / tot if tot else 0.0}


def avg_trade_amount(df: pd.DataFrame) -> pd.Series:
    """笔均成交额 = ΔAmt / ΔCnt（仅 ΔCnt>0）。"""
    inc = to_increments(df)
    inc = inc[inc["dCnt"] > 0]
    return (inc["dAmt"] / inc["dCnt"]).rename("avg_trade")


def trade_size_buckets(df: pd.DataFrame,
                       thresholds: tuple = (4e4, 20e4, 100e4),
                       labels: tuple = ("小单", "中单", "大单", "特大单")) -> pd.Series:
    """按笔均把成交额分到 小/中/大/特大 单。thresholds 为 3 个上界。"""
    inc = to_increments(df)
    inc = inc[inc["dCnt"] > 0].copy()
    avg = inc["dAmt"] / inc["dCnt"]
    edges = [0.0, *thresholds, float("inf")]
    inc["bucket"] = pd.cut(avg, bins=edges, labels=labels, right=False)
    return inc.groupby("bucket", observed=False)["dAmt"].sum()


def main_force_flow(df: pd.DataFrame, big_threshold: float = 50e4,
                    method: str = "bvc") -> dict:
    """主力（单切片 ΔAmt≥big_threshold 视为大单脉冲）主动买/卖/净额，
    含日内累计净额 Series 与 开盘(≤10:00)/尾盘(≥14:30) 主力净额。"""
    inc = _signed_increments(df, method)
    big_mask = inc["dAmt"] >= big_threshold
    big = inc[big_mask]
    cum = inc["signed"].where(big_mask, 0.0).cumsum()
    t = inc["datetime"].dt.time
    open_net = float(inc[big_mask & (t <= _dt.time(10, 0))]["signed"].sum())
    tail_net = float(inc[big_mask & (t >= _dt.time(14, 30))]["signed"].sum())
    return {
        "big_buy": float(big["buy_amt"].sum()),
        "big_sell": float(big["sell_amt"].sum()),
        "net": float(big["signed"].sum()),
        "cum_net": cum,
        "open_net": open_net,
        "tail_net": tail_net,
        "big_count": int(big_mask.sum()),
    }
