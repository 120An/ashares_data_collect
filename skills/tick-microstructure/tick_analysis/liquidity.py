"""流动性：有效价差、Kyle λ、Amihud 非流动性、Roll 隐含价差。

均为单日值；Amihud/Kyle λ 单日噪声大，建议上层多日聚合。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .preprocess import to_increments
from .moneyflow import _signed_increments


def effective_spread(df: pd.DataFrame, in_bps: bool = True) -> pd.Series:
    """有效价差 = 2·|price - mid|/mid，mid 取成交所在快照的**同期**中价。

    有效价差是成本度量（衡量已发生成交相对中价的偏离），用同期中价是标准做法；
    look-ahead 仅与方向分类相关，与成本度量无关，故此处不用滞后中价（用滞后会把
    切片间漂移注入估计、在趋势行情高估成本）。单边盘口(价<=0)返回 NaN。
    """
    inc = to_increments(df)
    inc = inc[inc["dAmt"] > 0]
    bid, ask = inc["bid_price_1"], inc["ask_price_1"]
    mid = (bid + ask) / 2.0
    valid = (bid > 0) & (ask > 0)
    es = (2.0 * (inc["last_price"] - mid).abs() / mid).where(valid).rename("eff_spread")
    return es * 1e4 if in_bps else es


def kyle_lambda(df: pd.DataFrame, freq: str = "1min",
                method: str = "lee_ready") -> float:
    """Kyle λ：按 freq 聚合，回归 Δprice ~ 签名成交量 的斜率。"""
    inc = _signed_increments(df, method)
    if inc.empty:
        return float("nan")
    g = (inc.set_index("datetime")
            .resample(freq)
            .agg(p=("last_price", "last"), sv=("signed_vol", "sum"))
            .dropna())
    if len(g) < 3:
        return float("nan")
    dp = g["p"].diff().dropna()
    sv = g["sv"].iloc[1:]
    if sv.nunique() < 2:
        return float("nan")
    slope = np.polyfit(sv.values, dp.values, 1)[0]
    return float(slope)


def amihud_illiquidity(df: pd.DataFrame, freq: str = "1min") -> float:
    """Amihud：median(|ret| / 成交额(亿元))，按 freq 聚合。日内代理(非原始 Amihud 2002 日度)；
    用 median 因低量分钟的 |ret|/amt 会爆大、mean 不稳健。"""
    inc = to_increments(df)
    g = (inc.set_index("datetime")
            .resample(freq)
            .agg(p=("last_price", "last"), amt=("dAmt", "sum"))
            .dropna())
    ret = g["p"].pct_change().abs()
    amt_yi = g["amt"] / 1e8
    val = (ret / amt_yi).replace([np.inf, -np.inf], np.nan).dropna()
    return float(val.median()) if len(val) else float("nan")


def roll_spread(df: pd.DataFrame) -> float:
    """Roll(1984) 隐含价差 = 2·sqrt(-cov(Δp_t, Δp_{t-1}))；正自协方差时返回 NaN。"""
    inc = to_increments(df)
    p = inc[inc["dAmt"] > 0]["last_price"]
    dp = p.diff().dropna().values
    if len(dp) < 3:
        return float("nan")
    cov = np.cov(dp[1:], dp[:-1])[0, 1]
    return float(2.0 * np.sqrt(-cov)) if cov < 0 else float("nan")
