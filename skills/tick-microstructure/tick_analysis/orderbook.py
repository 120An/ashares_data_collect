"""盘口压力：订单簿不平衡、微价、委比委差、深度、报价价差。"""
from __future__ import annotations

import pandas as pd


def _sum_levels(df: pd.DataFrame, prefix: str, levels: int) -> pd.Series:
    cols = [f"{prefix}_{i}" for i in range(1, levels + 1) if f"{prefix}_{i}" in df.columns]
    return df[cols].sum(axis=1)


def order_book_imbalance(df: pd.DataFrame, levels: int = 1) -> pd.Series:
    """OBI = (Σbid_vol - Σask_vol)/(Σbid_vol + Σask_vol) ∈ [-1,1]。"""
    bv = _sum_levels(df, "bid_vol", levels)
    av = _sum_levels(df, "ask_vol", levels)
    denom = (bv + av).replace(0, pd.NA)
    return ((bv - av) / denom).astype(float).rename("obi")


def microprice(df: pd.DataFrame) -> pd.Series:
    """微价（Stoikov）：用对侧量加权一档买卖价，偏向更可能成交的一侧。
    单边盘口(买一或卖一价<=0，如一字板)返回 NaN。"""
    bp, ap = df["bid_price_1"], df["ask_price_1"]
    bv, av = df["bid_vol_1"], df["ask_vol_1"]
    denom = (bv + av).replace(0, pd.NA)
    mp = ((bp * av + ap * bv) / denom).astype(float)
    valid = (bp > 0) & (ap > 0)
    return mp.where(valid).rename("microprice")


def quoted_spread(df: pd.DataFrame, in_bps: bool = False) -> pd.Series:
    """报价价差 ask1-bid1（in_bps 时相对中价的 bp）。单边盘口(价<=0)返回 NaN。"""
    valid = (df["bid_price_1"] > 0) & (df["ask_price_1"] > 0)
    sp = (df["ask_price_1"] - df["bid_price_1"]).where(valid)
    if in_bps:
        mid = ((df["ask_price_1"] + df["bid_price_1"]) / 2.0).where(valid)
        return (sp / mid * 1e4).rename("spread_bp")
    return sp.rename("spread")


def weiba_weicha(df: pd.DataFrame, levels: int = 5) -> pd.DataFrame:
    """委差 = Σbid_vol - Σask_vol；委比 = 委差/(Σbid+Σask)。"""
    bv = _sum_levels(df, "bid_vol", levels)
    av = _sum_levels(df, "ask_vol", levels)
    out = pd.DataFrame(index=df.index)
    out["weicha"] = bv - av
    denom = (bv + av).replace(0, pd.NA)
    out["weibi"] = ((bv - av) / denom).astype(float)
    return out


def book_depth(df: pd.DataFrame, levels: int = 5, side: str = "both") -> pd.Series:
    """挂单深度（Σ 价×量）。side: bid/ask/both。注意 bid/ask 挂单量为源生单位(QMT 为手，
    未随成交 volume 转股)，故为相对量纲，仅同股同日可比。"""
    bid = sum(df[f"bid_price_{i}"] * df[f"bid_vol_{i}"]
              for i in range(1, levels + 1) if f"bid_price_{i}" in df.columns)
    ask = sum(df[f"ask_price_{i}"] * df[f"ask_vol_{i}"]
              for i in range(1, levels + 1) if f"ask_price_{i}" in df.columns)
    if side == "bid":
        return bid.rename("depth_bid")
    if side == "ask":
        return ask.rename("depth_ask")
    return (bid + ask).rename("depth")
