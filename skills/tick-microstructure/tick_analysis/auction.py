"""集合竞价：开盘(9:15-9:25)、收盘(14:57-15:00) 特征。"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .preprocess import split_sessions


def opening_auction(df: pd.DataFrame) -> dict:
    """开盘竞价特征：开盘价(连续段首行 last_price)、前收、竞价涨跌幅、撮合量、竞价快照数。"""
    sess = split_sessions(df)
    au, cont = sess["open_auction"], sess["continuous"]
    if cont.empty:
        return {}
    open_row = cont.iloc[0]
    prev_close = float(df["last_close"].iloc[0]) if "last_close" in df.columns else np.nan
    open_px = float(open_row["last_price"])
    return {
        "open_price": open_px,
        "prev_close": prev_close,
        "auction_pct": (open_px / prev_close - 1) * 100 if prev_close else np.nan,
        "auction_volume": float(open_row["volume"]),
        "open_n_snapshots": int(len(au)),
    }


def closing_auction(df: pd.DataFrame) -> dict:
    """收盘竞价特征：收盘价(收盘段末行 last_price)、竞价快照数。"""
    sess = split_sessions(df)
    ca = sess["close_auction"]
    if ca.empty:
        return {}
    close_px = float(ca["last_price"].iloc[-1])
    return {"close_price": close_px, "close_n_snapshots": int(len(ca))}


def auction_features(df: pd.DataFrame) -> dict:
    """合并开/收盘竞价关键特征。"""
    out = {}
    out.update(opening_auction(df))
    out.update(closing_auction(df))
    return out
