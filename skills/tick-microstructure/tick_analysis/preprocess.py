"""预处理：累计→增量、会话切分、中价、滞后盘口。"""
from __future__ import annotations

import datetime as _dt

import pandas as pd

_OPEN_AUCTION_END = _dt.time(9, 25)
_CONT_START = _dt.time(9, 30)
_CONT_END = _dt.time(14, 57)
_CLOSE_END = _dt.time(15, 0)


def to_increments(df: pd.DataFrame) -> pd.DataFrame:
    """由累计 volume/amount/transaction_num 生成 dVol/dAmt/dCnt（首行为 NaN）。"""
    out = df.copy()
    out["dVol"] = out["volume"].diff()
    out["dAmt"] = out["amount"].diff()
    out["dCnt"] = out["transaction_num"].diff()
    return out


def mid_price(df: pd.DataFrame) -> pd.Series:
    """一档中价 (bid1+ask1)/2。"""
    return (df["bid_price_1"] + df["ask_price_1"]) / 2.0


def prevailing_quote(df: pd.DataFrame) -> pd.DataFrame:
    """加 prev_bid1/prev_ask1（滞后一个快照），用于 look-ahead-safe 的方向判断。"""
    out = df.copy()
    out["prev_bid1"] = out["bid_price_1"].shift(1)
    out["prev_ask1"] = out["ask_price_1"].shift(1)
    return out


def split_sessions(df: pd.DataFrame) -> dict:
    """按时间切：open_auction / continuous / close_auction / after_hours。"""
    t = df["datetime"].dt.time
    return {
        "open_auction": df[t <= _OPEN_AUCTION_END].copy(),
        "continuous": df[(t >= _CONT_START) & (t < _CONT_END)].copy(),
        "close_auction": df[(t >= _CONT_END) & (t <= _CLOSE_END)].copy(),
        "after_hours": df[t > _CLOSE_END].copy(),
    }


def continuous(df: pd.DataFrame) -> pd.DataFrame:
    """连续竞价段增量帧：dVol>0 且时间 ∈ [09:30,14:57)。"""
    inc = to_increments(df)
    t = inc["datetime"].dt.time
    mask = (inc["dVol"] > 0) & (t >= _CONT_START) & (t < _CONT_END)
    return inc[mask].copy()
