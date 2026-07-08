"""执行分析(TCA)：VWAP/TWAP 完整实现；滑点/参与率/IS 需调用方提供成交记录 fills。

fills: DataFrame[price, volume, side('buy'/'sell')]。
"""
from __future__ import annotations

import pandas as pd

from .preprocess import to_increments, continuous


def _window(df: pd.DataFrame, start, end) -> pd.DataFrame:
    inc = to_increments(df)
    inc = inc[inc["dVol"] > 0]
    if start is not None:
        inc = inc[inc["datetime"] >= pd.Timestamp(start)]
    if end is not None:
        inc = inc[inc["datetime"] <= pd.Timestamp(end)]
    return inc


def vwap(df: pd.DataFrame, start=None, end=None) -> float:
    """成交量加权均价 = Σ dAmt / Σ dVol（区间可选）。"""
    inc = _window(df, start, end)
    vol = inc["dVol"].sum()
    return float(inc["dAmt"].sum() / vol) if vol else float("nan")


def twap(df: pd.DataFrame, start=None, end=None) -> float:
    """时间加权均价 = 连续段 last_price 均值（区间可选）。"""
    cont = continuous(df)
    if start is not None:
        cont = cont[cont["datetime"] >= pd.Timestamp(start)]
    if end is not None:
        cont = cont[cont["datetime"] <= pd.Timestamp(end)]
    return float(cont["last_price"].mean()) if len(cont) else float("nan")


def slippage_vs_vwap(fills: pd.DataFrame, df: pd.DataFrame) -> float:
    """成交相对 VWAP 的滑点(bp)，买正卖负：sign·(avg_fill - vwap)/vwap·1e4。"""
    v = vwap(df)
    avg_fill = (fills["price"] * fills["volume"]).sum() / fills["volume"].sum()
    sign = 1.0 if str(fills["side"].iloc[0]).lower() == "buy" else -1.0
    return float(sign * (avg_fill - v) / v * 1e4)


def participation_rate(fills: pd.DataFrame, df: pd.DataFrame,
                       start=None, end=None) -> float:
    """参与率 = 自身成交量 / 区间市场成交量。
    注意：fills.volume 必须与契约 volume 同单位(股)，否则比值偏 100 倍。"""
    inc = _window(df, start, end)
    mkt = inc["dVol"].sum()
    return float(fills["volume"].sum() / mkt) if mkt else float("nan")


def implementation_shortfall(arrival_price: float, fills: pd.DataFrame) -> dict:
    """实现差额：(成交均价 - 到达价)/到达价（买方向；卖方向自行取负）。"""
    avg_fill = (fills["price"] * fills["volume"]).sum() / fills["volume"].sum()
    is_bps = (avg_fill - arrival_price) / arrival_price * 1e4
    return {"avg_fill": float(avg_fill), "arrival_price": float(arrival_price),
            "is_bps": float(is_bps)}
