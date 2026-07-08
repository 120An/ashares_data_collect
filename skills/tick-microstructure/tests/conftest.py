import numpy as np
import pandas as pd
import pytest


def _row(t, last, b1, a1, bv1, av1, vol, amt, cnt,
         b=(0, 0, 0, 0), a=(0, 0, 0, 0), bv=(0, 0, 0, 0), av=(0, 0, 0, 0)):
    """构造一行 tick：b/a/bv/av 为 2..5 档价/量（默认 0）。"""
    d = {
        "datetime": pd.Timestamp(t),
        "last_price": last, "volume": vol, "amount": amt, "transaction_num": cnt,
        "last_close": 10.00,
        "bid_price_1": b1, "ask_price_1": a1, "bid_vol_1": bv1, "ask_vol_1": av1,
    }
    for i in range(2, 6):
        d[f"bid_price_{i}"] = b[i - 2]
        d[f"ask_price_{i}"] = a[i - 2]
        d[f"bid_vol_{i}"] = bv[i - 2]
        d[f"ask_vol_{i}"] = av[i - 2]
    return d


@pytest.fixture
def simple_frame():
    """4 行连续竞价 tick，数值手工设计，分类/资金流可精确断言。

    row0: 起始(dVol=0，被增量丢弃)
    row1: 主动买  dAmt=10010
    row2: 主动卖  dAmt=5000
    row3: 主动买  dAmt=15030 (>=12000 视为大单)
    """
    rows = [
        _row("2026-06-01 09:30:00", 10.00, 9.99, 10.01, 100, 100, 0, 0, 0),
        _row("2026-06-01 09:30:03", 10.01, 10.00, 10.02, 200, 50, 1000, 10010, 5),
        _row("2026-06-01 09:30:06", 10.00, 9.99, 10.01, 80, 300, 1500, 15010, 8),
        _row("2026-06-01 09:30:09", 10.02, 10.01, 10.03, 400, 60, 3000, 30040, 12),
    ]
    return pd.DataFrame(rows)


@pytest.fixture
def auction_frame():
    """含开盘集合竞价(09:15-09:25) + 连续段 + 收盘集合竞价(14:57-15:00)。"""
    rows = [
        _row("2026-06-01 09:15:03", 0.0, 9.80, 9.80, 100, 100, 0, 0, 0),
        _row("2026-06-01 09:20:00", 0.0, 10.20, 10.20, 500, 500, 0, 0, 0),
        _row("2026-06-01 09:30:00", 10.20, 10.19, 10.21, 100, 100, 2000, 20400, 50),
        _row("2026-06-01 13:00:00", 10.30, 10.29, 10.31, 100, 100, 5000, 51400, 120),
        _row("2026-06-01 14:57:03", 10.40, 10.39, 10.41, 100, 100, 8000, 82600, 200),
        _row("2026-06-01 15:00:00", 10.45, 10.44, 10.46, 100, 100, 9000, 93700, 230),
    ]
    return pd.DataFrame(rows)
