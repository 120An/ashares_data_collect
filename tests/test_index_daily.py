"""index_daily._normalize 测试（不依赖 xtquant 或数据库）。"""
from datetime import date

import pandas as pd

from data_collect.jobs.index_daily import _normalize


def test_normalize_keeps_dotted_code_and_beijing_date():
    # 1704335400000 ms = 2024-01-04 02:30:00 UTC → +8h = 2024-01-04 10:30 北京 → date 2024-01-04
    raw = pd.DataFrame({
        "time": [1704335400000],
        "open": [3000.0], "high": [3010.0], "low": [2990.0], "close": [3005.0],
        "volume": [1000000.0], "amount": [3000000000.0],
    })
    out = _normalize(raw, "000300.SH", "20240104")
    assert list(out.columns) == [
        "index_code", "trade_date", "open", "high", "low", "close", "volume", "amount"
    ]
    row = out.iloc[0]
    assert row["index_code"] == "000300.SH"   # 带点透传，不转裸码
    assert row["trade_date"] == date(2024, 1, 4)
    assert row["close"] == 3005.0


def test_normalize_empty_returns_empty():
    assert _normalize(pd.DataFrame(), "000300.SH", "20240104").empty
    assert _normalize(None, "000300.SH", "20240104").empty
