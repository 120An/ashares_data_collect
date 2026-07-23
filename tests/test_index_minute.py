"""index_minute 测试（不依赖 xtquant 或数据库）。"""
from datetime import date, time

import pandas as pd
import pytest

from data_collect.jobs.index_minute import (
    build_index_minute_df,
    _month_partition_ddl,
    run_backfill,
)


def test_build_index_minute_df_maps_by_name_with_native_types():
    # 1704335400000 ms = 2024-01-04 02:30:00 UTC → +8h = 2024-01-04 10:30:00 北京
    raw = pd.DataFrame({
        "time": [1704335400000],
        "open": [3000.0], "high": [3010.0], "low": [2990.0], "close": [3005.0],
        "volume": [1000000.0], "amount": [3000000000.0],
    })
    out = build_index_minute_df(raw, "000300.SH")
    assert list(out.columns) == [
        "index_code", "bar_time", "trade_date",
        "open", "high", "low", "close", "volume", "amount",
    ]
    row = out.iloc[0]
    assert row["index_code"] == "000300.SH"
    assert row["bar_time"] == time(10, 30, 0)     # datetime.time（非 Timestamp）
    assert row["trade_date"] == date(2024, 1, 4)  # datetime.date（非字符串）
    assert row["open"] == 3000.0
    assert row["close"] == 3005.0


def test_build_index_minute_df_empty():
    assert build_index_minute_df(pd.DataFrame(), "000300.SH").empty
    assert build_index_minute_df(None, "000300.SH").empty


def test_month_partition_ddl_boundaries():
    ddl = _month_partition_ddl("20260721")
    assert "index_minute_2026_07" in ddl
    assert "IF NOT EXISTS" in ddl
    assert "FROM ('2026-07-01') TO ('2026-08-01')" in ddl


def test_month_partition_ddl_december_rolls_year():
    ddl = _month_partition_ddl("20261215")
    assert "index_minute_2026_12" in ddl
    assert "FROM ('2026-12-01') TO ('2027-01-01')" in ddl


def test_run_backfill_raises():
    with pytest.raises(NotImplementedError):
        run_backfill("20240101", "20240105")
