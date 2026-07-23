import pandas as pd

from data_collect.jobs.etf_minute import build_etf_minute_df


def test_build_maps_ochl_by_name():
    raw = pd.DataFrame({
        "time": [1704330600000],  # 有效 UTC 毫秒时间戳
        "open": [1.10], "high": [1.20], "low": [1.05], "close": [1.15],
        "volume": [100.0], "amount": [1150.0],
    })
    out = build_etf_minute_df(raw, "159915")
    assert list(out.columns) == ["日期", "时间", "代码", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "最新价"]
    row = out.iloc[0]
    assert row["开盘"] == 1.10
    assert row["收盘"] == 1.15   # close -> 收盘（不是 high!）
    assert row["最高"] == 1.20   # high -> 最高
    assert row["最低"] == 1.05
    assert row["成交量"] == 100.0
    assert row["成交额"] == 1150.0
    assert row["最新价"] == 1.15  # 最新价 = close
    assert row["代码"] == "159915"


def test_build_empty_returns_empty():
    assert build_etf_minute_df(pd.DataFrame(), "159915").empty
    assert build_etf_minute_df(None, "159915").empty
