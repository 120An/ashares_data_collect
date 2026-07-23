import pandas as pd

from data_collect.utils.akshare_utils import build_nav_spot_df, build_nav_official_df


def test_build_nav_spot_df():
    spot = pd.DataFrame({
        "代码": ["510300", "159915", "000001"],  # 000001 非ETF号段，应被过滤
        "名称": ["300ETF", "创业板ETF", "x"],
        "最新价": [4.80, 2.10, 1.0],
        "IOPV实时估值": [4.79, 2.11, 1.0],
        "基金折价率": [0.21, -0.47, 0.0],
    })
    out = build_nav_spot_df(spot, "2026-07-06")
    assert set(out["code"]) == {"510300", "159915"}
    r = out[out["code"] == "510300"].iloc[0]
    assert r["close"] == 4.80 and r["iopv"] == 4.79 and r["discount_rate"] == 0.21
    assert str(r["trade_date"]) == "2026-07-06"


def test_build_nav_official_df():
    hist = pd.DataFrame({
        "净值日期": ["2025-01-02", "2025-01-03"],
        "单位净值": [3.9061, 3.8599],
        "累计净值": [4.10, 4.05],
        "日增长率": [-2.91, -1.18],
    })
    out = build_nav_official_df(hist, "510300")
    assert list(out["code"]) == ["510300", "510300"]
    r = out.iloc[0]
    assert r["unit_nav"] == 3.9061 and r["accum_nav"] == 4.10 and r["daily_growth"] == -2.91
    assert str(r["trade_date"]) == "2025-01-02" and str(r["nav_date"]) == "2025-01-02"
