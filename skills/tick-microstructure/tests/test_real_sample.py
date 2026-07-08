import os

import pytest

import tick_analysis as ta

SAMPLE = r"Z:\A股冷数据\tick_by_day\2026\06\26.parquet"


@pytest.mark.skipif(not os.path.exists(SAMPLE), reason="Z 盘 tick 样例不可用")
def test_real_002602_analyze():
    df = ta.from_packed_parquet(SAMPLE, stock_code="002602.SZ")
    assert len(df) > 1000
    r = ta.analyze(df, big_threshold=50e4)
    assert 5.0 < r["vwap"] < 50.0
    mf = r["money_flow"]
    assert mf["buy"] + mf["sell"] > 1e8
    avg = ta.avg_trade_amount(df)
    assert avg.notna().any()
