import pytest

import tick_analysis as ta


def test_public_api_exports():
    for name in ["money_flow", "main_force_flow", "order_book_imbalance",
                 "amihud_illiquidity", "opening_auction", "vwap", "analyze",
                 "from_xtquant", "validate_tick_frame"]:
        assert hasattr(ta, name)


def test_analyze_battery(simple_frame):
    r = ta.analyze(simple_frame, big_threshold=12000, method="lee_ready")
    assert r["money_flow"]["net"] == 20040
    assert r["main_force"]["net"] == 15030
    assert "obi_mean" in r["orderbook"]
    assert "amihud" in r["liquidity"]
    assert "vwap" in r


def test_analyze_default_method_is_bvc(simple_frame):
    # 默认 method=bvc：分数拆分不漏量，买+卖=总成交额
    r = ta.analyze(simple_frame, big_threshold=12000)
    mf = r["money_flow"]
    assert mf["buy"] + mf["sell"] == pytest.approx(30040)
