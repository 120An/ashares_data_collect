import pytest

from tick_analysis.moneyflow import (
    classify_trades, money_flow, avg_trade_amount, trade_size_buckets,
    main_force_flow,
)


def test_classify_lee_ready(simple_frame):
    d = classify_trades(simple_frame, method="lee_ready")
    assert list(d) == [1, -1, 1]


def test_money_flow_net(simple_frame):
    mf = money_flow(simple_frame, method="lee_ready")
    assert mf["buy"] == 10010 + 15030
    assert mf["sell"] == 5000
    assert mf["net"] == 20040
    assert mf["net_ratio"] == pytest.approx(20040 / 30040)


def test_avg_trade_amount(simple_frame):
    avg = avg_trade_amount(simple_frame)
    assert avg.iloc[0] == pytest.approx(10010 / 5)


def test_trade_size_buckets(simple_frame):
    g = trade_size_buckets(simple_frame, thresholds=(4e4, 20e4, 100e4))
    assert g.loc["小单"] == pytest.approx(30040)
    assert g.loc["大单"] == 0


def test_main_force_flow(simple_frame):
    r = main_force_flow(simple_frame, big_threshold=12000, method="lee_ready")
    assert r["net"] == pytest.approx(15030)
    assert r["big_buy"] == pytest.approx(15030)
    assert r["big_sell"] == 0
    assert r["big_count"] == 1
    assert r["cum_net"].iloc[-1] == pytest.approx(15030)


def test_bvc_buy_fraction_bounded(simple_frame):
    mf = money_flow(simple_frame, method="bvc")
    assert mf["buy"] >= 0 and mf["sell"] >= 0
    assert mf["buy"] + mf["sell"] == pytest.approx(30040)
