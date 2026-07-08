import pytest

from tick_analysis.orderbook import (
    order_book_imbalance, microprice, quoted_spread, weiba_weicha, book_depth,
)


def test_obi_level1(simple_frame):
    obi = order_book_imbalance(simple_frame, levels=1)
    assert obi.iloc[1] == pytest.approx(0.6)
    assert obi.iloc[2] == pytest.approx((80 - 300) / 380)


def test_microprice(simple_frame):
    mp = microprice(simple_frame)
    assert mp.iloc[1] == pytest.approx((10.00 * 50 + 10.02 * 200) / 250)


def test_quoted_spread(simple_frame):
    sp = quoted_spread(simple_frame)
    assert sp.iloc[1] == pytest.approx(0.02)
    spb = quoted_spread(simple_frame, in_bps=True)
    assert spb.iloc[1] == pytest.approx(0.02 / 10.01 * 1e4)


def test_weiba(simple_frame):
    w = weiba_weicha(simple_frame, levels=1)
    assert w["weicha"].iloc[1] == 150
    assert w["weibi"].iloc[1] == pytest.approx(0.6)


def test_book_depth_levels(simple_frame):
    d1 = book_depth(simple_frame, levels=1)
    d5 = book_depth(simple_frame, levels=5)
    assert d1.iloc[1] == pytest.approx(d5.iloc[1])
