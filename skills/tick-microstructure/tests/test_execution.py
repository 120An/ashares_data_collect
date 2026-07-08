import pandas as pd
import pytest

from tick_analysis.execution import (
    vwap, twap, slippage_vs_vwap, participation_rate, implementation_shortfall,
)


def test_vwap(simple_frame):
    v = vwap(simple_frame)
    assert v == pytest.approx(30040 / 3000)


def test_twap(simple_frame):
    t = twap(simple_frame)
    assert t == pytest.approx((10.01 + 10.00 + 10.02) / 3)


def test_slippage_buy(simple_frame):
    fills = pd.DataFrame({"price": [10.02], "volume": [100], "side": ["buy"]})
    s = slippage_vs_vwap(fills, simple_frame)
    assert s > 0


def test_participation_rate(simple_frame):
    fills = pd.DataFrame({"price": [10.0], "volume": [300], "side": ["buy"]})
    pr = participation_rate(fills, simple_frame)
    assert pr == pytest.approx(300 / 3000)


def test_implementation_shortfall(simple_frame):
    fills = pd.DataFrame({"price": [10.02], "volume": [100], "side": ["buy"]})
    r = implementation_shortfall(10.00, fills)
    assert r["is_bps"] == pytest.approx((10.02 - 10.00) / 10.00 * 1e4)
