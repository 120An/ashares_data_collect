import numpy as np

from tick_analysis.preprocess import (
    to_increments, mid_price, prevailing_quote, split_sessions, continuous,
)


def test_to_increments(simple_frame):
    inc = to_increments(simple_frame)
    assert np.isnan(inc["dAmt"].iloc[0])
    assert inc["dAmt"].iloc[1] == 10010
    assert inc["dCnt"].iloc[2] == 3
    assert inc["dVol"].iloc[3] == 1500


def test_mid_price(simple_frame):
    mid = mid_price(simple_frame)
    assert mid.iloc[1] == (10.00 + 10.02) / 2


def test_prevailing_quote(simple_frame):
    pq = prevailing_quote(simple_frame)
    assert pq["prev_ask1"].iloc[1] == 10.01
    assert pq["prev_bid1"].iloc[2] == 10.00


def test_continuous_drops_zero_volume(simple_frame):
    cont = continuous(simple_frame)
    assert len(cont) == 3
    assert (cont["dVol"] > 0).all()


def test_split_sessions(auction_frame):
    s = split_sessions(auction_frame)
    assert len(s["open_auction"]) == 2
    assert len(s["close_auction"]) == 2
    assert len(s["continuous"]) == 2
