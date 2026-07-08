import pytest

from tick_analysis.auction import opening_auction, closing_auction, auction_features


def test_opening_auction(auction_frame):
    r = opening_auction(auction_frame)
    assert r["open_price"] == pytest.approx(10.20)
    assert r["prev_close"] == pytest.approx(10.00)
    assert r["auction_pct"] == pytest.approx(2.0)
    assert r["auction_volume"] == pytest.approx(2000)
    assert r["open_n_snapshots"] == 2


def test_closing_auction(auction_frame):
    r = closing_auction(auction_frame)
    assert r["close_price"] == pytest.approx(10.45)
    assert r["close_n_snapshots"] == 2


def test_auction_features_keys(auction_frame):
    f = auction_features(auction_frame)
    assert "open_price" in f and "close_price" in f
