"""etf_option_daily 解析测试（不依赖网络/数据库）。"""
import datetime as dt

import pytest

from data_collect.jobs.etf_option_daily import (
    parse_tquote,
    parse_greeks,
    build_option_row,
    run_backfill,
)


def _tq_fields():
    """51 字段合成 T 型报价（关键位：0..11 + 37..42）。"""
    v = ["0"] * 51
    v[0], v[1], v[2], v[3], v[4] = "10", "0.1", "0.15", "0.16", "20"
    v[5], v[6], v[7], v[8] = "1000", "5.5", "2.75", "0.14"
    v[9], v[10], v[11] = "0.14", "0.5", "0.01"
    v[37] = "50ETF购8月2750"
    v[38], v[39], v[40], v[41], v[42] = "3.3", "0.17", "0.12", "888", "123456"
    return v


def test_parse_tquote_positions():
    q = parse_tquote(_tq_fields())
    assert q["bid_vol"] == 10.0
    assert q["bid"] == 0.1
    assert q["last"] == 0.15
    assert q["ask"] == 0.16
    assert q["open_interest"] == 1000.0
    assert q["strike"] == 2.75
    assert q["name"] == "50ETF购8月2750"
    assert q["volume"] == 888.0
    assert q["amount"] == 123456.0


def test_parse_tquote_short_returns_empty():
    assert parse_tquote(["1", "2", "3"]) == {}


def test_parse_greeks_skips_three_empty():
    raw = ["50ETF购8月2750", "", "", "", "888", "0.55", "1.2", "-0.9", "0.33",
           "0.1735", "0.17", "0.12", "510050C2608M02750", "2.75", "0.15", "0.149"]
    g = parse_greeks(raw)
    assert g["delta"] == 0.55
    assert g["gamma"] == 1.2
    assert g["theta"] == -0.9
    assert g["vega"] == 0.33
    assert g["iv"] == pytest.approx(0.1735)
    assert g["theory"] == pytest.approx(0.149)


def test_parse_greeks_short_returns_empty():
    assert parse_greeks(["a", "b"]) == {}


def test_build_option_row_merges():
    row = build_option_row(
        trade_date="20260723", option_code="10009269",
        underlying="510050", call_put="C", expiry_month="2608",
        tq=parse_tquote(_tq_fields()),
        greeks=parse_greeks(["n", "", "", "", "888", "0.55", "1.2", "-0.9",
                             "0.33", "0.1735", "0.17", "0.12", "x", "2.75",
                             "0.15", "0.149"]))
    assert row["trade_date"] == dt.date(2026, 7, 23)
    assert row["option_code"] == "10009269"
    assert row["call_put"] == "C"
    assert row["expiry_month"] == "2608"
    assert row["strike"] == 2.75
    assert row["iv"] == pytest.approx(0.1735)
    assert row["last"] == 0.15
    assert row["open_interest"] == 1000.0


def test_run_backfill_raises():
    with pytest.raises(NotImplementedError):
        run_backfill("20260101", "20260105")
