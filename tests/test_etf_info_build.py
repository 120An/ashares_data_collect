import pandas as pd

from data_collect.jobs.etf_info import build_info_record


def test_build_info_record_merges_sources():
    detail = {"InstrumentName": "沪深300ETF华泰柏瑞", "ExtendName": "300ETF", "ExchangeID": "SH",
              "OpenDate": "20120528", "TotalVolume": 7912387690.0, "PriceTick": 0.001}
    spot_row = {"总市值": 71080259.0, "最新份额": 91716463.0}
    name_row = ("沪深300ETF", "指数型-股票")
    rec = build_info_record(detail, spot_row, name_row)
    assert rec["name"] == "沪深300ETF华泰柏瑞"
    assert rec["list_date"] == "20120528"
    assert rec["total_volume"] == 7912387690.0
    assert rec["fund_type"] == "指数型-股票"
    assert rec["total_mv"] == 71080259.0


def test_build_info_record_handles_missing():
    rec = build_info_record({}, None, None)
    assert rec["name"] is None and rec["fund_type"] is None


def test_build_info_record_with_series_spot():
    # 生产里 spot_row 曾是 pandas Series → `if spot_row:` 报 truth-ambiguous 被吞，导致漏采
    detail = {"InstrumentName": "创业板ETF"}
    spot_row = pd.Series({"总市值": 123.0, "最新份额": 456.0})
    rec = build_info_record(detail, spot_row, None)
    assert rec["total_mv"] == 123.0 and rec["latest_share"] == 456.0
