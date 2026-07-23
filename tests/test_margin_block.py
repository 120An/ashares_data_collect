"""a_share_margin / a_share_block_trade 归一化测试（不依赖网络/数据库）。"""
import datetime as dt

import pytest

from data_collect.jobs.a_share_margin import normalize_margin
from data_collect.jobs.a_share_block_trade import normalize_block


def test_normalize_margin_maps_keys():
    raw = [{"DATE": "2026-07-21 00:00:00", "MARKET": "沪市", "SCODE": "600519",
            "SECNAME": "贵州茅台", "RZYE": 1.2e10, "RZMRE": 3.4e8, "RZCHE": 2.9e8,
            "RZYEZB": 1.23, "RQYE": 5.6e8, "RQYL": 320000, "RQMCL": 12000,
            "RQCHL": 9000, "RZRQYE": 1.26e10, "RZRQYECZ": 5.0e7}]
    df = normalize_margin(raw)
    row = df.iloc[0]
    assert row["trade_date"] == dt.date(2026, 7, 21)
    assert row["stock_code"] == "600519"
    assert row["name"] == "贵州茅台"
    assert row["rzye"] == pytest.approx(1.2e10)
    assert row["rqyl"] == pytest.approx(320000)
    assert row["rzrqye"] == pytest.approx(1.26e10)


def test_normalize_margin_missing_key_raises():
    with pytest.raises(KeyError):
        normalize_margin([{"DATE": "2026-07-21 00:00:00", "SECNAME": "X"}])  # 缺 SCODE


def test_normalize_block_maps_keys():
    raw = [{"TRADE_DATE": "2026-07-21 00:00:00", "SECURITY_CODE": "600519",
            "SECURITY_NAME_ABBR": "贵州茅台", "DEAL_PRICE": 1500.0,
            "CLOSE_PRICE": 1550.0, "PREMIUM_RATIO": -3.2258,
            "DEAL_VOLUME": 100000, "DEAL_AMT": 1.5e8,
            "BUYER_NAME": "机构专用", "SELLER_NAME": "某某营业部"}]
    df = normalize_block(raw)
    row = df.iloc[0]
    assert row["trade_date"] == dt.date(2026, 7, 21)
    assert row["stock_code"] == "600519"
    assert row["deal_price"] == pytest.approx(1500.0)
    assert row["premium_pct"] == pytest.approx(-3.2258)
    assert row["buyer_name"] == "机构专用"
    assert row["deal_amount"] == pytest.approx(1.5e8)


def test_normalize_empty():
    assert normalize_margin([]).empty
    assert normalize_block([]).empty
