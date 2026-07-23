"""a_share_limit_pool 归一化测试（不依赖网络/数据库）。"""
import datetime as dt

import pytest

from data_collect.jobs.a_share_limit_pool import (
    _fmt_zt_time,
    normalize_pool,
    normalize_ths,
)


def test_fmt_zt_time():
    assert _fmt_zt_time(92500) == dt.time(9, 25, 0)
    assert _fmt_zt_time(133005) == dt.time(13, 30, 5)
    assert _fmt_zt_time(None) is None
    assert _fmt_zt_time(0) is None


def test_normalize_zt_pool_divides_price_and_maps():
    raw = [{"c": "000815", "n": "美利云", "p": 16130, "zdp": 10.03,
            "amount": 1447901072, "ltsz": 1.12e10, "hs": 13.2, "lbc": 2,
            "fbt": 92500, "lbt": 145600, "fund": 5.6e8, "zbc": 1,
            "hybk": "造纸印刷", "zttj": {"days": 3, "ct": 2}}]
    df = normalize_pool("zt", raw, "20260722")
    row = df.iloc[0]
    assert row["pool_type"] == "zt"
    assert row["stock_code"] == "000815"
    assert row["price"] == pytest.approx(16.13)      # ÷1000
    assert row["trade_date"] == dt.date(2026, 7, 22)
    assert row["first_seal"] == dt.time(9, 25, 0)
    assert row["last_seal"] == dt.time(14, 56, 0)
    assert row["limit_days"] == 2
    assert row["seal_fund"] == pytest.approx(5.6e8)
    assert row["zt_stat"] == "3天2板"


def test_normalize_dt_pool_specific_fields():
    raw = [{"c": "300001", "n": "X", "p": 5000, "zdp": -10.0, "hs": 3.3,
            "pe": 22.5, "fund": 1.2e7, "lbt": 100000, "fba": 8.8e6,
            "days": 2, "oc": 3, "hybk": "光伏"}]
    df = normalize_pool("dt", raw, "20260722")
    row = df.iloc[0]
    assert row["pool_type"] == "dt"
    assert row["seal_fund"] == pytest.approx(1.2e7)
    assert row["board_amount"] == pytest.approx(8.8e6)
    assert row["dt_days"] == 2
    assert row["break_times"] == 3                    # dt: oc 开板次数
    assert row["last_seal"] == dt.time(10, 0, 0)


def test_normalize_yzt_pool_specific_fields():
    raw = [{"c": "600001", "n": "Y", "p": 8000, "zdp": 3.2, "hs": 9.9,
            "zf": 7.7, "zs": 1.1, "yfbt": 93100, "ylbc": 4,
            "hybk": "芯片", "zttj": {"days": 5, "ct": 4}}]
    df = normalize_pool("yzt", raw, "20260722")
    row = df.iloc[0]
    assert row["y_first_seal"] == dt.time(9, 31, 0)
    assert row["y_limit_days"] == 4
    assert row["amplitude"] == pytest.approx(7.7)


def test_normalize_pool_missing_key_raises():
    with pytest.raises(KeyError):
        normalize_pool("zt", [{"c": "000001", "n": "X"}], "20260722")  # 缺 p 等关键列


def test_normalize_ths_unix_to_time():
    # 1784775960 = 北京 2026-07-23 11:06:00
    raw = [{"code": "000815", "name": "美利云", "latest": 16.13, "change_rate": 10.03,
            "reason_type": "固废处理+稀土永磁", "limit_up_type": "换手板",
            "limit_up_suc_rate": 0.83, "open_num": 1, "order_amount": 5.6e8,
            "high_days": "3天2板", "first_limit_up_time": 1784775960, "is_again_limit": 0}]
    df = normalize_ths(raw, "20260722")
    row = df.iloc[0]
    assert row["stock_code"] == "000815"
    assert row["reason"] == "固废处理+稀土永磁"
    assert row["first_time"] == dt.time(11, 6, 0)
    assert row["seal_rate"] == pytest.approx(0.83)
    assert row["trade_date"] == dt.date(2026, 7, 22)


def test_normalize_empty():
    assert normalize_pool("zt", [], "20260722").empty
    assert normalize_ths([], "20260722").empty
