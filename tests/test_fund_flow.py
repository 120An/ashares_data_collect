"""a_share_fund_flow 测试（不依赖网络/数据库）。"""
import datetime as dt

import pytest

from data_collect.jobs.a_share_fund_flow import (
    _safe_max_date,
    normalize_clist,
    parse_daykline,
    run_backfill,
)


def test_safe_max_date_before_close_is_yesterday():
    now = dt.datetime(2026, 7, 23, 11, 30)
    assert _safe_max_date(now) == dt.date(2026, 7, 22)


def test_safe_max_date_after_close_is_today():
    now = dt.datetime(2026, 7, 23, 15, 6)
    assert _safe_max_date(now) == dt.date(2026, 7, 23)


def test_normalize_clist_maps_fields():
    diff = [{"f12": "601138", "f14": "工业富联", "f2": 21.3, "f3": 5.5,
             "f62": 870580176.0, "f66": 5.1e8, "f72": 3.6e8,
             "f78": -1.2e8, "f84": -7.5e8, "f184": 12.3}]
    df = normalize_clist(diff, "20260722")
    row = df.iloc[0]
    assert row["trade_date"] == dt.date(2026, 7, 22)
    assert row["stock_code"] == "601138"
    assert row["main_net"] == pytest.approx(870580176.0)
    assert row["super_net"] == pytest.approx(5.1e8)
    assert row["small_net"] == pytest.approx(-7.5e8)
    assert row["main_net_pct"] == pytest.approx(12.3)


def test_normalize_clist_dash_becomes_none():
    diff = [{"f12": "600000", "f14": "浦发银行", "f2": "-", "f3": "-",
             "f62": "-", "f66": "-", "f72": "-", "f78": "-", "f84": "-", "f184": "-"}]
    df = normalize_clist(diff, "20260722")
    assert df.iloc[0]["main_net"] is None or df.iloc[0]["main_net"] != df.iloc[0]["main_net"]


def test_parse_daykline_line():
    # daykline 行：date,main,small,mid,large,super,...(百分比列忽略)
    rows = parse_daykline("600519", [
        "2026-07-21,870580176.0,-1.2e8,-3.4e7,2.2e8,6.5e8,1.1,2.2,3.3,4.4,5.5",
        "2026-07-22,-5.0e7,1.0e7,4.0e7,-2.0e7,-3.0e7,0.1,0.2,0.3,0.4,0.5",
    ])
    assert len(rows) == 2
    r0 = rows[0]
    assert r0["trade_date"] == dt.date(2026, 7, 21)
    assert r0["stock_code"] == "600519"
    assert r0["main_net"] == pytest.approx(870580176.0)
    assert r0["small_net"] == pytest.approx(-1.2e8)
    assert r0["super_net"] == pytest.approx(6.5e8)


def test_run_backfill_beyond_window_raises():
    with pytest.raises(ValueError):
        run_backfill("20200101", "20200110")   # 远超 120 交易日窗
