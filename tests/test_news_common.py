"""news_common 纯函数单测（strip_raw / verify_dates——审核后收敛的共享契约）。"""

import datetime

from data_collect.utils import news_common as nc


# ---------- strip_raw（信封→索引 契约：三 job 字节一致的剥离逻辑收敛于此） ----------

def test_strip_raw_removes_archive_only_keys():
    doc = nc.strip_raw({"_id": "x", "title": "t", "raw_title": "T",
                        "raw_content": "{}", "time_estimated": True})
    assert doc == {"_id": "x", "title": "t"}


def test_strip_raw_noop_without_raw_keys():
    doc = {"_id": "x", "title": "t"}
    assert nc.strip_raw(doc) == doc


# ---------- normalize_date8（CLI --date 归一化，各 job run 共享） ----------

def test_normalize_date8_accepts_both_forms():
    assert nc.normalize_date8("20260708") == "20260708"
    assert nc.normalize_date8("2026-07-08") == "20260708"
    assert nc.normalize_date8("  2026-07-08  ") == "20260708"


def test_normalize_date8_rejects_malformed():
    import pytest
    for bad in ("2026-7-8", "abcd", "2026070", "202607089", ""):
        with pytest.raises(ValueError):
            nc.normalize_date8(bad)


# ---------- verify_dates（四 job 的自然日窗口收敛于此） ----------

_TODAY = datetime.date(2026, 7, 6)


def test_verify_dates_include_today():
    """归档重放类（flash/stock）：[today-N, today] 含今天，升序。"""
    assert nc.verify_dates(2, _TODAY, include_today=True) == \
        ["20260704", "20260705", "20260706"]


def test_verify_dates_exclude_today():
    """源重采类（cctv/announcement）：[today-N, today-1] 不含今天。"""
    assert nc.verify_dates(3, _TODAY, include_today=False) == \
        ["20260703", "20260704", "20260705"]


def test_verify_dates_clamps_negative():
    """days_back<0 统一按 0 处理（四 job 原先 clamp 行为不一致，收敛后统一）。"""
    assert nc.verify_dates(-2, _TODAY, include_today=True) == ["20260706"]
    assert nc.verify_dates(-2, _TODAY, include_today=False) == []


def test_verify_dates_zero():
    assert nc.verify_dates(0, _TODAY, include_today=True) == ["20260706"]
    assert nc.verify_dates(0, _TODAY, include_today=False) == []
