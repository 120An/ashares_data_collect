"""news_flash cls 直连 v1 API 复活 + 溢出告警节流测试（mock 网络）。"""
import datetime as dt

from data_collect.jobs.news_flash import (
    _cls_sign,
    _fetch_cls,
    _mark_overflow_alerted,
    _should_alert_overflow,
    _REQUIRED_COLUMNS,
)


def test_overflow_alert_exempt_sina(tmp_path, monkeypatch):
    import data_collect.jobs.news_flash as nf
    monkeypatch.setattr(nf, "_state_file", lambda: tmp_path / "state.json")
    assert _should_alert_overflow("sina") is False      # 豁免源永不告警


def test_overflow_alert_daily_cooldown(tmp_path, monkeypatch):
    import data_collect.jobs.news_flash as nf
    monkeypatch.setattr(nf, "_state_file", lambda: tmp_path / "state.json")
    monkeypatch.setattr(nf, "_today", lambda: "20260723")
    assert _should_alert_overflow("em") is True         # 今日首次 → 告警
    _mark_overflow_alerted("em")
    assert _should_alert_overflow("em") is False        # 今日已告 → 静默
    monkeypatch.setattr(nf, "_today", lambda: "20260724")
    assert _should_alert_overflow("em") is True         # 次日复位


def test_cls_sign_deterministic_and_order_independent():
    p1 = {"b": "2", "a": "1", "rn": "20"}
    p2 = {"rn": "20", "a": "1", "b": "2"}
    s1, s2 = _cls_sign(p1), _cls_sign(p2)
    assert s1 == s2                      # 字典序拼接，与传入顺序无关
    assert len(s1) == 32
    assert all(c in "0123456789abcdef" for c in s1)


def test_fetch_cls_returns_contract_columns(monkeypatch):
    # 1784775960 = 北京时间 2026-07-23 11:06:00（+8 时区显式换算）
    payload = {"errno": 0, "data": {"roll_data": [
        {"title": "标题A", "content": "内容A", "ctime": 1784775960},
        {"title": "", "brief": "brief兜底", "content": "", "ctime": 1784775900},
    ]}}

    class FakeResp:
        status_code = 200
        def json(self):
            return payload
        def raise_for_status(self):
            pass

    import data_collect.jobs.news_flash as nf
    monkeypatch.setattr(nf.requests, "get", lambda *a, **k: FakeResp())

    df = _fetch_cls()
    # 列契约与 _REQUIRED_COLUMNS["cls"] 一致，下游零改动
    for col in _REQUIRED_COLUMNS["cls"]:
        assert col in df.columns
    assert len(df) == 2
    row = df.iloc[0]
    assert row["标题"] == "标题A"
    assert isinstance(row["发布日期"], dt.date)
    assert isinstance(row["发布时间"], dt.time)
    assert row["发布时间"] == dt.time(11, 6, 0)      # 北京时间（+8 显式换算）
    assert row["发布日期"] == dt.date(2026, 7, 23)
    # title 空回退 brief
    assert df.iloc[1]["标题"] == "brief兜底"


def test_fetch_cls_timeout_passed(monkeypatch):
    """必须显式 timeout（akshare 无超时挂死病根的回归防线）。"""
    seen = {}

    class FakeResp:
        status_code = 200
        def json(self):
            return {"errno": 0, "data": {"roll_data": []}}
        def raise_for_status(self):
            pass

    import data_collect.jobs.news_flash as nf

    def fake_get(url, **kw):
        seen["timeout"] = kw.get("timeout")
        return FakeResp()

    monkeypatch.setattr(nf.requests, "get", fake_get)
    df = _fetch_cls()
    assert seen["timeout"] is not None and seen["timeout"] <= 30
    assert df.empty
