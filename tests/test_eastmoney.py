"""utils/eastmoney.py 测试（mock 网络，零真实请求）。"""
import types

import data_collect.utils.eastmoney as em


class FakeResp:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200

    def json(self):
        return self._payload


def test_em_get_throttles(monkeypatch):
    """两次连续调用之间必须 sleep（间隔不足时）。"""
    calls = {"sleep": [], "get": 0}
    monkeypatch.setattr(em.time, "sleep", lambda s: calls["sleep"].append(s))
    monkeypatch.setattr(em.random, "uniform", lambda a, b: 0.0)
    monkeypatch.setattr(em._SESSION, "get",
                        lambda *a, **k: calls.__setitem__("get", calls["get"] + 1) or FakeResp({}))
    em._last_call[0] = 0.0
    em.em_get("https://x.eastmoney.com/a")      # 距上次很久，不 sleep
    em.em_get("https://x.eastmoney.com/b")      # 紧接上次，必 sleep
    assert calls["get"] == 2
    assert len(calls["sleep"]) == 1
    assert calls["sleep"][0] > 0


def test_datacenter_query_paginates(monkeypatch):
    """count>pageSize 时自动翻页并拼接全部行。"""
    pages = {
        1: {"result": {"data": [{"i": 1}, {"i": 2}], "count": 3}},
        2: {"result": {"data": [{"i": 3}], "count": 3}},
    }
    seen = []

    def fake_get(url, params=None, headers=None, timeout=15, **kw):
        pn = int(params["pageNumber"])
        seen.append(pn)
        return FakeResp(pages[pn])

    monkeypatch.setattr(em, "em_get", fake_get)
    rows = em.datacenter_query("RPT_X", filter_str="(A='1')", page_size=2)
    assert [r["i"] for r in rows] == [1, 2, 3]
    assert seen == [1, 2]


def test_datacenter_query_empty(monkeypatch):
    monkeypatch.setattr(em, "em_get",
                        lambda *a, **k: FakeResp({"result": None}))
    assert em.datacenter_query("RPT_X") == []


def test_clist_query_paginates(monkeypatch):
    pages = {
        1: {"data": {"total": 3, "diff": [{"f12": "000001"}, {"f12": "000002"}]}},
        2: {"data": {"total": 3, "diff": [{"f12": "000003"}]}},
    }

    def fake_get(url, params=None, headers=None, timeout=15, **kw):
        return FakeResp(pages[int(params["pn"])])

    monkeypatch.setattr(em, "em_get", fake_get)
    rows = em.clist_query(fs="m:0", fields="f12", fid="f62", page_size=2)
    assert [r["f12"] for r in rows] == ["000001", "000002", "000003"]


def test_clist_query_null_data(monkeypatch):
    monkeypatch.setattr(em, "em_get", lambda *a, **k: FakeResp({"data": None}))
    assert em.clist_query(fs="m:0", fields="f12", fid="f62") == []
