"""cninfo 巨潮客户端单测：全 mock _http_post / _post_via_*，不触网。

覆盖：
- _http_post：200 用 requests 不回退；requests 抛异常 / 非 200 → 回退 curl_cffi；
- fetch_announcements：单页、多页 totalpages 分页合并、页间限速、空日 []、
  page1 失败抛 CninfoError、中途页失败尽力收集不中断；
- 集成 @integration：真连巨潮取当日结构（默认跳过）。
"""

import json

import pytest

from data_collect.utils import cninfo


def _resp(total_pages, records):
    """构造巨潮响应 JSON 文本。"""
    return json.dumps({
        "totalRecordNum": total_pages * 30, "totalpages": total_pages,
        "hasMore": True, "announcements": records,
    })


# ---------- _http_post：requests → curl_cffi 兜底 ----------

def test_http_post_uses_requests_on_200(monkeypatch):
    monkeypatch.setattr(cninfo, "_post_via_requests", lambda data: (200, "OK"))
    monkeypatch.setattr(cninfo, "_post_via_curl",
                        lambda data: pytest.fail("不应回退 curl"))
    assert cninfo._http_post({"pageNum": 1}) == (200, "OK")


def test_http_post_falls_back_on_exception(monkeypatch):
    def boom(data):
        raise ConnectionError("reset")
    monkeypatch.setattr(cninfo, "_post_via_requests", boom)
    monkeypatch.setattr(cninfo, "_post_via_curl", lambda data: (200, "CURL"))
    assert cninfo._http_post({"pageNum": 1}) == (200, "CURL")


def test_http_post_falls_back_on_non_200(monkeypatch):
    monkeypatch.setattr(cninfo, "_post_via_requests", lambda data: (403, "blocked"))
    monkeypatch.setattr(cninfo, "_post_via_curl", lambda data: (200, "CURL"))
    assert cninfo._http_post({"pageNum": 1}) == (200, "CURL")


# ---------- fetch_announcements：单查询全市场分页 ----------

@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(cninfo.time, "sleep", lambda s: None)


def test_fetch_single_page(monkeypatch):
    recs = [{"announcementId": "1"}, {"announcementId": "2"}]
    monkeypatch.setattr(cninfo, "_http_post", lambda data: (200, _resp(1, recs)))
    out = cninfo.fetch_announcements("2026-07-06")
    assert [r["announcementId"] for r in out] == ["1", "2"]


def test_fetch_paginates_by_totalpages(monkeypatch):
    pages = {
        1: _resp(3, [{"announcementId": "a"}]),
        2: _resp(3, [{"announcementId": "b"}]),
        3: _resp(3, [{"announcementId": "c"}]),
    }
    seen_pages = []

    def fake_post(data):
        seen_pages.append(int(data["pageNum"]))
        return (200, pages[int(data["pageNum"])])

    monkeypatch.setattr(cninfo, "_http_post", fake_post)
    out = cninfo.fetch_announcements("2026-07-06")
    assert [r["announcementId"] for r in out] == ["a", "b", "c"]
    assert seen_pages == [1, 2, 3]                 # 按 totalpages 翻全 3 页


def test_fetch_sends_whole_market_params(monkeypatch):
    captured = {}

    def fake_post(data):
        captured.update(data)
        return (200, _resp(1, []))

    monkeypatch.setattr(cninfo, "_http_post", fake_post)
    cninfo.fetch_announcements("2026-07-06")
    assert captured["column"] == "szse" and captured["plate"] == ""   # 全市场
    assert captured["seDate"] == "2026-07-06~2026-07-06"              # 单日窗口
    assert captured["tabName"] == "fulltext"


def test_fetch_empty_day_returns_empty(monkeypatch):
    monkeypatch.setattr(cninfo, "_http_post", lambda data: (200, _resp(0, [])))
    assert cninfo.fetch_announcements("2026-07-06") == []


def test_fetch_page1_error_raises(monkeypatch):
    monkeypatch.setattr(cninfo, "_http_post", lambda data: (200, "<html>blocked"))
    monkeypatch.setattr(cninfo, "_post_via_curl",
                        lambda data: (200, "<html>still blocked"))   # curl 重试也 HTML
    with pytest.raises(cninfo.CninfoError):
        cninfo.fetch_announcements("2026-07-06")


def test_fetch_tolerates_midsweep_page_error(monkeypatch):
    def fake_post(data):
        page = int(data["pageNum"])
        if page == 2:
            return (200, "<html>oops")        # 中途页坏 → 尽力收集，不中断
        return (200, _resp(3, [{"announcementId": f"p{page}"}]))

    monkeypatch.setattr(cninfo, "_http_post", fake_post)
    monkeypatch.setattr(cninfo, "_post_via_curl", lambda data: (200, "<html>oops"))
    out = cninfo.fetch_announcements("2026-07-06")
    assert [r["announcementId"] for r in out] == ["p1", "p3"]   # 第2页跳过


def test_fetch_soft_block_recovers_via_curl(monkeypatch):
    """回归锚点（审核#7）：WAF 软封锁 = 200+HTML 挑战页——_http_post 视 200 为成功
    不回退；_fetch_page 须在 JSON 解析失败时显式 curl_cffi 重试该页。"""
    monkeypatch.setattr(cninfo, "_post_via_requests",
                        lambda data: (200, "<html>challenge page</html>"))
    recs = [{"announcementId": "1"}]
    monkeypatch.setattr(cninfo, "_post_via_curl", lambda data: (200, _resp(1, recs)))
    out = cninfo.fetch_announcements("2026-07-06")
    assert [r["announcementId"] for r in out] == ["1"]          # curl 指纹过 WAF 拿到数据


@pytest.mark.integration
def test_fetch_announcements_live():
    """真连巨潮取当日全市场公告（默认跳过；`pytest -m integration` 触发）。"""
    import datetime
    today = datetime.date.today().isoformat()
    out = cninfo.fetch_announcements(today)
    # 交易日应有数据；非交易日放宽为"不报错即可"
    assert isinstance(out, list)
    if out:
        r = out[0]
        assert "announcementId" in r and "announcementTitle" in r
        assert "adjunctUrl" in r and "secCode" in r
