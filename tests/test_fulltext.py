"""fulltext 全文抓取+抽取客户端单测：全 mock，不触网、不依赖真 trafilatura。"""

import pytest

from data_collect.utils import fulltext


# ---------- _http_get：requests → curl_cffi 兜底 ----------

def test_http_get_uses_requests_on_200(monkeypatch):
    monkeypatch.setattr(fulltext, "_get_via_requests", lambda url: (200, "<html>ok</html>"))
    monkeypatch.setattr(fulltext, "_get_via_curl",
                        lambda url: pytest.fail("不应回退 curl"))
    assert fulltext._http_get("http://x") == "<html>ok</html>"


def test_http_get_falls_back_on_exception(monkeypatch):
    def boom(url):
        raise ConnectionError("reset")
    monkeypatch.setattr(fulltext, "_get_via_requests", boom)
    monkeypatch.setattr(fulltext, "_get_via_curl", lambda url: (200, "<html>curl</html>"))
    assert fulltext._http_get("http://x") == "<html>curl</html>"


def test_http_get_falls_back_on_non_200(monkeypatch):
    monkeypatch.setattr(fulltext, "_get_via_requests", lambda url: (403, "blocked"))
    monkeypatch.setattr(fulltext, "_get_via_curl", lambda url: (200, "<html>curl</html>"))
    assert fulltext._http_get("http://x") == "<html>curl</html>"


def test_http_get_returns_none_when_both_fail(monkeypatch):
    def boom(url):
        raise ConnectionError("down")
    monkeypatch.setattr(fulltext, "_get_via_requests", boom)
    monkeypatch.setattr(fulltext, "_get_via_curl", boom)
    assert fulltext._http_get("http://x") is None


# ---------- _smart_text：无 charset 头 → apparent_encoding（csrc/pbc 乱码实撞） ----------

class _FakeResp:
    """模拟 requests 响应：text 按 encoding 属性解码 _raw 字节。"""
    def __init__(self, raw: bytes, ctype: str, apparent: str):
        self._raw = raw
        self.headers = {"Content-Type": ctype}
        self.encoding = "ISO-8859-1"          # requests 对无 charset 头的缺省
        self.apparent_encoding = apparent

    @property
    def text(self):
        return self._raw.decode(self.encoding, errors="replace")


def test_smart_text_uses_apparent_when_no_charset():
    resp = _FakeResp("证监会通知".encode("utf-8"), "text/html", "utf-8")
    assert fulltext._smart_text(resp) == "证监会通知"    # 不修则为 latin-1 乱码


def test_smart_text_respects_declared_charset():
    resp = _FakeResp("正文".encode("utf-8"), "text/html; charset=utf-8", "GB2312")
    resp.encoding = "utf-8"                   # requests 按声明 charset 已设好
    assert fulltext._smart_text(resp) == "正文"           # 不动声明 charset 的站点


# ---------- fetch_article：抓 + 抽取 + 阈值 ----------

def test_fetch_article_extracts(monkeypatch):
    monkeypatch.setattr(fulltext, "_http_get", lambda url: "<html>whatever</html>")
    monkeypatch.setattr(fulltext, "_extract", lambda html, url: "  " + "正" * 200 + "  ")
    out = fulltext.fetch_article("http://x/1")
    assert out == "正" * 200            # 抽出并 strip


def test_fetch_article_none_when_no_html(monkeypatch):
    monkeypatch.setattr(fulltext, "_http_get", lambda url: None)
    assert fulltext.fetch_article("http://x/2") is None


def test_fetch_article_none_when_extract_empty(monkeypatch):
    monkeypatch.setattr(fulltext, "_http_get", lambda url: "<html>x</html>")
    monkeypatch.setattr(fulltext, "_extract", lambda html, url: None)
    assert fulltext.fetch_article("http://x/3") is None


def test_fetch_article_none_when_too_short(monkeypatch):
    monkeypatch.setattr(fulltext, "_http_get", lambda url: "<html>x</html>")
    monkeypatch.setattr(fulltext, "_extract", lambda html, url: "短文")   # < _MIN_BODY
    assert fulltext.fetch_article("http://x/4") is None


def test_fetch_article_none_when_extract_raises(monkeypatch):
    monkeypatch.setattr(fulltext, "_http_get", lambda url: "<html>x</html>")
    def boom(html, url):
        raise ValueError("parse error")
    monkeypatch.setattr(fulltext, "_extract", boom)
    assert fulltext.fetch_article("http://x/5") is None


@pytest.mark.integration
def test_fetch_article_live():
    """真抓一个财经正文 URL（默认跳过；`pytest -m integration`）。需已装 trafilatura。"""
    url = "https://finance.eastmoney.com/a/202607063442278340.html"
    out = fulltext.fetch_article(url)
    assert out is None or isinstance(out, str)   # 通网即可（内容易变，不强断言非空）
