"""source_adapters 单测：feed 统一抓取的防御契约 + 外层硬超时推导。"""

from types import SimpleNamespace

import pytest

from data_collect.utils import source_adapters as sa


class _Resp:
    def __init__(self, status=200, content=b"x"):
        self.status_code = status
        self.content = content


def _patch_requests(monkeypatch, resp, captured=None):
    import sys

    def get(url, **kw):
        if captured is not None:
            captured.update(kw, url=url)
        return resp

    monkeypatch.setitem(sys.modules, "requests", SimpleNamespace(get=get))


def _patch_feedparser(monkeypatch, entries, bozo=0):
    import sys
    feed = SimpleNamespace(entries=entries, bozo=bozo,
                           bozo_exception=ValueError("not xml"))
    monkeypatch.setitem(sys.modules, "feedparser",
                        SimpleNamespace(parse=lambda content: feed))


def test_fetch_feed_passes_headers_proxy_timeout(monkeypatch):
    """headers/proxy/timeout 全部透传（Source.timeout 接线的落点）。"""
    captured = {}
    _patch_requests(monkeypatch, _Resp(), captured)
    _patch_feedparser(monkeypatch, [{"title": "t"}])
    entries = sa.fetch_feed("https://x/f.xml", headers={"User-Agent": "ua"},
                            proxy="http://127.0.0.1:7890", timeout=42)
    assert entries == [{"title": "t"}]
    assert captured["headers"] == {"User-Agent": "ua"}
    assert captured["proxies"] == {"http": "http://127.0.0.1:7890",
                                   "https": "http://127.0.0.1:7890"}
    assert captured["timeout"] == 42


def test_fetch_feed_no_proxy_direct(monkeypatch):
    """空 proxy → 直连（不传 proxies，沿用环境行为——同原 news_us 语义）。"""
    captured = {}
    _patch_requests(monkeypatch, _Resp(), captured)
    _patch_feedparser(monkeypatch, [{"title": "t"}])
    sa.fetch_feed("https://x/f.xml")
    assert "proxies" not in captured


def test_fetch_feed_non_200_raises(monkeypatch):
    _patch_requests(monkeypatch, _Resp(status=503))
    with pytest.raises(RuntimeError, match="HTTP 503"):
        sa.fetch_feed("https://x/f.xml", label="RSS x")


def test_fetch_feed_bozo_and_empty_raises(monkeypatch):
    """bozo 且空 → 抛错（半残 feed 静默空信封比失败更糟，三 job 共同防御）。"""
    _patch_requests(monkeypatch, _Resp())
    _patch_feedparser(monkeypatch, [], bozo=1)
    with pytest.raises(RuntimeError, match="bozo"):
        sa.fetch_feed("https://x/f.xml")


def test_fetch_feed_bozo_with_entries_ok(monkeypatch):
    """bozo 但有 entries 视为可用（RSS 普遍有小毛病）。"""
    _patch_requests(monkeypatch, _Resp())
    _patch_feedparser(monkeypatch, [{"title": "t"}], bozo=1)
    assert len(sa.fetch_feed("https://x/f.xml")) == 1


def test_hard_timeout_floor_and_headroom():
    assert sa.hard_timeout(30, 60) == 60     # 常规：job 地板兜底
    assert sa.hard_timeout(90, 60) == 105    # yaml 配大超时 → 外层跟随，防死配置


# ---------- 冒烟分派（manage_sources `test <id>` 统一入口） ----------

from data_collect.utils.source_registry import Source  # noqa: E402


def test_smoke_dispatch_rss(monkeypatch):
    monkeypatch.setattr(sa, "fetch_feed",
                        lambda url, **kw: [{"title": "首条标题"}])
    s = Source(id="x", adapter="rss", channel="policy", job="news_policy",
               url="https://x/rss")
    out = sa.smoke_test(s)
    assert "1 条" in out and "首条标题" in out


def test_smoke_dispatch_rsshub(monkeypatch):
    import data_collect.config as cfg
    captured = {}
    monkeypatch.setattr(cfg, "get_news_config",
                        lambda: {"rsshub_base": "http://rh:1200/"})
    monkeypatch.setattr(sa, "fetch_feed",
                        lambda url, **kw: (captured.update(url=url),
                                           [{"title": "t"}])[1])
    s = Source(id="x", adapter="rsshub", channel="media",
               job="news_regulator", route="/r")
    sa.smoke_test(s)
    assert captured["url"] == "http://rh:1200/r"        # base 剥尾斜杠再拼


def test_smoke_rsshub_requires_base(monkeypatch):
    import data_collect.config as cfg
    monkeypatch.setattr(cfg, "get_news_config", lambda: {})
    s = Source(id="x", adapter="rsshub", channel="media",
               job="news_regulator", route="/r")
    with pytest.raises(RuntimeError, match="rsshub_base"):
        sa.smoke_test(s)


def test_smoke_listpage_missing_config_raises():
    s = Source(id="ghost", adapter="listpage", channel="policy",
               job="news_regulator")
    with pytest.raises(RuntimeError, match="脱节"):
        sa.smoke_test(s)


def test_smoke_unknown_adapter_raises():
    s = Source(id="x", adapter="ftp", channel="policy", job="news_policy")
    with pytest.raises(NotImplementedError):
        sa.smoke_test(s)
