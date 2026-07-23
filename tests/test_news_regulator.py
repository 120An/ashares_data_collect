"""news_regulator 单测：全 mock。B 档（rsshub=发改委 + listpage=证监会/央行）。"""

import datetime
import hashlib
import time
from types import SimpleNamespace

import pytest

from data_collect.jobs import news_regulator as nr
from data_collect.utils.source_registry import Source

_FETCH_TIME = "2026-07-08 16:00:00"


def _rsshub_src(sid, route, channel):
    return Source(id=sid, adapter="rsshub", channel=channel,
                  job="news_regulator", route=route)


def _sha1(text):
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


# ---------- listpage：链接正则锚点 ----------

def test_csrc_link_regex():
    html = ('<a href="/csrc/c100028/c1615676/content.shtml">通知一</a>'
            '<a href="/csrc/c100028/c1615671/content.shtml">通知二</a>'
            '<a href="/csrc/c100028/c1615676/content.shtml">重复</a>'
            '<a href="/csrc/other.shtml">无关</a>')
    urls = nr._extract_article_urls("csrc", html)
    assert urls == ["http://www.csrc.gov.cn/csrc/c100028/c1615676/content.shtml",
                    "http://www.csrc.gov.cn/csrc/c100028/c1615671/content.shtml"]


def test_pbc_removed_from_listpages():
    """审查修复：pbc（央行）结构性 0 产出+每小时白锤 WAF，已从源表移除。"""
    assert "pbc" not in nr._LISTPAGES


def test_mof_link_regex():
    """财政部：`./202607/txxx.htm` 相对链接 → 列表页目录绝对化。"""
    html = ('<a href="./202607/t20260707_3993044.htm">新闻一</a>'
            '<a href="./202607/t20260706_3992894.htm">新闻二</a>'
            '<a href="/other/x.htm">无关</a>')
    urls = nr._extract_article_urls("mof", html)
    assert urls == [
        "https://www.mof.gov.cn/zhengwuxinxi/caizhengxinwen/202607/t20260707_3993044.htm",
        "https://www.mof.gov.cn/zhengwuxinxi/caizhengxinwen/202607/t20260706_3992894.htm"]


def test_people_link_regex():
    html = ('<a href="http://finance.people.com.cn/n1/2026/0626/c1004-40748358.html">甲</a>'
            '<a href="/n1/2026/0708/c1004-40756129.html">乙</a>'
            '<a href="/n2/xx.html">无关</a>')
    urls = nr._extract_article_urls("people", html)
    assert urls == [
        "http://finance.people.com.cn/n1/2026/0626/c1004-40748358.html",
        "http://finance.people.com.cn/n1/2026/0708/c1004-40756129.html"]


# ---------- listpage：文章信封 ----------

_ARTICLE_HTML = ("<html><head><title>某某办法公开征求意见_中国证券监督管理委员会"
                 "</title></head><body>发布时间：2026-07-07 16:30 正文…</body></html>")


def test_article_envelope_contract(monkeypatch):
    monkeypatch.setattr(nr.fulltext, "_extract",
                        lambda html, url: "正文Ａ" + "字" * 200)
    url = "http://www.csrc.gov.cn/csrc/c100028/c1615676/content.shtml"
    env = nr._article_envelope("csrc", "policy", url, _ARTICLE_HTML,
                               _FETCH_TIME, {})
    assert env["_id"] == _sha1(url)                      # 二级 sha1(url)
    assert env["title"] == "某某办法公开征求意见"          # 站名后缀剥除
    assert env["pub_time"] == "2026-07-07 16:30:00"      # HTML 嗅探 + 补秒
    assert env["content"].startswith("正文A")             # clean_text
    assert env["channel"] == "policy" and env["source"] == "csrc"
    assert env["vec_status"] == "pending"


def test_article_envelope_rejects_challenge_page(monkeypatch):
    """WAF 挑战/跳首页：title 即站名 → 拒收（不入库，同 URL 留待下轮重采）。

    实撞（2026-07-08）：csrc 连抓 20 篇触发限流，服务器改发首页——title 只剩
    '中国证券监督管理委员会'、正文是公告导航列表；若不拒收，垃圾文档占住
    _id=sha1(url)，create-only 下正确版本永远进不来。
    """
    monkeypatch.setattr(nr.fulltext, "_extract", lambda html, url: "字" * 200)
    env = nr._article_envelope(
        "csrc", "policy", "http://x/a.shtml",
        "<title>中国证券监督管理委员会</title>", _FETCH_TIME, {})
    assert env is None


def test_article_envelope_rejects_short_content(monkeypatch):
    monkeypatch.setattr(nr.fulltext, "_extract", lambda html, url: "太短")
    env = nr._article_envelope("csrc", "policy", "http://x/a.shtml",
                               _ARTICLE_HTML, _FETCH_TIME, {})
    assert env is None


def test_sniff_pub_time_prefers_labeled_date():
    """审查修复：优先取"发布时间"标签后的日期，不取靠前的导航/版权日期。"""
    html = ('<div class="nav">2020-01-01 版权所有</div>'
            '<div class="info">发布时间：2026-07-07 16:30</div>')
    assert nr._sniff_pub_time(html) == "2026-07-07 16:30:00"


def test_sniff_pub_time_fallback_first_date():
    """无标签命中 → 回退首个日期（best-effort）。"""
    assert nr._sniff_pub_time("<p>某文 2026-07-05 发布</p>") == "2026-07-05 00:00:00"


def test_article_envelope_time_fallback(monkeypatch):
    monkeypatch.setattr(nr.fulltext, "_extract", lambda html, url: "字" * 200)
    env = nr._article_envelope("csrc", "policy", "http://x/a.shtml",
                               "<title>标题_中国证券监督管理委员会</title>",
                               _FETCH_TIME, {})
    assert env["pub_time"] == _FETCH_TIME
    assert env["time_estimated"] is True


# ---------- run：抓前去重 / 源级隔离 ----------

@pytest.fixture
def env(monkeypatch):
    calls = SimpleNamespace(bulk=[], alerts=[], fetched_pages=[])
    e = SimpleNamespace(
        calls=calls,
        list_html={"csrc": '<a href="/csrc/c100028/c1/content.shtml">一</a>'
                           '<a href="/csrc/c100028/c2/content.shtml">二</a>',
                   "mof": '<a href="./202607/t20260708_1.htm">财政</a>',
                   "people": '<a href="/n1/2026/0708/c1004-1.html">人民</a>'},
        existing_ids=set(),
        rsshub_entries=[{"title": "发改委新闻", "link": "https://ndrc.gov.cn/1.htm",
                         "id": "ndrc-1", "summary": "成品油价格调整",
                         "published_parsed": time.struct_time(
                             (2026, 7, 8, 2, 0, 0, 0, 0, 0))}])

    def fake_http_get(url):
        calls.fetched_pages.append(url)
        for source, cfg in nr._LISTPAGES.items():
            if url == cfg["list_url"]:
                return e.list_html[source]
        return _ARTICLE_HTML          # 文章页

    # 注册表 shim 打桩：只留 ndrc（rsshub）+ 全部 listpage，语义等价原"路由感知"
    monkeypatch.setattr(nr, "_load_sources",
                        lambda: ({"ndrc": _rsshub_src("ndrc", "/gov/ndrc/xwdt",
                                                      "policy")},
                                 dict(nr._LISTPAGES)))
    monkeypatch.setattr(nr.fulltext, "_http_get", fake_http_get)
    monkeypatch.setattr(nr.fulltext, "_extract", lambda html, url: "字" * 200)
    monkeypatch.setattr(nr, "_fetch_rsshub_entries",
                        lambda url, **kw: e.rsshub_entries
                        if "/gov/ndrc" in url else [])
    monkeypatch.setattr(nr, "_existing_ids",
                        lambda client, ids: {i for i in ids if i in e.existing_ids})
    monkeypatch.setattr(nr, "_archive_source", lambda s, envs: len(envs))
    monkeypatch.setattr(nr, "_now",
                        lambda: datetime.datetime(2026, 7, 8, 16, 0, 0))
    monkeypatch.setattr(nr, "_flush_spool_safe", lambda: None)
    monkeypatch.setattr(nr, "get_news_config",
                        lambda: {"rsshub_base": "http://192.168.9.10:1200",
                                 "verify_days_back": 1})
    monkeypatch.setattr(nr.osu, "get_client", lambda: object())
    monkeypatch.setattr(nr.osu, "bulk_create",
                        lambda c, docs: (calls.bulk.append(list(docs)),
                                         (len(list(docs)), 0))[1])
    monkeypatch.setattr(nr, "_alert", lambda msg: calls.alerts.append(msg))
    monkeypatch.setattr(nr.nn, "load_name_dict", lambda: {})
    monkeypatch.setattr(nr.time, "sleep", lambda s: None)
    return e


def test_run_merges_sources(env):
    summary = nr.run()
    docs = env.calls.bulk[0]
    # ndrc 1 + csrc 2 + mof 1 + people 1（其余 rsshub 源路由感知返回空）
    assert len(docs) == 5
    assert {d["source"] for d in docs} == {"ndrc", "csrc", "mof", "people"}
    assert all("raw_title" not in d for d in docs)
    assert "监管:" in summary and "入库新增 5" in summary
    assert "失败源" not in summary          # listpage 全配好 → 无失败源


def test_run_skips_existing_articles(env):
    """抓前去重：已入库的文章 URL 不再抓正文页。"""
    env.existing_ids = {_sha1("http://www.csrc.gov.cn/csrc/c100028/c1/content.shtml")}
    nr.run()
    fetched_articles = [u for u in env.calls.fetched_pages if "c1/content" in u]
    assert fetched_articles == []                        # c1 被去重，未抓
    docs = env.calls.bulk[0]
    assert len([d for d in docs if d["source"] == "csrc"]) == 1   # 只剩 c2
    assert len(docs) == 4                                          # ndrc1+csrc1+mof1+people1


def test_run_isolates_failed_source(env, monkeypatch):
    monkeypatch.setattr(nr, "_fetch_rsshub_entries",
                        lambda url, **kw: (_ for _ in ()).throw(
                            RuntimeError("路由腐烂 503")))
    summary = nr.run()
    assert "ndrc 失败" in summary
    assert len(env.calls.alerts) == 1
    assert {d["source"] for d in env.calls.bulk[0]} == {"csrc", "mof", "people"}


def test_run_rsshub_base_missing(env, monkeypatch):
    """未配置 rsshub_base → rsshub 类源计失败并提示配置，listpage 源照常。"""
    monkeypatch.setattr(nr, "get_news_config", lambda: {"verify_days_back": 1})
    summary = nr.run()
    assert "ndrc 失败" in summary
    assert len(env.calls.bulk[0]) == 4                   # csrc 2 + mof 1 + people 1


def test_run_all_failed_raises(env, monkeypatch):
    monkeypatch.setattr(nr.fulltext, "_http_get", lambda url: None)
    monkeypatch.setattr(nr, "_fetch_rsshub_entries",
                        lambda url, **kw: (_ for _ in ()).throw(
                            RuntimeError("boom")))
    with pytest.raises(RuntimeError, match="全部采集失败"):
        nr.run()


def test_verify_replays_archive(env, monkeypatch):
    monkeypatch.setattr(nr, "_today", lambda: datetime.date(2026, 7, 8))
    monkeypatch.setattr(nr.news_archive, "replay",
                        lambda s, dr: iter([{"_id": "x", "title": "t",
                                             "raw_title": "t", "source": s}])
                        if s == "csrc" else iter([]))
    summary = nr.run_verify("20990101", "20990102")
    assert env.calls.bulk[0][0]["_id"] == "x"
    assert "raw_title" not in env.calls.bulk[0][0]
    assert "重放" in summary


def test_load_sources_rejects_unknown_listpage_id(monkeypatch):
    """注册表登记了 listpage 源但代码个性表缺配置 → 响亮 raise（脱节 fail-fast）。"""
    from data_collect.utils.source_registry import Source
    ghost = Source(id="ghost", adapter="listpage", channel="policy",
                   job="news_regulator")
    monkeypatch.setattr(nr.source_registry, "load_sources",
                        lambda job, **kw: [ghost])
    with pytest.raises(RuntimeError, match="脱节"):
        nr._load_sources()
