"""bodyfill 全文回填共享核心单测：全 mock。news_fulltext/news_announce_pdf 共用。"""

from types import SimpleNamespace

import pytest

from data_collect.utils import bodyfill


def _snap_resp(hits):
    return {"hits": {"hits": hits}}


def test_snapshot_pending_shape(monkeypatch):
    """快照查询按参数拼装：channel ∧ {status_field}=pending / size / fetch_time asc。"""
    captured = {}

    def fake_search(client, body, index=bodyfill.osu.ALIAS):
        captured["body"] = body
        return _snap_resp([
            {"_index": "news-2026", "_id": "a", "_source": {"url": "http://x/1"}}])

    monkeypatch.setattr(bodyfill.osu, "search_raw", fake_search)
    snap = bodyfill._snapshot_pending(object(), "announcement", "pdf_status", 200)
    assert snap == [("news-2026", "a", "http://x/1")]
    q = captured["body"]["query"]["bool"]["must"]
    assert {"term": {"channel": "announcement"}} in q
    assert {"term": {"pdf_status": "pending"}} in q
    assert captured["body"]["size"] == 200
    assert captured["body"]["_source"] == ["url"]
    assert captured["body"]["sort"] == [{"fetch_time": {"order": "asc"}}]


def test_snapshot_pending_query_error_returns_empty(monkeypatch):
    monkeypatch.setattr(bodyfill.osu, "search_raw",
                        lambda c, b, index=None: (_ for _ in ()).throw(RuntimeError("404")))
    assert bodyfill._snapshot_pending(object(), "stock", "body_status", 500) == []


@pytest.fixture
def env(monkeypatch):
    calls = SimpleNamespace(bulk=[])
    monkeypatch.setattr(bodyfill.osu, "bulk_update",
                        lambda c, ups: (calls.bulk.append(list(ups)), len(list(ups)))[1])
    return SimpleNamespace(calls=calls)


def _fill(snapshot, extract_fn, env_mp, monkeypatch, body_max=50_000):
    monkeypatch.setattr(bodyfill, "_snapshot_pending",
                        lambda client, ch, sf, bl: snapshot)
    return bodyfill.fill_bodies(
        object(), channel="announcement", status_field="pdf_status",
        extract_fn=extract_fn, batch_limit=200, max_workers=2,
        body_max_chars=body_max, label="公告PDF")


def test_fill_splits_done_and_failed(env, monkeypatch):
    snap = [("news-2026", "a", "http://good/1"), ("news-2026", "b", "http://bad/2")]
    msg = _fill(snap, lambda url: "正文" * 100 if "good" in url else None,
                env, monkeypatch)
    by_id = {u["_id"]: u["doc"] for u in env.calls.bulk[0]}
    assert by_id["a"] == {"body": "正文" * 100, "pdf_status": "done",
                          "vec_status": "pending"}
    assert by_id["b"] == {"pdf_status": "failed"}
    assert "公告PDF" in msg and "成功 1" in msg and "失败 1" in msg and "写回 2" in msg


def test_fill_truncates_and_marks(env, monkeypatch):
    """超长正文截断 body_max_chars 并加 body_truncated 标记。"""
    snap = [("news-2026", "a", "http://good/1")]
    _fill(snap, lambda url: "长" * 100, env, monkeypatch, body_max=30)
    doc = env.calls.bulk[0][0]["doc"]
    assert doc["body"] == "长" * 30
    assert doc["body_truncated"] is True
    assert doc["pdf_status"] == "done"


def test_fill_cleans_text(env, monkeypatch):
    """extract_fn 输出经 clean_text（NFKC 全角Ａ→A）。"""
    snap = [("news-2026", "a", "http://good/1")]
    _fill(snap, lambda url: "正文Ａ" + "字" * 200, env, monkeypatch)
    assert env.calls.bulk[0][0]["doc"]["body"].startswith("正文A")


def test_fill_isolates_extractor_exception(env, monkeypatch):
    """extract_fn 抛错 → 该篇 failed，不拖垮整轮。"""
    snap = [("news-2026", "a", "http://boom/1"), ("news-2026", "b", "http://good/2")]

    def flaky(url):
        if "boom" in url:
            raise RuntimeError("boom")
        return "正文" * 100

    _fill(snap, flaky, env, monkeypatch)
    by_id = {u["_id"]: u["doc"] for u in env.calls.bulk[0]}
    assert by_id["a"] == {"pdf_status": "failed"}
    assert by_id["b"]["pdf_status"] == "done"


def test_fill_empty_snapshot_early_return(env, monkeypatch):
    msg = _fill([], lambda url: "x", env, monkeypatch)
    assert "无待抓" in msg
    assert env.calls.bulk == []


def test_fill_isolates_clean_text_exception(env, monkeypatch):
    """审核修复锚点：clean_text 对毒文档抛错 → 该篇 failed，整轮存活——否则
    fut.result() 上抛丢弃全部进度，且毒文档永不置态、每轮重下重崩（队头卡死）。"""
    snap = [("news-2026", "a", "http://poison/1"), ("news-2026", "b", "http://good/2")]

    real_clean = bodyfill.nn.clean_text

    def poison_clean(text):
        if "毒" in text:
            raise RuntimeError("opencc backend boom")
        return real_clean(text)

    monkeypatch.setattr(bodyfill.nn, "clean_text", poison_clean)
    _fill(snap, lambda url: "毒文档" * 100 if "poison" in url else "正文" * 100,
          env, monkeypatch)
    all_updates = [u for batch in env.calls.bulk for u in batch]
    by_id = {u["_id"]: u["doc"] for u in all_updates}
    assert by_id["a"] == {"pdf_status": "failed"}
    assert by_id["b"]["pdf_status"] == "done"


def test_fill_flushes_in_chunks(env, monkeypatch):
    """审核修复锚点：每 _FLUSH_EVERY 篇分段 bulk_update——超时硬杀最多丢一段，
    已写回的 done/failed 使下轮快照缩小（终结'整批重下重死'活锁）。"""
    monkeypatch.setattr(bodyfill, "_FLUSH_EVERY", 2)
    snap = [("news-2026", f"id{i}", f"http://good/{i}") for i in range(5)]
    msg = _fill(snap, lambda url: "正文" * 100, env, monkeypatch)
    assert len(env.calls.bulk) == 3                       # 2+2+1 三段
    assert [len(b) for b in env.calls.bulk] == [2, 2, 1]
    assert "写回 5" in msg                                # 计数跨段累计
