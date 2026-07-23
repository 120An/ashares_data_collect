"""news_fulltext 单测：全 mock。核心逻辑（快照/并发/截断）已迁 test_bodyfill，
此处锁 job 层：trafilatura 预检 fail-fast、bodyfill 参数拼装、done/failed 分流回归。"""

from types import SimpleNamespace

import pytest

from data_collect.jobs import news_fulltext as nf


@pytest.fixture
def env(monkeypatch):
    calls = SimpleNamespace(bulk=[])
    monkeypatch.setattr(nf.osu, "get_client", lambda: object())
    monkeypatch.setattr(nf.bodyfill.osu, "bulk_update",
                        lambda c, ups: (calls.bulk.append(list(ups)), len(list(ups)))[1])
    monkeypatch.setattr(nf, "_trafilatura_available", lambda: True)   # 单测环境视作已装
    return SimpleNamespace(calls=calls)


def test_run_splits_done_and_failed(env, monkeypatch):
    """回归锁：run 经 bodyfill 完成 stock/body_status 分流（行为与瘦身前一致）。"""
    snap = [("news-2026", "a", "http://good/1"),
            ("news-2026", "b", "http://bad/2"),
            ("news-2026", "c", "http://good/3")]
    monkeypatch.setattr(nf.bodyfill, "_snapshot_pending",
                        lambda client, ch, sf, bl: snap)
    monkeypatch.setattr(nf.fulltext, "fetch_article",
                        lambda url: "正文" * 100 if "good" in url else None)

    msg = nf.run()

    by_id = {u["_id"]: u["doc"] for u in env.calls.bulk[0]}
    assert by_id["a"] == {"body": "正文" * 100, "body_status": "done",
                          "vec_status": "pending"}
    assert by_id["b"] == {"body_status": "failed"}
    assert "成功 2" in msg and "失败 1" in msg and "写回 3" in msg


def test_run_passes_stock_params(env, monkeypatch):
    """参数拼装锚点：channel=stock、status_field=body_status、批/并发/截断常量。"""
    captured = {}

    def fake_fill(client, **kwargs):
        captured.update(kwargs)
        return "全文抽取: 无待抓 pending"

    monkeypatch.setattr(nf.bodyfill, "fill_bodies", fake_fill)
    nf.run()
    assert captured["channel"] == "stock"
    assert captured["status_field"] == "body_status"
    assert captured["batch_limit"] == nf._BATCH_LIMIT
    assert captured["max_workers"] == nf._MAX_WORKERS
    assert captured["body_max_chars"] == nf._BODY_MAX_CHARS
    assert captured["extract_fn"] is nf.fulltext.fetch_article


def test_run_empty_snapshot_early_return(env, monkeypatch):
    monkeypatch.setattr(nf.bodyfill, "_snapshot_pending", lambda client, ch, sf, bl: [])
    msg = nf.run()
    assert "无待抓" in msg
    assert env.calls.bulk == []


def test_run_fails_fast_without_trafilatura(monkeypatch):
    """部署险情锚点：trafilatura 未装 → raise 终止，绝不进抽取/标 failed。"""
    monkeypatch.setattr(nf, "_trafilatura_available", lambda: False)
    monkeypatch.setattr(nf.osu, "get_client", lambda: object())
    bulk_calls = []
    monkeypatch.setattr(nf.bodyfill.osu, "bulk_update",
                        lambda c, ups: bulk_calls.append(list(ups)))
    with pytest.raises(RuntimeError, match="trafilatura"):
        nf.run()
    assert bulk_calls == []
