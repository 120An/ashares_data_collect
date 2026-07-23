"""news_announce_pdf 单测：全 mock。核心在 bodyfill，此处锁 job 层：
pymupdf 预检 fail-fast + announcement/pdf_status 参数拼装。"""

import pytest

from data_collect.jobs import news_announce_pdf as nap


def test_run_passes_announcement_params(monkeypatch):
    monkeypatch.setattr(nap, "_pymupdf_available", lambda: True)
    monkeypatch.setattr(nap.osu, "get_client", lambda: object())
    captured = {}

    def fake_fill(client, **kwargs):
        captured.update(kwargs)
        return "公告PDF: 无待抓 pending"

    monkeypatch.setattr(nap.bodyfill, "fill_bodies", fake_fill)
    msg = nap.run()
    assert captured["channel"] == "announcement"
    assert captured["status_field"] == "pdf_status"
    assert captured["batch_limit"] == nap._BATCH_LIMIT
    assert captured["max_workers"] == nap._MAX_WORKERS
    assert captured["body_max_chars"] == nap._BODY_MAX_CHARS
    assert captured["extract_fn"] is nap.pdftext.fetch_pdf_text
    assert "公告PDF" in msg


def test_run_fails_fast_without_pymupdf(monkeypatch):
    """缺 pymupdf → raise 终止（不标 failed），同 trafilatura 教训。"""
    monkeypatch.setattr(nap, "_pymupdf_available", lambda: False)
    monkeypatch.setattr(nap.osu, "get_client", lambda: object())
    monkeypatch.setattr(nap.bodyfill, "fill_bodies",
                        lambda client, **kw: pytest.fail("缺依赖不应进 bodyfill"))
    with pytest.raises(RuntimeError, match="pymupdf"):
        nap.run()
