"""pdftext PDF 下载+抽取客户端单测：全 mock，不触网、不依赖真 pymupdf。"""

import pytest

from data_collect.utils import pdftext


# ---------- _http_get_bytes：requests → curl_cffi 兜底 ----------

def test_get_bytes_uses_requests_first(monkeypatch):
    monkeypatch.setattr(pdftext, "_get_bytes_via_requests", lambda url: b"%PDF-1.4 data")
    monkeypatch.setattr(pdftext, "_get_bytes_via_curl",
                        lambda url: pytest.fail("不应回退 curl"))
    assert pdftext._http_get_bytes("http://x") == b"%PDF-1.4 data"


def test_get_bytes_falls_back_on_none(monkeypatch):
    monkeypatch.setattr(pdftext, "_get_bytes_via_requests", lambda url: None)
    monkeypatch.setattr(pdftext, "_get_bytes_via_curl", lambda url: b"%PDF-curl")
    assert pdftext._http_get_bytes("http://x") == b"%PDF-curl"


def test_get_bytes_falls_back_on_exception(monkeypatch):
    def boom(url):
        raise ConnectionError("reset")
    monkeypatch.setattr(pdftext, "_get_bytes_via_requests", boom)
    monkeypatch.setattr(pdftext, "_get_bytes_via_curl", lambda url: b"%PDF-curl")
    assert pdftext._http_get_bytes("http://x") == b"%PDF-curl"


def test_get_bytes_none_when_both_fail(monkeypatch):
    def boom(url):
        raise ConnectionError("down")
    monkeypatch.setattr(pdftext, "_get_bytes_via_requests", boom)
    monkeypatch.setattr(pdftext, "_get_bytes_via_curl", boom)
    assert pdftext._http_get_bytes("http://x") is None


# ---------- fetch_pdf_text：魔数 + 抽取 + 阈值 ----------

def test_fetch_pdf_text_extracts(monkeypatch):
    monkeypatch.setattr(pdftext, "_http_get_bytes", lambda url: b"%PDF-1.7 fake")
    monkeypatch.setattr(pdftext, "_extract_pdf", lambda data: "  " + "正" * 200 + "  ")
    assert pdftext.fetch_pdf_text("http://x/1.pdf") == "正" * 200


def test_fetch_pdf_text_none_on_no_bytes(monkeypatch):
    monkeypatch.setattr(pdftext, "_http_get_bytes", lambda url: None)
    assert pdftext.fetch_pdf_text("http://x/2.pdf") is None


def test_fetch_pdf_text_none_on_bad_magic(monkeypatch):
    """404 页/HTML 挑战页伪装 PDF → %PDF 魔数拦截，不喂给 PyMuPDF。"""
    monkeypatch.setattr(pdftext, "_http_get_bytes", lambda url: b"<html>not found")
    monkeypatch.setattr(pdftext, "_extract_pdf",
                        lambda data: pytest.fail("非 PDF 不应进抽取"))
    assert pdftext.fetch_pdf_text("http://x/3.pdf") is None


def test_fetch_pdf_text_none_on_scan_pdf(monkeypatch):
    """扫描版：有 PDF 无文本层 → 抽出空/过短 → None（不 OCR）。"""
    monkeypatch.setattr(pdftext, "_http_get_bytes", lambda url: b"%PDF-1.4 scan")
    monkeypatch.setattr(pdftext, "_extract_pdf", lambda data: "   ")
    assert pdftext.fetch_pdf_text("http://x/4.pdf") is None


def test_fetch_pdf_text_none_on_extract_raise(monkeypatch):
    monkeypatch.setattr(pdftext, "_http_get_bytes", lambda url: b"%PDF-broken")
    def boom(data):
        raise ValueError("corrupt pdf")
    monkeypatch.setattr(pdftext, "_extract_pdf", boom)
    assert pdftext.fetch_pdf_text("http://x/5.pdf") is None


def test_fetch_pdf_text_none_on_empty_url():
    assert pdftext.fetch_pdf_text("") is None
