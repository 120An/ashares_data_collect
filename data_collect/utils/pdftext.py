"""公告 PDF 下载 + PyMuPDF 文本抽取薄客户端（新闻子系统 ⑤ Phase 2）。

`fetch_pdf_text(url)`：下载 PDF 字节（≤20MB、%PDF 魔数校验、requests→curl_cffi
兜底）→ PyMuPDF（懒导入）逐页抽文本层。best-effort：任何失败返回 None（job 层
置 pdf_status=failed，title 兜底）。`_get_bytes_via_*`/`_extract_pdf` 可 monkeypatch。
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
}
_TIMEOUT = 30
_MAX_PDF_BYTES = 20 * 1024 * 1024   # 年报级巨兽上限：超限视为失败（不 OCR 不硬啃）
_MIN_BODY = 120                     # 抽出正文过短（封面页/空文本层）→ 失败


def _get_bytes_via_requests(url: str) -> bytes | None:
    """流式下载（边下边计数，超限中止）；非 200/超限返回 None。"""
    import requests
    with requests.get(url, headers=_HEADERS, timeout=_TIMEOUT, stream=True) as r:
        if r.status_code != 200:
            return None
        length = r.headers.get("Content-Length")
        if length and length.isdigit() and int(length) > _MAX_PDF_BYTES:
            logger.debug(f"PDF 超限（Content-Length {length}）: {url}")
            return None
        chunks: list = []
        total = 0
        for chunk in r.iter_content(1 << 16):
            total += len(chunk)
            if total > _MAX_PDF_BYTES:
                logger.debug(f"PDF 超限（流中止 >{_MAX_PDF_BYTES}）: {url}")
                return None
            chunks.append(chunk)
        return b"".join(chunks)


def _get_bytes_via_curl(url: str) -> bytes | None:
    from curl_cffi import requests as creq
    r = creq.get(url, headers=_HEADERS, timeout=_TIMEOUT, impersonate="chrome120")
    if r.status_code != 200 or len(r.content) > _MAX_PDF_BYTES:
        return None
    return r.content


def _http_get_bytes(url: str) -> bytes | None:
    """requests → curl_cffi 兜底取字节；两者皆失败返回 None（best-effort）。"""
    try:
        data = _get_bytes_via_requests(url)
        if data is not None:
            return data
        logger.debug(f"PDF requests 未取到（非200/超限），回退 curl_cffi: {url}")
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"PDF requests 异常（{exc!r}），回退 curl_cffi: {url}")
    try:
        return _get_bytes_via_curl(url)
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"PDF curl_cffi 也失败: {url}: {exc!r}")
        return None


def _extract_pdf(data: bytes) -> str | None:
    """PyMuPDF 逐页抽文本层（懒导入；薄封装便于单测 monkeypatch）。"""
    import fitz  # pymupdf
    with fitz.open(stream=data, filetype="pdf") as doc:
        return "\n".join(page.get_text() for page in doc)


def fetch_pdf_text(url: str) -> str | None:
    """抓 PDF 正文：下载 → `%PDF` 魔数校验 → PyMuPDF 抽文本 → 阈值过滤。

    best-effort：下载失败/非 PDF（404 页伪装）/损坏/扫描版（无文本层）/过短
    均返回 None，由 job 层置 pdf_status=failed（title 兜底）。异常吞掉不上抛。
    """
    if not url:
        return None
    data = _http_get_bytes(url)
    if not data or not data[:5].startswith(b"%PDF"):
        return None
    try:
        text = _extract_pdf(data)
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"PDF 抽取异常 {url}: {exc!r}")
        return None
    if not text or len(text.strip()) < _MIN_BODY:
        return None
    return text.strip()
