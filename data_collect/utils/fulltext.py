"""新闻正文抓取 + 抽取薄客户端（新闻子系统 ⑥ Phase 2 全文）。

`fetch_article(url)`：requests→curl_cffi 兜底抓 HTML + trafilatura 抽主文 → 正文串。
HTTP 层 requests→curl_cffi 切换集中在 `_http_get` 一处（同 cninfo）；trafilatura 经
`_extract` 薄封装（懒导入，单测可 monkeypatch）。纯函数式，不写库、不做业务清洗
（清洗在 job 层 nn.clean_text）。
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
}
_TIMEOUT = 20
_MIN_BODY = 120        # 抽取后正文过短（stub/付费墙提示页）→ 视为失败


def _smart_text(resp) -> str:
    """响应头无 charset 时改用 apparent_encoding 解码（chardet 探测）。

    背景（2026-07-08 csrc/pbc 实撞）：政府站响应头常只给 `text/html` 不带
    charset，requests 按 RFC 缺省 ISO-8859-1 解码 → 中文全乱码（乱码文档一旦
    create-only 入库，同 _id 永远无法覆盖，只能删后重采）。header 带 charset
    的站点保持原行为不变。
    """
    ctype = (resp.headers.get("Content-Type") or "")
    if "charset" not in ctype.lower():
        apparent = getattr(resp, "apparent_encoding", None)
        if apparent:
            resp.encoding = apparent
    return resp.text


def _get_via_requests(url: str) -> tuple[int, str]:
    import requests
    r = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
    return r.status_code, _smart_text(r)


def _get_via_curl(url: str) -> tuple[int, str]:
    from curl_cffi import requests as creq
    r = creq.get(url, headers=_HEADERS, timeout=_TIMEOUT, impersonate="chrome120")
    return r.status_code, _smart_text(r)   # 同 requests 路径：无 charset 头按探测编码解码


def _http_get(url: str) -> str | None:
    """requests → curl_cffi 兜底抓 HTML；非 200/异常回退，两者皆失败返回 None。

    与 cninfo._http_post 的差异：这里两者皆失败**不上抛**而返回 None——全文抽取
    是 best-effort（job 层置 failed、摘要兜底），单 URL 失败不值得异常语义。
    """
    try:
        status, text = _get_via_requests(url)
        if status == 200:
            return text
        logger.debug(f"全文 requests 非 200（{status}），回退 curl_cffi: {url}")
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"全文 requests 异常（{exc!r}），回退 curl_cffi: {url}")
    try:
        status, text = _get_via_curl(url)
        return text if status == 200 else None
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"全文 curl_cffi 也失败: {url}: {exc!r}")
        return None


def _extract(html: str, url: str) -> str | None:
    """trafilatura 抽主文（懒导入；薄封装便于单测 monkeypatch，不装 trafilatura 也能测）。"""
    import trafilatura
    return trafilatura.extract(html, url=url, include_comments=False,
                               include_tables=True, favor_precision=True)


def fetch_article(url: str) -> str | None:
    """抓 URL 正文：`_http_get` → `_extract` → 阈值过滤；失败/无正文/过短返回 None。

    best-effort：抓不到（404/JS/付费墙/超时）、抽不出、抽出过短（stub 页）均 None，
    由 job 层置 body_status=failed（摘要兜底）。抽取异常吞掉返回 None（不上抛）。
    """
    html = _http_get(url)
    if not html:
        return None
    try:
        text = _extract(html, url)
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"全文抽取异常 {url}: {exc!r}")
        return None
    if not text or len(text.strip()) < _MIN_BODY:
        return None
    return text.strip()
