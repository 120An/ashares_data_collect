"""巨潮资讯网 hisAnnouncement/query 官方 JSON 接口薄客户端（新闻子系统 ⑤ 公司公告）。

**单查询全市场**（探针实证 2026-07-06）：`column` 不过滤市场、`plate` 才是过滤器；
`(column=szse, plate='')` 一次返回深沪京 A 股并集（`column=''` 会混入债/基，不可用）。
分页按 `totalpages`（响应 `hasMore` 恒 True 不可依赖）。

HTTP 层 requests → curl_cffi 兜底集中在 `_http_post` 一处（上层不感知）：官方公开
JSON、礼貌页间限速；被限（连接异常/非 200）才切 curl_cffi（TLS 指纹，不启浏览器）。
`_http_post` 模块级可 monkeypatch，单测不触网。
"""

from __future__ import annotations

import json
import logging
import time

logger = logging.getLogger(__name__)

_URL = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Accept": "application/json, text/plain, */*",
    "Origin": "http://www.cninfo.com.cn",
    "Referer": "http://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice",
    "X-Requested-With": "XMLHttpRequest",
}
_PAGE_SIZE = 30
_TIMEOUT = 25
_PAGE_SLEEP_SECONDS = 0.5   # 页间礼貌限速（测试 patch 为 0）
# 单查询全市场：column 任意主板值即可，plate='' 返回深沪京并集
_COLUMN = "szse"
_PLATE = ""


class CninfoError(RuntimeError):
    """巨潮接口层错误（网络/非 200/JSON 解析失败）；job 层据此决定容错。"""


def _post_via_requests(data: dict) -> tuple[int, str]:
    import requests
    r = requests.post(_URL, data=data, headers=_HEADERS, timeout=_TIMEOUT)
    return r.status_code, r.text


def _post_via_curl(data: dict) -> tuple[int, str]:
    from curl_cffi import requests as creq
    r = creq.post(_URL, data=data, headers=_HEADERS, timeout=_TIMEOUT,
                  impersonate="chrome120")
    return r.status_code, r.text


def _http_post(data: dict) -> tuple[int, str]:
    """requests → curl_cffi 兜底（一处切换），返回 (status_code, text)。

    requests 抛异常（连接重置等）或返回非 200（疑限频）→ 回退 curl_cffi；
    两者均失败则原样上抛。测试可整体 monkeypatch 本函数或分别 patch _post_via_*。
    """
    try:
        status, text = _post_via_requests(data)
        if status == 200:
            return status, text
        logger.warning(f"巨潮 requests 返回非 200（{status}），回退 curl_cffi")
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"巨潮 requests 异常（{exc!r}），回退 curl_cffi")
    return _post_via_curl(data)


def _form(se_date: str, page_num: int) -> dict:
    """构造单页表单参数（单查询全市场：column=szse, plate=''）。"""
    return {
        "pageNum": page_num, "pageSize": _PAGE_SIZE,
        "column": _COLUMN, "plate": _PLATE, "tabName": "fulltext",
        "stock": "", "searchkey": "", "secid": "", "category": "",
        "trade": "", "seDate": se_date, "sortName": "", "sortType": "",
        "isHLtitle": "false",
    }


def _fetch_page(se_date: str, page_num: int) -> dict:
    """取单页并解析为 dict；网络/非 200/JSON 解析失败 → CninfoError。

    **软封锁兜底**：WAF 常以 200+HTML 挑战页应答（非 200 才回退的 _http_post
    视其为成功）——JSON 解析失败时显式用 curl_cffi（TLS 指纹）重试一次该页，
    仍拿不到 JSON 才抛。
    """
    status, text = _http_post(_form(se_date, page_num))
    if status != 200:
        raise CninfoError(f"巨潮返回非 200: status={status} page={page_num}")
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        logger.warning(f"巨潮 200+非 JSON（疑 WAF 软封锁），curl_cffi 重试: page={page_num}")
    try:
        status2, text2 = _post_via_curl(_form(se_date, page_num))
        if status2 == 200:
            return json.loads(text2)
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"巨潮 curl_cffi 重试失败: page={page_num}: {exc!r}")
    raise CninfoError(
        f"巨潮响应非 JSON（疑限频/HTML，curl_cffi 重试亦失败）: page={page_num} "
        f"text[:120]={text[:120]!r}"
    )


def fetch_announcements(date: str) -> list[dict]:
    """取**当日全市场**公告原始记录（单查询分页，不做业务映射）。

    date: ``"YYYY-MM-DD"`` ISO 日期（seDate 用 start==end==当日）。
    - page1 失败 → 抛 CninfoError（job 层决定容错，通常上抛触发框架 retry）；
    - 空日（totalpages 0 / announcements []）→ 返回 []；
    - page2..totalpages 逐页取，**页级异常记日志继续**（尽力收集，缺页交 verify 补）；
      页间 sleep 礼貌限速。
    """
    se_date = f"{date}~{date}"
    first = _fetch_page(se_date, 1)
    total_pages = int(first.get("totalpages") or 0)
    records = list(first.get("announcements") or [])
    for page in range(2, total_pages + 1):
        time.sleep(_PAGE_SLEEP_SECONDS)
        try:
            page_json = _fetch_page(se_date, page)
            records.extend(page_json.get("announcements") or [])
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                f"巨潮公告 {date} 第 {page}/{total_pages} 页取数失败"
                f"（尽力收集，交 verify 补）: {exc!r}")
    logger.info(f"巨潮公告 {date}: 取 {len(records)} 条（totalpages={total_pages}）")
    return records
