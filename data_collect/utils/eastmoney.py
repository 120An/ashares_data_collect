"""东财 HTTP 统一入口：节流 em_get + datacenter/clist 自动翻页。

风控背景（a-stock-data skill 实测）：东财系（push2/push2ex/datacenter/np-weblist）
每秒 >5 次 / 单 IP 并发 ≥10 / 分钟 ≥200 次会临时封 IP。铁律：串行、间隔 ≥1s+抖动、
复用会话、带 UA/Referer。所有 eastmoney.com 请求必须走 em_get()——job 内绝不裸 requests。
403 是风控信号不重试（重试加重）；429/5xx 指数退避重试。
"""
from __future__ import annotations

import logging
import random
import time
from typing import List

import requests

logger = logging.getLogger(__name__)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
DATACENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
CLIST_URL = "https://push2.eastmoney.com/api/qt/clist/get"

MIN_INTERVAL = 1.0            # 两次东财请求最小间隔（秒）；批量场景勿调小
_last_call = [0.0]            # 模块级上次请求时间戳（进程内串行节流）

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": UA})
try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    _adapter = HTTPAdapter(max_retries=Retry(
        total=3, connect=3, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"]))
    _SESSION.mount("https://", _adapter)
    _SESSION.mount("http://", _adapter)
except Exception:               # 老版本 urllib3 缺参数时降级无重试，不影响主流程
    pass


def beijing_now() -> "datetime.datetime":
    """服务器北京时间（HTTP Date 头，**不信本机钟**）。

    背景：本机时区曾被发现误设 UTC-7（2026-07-22），datetime.now() 的日期在北京
    上午时段会差一天。凡"判定某交易日是否已收盘"必须用本函数，勿用本机时钟。
    请求失败降级本机时钟（有告警日志）。
    """
    import datetime as _dt
    from email.utils import parsedate_to_datetime
    try:
        r = _SESSION.head("https://quote.eastmoney.com/", timeout=8)
        server = parsedate_to_datetime(r.headers["Date"])
        return server.astimezone(_dt.timezone(_dt.timedelta(hours=8)))
    except Exception as exc:
        logger.warning(f"beijing_now 取服务器时间失败，降级本机钟: {exc}")
        return _dt.datetime.now(_dt.timezone(_dt.timedelta(hours=8)))


def em_get(url: str, params: dict | None = None, headers: dict | None = None,
           timeout: int = 15, **kwargs):
    """东财统一请求：自动节流（≥MIN_INTERVAL+抖动）+ 复用会话 + 默认 UA。"""
    wait = MIN_INTERVAL - (time.time() - _last_call[0])
    if wait > 0:
        time.sleep(wait + random.uniform(0.1, 0.5))
    try:
        return _SESSION.get(url, params=params, headers=headers, timeout=timeout, **kwargs)
    finally:
        _last_call[0] = time.time()


def datacenter_query(report_name: str, filter_str: str = "", columns: str = "ALL",
                     sort_columns: str = "", sort_types: str = "-1",
                     page_size: int = 500, max_pages: int = 200) -> List[dict]:
    """东财数据中心查询（龙虎榜/两融/大宗/解禁等共用），自动翻页返回全部行。

    max_pages 为跑飞保险（500×200=10万行远超单日任何报表）。
    """
    rows: List[dict] = []
    page = 1
    while page <= max_pages:
        r = em_get(DATACENTER_URL, params={
            "reportName": report_name, "columns": columns, "filter": filter_str,
            "pageNumber": str(page), "pageSize": str(page_size),
            "sortColumns": sort_columns, "sortTypes": sort_types,
            "source": "WEB", "client": "WEB"})
        result = (r.json() or {}).get("result") or {}
        data = result.get("data") or []
        if not data:
            break
        rows.extend(data)
        if len(rows) >= (result.get("count") or 0):
            break
        page += 1
    return rows


def clist_query(fs: str, fields: str, fid: str,
                page_size: int = 100, max_pages: int = 200) -> List[dict]:
    """东财行情列表（push2 clist，全市场快照类），自动翻页返回全部 diff 行。"""
    rows: List[dict] = []
    page = 1
    while page <= max_pages:
        r = em_get(CLIST_URL, params={
            "pn": page, "pz": page_size, "po": 1, "np": 1, "fltt": 2, "invt": 2,
            "fid": fid, "fs": fs, "fields": fields},
            headers={"Referer": "https://quote.eastmoney.com/"})
        data = (r.json() or {}).get("data") or {}
        diff = data.get("diff") or []
        if not diff:
            break
        rows.extend(diff)
        if len(rows) >= (data.get("total") or 0):
            break
        page += 1
    return rows
