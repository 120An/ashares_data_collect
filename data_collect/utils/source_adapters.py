"""采集适配器层（注册表三期，backlog 候选1）："怎么采"的统一实现与冒烟分派。

两层架构（适配器归代码、源归配置）的代码侧收口：
- feed 类（rss/rsshub 同为 feedparser 协议）单一实现 ``fetch_feed()``——收口
  news_policy/news_us/news_regulator 三处 feedparser+bozo 防御克隆。各 job 以
  模块级别名引用（如 ``_fetch_rss_entries = fetch_feed``）：既复用实现又保留
  原私有名，测试 monkeypatch 不破（news_common 同款约定）。
- ``hard_timeout()``：Source.timeout（HTTP 层）接线后，外层守护线程硬超时的
  配套推导——防 yaml 把 timeout 配得比外层地板还大时被外层先杀成死配置。
- ``smoke_test()``：manage_sources `test <id>` 的统一分派入口。
  listpage/akshare/api 个性逻辑仍归各 job/utils（勿配置化），冒烟实现函数内
  lazy import 引用（jobs → 本模块 → (lazy) jobs，无环）。
"""

from __future__ import annotations

import datetime
import logging

from data_collect.utils.news_common import fetch_with_timeout

logger = logging.getLogger(__name__)

DEFAULT_HTTP_TIMEOUT = 30   # fetch_feed 缺省（生产调用方显式传 Source.timeout）


def fetch_feed(url, *, headers=None, proxy="", timeout=DEFAULT_HTTP_TIMEOUT,
               label="feed") -> list:
    """拉取并解析 RSS/Atom → entries 列表（lazy import feedparser/requests）。

    防御（原三 job 克隆的共同语义）：非 200 抛错；bozo（解析半残）且无 entries
    抛错——半残 feed 静默产出空信封比失败更糟；bozo 但有 entries 视为可用
    （RSS 普遍有小毛病，feedparser 容错解析出的条目仍完整）。
    proxy 精确到源（空=直连）：不用全局环境变量——NO_PROXY 漏国内域名会重演
    cls 挂起事故（见 news_us 模块说明）。
    """
    import feedparser
    import requests

    kwargs: dict = {"headers": headers or {}, "timeout": timeout}
    if proxy:
        kwargs["proxies"] = {"http": proxy, "https": proxy}
    resp = requests.get(url, **kwargs)
    if resp.status_code != 200:
        raise RuntimeError(f"{label} HTTP {resp.status_code}: {url}")
    feed = feedparser.parse(resp.content)
    entries = list(feed.entries or [])
    if not entries and getattr(feed, "bozo", 0):
        raise RuntimeError(
            f"{label} 解析失败（bozo={getattr(feed, 'bozo_exception', None)!r}）: {url}")
    return entries


def hard_timeout(http_timeout, floor) -> float:
    """外层 fetch_with_timeout 硬超时：不低于 job 地板 floor，且给足 HTTP 超时
    +15s 解析余量（yaml 把 timeout 配大时外层跟随，不产生"配了也白配"死配置）。"""
    return max(floor, int(http_timeout) + 15)


# ======== 冒烟分派（manage_sources `test <id>` 统一入口） ========

_SMOKE_TIMEOUT = 60       # 冒烟统一硬超时（CLI 诊断工具不许挂死，评审 S2）


def _first_title(entries) -> str:
    return str((entries[0].get("title") if entries else "") or "")[:60]


def _smoke_rss(s) -> str:
    entries = fetch_feed(s.url, headers=s.headers, proxy=s.proxy_url,
                         timeout=s.timeout, label=f"rss {s.id}")
    return f"{len(entries)} 条 | 首条: {_first_title(entries)}"


def _smoke_rsshub(s) -> str:
    from data_collect.config import get_news_config
    base = str(get_news_config().get("rsshub_base") or "").rstrip("/")
    if not base:
        raise RuntimeError("需在 config.yaml 配置 news.rsshub_base"
                           "（如 http://192.168.9.10:1200）")
    entries = fetch_feed(f"{base}{s.route}", timeout=s.timeout,
                         label=f"rsshub {s.id}")
    return f"{len(entries)} 条 | 首条: {_first_title(entries)}"


def _smoke_listpage(s) -> str:
    """listpage 个性配置（link_re）在 news_regulator，lazy import 无环。"""
    from data_collect.jobs.news_regulator import _LISTPAGES, _extract_article_urls
    from data_collect.utils import fulltext
    cfg = _LISTPAGES.get(s.id)
    if cfg is None:
        raise RuntimeError(f"listpage 源 {s.id} 在代码个性表 _LISTPAGES 无配置"
                           f"（注册表/代码脱节）")
    html = fetch_with_timeout(lambda: fulltext._http_get(cfg["list_url"]),
                              30, f"listpage {s.id} 冒烟")
    urls = _extract_article_urls(s.id, html or "")
    return f"列表页 {len(urls)} 链接 | 首条: {urls[0] if urls else '-'}"


def _smoke_akshare(s) -> str:
    """akshare 单源 job 冒烟：按 id 分派到 job 取数函数（真拉不归档不入库）。"""
    if s.id == "cctv":
        from data_collect.jobs.news_cctv import _fetch_cctv
        date8 = (datetime.date.today()
                 - datetime.timedelta(days=1)).strftime("%Y%m%d")
        df = fetch_with_timeout(lambda: _fetch_cctv(date8), _SMOKE_TIMEOUT,
                                f"akshare {s.id} 冒烟")
        return f"昨日({date8}) {0 if df is None else len(df)} 行"
    if s.id == "em_stock":
        from data_collect.jobs.news_stock import _fetch_stock_news
        df = fetch_with_timeout(lambda: _fetch_stock_news("000001"),
                                _SMOKE_TIMEOUT, f"akshare {s.id} 冒烟")
        return f"探针票 000001 {0 if df is None else len(df)} 条"
    raise NotImplementedError(f"akshare 源 {s.id} 无冒烟实现"
                              f"（在 source_adapters 补一个分支）")


def _smoke_api(s) -> str:
    if s.id == "cninfo":
        from data_collect.utils.cninfo import fetch_announcements
        date_iso = datetime.date.today().strftime("%Y-%m-%d")
        rows = fetch_with_timeout(lambda: fetch_announcements(date_iso), 120,
                                  f"api {s.id} 冒烟")
        return f"今日({date_iso}) {len(rows or [])} 条公告"
    raise NotImplementedError(f"api 源 {s.id} 无冒烟实现"
                              f"（在 source_adapters 补一个分支）")


_SMOKE_FNS = {"rss": _smoke_rss, "rsshub": _smoke_rsshub,
              "listpage": _smoke_listpage, "akshare": _smoke_akshare,
              "api": _smoke_api}


def smoke_test(source) -> str:
    """单源冒烟（真拉一次，不归档不入库）：返回单行结果串，失败原样抛。

    与生产同路径：headers/proxy/timeout 全部取自 Source（加载器已解析）。
    """
    fn = _SMOKE_FNS.get(source.adapter)
    if fn is None:
        raise NotImplementedError(f"adapter={source.adapter} 冒烟未支持")
    return fn(source)
