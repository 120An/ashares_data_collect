"""迁移对账：sources.yaml 必须包含迁移前代码字典的全部 (id, url/route, channel)。

id 即归档目录名（不可变契约）——本测试是防"迁移改错 id → 归档目录漂移"的
强制门。允许注册表**新增**源（不断言全集），但迁移源的三元组必须逐字保留；
若某源被有意改名/下线，须同步更新本快照并在 commit 说明归档迁移方案。
"""

from data_collect.utils import source_registry as sr

# 关键源冻结快照 (id → url|route, channel, job)：一期迁移源（news_policy._RSS_SOURCES /
# news_us._RSS_SOURCES / news_regulator._RSSHUB_ROUTES+_LISTPAGES @ commit e46e0b3）
# + 二期单源 job（cctv/cninfo/em_stock）。id=归档目录名不可变，任何漂移即失败。
_FROZEN = {
    # news_policy（url, channel, job）
    "govcn_policy": ("https://www.gov.cn/pushinfo/v150203/rss.xml", "policy", "news_policy"),
    "govcn_gwy": ("https://www.gov.cn/guowuyuan/rss.xml", "policy", "news_policy"),
    "stats": ("https://www.stats.gov.cn/sj/zxfb/rss.xml", "policy", "news_policy"),
    # news_us
    "fed": ("https://www.federalreserve.gov/feeds/press_all.xml", "us_policy", "news_us"),
    "sec_8k": ("https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent"
               "&type=8-K&company=&dateb=&owner=include&count=400&output=atom",
               "us_filing", "news_us"),
    "nyt_business": ("https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
                     "us_news", "news_us"),
    "nyt_economy": ("https://rss.nytimes.com/services/xml/rss/nyt/Economy.xml",
                    "us_news", "news_us"),
    "wsj_markets": ("https://feeds.a.dj.com/rss/RSSMarketsMain.xml", "us_news", "news_us"),
    "wsj_business": ("https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml", "us_news", "news_us"),
    "wsj_world": ("https://feeds.a.dj.com/rss/RSSWorldNews.xml", "us_news", "news_us"),
    "bloomberg_markets": ("https://feeds.bloomberg.com/markets/news.rss", "us_news", "news_us"),
    "guardian_business": ("https://www.theguardian.com/business/rss", "us_news", "news_us"),
    "marketwatch": ("https://feeds.content.dowjones.io/public/rss/mw_topstories",
                    "us_news", "news_us"),
    # news_regulator rsshub（route, channel, job）
    "ndrc": ("/gov/ndrc/xwdt", "policy", "news_regulator"),
    "gov_bmwj": ("/gov/zhengce/zhengceku/bmwj", "policy", "news_regulator"),
    "cls_red": ("/cls/telegraph/red", "flash", "news_regulator"),
    "gelonghui": ("/gelonghui/live", "flash", "news_regulator"),
    "cls_depth": ("/cls/depth/1000", "media", "news_regulator"),
    "caixin": ("/caixin/latest", "media", "news_regulator"),
    "thepaper": ("/thepaper/featured", "media", "news_regulator"),
    "guancha": ("/guancha/headline", "media", "news_regulator"),
    "ckxx": ("/cankaoxiaoxi/column/zhongguo", "media", "news_regulator"),
    "cctv_world": ("/cctv/world", "media", "news_regulator"),
    "em_report_strategy": ("/eastmoney/report/strategyreport", "report", "news_regulator"),
    "em_report_industry": ("/eastmoney/report/industry", "report", "news_regulator"),
    "em_report_macro": ("/eastmoney/report/macresearch", "report", "news_regulator"),
    "em_report_stock": ("/eastmoney/report/stock", "report", "news_regulator"),
    "wallstreetcn_news": ("/wallstreetcn/news/global", "media", "news_regulator"),
    "wallstreetcn_live": ("/wallstreetcn/live/global", "media", "news_regulator"),
    "yicai": ("/yicai/headline", "media", "news_regulator"),
    "jiemian": ("/jiemian/lists/1", "media", "news_regulator"),
    "kr36_flash": ("/36kr/newsflashes", "media", "news_regulator"),
    "kr36_hot": ("/36kr/hot-list", "media", "news_regulator"),
    "latepost": ("/latepost", "media", "news_regulator"),
    "huxiu": ("/huxiu/article", "media", "news_regulator"),
    "szse_notice": ("/szse/notice", "policy", "news_regulator"),
    # news_regulator listpage（无 url/route；channel, job）
    "csrc": (None, "policy", "news_regulator"),
    "mof": (None, "policy", "news_regulator"),
    "people": (None, "media", "news_regulator"),
    # 二期单源 job（akshare/api，无 url/route；id=归档目录名，同样不可变）
    "cctv": (None, "cctv", "news_cctv"),
    "cninfo": (None, "announcement", "news_announcement"),
    "em_stock": (None, "stock", "news_stock"),
}


def test_real_registry_validates():
    """真实 sources.yaml 必须通过 schema 校验（本测试读真文件是有意的）。"""
    assert sr.validate_registry() == []


def test_migrated_sources_intact():
    by_id = {s.id: s for s in sr.load_all()}
    missing = [sid for sid in _FROZEN if sid not in by_id]
    assert missing == [], f"迁移源在注册表缺失（id 不可变契约）: {missing}"
    for sid, (locator, channel, job) in _FROZEN.items():
        s = by_id[sid]
        assert s.channel == channel, f"{sid} channel 漂移: {s.channel} != {channel}"
        assert s.job == job, f"{sid} job 漂移: {s.job} != {job}"
        if locator is not None:
            actual = s.url or s.route
            assert actual == locator, f"{sid} url/route 漂移: {actual} != {locator}"


# 运营停用清单：迁移源中**有意**下线的（源头挂死/衰减，kill-switch 生效中）。
# 本清单是"迁移不改行为"守门的显式例外——每项必须带停用原因与日期；
# 源恢复并 `manage_sources.py test <id>` 冒烟通过后，恢复 enabled 并从此处移出。
_OPS_DISABLED = {
    "stats",   # 2026-07-16 起源头挂死（60s 超时每小时告警刷屏），2026-07-23 停用
}


def test_migrated_sources_enabled():
    """迁移的源必须 enabled（迁移不改行为；_OPS_DISABLED 运营停用例外）；
    候选源（intl_news）必须全部 disabled（未冒烟不上线）。"""
    for s in sr.load_all():
        if s.id in _FROZEN and s.id not in _OPS_DISABLED:
            assert s.enabled, f"迁移源 {s.id} 不应停用"
        if s.id in _OPS_DISABLED:
            assert not s.enabled, f"运营停用源 {s.id} 应保持 disabled（或从清单移出）"
        if s.channel == "intl_news":
            assert not s.enabled, f"候选源 {s.id} 未冒烟不得启用"
