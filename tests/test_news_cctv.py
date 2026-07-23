"""news_cctv 单元测试：全 mock——不连 OpenSearch/PG、不真调 akshare、不碰归档盘。

覆盖（计划 T6 + 设计决策 + 审查修订）：
- run 正常入库：信封契约（channel=cctv、pub_time 固定 19:00、vec_status=pending、
  _id 以原始 title 计算且决定论、stocks 打标）、归档只 append 新 _id、摘要计数；
- 原始归档：归档信封含 raw_title/raw_content（保留 <b> 等原始形态），
  bulk_create 收到的入库 doc 无 raw_* 键（防动态 mapping 长出计划外字段）；
- 空日不误标：不调 bulk/append、无 marker、摘要"0 条"；
- run_verify 自然日语义：周一回溯含上周末；**显式传入交易日窗口被忽略**（防框架
  语义漂移的回归锚点）；窗口只由 verify_days_back 决定且不含今天；已有数据日
  skip；查询异常视为无数据（首日别名未建）；逐日异常隔离（后续日仍被处理）+
  failed>0 末尾 raise 保留框架失败通知语义（异常消息即摘要，含失败日期）；
- T+2 空日标记：marker 信封（channel=marker、vec_status=done、_id 格式）+ 告警
  一次；marker 已存在幂等跳过（不重采/不重写/不重告警）；T+1 空日不标记；
- 降级告警：归档 load_ids 断写损坏（EOFError）→ 降级空集继续采集 + 告警；
  简称词典加载失败 → stocks 降级 [] + 告警（create-only 下当晚打标永久缺失）；
- 失败传播：run 中 fetch/bulk_create 抛错须上抛（锁定框架 retry 契约，
  防将来被好心 try/except 吞掉）；
- run_backfill：逐日 + 日间限速 sleep、start 早于 akshare 下限被 clamp、
  单日异常不中断且摘要含失败日期列表。
"""

import datetime
import json
import re
from types import SimpleNamespace

import pandas as pd
import pytest

from data_collect.jobs import news_cctv as cctv
from data_collect.utils import news_normalize as nn


_MONDAY = datetime.date(2026, 7, 6)   # 周一（weekday=0），回溯 3 自然日覆盖上周末


def _df(rows):
    """构造 akshare news_cctv 形状的 DataFrame（列 date/title/content）。"""
    return pd.DataFrame(rows, columns=["date", "title", "content"])


@pytest.fixture
def env(monkeypatch):
    """全隔离环境：akshare/OpenSearch/归档/钉钉/词典/时钟全打桩并记录调用。

    fetch_map: {date: DataFrame|Exception}——缺省空 DataFrame（源无该日数据），
    Exception 值则在抓取时抛出（模拟单日源故障）。
    """
    calls = SimpleNamespace(fetched=[], bulk=[], archived=[], searches=[],
                            alerts=[], flushes=[], sleeps=[])
    e = SimpleNamespace(calls=calls, fetch_map={})

    def fake_fetch(date):
        calls.fetched.append(date)
        value = e.fetch_map.get(date)
        if isinstance(value, Exception):
            raise value
        return value if value is not None else pd.DataFrame()

    def fake_flush_spool():
        calls.flushes.append(1)
        return 0

    def fake_append(source, date, envelopes):
        envelopes = list(envelopes)
        calls.archived.append((source, date, envelopes))
        return len(envelopes)

    def fake_bulk_create(client, docs):
        calls.bulk.append(list(docs))
        return len(docs), 0

    def fake_search_raw(client, body, index=cctv.osu.ALIAS):
        calls.searches.append(body)
        return {"hits": {"total": {"value": 0, "relation": "eq"}}}

    monkeypatch.setattr(cctv, "_fetch_cctv", fake_fetch)
    monkeypatch.setattr(cctv, "_fetch_cctv_official", lambda date8: [])  # 备源默认空（不触网）
    monkeypatch.setattr(cctv, "_rsshub_episode_counts", lambda: {})      # 核验默认空（不触网）
    monkeypatch.setattr(cctv, "_today", lambda: _MONDAY)
    monkeypatch.setattr(cctv, "get_news_config", lambda: {"verify_days_back": 3})
    monkeypatch.setattr(cctv, "send_dingtalk", lambda msg: calls.alerts.append(msg))
    monkeypatch.setattr(cctv.news_archive, "flush_spool", fake_flush_spool)
    monkeypatch.setattr(cctv.news_archive, "load_ids", lambda source, date: set())
    monkeypatch.setattr(cctv.news_archive, "append", fake_append)
    monkeypatch.setattr(cctv.osu, "get_client", lambda: object())
    monkeypatch.setattr(cctv.osu, "bulk_create", fake_bulk_create)
    monkeypatch.setattr(cctv.osu, "search_raw", fake_search_raw)
    monkeypatch.setattr(cctv.nn, "load_name_dict", lambda: {"贵州茅台": "600519.SH"})
    monkeypatch.setattr(cctv.time, "sleep", lambda s: calls.sleeps.append(s))
    return e


# ---------- run ----------

def test_run_normal_ingest_envelope_contract(env):
    raw_title = "贵州茅台调研<b>报道</b>"          # 含 HTML：验证 _id 用原始、存库用清洗后
    env.fetch_map["20260703"] = _df([
        {"date": "20260703", "title": raw_title, "content": "正文A"},
        {"date": "20260703", "title": "国务院常务会议", "content": "正文B"},
    ])

    summary = cctv.run("20260703")

    assert env.calls.fetched == ["20260703"]
    assert len(env.calls.bulk) == 1
    docs = env.calls.bulk[0]
    assert len(docs) == 2
    for doc in docs:
        assert doc["source"] == "cctv"
        assert doc["channel"] == "cctv"
        assert doc["pub_time"] == "2026-07-03 19:00:00"     # 播出时间固定 19:00
        assert doc["vec_status"] == "pending"
        assert "url" not in doc                              # 联播无 url
        # 入库 doc 剥除 raw_*（原文只进归档；不剥会被动态 mapping 长出计划外字段）
        assert "raw_title" not in doc and "raw_content" not in doc
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", doc["fetch_time"])

    # _id 以**原始** title（clean_text 之前）计算，且决定论
    expected_id = nn.make_id(
        {"source": "cctv", "pub_time": "2026-07-03 19:00:00", "title": raw_title})
    assert docs[0]["_id"] == expected_id
    assert docs[0]["title"] == "贵州茅台调研报道"            # 存库 title 已清洗（HTML 剥除）
    assert docs[0]["stocks"] == ["600519.SH"]               # 简称词典打标生效
    assert docs[1]["stocks"] == []

    # 归档收到全部信封（当日无已归档 id），且为**含 raw_* 的完整信封**：
    # 归档层是"原始归档"，clean_text 的 bug 不能永久毁掉原文
    assert len(env.calls.archived) == 1
    source, date, archived_envs = env.calls.archived[0]
    assert (source, date) == ("cctv", "20260703")
    assert [d["_id"] for d in archived_envs] == [d["_id"] for d in docs]
    assert archived_envs[0]["raw_title"] == raw_title        # <b> 原始形态保留
    assert archived_envs[0]["raw_content"] == "正文A"
    assert archived_envs[0]["title"] == "贵州茅台调研报道"    # clean 后版本并存（重放直接入库）

    assert "源 2 条" in summary and "新增 2" in summary
    assert env.calls.flushes == [1]                          # run 开头搬运 spool


def test_run_archive_appends_only_new_ids(env, monkeypatch):
    """归档 append 只收 load_ids 之外的新 _id；bulk_create 仍收全部（409→dup 兜幂等）。"""
    env.fetch_map["20260703"] = _df([
        {"date": "20260703", "title": "标题甲", "content": "A"},
        {"date": "20260703", "title": "标题乙", "content": "B"},
    ])
    known = nn.make_id(
        {"source": "cctv", "pub_time": "2026-07-03 19:00:00", "title": "标题甲"})
    monkeypatch.setattr(cctv.news_archive, "load_ids", lambda s, d: {known})

    cctv.run("20260703")

    _, _, archived_envs = env.calls.archived[0]
    assert [d["title"] for d in archived_envs] == ["标题乙"]   # 只归档新条目
    assert len(env.calls.bulk[0]) == 2                         # 入库仍全量


def test_run_empty_source_no_ingest_no_marker(env):
    """当天空 → 不入库不归档不告警不写 marker（交 verify T+2 判定）。"""
    summary = cctv.run("20260703")

    assert env.calls.bulk == []
    assert env.calls.archived == []
    assert env.calls.alerts == []
    assert "0 条" in summary


def test_run_fetch_returns_none_treated_as_empty(env, monkeypatch):
    monkeypatch.setattr(cctv, "_fetch_cctv", lambda date: None)
    summary = cctv.run("20260703")
    assert env.calls.bulk == []
    assert "0 条" in summary


# ---------- 官网备源（主备双活，2026-07-08 用户批准：央视官网直抓） ----------

_DAY_HTML = (
    '<a href="https://tv.cctv.com/2026/07/03/VIDEfull0703.shtml">完整版</a>'
    '<a href="https://tv.cctv.com/2026/07/03/VIDEitem10703.shtml">条1</a>'
    '<a href="https://tv.cctv.com/2026/07/03/VIDEitem10703.shtml">重复链接</a>')
_FULL_HTML = ('<title>《新闻联播》 20260703 19:00_CCTV节目官网</title>'
              '<div id="content_area">编辑：某某</div></div></div>')
_ITEM_HTML = ('<title>[视频]贵州茅台调研报道_CCTV节目官网-CCTV-13_央视网</title>'
              '<div id="content_area"><p>央视网消息（新闻联播）：正文甲</p>'
              '</div></div></div>')


def test_fetch_official_parses_day_and_items(monkeypatch):
    """按日页→分条页解析：完整版跳过、链接去重、title 剥前后缀、正文剥标签。"""
    pages = {
        cctv._OFFICIAL_DAY_URL.format(date8="20260703"): _DAY_HTML,
        "https://tv.cctv.com/2026/07/03/VIDEfull0703.shtml": _FULL_HTML,
        "https://tv.cctv.com/2026/07/03/VIDEitem10703.shtml": _ITEM_HTML,
    }
    monkeypatch.setattr(cctv, "_http_get_text", lambda url: pages[url])
    monkeypatch.setattr(cctv.time, "sleep", lambda s: None)
    rows = cctv._fetch_cctv_official("20260703")
    assert rows == [{"title": "贵州茅台调研报道",
                     "content": "央视网消息（新闻联播）：正文甲"}]


def test_run_backup_used_when_primary_empty(env, monkeypatch):
    """主源（akshare）空 → 官网备源顶上，正常信封化入库。"""
    monkeypatch.setattr(cctv, "_fetch_cctv_official", lambda date8: [
        {"title": "备源标题", "content": "备源正文"}])
    cctv.run("20260703")     # fetch_map 空 → 主源 0 条
    assert len(env.calls.bulk) == 1
    assert env.calls.bulk[0][0]["title"] == "备源标题"
    assert env.calls.bulk[0][0]["pub_time"] == "2026-07-03 19:00:00"


def test_run_backup_not_called_when_primary_ok(env, monkeypatch):
    monkeypatch.setattr(cctv, "_fetch_cctv_official",
                        lambda date8: pytest.fail("主源正常不应触备源"))
    env.fetch_map["20260703"] = _df(
        [{"date": "20260703", "title": "头条", "content": "正文"}])
    cctv.run("20260703")
    assert len(env.calls.bulk) == 1


def test_run_backup_failure_keeps_empty_semantics(env, monkeypatch):
    """双源皆失败/空 → 维持原空日语义（0 条，T+2 标记规则不变）。"""
    def boom(date8):
        raise RuntimeError("官网 503")
    monkeypatch.setattr(cctv, "_fetch_cctv_official", boom)
    summary = cctv.run("20260703")
    assert env.calls.bulk == []
    assert "0 条" in summary


# ---------- RSSHub 交叉核验（用户 2026-07-08 提议；语义=单向审计告警，非取交集） ----------

def test_rsshub_episode_counts_parse(monkeypatch):
    """每期 title 提日期、summary 数 [视频] 分条（完整版条目不带前缀，天然排除）；
    同日两版（19:00/21:00）取 max。summary 形态按 2026-07-08 实测：分条 <a> 链接清单。"""
    entries = [
        {"title": "新闻联播 2026/07/07",
         "summary": ('<a href="https://tv.cctv.com/x1.shtml">《新闻联播》 20260707 19:00 ⏱00:30</a><br/>'
                     '<a href="https://tv.cctv.com/x2.shtml">[视频]习近平对防汛救灾作出指示 ⏱00:01</a><br/>'
                     '<a href="https://tv.cctv.com/x3.shtml">[视频]科技强国建设 ⏱00:06</a><br/>'
                     '<a href="https://tv.cctv.com/x4.shtml">[视频]国内联播快讯 ⏱00:05</a>')},
        {"title": "新闻联播 2026/07/06",
         "summary": ('<a href="https://tv.cctv.com/y1.shtml">《新闻联播》 20260706 19:00</a><br/>'
                     '<a href="https://tv.cctv.com/y2.shtml">[视频]只此一条</a>')},
        {"title": "无日期垃圾", "summary": ""},
    ]
    monkeypatch.setattr(cctv, "_fetch_xwlb_entries", lambda: entries)
    monkeypatch.setattr(cctv, "get_news_config",
                        lambda: {"rsshub_base": "http://192.168.9.10:1200"})
    assert cctv._rsshub_episode_counts() == {"20260707": 3, "20260706": 1}


def test_verify_crosscheck_alerts_on_missing(env, monkeypatch):
    """库内条数 < 摘要条数 → 钉钉告警 + 摘要标注（fixture 库内恒 0 条）。"""
    monkeypatch.setattr(cctv, "_rsshub_episode_counts",
                        lambda: {"20260705": 5})          # 窗口内某日摘要列 5 条
    summary = cctv.run_verify("ignored", "ignored")
    assert "核验缺漏" in summary and "20260705" in summary
    assert any("交叉核验" in a for a in env.calls.alerts)


def test_verify_crosscheck_silent_when_rsshub_empty(env):
    """RSSHub 不可用/未配置 → 核验静默跳过（审计增强，不是采集依赖）。"""
    summary = cctv.run_verify("ignored", "ignored")
    assert "核验缺漏" not in summary


def test_run_defaults_to_today(env):
    env.fetch_map["20260706"] = _df(
        [{"date": "20260706", "title": "今日头条", "content": "C"}])
    summary = cctv.run()
    assert env.calls.fetched == ["20260706"]     # _today 固定为 20260706
    assert "20260706" in summary


def test_run_flush_spool_failure_not_blocking(env, monkeypatch):
    def boom():
        raise OSError("NAS 不可达")

    monkeypatch.setattr(cctv.news_archive, "flush_spool", boom)
    env.fetch_map["20260703"] = _df(
        [{"date": "20260703", "title": "标题", "content": "C"}])

    summary = cctv.run("20260703")
    assert "源 1 条" in summary                   # 采集入库不受 flush 失败影响


def test_run_load_ids_corrupt_degrades_and_alerts(env, monkeypatch):
    """归档断写损坏（EOFError）→ 降级空集继续采集入库 + 钉钉告警一条。"""
    def boom(source, date):
        raise EOFError("Compressed file ended before the end-of-stream marker")

    monkeypatch.setattr(cctv.news_archive, "load_ids", boom)
    env.fetch_map["20260703"] = _df(
        [{"date": "20260703", "title": "标题", "content": "C"}])

    summary = cctv.run("20260703")

    assert len(env.calls.bulk[0]) == 1            # 入库照常（create-only 使去重缺失无害）
    assert len(env.calls.archived[0][2]) == 1     # 归档 append 照常（全量视为新）
    assert len(env.calls.alerts) == 1             # 告警一条
    assert "源 1 条" in summary


def test_run_name_dict_failure_degrades_and_alerts(env, monkeypatch):
    """词典加载失败 → stocks 降级（简称打不中）继续入库 + 钉钉告警一条。

    告警必须发：create-only 文档不可变，当晚入库文档的简称打标为空即永久缺失。
    """
    def boom():
        raise RuntimeError("PG 不可用")

    monkeypatch.setattr(cctv.nn, "load_name_dict", boom)
    env.fetch_map["20260703"] = _df(
        [{"date": "20260703", "title": "贵州茅台专题", "content": "C"}])

    summary = cctv.run("20260703")

    assert env.calls.bulk[0][0]["stocks"] == []   # 词典缺失 → 简称打不中（正则打标仍在）
    assert len(env.calls.alerts) == 1             # 降级须当晚可见
    assert "源 1 条" in summary


def test_run_fetch_error_propagates(env):
    """fetch 抛错必须上抛（框架 retry/失败通知契约，禁止被吞成'空日'）。"""
    env.fetch_map["20260703"] = RuntimeError("akshare 接口抽风")
    with pytest.raises(RuntimeError, match="抽风"):
        cctv.run("20260703")


def test_run_bulk_error_propagates(env, monkeypatch):
    """bulk_create 抛错必须上抛（入库失败不能被摘要吞掉）。"""
    def boom(client, docs):
        raise RuntimeError("bulk 写入失败 3 条")

    monkeypatch.setattr(cctv.osu, "bulk_create", boom)
    env.fetch_map["20260703"] = _df(
        [{"date": "20260703", "title": "标题", "content": "C"}])

    with pytest.raises(RuntimeError, match="bulk"):
        cctv.run("20260703")


# ---------- run_verify：自然日语义 ----------

def test_verify_natural_day_window_covers_weekend(env):
    """周一回溯 3 自然日 = 上周五/六/日——交易日语义会漏掉的周末必须在窗口内。"""
    assert _MONDAY.weekday() == 0
    for d in ("20260703", "20260704", "20260705"):
        env.fetch_map[d] = _df([{"date": d, "title": f"标题{d}", "content": "C"}])

    summary = cctv.run_verify("20260101", "20260102")

    assert env.calls.fetched == ["20260703", "20260704", "20260705"]
    assert datetime.date(2026, 7, 4).weekday() == 5     # 周六在窗口内
    assert datetime.date(2026, 7, 5).weekday() == 6     # 周日在窗口内
    assert "[20260703~20260705]" in summary and "补采 3 日" in summary


def test_verify_ignores_passed_trade_window(env):
    """回归锚点：显式传入交易日窗口必须被忽略——窗口只由 _today+verify_days_back 决定。"""
    for d in ("20260703", "20260704", "20260705"):
        env.fetch_map[d] = _df([{"date": d, "title": f"标题{d}", "content": "C"}])

    cctv.run_verify("20200101", "20200102")       # 框架按交易日推算的（错误语义）窗口

    assert env.calls.fetched == ["20260703", "20260704", "20260705"]   # 与传入无关
    assert all("2020" not in json.dumps(b) for b in env.calls.searches)


def test_verify_window_from_config_days_back_excludes_today(env, monkeypatch):
    """窗口 = [today-N, today-1]：verify_days_back=1 只查昨天，今天不在窗口（归 run 管）。"""
    monkeypatch.setattr(cctv, "get_news_config", lambda: {"verify_days_back": 1})

    cctv.run_verify("20200101", "20200102")

    assert env.calls.fetched == ["20260705"]      # 仅昨天；今天 20260706 不查


def test_verify_skips_days_with_existing_data(env, monkeypatch):
    def fake_search(client, body, index=cctv.osu.ALIAS):
        env.calls.searches.append(body)
        if "ids" in body["query"]:
            return {"hits": {"total": {"value": 0}}}
        return {"hits": {"total": {"value": 30}}}   # 每日均已有数据

    monkeypatch.setattr(cctv.osu, "search_raw", fake_search)

    summary = cctv.run_verify("20200101", "20200102")

    assert env.calls.fetched == []                  # 已有数据日不重采
    assert env.calls.bulk == []
    assert "已有 3 日" in summary


def test_verify_t2_empty_writes_marker_and_alerts(env):
    """0703(T+3)/0704(T+2) 重采仍空 → 各写一枚 marker + 各告警一次；0705(T+1) 待定。"""
    summary = cctv.run_verify("20200101", "20200102")

    marker_docs = [doc for batch in env.calls.bulk for doc in batch]
    assert [m["_id"] for m in marker_docs] == [
        "marker-cctv-empty-20260703", "marker-cctv-empty-20260704"]
    for m in marker_docs:
        assert m["channel"] == "marker"
        assert m["source"] == "cctv"
        assert m["vec_status"] == "done"            # marker 不需向量，防 news_embed 编码
        assert m["stocks"] == []
    assert marker_docs[0]["pub_time"] == "2026-07-03 19:00:00"
    assert len(env.calls.alerts) == 2               # 每个标记日告警一次
    assert env.calls.archived == []                 # marker 不进归档
    assert "新标记 2 日" in summary and "待定 1 日" in summary


def test_verify_existing_marker_skip_idempotent(env, monkeypatch):
    """marker 已存在 → 不重采、不重写、不重告警（幂等，重试已终止）。"""
    def fake_search(client, body, index=cctv.osu.ALIAS):
        env.calls.searches.append(body)
        if "ids" in body["query"]:
            return {"hits": {"total": {"value": 1}}}   # 三日 marker 均已存在
        return {"hits": {"total": {"value": 0}}}

    monkeypatch.setattr(cctv.osu, "search_raw", fake_search)

    summary = cctv.run_verify("20200101", "20200102")

    assert env.calls.fetched == []
    assert env.calls.bulk == []
    assert env.calls.alerts == []
    assert "已有 3 日" in summary and "新标记 0 日" in summary


def test_verify_t1_empty_not_marked(env, monkeypatch):
    """T+1 空日不标记（源可能只是晚点），留待下轮 verify。"""
    monkeypatch.setattr(cctv, "get_news_config", lambda: {"verify_days_back": 1})

    summary = cctv.run_verify("20200101", "20200102")

    assert env.calls.fetched == ["20260705"]        # 重采过
    assert env.calls.bulk == []                     # 但不写 marker
    assert env.calls.alerts == []
    assert "待定 1 日" in summary and "新标记 0 日" in summary


def test_verify_search_error_treated_as_no_data(env, monkeypatch):
    """查询异常（如首日别名尚未建 404）→ 视为无数据走重采路径，verify 不炸。"""
    def boom(client, body, index=cctv.osu.ALIAS):
        raise RuntimeError("NotFoundError: no such index [news]")

    monkeypatch.setattr(cctv.osu, "search_raw", boom)
    for d in ("20260703", "20260704", "20260705"):
        env.fetch_map[d] = _df([{"date": d, "title": f"标题{d}", "content": "C"}])

    summary = cctv.run_verify("20200101", "20200102")

    assert "补采 3 日" in summary


def test_verify_day_error_isolated_then_raises(env, monkeypatch):
    """逐日异常隔离：某日处理抛错 → 后续日仍被处理；末尾 raise RuntimeError
    保留框架"任务失败"通知语义，异常消息即摘要（含失败日期，可断言）。"""
    real_collect = cctv._collect_day

    def flaky_collect(run_date, client=None, name_dict=None):
        if run_date == "20260704":
            raise RuntimeError("bulk 写入失败")
        return real_collect(run_date, client=client, name_dict=name_dict)

    monkeypatch.setattr(cctv, "_collect_day", flaky_collect)
    for d in ("20260703", "20260705"):
        env.fetch_map[d] = _df([{"date": d, "title": f"标题{d}", "content": "C"}])

    with pytest.raises(RuntimeError) as excinfo:
        cctv.run_verify("20200101", "20200102")

    msg = str(excinfo.value)
    assert env.calls.fetched == ["20260703", "20260705"]   # 0704 抛错后 0705 仍被处理
    assert "补采 2 日" in msg                               # 摘要通过异常消息可见
    assert "失败 1 日" in msg and "20260704" in msg         # 失败计数 + 日期列表


# ---------- run_backfill ----------

def test_backfill_iterates_days_with_rate_limit(env):
    for d in ("20260701", "20260702", "20260703"):
        env.fetch_map[d] = _df([{"date": d, "title": f"标题{d}", "content": "C"}])

    summary = cctv.run_backfill("20260701", "20260703")

    assert env.calls.fetched == ["20260701", "20260702", "20260703"]
    assert env.calls.sleeps == [cctv._BACKFILL_SLEEP_SECONDS] * 2   # 日间限速，首日不 sleep
    assert "成功 3 日" in summary


def test_backfill_clamps_start_before_akshare_min(env):
    summary = cctv.run_backfill("20160130", "20160204")

    assert env.calls.fetched == ["20160203", "20160204"]   # 20160130~0202 被 clamp
    assert "20160203~20160204" in summary


def test_backfill_single_day_error_continues(env):
    env.fetch_map["20260701"] = _df(
        [{"date": "20260701", "title": "标题", "content": "C"}])
    env.fetch_map["20260702"] = RuntimeError("akshare 接口抽风")
    # 20260703 不在 fetch_map → 空日

    summary = cctv.run_backfill("20260701", "20260703")

    assert env.calls.fetched == ["20260701", "20260702", "20260703"]  # 异常日后继续
    assert "成功 1 日" in summary and "空 1 日" in summary and "失败 1 日" in summary
    assert "20260702" in summary                  # 摘要含失败日期列表（截断展示）


def test_run_skips_when_source_disabled(monkeypatch):
    """注册表 kill-switch（二期）：cctv enabled=false → run() no-op 跳过。"""
    monkeypatch.setattr(cctv.source_registry, "is_enabled", lambda sid: False)
    result = cctv.run()
    assert "禁用" in result and "跳过" in result
