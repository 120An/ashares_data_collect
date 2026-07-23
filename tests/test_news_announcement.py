"""news_announcement 单测：全 mock——不触网、不连 OpenSearch、不碰归档盘。

覆盖：信封契约（_id=cninfo-{annId}、channel/source、content=title、pub_time
epoch→北京、stocks 取权威 secCode、url 拼接、vec_status/pdf_status pending、
raw_content 存原始记录 JSON、入库剥 raw_*）；缺 id 丢弃；缺 pub_time 回退+标记；
run 空日；批内去重；run_verify 自然日窗口忽略入参 + 逐日隔离 + failed>0 raise。
"""

import datetime
import json
from types import SimpleNamespace

import pytest

from data_collect.jobs import news_announcement as ann
from data_collect.utils import cninfo


def _rec(**over):
    """构造一条巨潮公告原始记录。"""
    rec = {
        "announcementId": "1225412498",
        "announcementTitle": "关于使用部分自有资金进行现金管理的进展公告",
        "adjunctUrl": "finalpage/2026-07-06/1225412498.pdf",
        "announcementTime": 1783342968000,     # epoch ms
        "secCode": "301191", "secName": "菲菱科思", "adjunctType": "PDF",
    }
    rec.update(over)
    return rec


def test_record_to_envelope_basic():
    env = ann._record_to_envelope(_rec(), fetch_time="2026-07-06 21:30:00")
    assert env["_id"] == "cninfo-1225412498"
    assert env["source"] == "cninfo" and env["channel"] == "announcement"
    assert env["title"] == "关于使用部分自有资金进行现金管理的进展公告"
    assert env["content"] == env["title"]                       # Phase1 无正文
    assert env["pub_time"] == "2026-07-06 21:02:48"             # epoch ms → 北京
    assert env["stocks"] == ["301191.SZ"]                       # 权威 secCode
    assert env["url"] == "http://static.cninfo.com.cn/finalpage/2026-07-06/1225412498.pdf"
    assert env["vec_status"] == "pending" and env["pdf_status"] == "pending"
    # raw_content 存完整原始记录 JSON（供 Phase2 回填 announcementType 等）
    assert json.loads(env["raw_content"])["secCode"] == "301191"
    assert "ann_type" not in env                                # Phase1 不填（category 现 null）


def test_record_to_envelope_missing_id_returns_none():
    assert ann._record_to_envelope(_rec(announcementId=""), "2026-07-06 21:30:00") is None
    assert ann._record_to_envelope(_rec(announcementId=None), "2026-07-06 21:30:00") is None


def test_record_to_envelope_beijing_stock():
    env = ann._record_to_envelope(_rec(secCode="920001"), "2026-07-06 21:30:00")
    assert env["stocks"] == ["920001.BJ"]


def test_record_to_envelope_bad_seccode_empty_stocks():
    env = ann._record_to_envelope(_rec(secCode="199999"), "2026-07-06 21:30:00")
    assert env["stocks"] == []                                  # 号段推不出 → 空


def test_record_to_envelope_missing_pub_time_falls_back():
    env = ann._record_to_envelope(_rec(announcementTime=None), "2026-07-06 21:30:00")
    assert env["pub_time"] == "2026-07-06 21:30:00"             # 回退 fetch_time
    assert env["time_estimated"] is True


# ---------- _collect_day / _strip_raw ----------

@pytest.fixture
def env(monkeypatch):
    """全隔离：cninfo/归档/OpenSearch/钉钉/时钟全打桩并记录调用。"""
    calls = SimpleNamespace(fetched=[], bulk=[], archived=[], alerts=[], flushes=[])
    e = SimpleNamespace(calls=calls, fetch_map={})

    def fake_fetch(date):
        # _collect_day 传 ISO("2026-07-06")；fetch_map 按 date8 建键便于测试，故剥横线查表
        calls.fetched.append(date)
        value = e.fetch_map.get(date.replace("-", ""))
        if isinstance(value, Exception):
            raise value
        return value if value is not None else []

    def fake_append(source, date, envelopes):
        envelopes = list(envelopes)
        calls.archived.append((source, date, envelopes))
        return len(envelopes)

    def fake_bulk_create(client, docs):
        docs = list(docs)
        calls.bulk.append(docs)
        return len(docs), 0

    monkeypatch.setattr(ann.cninfo, "fetch_announcements", fake_fetch)
    monkeypatch.setattr(ann, "_flush_spool_safe", lambda: calls.flushes.append(1))
    monkeypatch.setattr(ann.news_archive, "load_ids", lambda source, date: set())
    monkeypatch.setattr(ann.news_archive, "append", fake_append)
    monkeypatch.setattr(ann.osu, "get_client", lambda: object())
    monkeypatch.setattr(ann.osu, "bulk_create", fake_bulk_create)
    monkeypatch.setattr(ann, "send_dingtalk", lambda msg: calls.alerts.append(msg))
    monkeypatch.setattr(ann, "_today", lambda: datetime.date(2026, 7, 6))
    monkeypatch.setattr(ann, "get_news_config", lambda: {"verify_days_back": 3})
    return e


def test_strip_raw_removes_archive_only_keys():
    doc = ann._strip_raw({"_id": "x", "title": "t", "raw_title": "T",
                          "raw_content": "{}", "time_estimated": True})
    assert doc == {"_id": "x", "title": "t"}


def test_collect_day_archives_and_stores(env):
    env.fetch_map["20260706"] = [_rec(announcementId="1"), _rec(announcementId="2")]
    total, ok, dup, archived, dropped = ann._collect_day("20260706")
    assert (total, ok, dup, archived, dropped) == (2, 2, 0, 2, 0)
    # 入库 doc 剥了 raw_*（防动态 mapping 长计划外字段）
    stored = env.calls.bulk[0]
    assert all("raw_title" not in d and "raw_content" not in d for d in stored)
    assert env.calls.archived[0][0] == "cninfo"


def test_collect_day_dedups_within_batch(env):
    env.fetch_map["20260706"] = [_rec(announcementId="9"), _rec(announcementId="9")]
    total, ok, dup, archived, dropped = ann._collect_day("20260706")
    assert total == 1                              # 同 announcementId 批内去重保首见


def test_collect_day_drops_records_missing_id(env):
    env.fetch_map["20260706"] = [_rec(announcementId="1"), _rec(announcementId="")]
    total, ok, dup, archived, dropped = ann._collect_day("20260706")
    assert (total, dropped) == (1, 1)


def test_collect_day_empty_source(env):
    env.fetch_map["20260706"] = []
    assert ann._collect_day("20260706") == (0, 0, 0, 0, 0)
    assert env.calls.bulk == [] and env.calls.archived == []


# ---------- run ----------

def test_run_default_today(env):
    env.fetch_map["20260706"] = [_rec(announcementId="1")]
    msg = ann.run()
    assert env.calls.fetched == ["2026-07-06"]     # 缺省今天（_today 已 patch 到 07-06）
    assert env.calls.flushes == [1]                # run 起始 flush spool
    assert "源 1 条" in msg and "入库新增 1" in msg


def test_run_explicit_date(env):
    env.fetch_map["20260703"] = [_rec(announcementId="1"), _rec(announcementId="2")]
    msg = ann.run("20260703")
    assert env.calls.fetched == ["2026-07-03"]
    assert "源 2 条" in msg


def test_run_accepts_iso_date(env):
    """回归锚点（审核#4）：run_job.py 帮助文本明示支持 YYYY-MM-DD——不归一化则
    切片拼出垃圾 seDate，任务以「源返回 0 条」静默成功，操作员误以为当日已采。"""
    env.fetch_map["20260703"] = [_rec(announcementId="1")]
    msg = ann.run("2026-07-03")
    assert env.calls.fetched == ["2026-07-03"]                # 归一化后正确取数
    assert "源 1 条" in msg


def test_run_rejects_garbage_date(env):
    with pytest.raises(ValueError, match="YYYYMMDD"):
        ann.run("garbage")
    with pytest.raises(ValueError, match="YYYYMMDD"):
        ann.run("2026/07/03")                                 # 斜杠格式不支持，显式报错


def test_run_empty_day(env):
    env.fetch_map["20260706"] = []
    msg = ann.run()
    assert "0 条" in msg
    assert env.calls.bulk == []


def test_run_propagates_fetch_error(env):
    env.fetch_map["20260706"] = cninfo.CninfoError("blocked")
    with pytest.raises(cninfo.CninfoError):          # 单源失败上抛 → 框架 retry
        ann.run()


# ---------- run_verify ----------

def test_verify_ignores_framework_window_natural_days(env, monkeypatch):
    """核心回归锚点：**显式传入的框架交易日窗口被忽略**，只由 verify_days_back 定窗。"""
    seen = []
    monkeypatch.setattr(ann, "_collect_day",
                        lambda d, client=None: seen.append(d) or (1, 1, 0, 1, 0))
    ann.run_verify("19000101", "19000102")         # 乱传窗口，应被忽略
    # verify_days_back=3, today=2026-07-06 → [07-03, 07-04, 07-05]（不含今天）
    assert seen == ["20260703", "20260704", "20260705"]


def test_verify_success_summary(env, monkeypatch):
    monkeypatch.setattr(ann, "_collect_day", lambda d, client=None: (2, 2, 0, 2, 0))
    msg = ann.run_verify("", "")
    assert "重采" in msg and "失败 0 日" in msg


def test_verify_per_day_isolation_raises(env, monkeypatch):
    def flaky(d, client=None):
        if d == "20260704":
            raise cninfo.CninfoError("day boom")
        return (1, 1, 0, 1, 0)
    monkeypatch.setattr(ann, "_collect_day", flaky)
    with pytest.raises(RuntimeError, match="20260704"):    # 失败日隔离但末尾 raise
        ann.run_verify("", "")


def test_run_skips_when_source_disabled(monkeypatch):
    """注册表 kill-switch（二期）：cninfo enabled=false → run() no-op 跳过。"""
    monkeypatch.setattr(ann.source_registry, "is_enabled", lambda sid: False)
    result = ann.run()
    assert "禁用" in result and "跳过" in result
