"""news_stock 单测：全 mock——不触网(akshare)、不连 OpenSearch/PG、不碰归档盘。

覆盖：信封契约（_id=sha1(url)、channel=stock、source=em_stock、content=清洗摘要、
stocks 聚合并集、vec/body_status=pending、剥 raw_*）；sweep 逐码隔离 + 跨码去重 +
stocks 并集；run 汇总/全失败 raise；run_verify 自然日归档重放 + 逐日隔离。
"""

import datetime
import hashlib
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from data_collect.jobs import news_stock as ns


def _entry(row, stocks):
    return {"row": row, "stocks": set(stocks)}


def _row(**over):
    r = {
        "关键词": "600519", "新闻标题": "白酒板块领涨",
        "新闻内容": "贵州茅台(600519.SH)、五粮液(000858.SZ)涨幅靠前。",
        "发布时间": "2026-07-06 10:00:00", "文章来源": "证券时报网",
        "新闻链接": "http://finance.example.com/a/1",
    }
    r.update(over)
    return r


def test_entry_to_envelope_basic():
    env = ns._entry_to_envelope(
        "http://finance.example.com/a/1",
        _entry(_row(), {"600519.SH", "000858.SZ"}),
        fetch_time="2026-07-06 22:00:00", name_dict={},
    )
    assert env["_id"] == hashlib.sha1(b"http://finance.example.com/a/1").hexdigest()
    assert env["channel"] == "stock" and env["source"] == "em_stock"
    assert env["title"] == "白酒板块领涨"
    assert "贵州茅台" in env["content"]                       # 清洗后摘要
    assert env["pub_time"] == "2026-07-06 10:00:00"
    assert env["stocks"] == ["000858.SZ", "600519.SH"]        # 排序并集
    assert env["url"] == "http://finance.example.com/a/1"
    assert env["vec_status"] == "pending" and env["body_status"] == "pending"
    assert json.loads(env["raw_content"])["文章来源"] == "证券时报网"   # 原始记录存归档


def test_entry_to_envelope_missing_pub_time_falls_back():
    env = ns._entry_to_envelope(
        "http://x/2", _entry(_row(发布时间=""), {"600519.SH"}),
        "2026-07-06 22:00:00", name_dict={})
    assert env["pub_time"] == "2026-07-06 22:00:00"           # 回退 fetch_time
    assert env["time_estimated"] is True


def test_entry_to_envelope_tags_on_cleaned_text():
    """回归锚点（审核#2）：打标必须在 clean_text 后的文本上做——词典键是 NFKC+t2s
    归一化形态（load_name_dict 对齐清洗后标题），原始文本含繁体/全角时在 raw 上
    永远打不中；create-only 不可变使漏标永久化。"""
    row = _row(新闻标题="乾照光电发布业绩预告", 新闻内容="内容无代码")   # 繁体 乾
    env = ns._entry_to_envelope(
        "http://x/3", _entry(row, set()),
        "2026-07-06 22:00:00", name_dict={"干照光电": "300102.SZ"})     # 词典键为 t2s 后形态
    assert env["stocks"] == ["300102.SZ"]                     # 清洗(乾→干)后命中


def test_entry_to_envelope_serializes_numpy_pandas_types():
    """回归锚点（审核#6）：akshare 未来把列变数值/时间 dtype 时，row 含 np.int64/
    Timestamp——json.dumps 必须 default=str 兜底，否则 55 分钟全量扫完后在信封化
    一步 TypeError，整批不可回补地丢失。"""
    import numpy as np
    row = _row(阅读量=np.int64(12345), 发布时间=pd.Timestamp("2026-07-06 10:00:00"))
    env = ns._entry_to_envelope("http://x/5", _entry(row, {"600519.SH"}),
                                "2026-07-06 22:00:00", name_dict={})
    assert env is not None                                    # 不抛 TypeError
    assert json.loads(env["raw_content"])["阅读量"] == "12345"   # default=str 兜底
    assert env["pub_time"] == "2026-07-06 10:00:00"           # Timestamp 正常解析


def test_entry_to_envelope_stocks_union_queried_and_tagged():
    """stocks = 被查码 ∪ 清洗后文本打标（摘要内代码由正则命中）。"""
    row = _row(新闻标题="白酒消息", 新闻内容="五粮液(000858.SZ)公告")
    env = ns._entry_to_envelope(
        "http://x/4", _entry(row, {"600519.SH"}),             # 被查码只有茅台
        "2026-07-06 22:00:00", name_dict={})
    assert env["stocks"] == ["000858.SZ", "600519.SH"]        # 文本正则补上五粮液


# ---------- _sweep：逐码取数 + 聚合 ----------

def _df(rows):
    return pd.DataFrame(rows, columns=list(_row().keys()))


@pytest.fixture
def swept(monkeypatch):
    """打桩 _fetch_stock_news / time.sleep，记录调用。"""
    calls = SimpleNamespace(fetched=[], sleeps=[])
    fetch_map = {}

    def fake_fetch(code):
        calls.fetched.append(code)
        v = fetch_map.get(code)
        if isinstance(v, list):          # 按调用次序消费（模拟首轮失败→重试成功）
            v = v.pop(0)
        if isinstance(v, Exception):
            raise v
        return v

    monkeypatch.setattr(ns, "_fetch_stock_news", fake_fetch)
    monkeypatch.setattr(ns.time, "sleep", lambda s: calls.sleeps.append(s))
    return SimpleNamespace(calls=calls, fetch_map=fetch_map)


def test_sweep_aggregates_queried_codes_and_dedups(swept):
    u1 = "http://x/1"      # 同一篇文章，600519 与 000858 两票都返回（跨码重复）
    u2 = "http://x/2"      # 仅 000858 返回
    swept.fetch_map["600519"] = _df([_row(新闻链接=u1)])
    swept.fetch_map["000858"] = _df([
        _row(新闻链接=u1, 关键词="000858"),
        _row(新闻链接=u2, 新闻标题="五粮液单独消息", 新闻内容="五粮液(000858.SZ)公告"),
    ])

    by_url, failed = ns._sweep(["600519", "000858"])

    assert set(by_url) == {u1, u2}                   # 按 url 去重
    # sweep 只聚合被查码（文本打标延后到信封化，在清洗后文本上做）
    assert by_url[u1]["stocks"] == {"600519.SH", "000858.SZ"}
    assert by_url[u2]["stocks"] == {"000858.SZ"}
    assert failed == 0
    assert swept.calls.fetched == ["600519", "000858"]
    assert swept.calls.sleeps == [ns._PACE_SECONDS] * 2            # 逐码限速


def test_sweep_missing_columns_skips_code(swept):
    swept.fetch_map["600519"] = pd.DataFrame([{"新闻标题": "缺列"}])   # 缺必需列
    by_url, failed = ns._sweep(["600519"])
    assert by_url == {} and failed == 1


def test_sweep_retries_failed_code_and_recovers(swept):
    """审核#5 缓解：首轮失败码整轮结束后重试一次——瞬时故障恢复则文章标的当轮聚齐，
    避免残缺 stocks[] 入库后被 create-only 409 永久锁死。"""
    u1 = "http://x/1"
    swept.fetch_map["600519"] = [KeyError("瞬时网络"), _df([_row(新闻链接=u1)])]
    by_url, failed = ns._sweep(["600519"])
    assert failed == 0                                    # 重试成功不计失败
    assert set(by_url) == {u1}                            # 文章找回
    assert swept.calls.fetched == ["600519", "600519"]    # 首轮 + 重试各一次
    assert swept.calls.sleeps == [ns._PACE_SECONDS] * 2   # 重试同样限速


def test_sweep_permanent_failure_counts_after_retry(swept):
    swept.fetch_map["831526"] = KeyError("废码")           # 每次都抛 → 重试仍失败
    by_url, failed = ns._sweep(["831526"])
    assert failed == 1
    assert swept.calls.fetched == ["831526", "831526"]    # 恰重试一次，不无限循环


# ---------- 归档/入库辅助 ----------

@pytest.fixture
def store_env(monkeypatch):
    calls = SimpleNamespace(bulk=[], archived=[], alerts=[])
    monkeypatch.setattr(ns.news_archive, "load_ids", lambda source, date: set())
    monkeypatch.setattr(ns.news_archive, "append",
                        lambda s, d, evs: calls.archived.append((s, d, list(evs))) or len(list(evs)))
    monkeypatch.setattr(ns.osu, "get_client", lambda: object())
    monkeypatch.setattr(ns.osu, "bulk_create",
                        lambda c, docs: (calls.bulk.append(list(docs)), (len(docs), 0))[1])
    monkeypatch.setattr(ns, "send_dingtalk", lambda m: calls.alerts.append(m))
    return calls


def _env(_id, day, stocks=("600519.SH",)):
    return {"_id": _id, "pub_time": f"{day} 10:00:00", "title": "t", "content": "c",
            "raw_title": "T", "raw_content": "{}", "source": "em_stock",
            "channel": "stock", "stocks": list(stocks), "url": f"http://x/{_id}",
            "vec_status": "pending", "body_status": "pending"}


def test_strip_raw_removes_archive_only_keys():
    doc = ns._strip_raw({"_id": "x", "title": "t", "raw_title": "T",
                         "raw_content": "{}", "time_estimated": True})
    assert doc == {"_id": "x", "title": "t"}


def test_archive_envelopes_groups_by_pub_day(store_env):
    envs = [_env("a", "2026-07-06"), _env("b", "2026-07-05"), _env("c", "2026-07-06")]
    n = ns._archive_envelopes(envs)
    assert n == 3
    days = sorted(d for _, d, _ in store_env.archived)
    assert days == ["20260705", "20260706"]        # 按 pub_time 日分组归档


def test_store_strips_raw_and_counts(store_env):
    ok, dup = ns._store([_env("a", "2026-07-06")])
    assert (ok, dup) == (1, 0)
    assert all("raw_title" not in d and "raw_content" not in d for d in store_env.bulk[0])


def test_load_name_dict_degrades(monkeypatch):
    calls = []
    monkeypatch.setattr(ns.nn, "load_name_dict",
                        lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    monkeypatch.setattr(ns, "send_dingtalk", lambda m: calls.append(m))
    assert ns._load_name_dict() == {}              # DB 挂降级空 dict
    assert calls                                    # 且告警


# ---------- run ----------

@pytest.fixture
def run_env(monkeypatch, swept, store_env):
    """在 swept + store_env 基础上补 run 需要的桩。"""
    monkeypatch.setattr(ns, "_flush_spool_safe", lambda: None)
    monkeypatch.setattr(ns.nn, "load_a_share_codes", lambda: ["600519", "000858"])
    monkeypatch.setattr(ns, "_load_name_dict", lambda: {})
    return SimpleNamespace(swept=swept, store=store_env)


def test_run_sweeps_aggregates_stores(run_env):
    u1 = "http://x/1"
    run_env.swept.fetch_map["600519"] = _df([_row(新闻链接=u1)])
    run_env.swept.fetch_map["000858"] = _df([_row(新闻链接=u1, 关键词="000858")])
    msg = ns.run()
    # 两票同一 url → 1 篇唯一文章，stocks 并集
    stored = run_env.store.bulk[0]
    assert len(stored) == 1
    assert stored[0]["stocks"] == ["000858.SZ", "600519.SH"]
    assert "唯一文章 1" in msg and "入库新增 1" in msg and "扫 2 票" in msg


def test_run_raises_when_all_codes_fail(run_env):
    run_env.swept.fetch_map["600519"] = KeyError("x")
    run_env.swept.fetch_map["000858"] = KeyError("y")
    with pytest.raises(RuntimeError, match="全部失败"):
        ns.run()


def test_run_alerts_on_high_failure_ratio(run_env):
    """失败率超阈值（非全挂）→ 钉钉告警但任务成功（部分失败日不再静默通过，
    审核#5：残缺 stocks[] 的文章无法事后补标，运维需要知道当晚有缺口）。"""
    u1 = "http://x/1"
    run_env.swept.fetch_map["600519"] = _df([_row(新闻链接=u1)])
    run_env.swept.fetch_map["000858"] = KeyError("持续失败")     # 1/2 = 50% > 5%
    msg = ns.run()
    assert "失败 1" in msg                                       # 任务照常成功
    assert any("失败率" in a for a in run_env.store.alerts)      # 但发了告警


# ---------- run_verify ----------

def test_verify_replays_archive_natural_days(monkeypatch):
    replayed = {}
    monkeypatch.setattr(ns, "_today", lambda: datetime.date(2026, 7, 6))
    monkeypatch.setattr(ns, "get_news_config", lambda: {"verify_days_back": 2})
    monkeypatch.setattr(ns.osu, "get_client", lambda: object())
    monkeypatch.setattr(ns.osu, "bulk_create", lambda c, docs: (len(list(docs)), 0))

    def fake_replay(source, window):
        replayed[window[0]] = replayed.get(window[0], 0) + 1
        return [_env("a", "2026-07-06")]        # 每日归档 1 条

    monkeypatch.setattr(ns.news_archive, "replay", fake_replay)
    msg = ns.run_verify("19000101", "19000102")     # 框架窗口被忽略
    # verify_days_back=2, today=07-06 → [07-04,07-05,07-06]（含今天）
    assert set(replayed) == {"20260704", "20260705", "20260706"}
    assert "重放" in msg and "失败 0" in msg


def test_verify_per_day_isolation_raises(monkeypatch):
    monkeypatch.setattr(ns, "_today", lambda: datetime.date(2026, 7, 6))
    monkeypatch.setattr(ns, "get_news_config", lambda: {"verify_days_back": 1})
    monkeypatch.setattr(ns.osu, "get_client", lambda: object())
    monkeypatch.setattr(ns.osu, "bulk_create", lambda c, docs: (len(list(docs)), 0))

    def flaky(source, window):
        if window[0] == "20260705":
            raise OSError("archive boom")
        return [_env("a", "2026-07-06")]

    monkeypatch.setattr(ns.news_archive, "replay", flaky)
    with pytest.raises(RuntimeError, match="20260705"):
        ns.run_verify("", "")


def test_run_skips_when_source_disabled(monkeypatch):
    """注册表 kill-switch（二期）：em_stock enabled=false → run() no-op 跳过。"""
    monkeypatch.setattr(ns.source_registry, "is_enabled", lambda sid: False)
    result = ns.run()
    assert "禁用" in result and "跳过" in result
