"""news_flash 单元测试：全 mock——不触网、不连 OpenSearch/PG、不碰归档盘。

覆盖（计划 T7 + 设计决策）：
- 双活合并：两源信封按源分目录归档、合并入库、摘要含各源窗口条数（回填架构
  §4.2 的数据可见性）；
- 信封契约与 `_id` 决定论：em 走二级 sha1(url)，cls 走三级 sha1(source|pub_time|
  title)（含源语义），同输入重跑同 `_id`；批内重复行只入一条；归档收含 raw_*
  的完整信封、入库 doc 剥净 raw_*/time_estimated；
- 源级隔离：单源异常仍成功（em 照常入库、摘要计失败源、不 raise）；
  双源全挂 → RuntimeError（框架 retry）；
- 溢出检测：某源整窗全新 + 已归档集合非空 → 告警一次 + 摘要标记；
  冷启动（涉及各日归档全空）豁免不告警；
- 断流状态机：某源连续 _SILENCE_ROUNDS 轮 0 条新数据 → 恰在第 N 轮发一条告警、
  再多轮不重发；恢复轮发恢复通知并复位（状态文件真实落 tmp_path，跨调用持久）；
- 跨午夜：滚动窗 pub_time 分属两日 → 逐 (source, day) load_ids/append；
- pub_time 解析失败：信封回退 fetch_time + time_estimated 只进归档（入库剥除），
  `_id` 不受影响仍决定论；
- verify：窗口 [today-N, today] 自然日**含今天**、忽略框架传入的交易日窗口、
  重放 doc 剥净、409 幂等计数、单 (source,day) 异常隔离 + 末尾 raise；
- 失败传播：run 中 bulk_create 抛错必须上抛（框架 retry 契约回归锁，同 T6）。
"""

import datetime
import hashlib
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from data_collect.jobs import news_flash as flash
from data_collect.utils import news_normalize as nn

_TODAY = datetime.date(2026, 7, 6)
_DAY = datetime.date(2026, 7, 3)


def _cls_df(rows):
    """构造 stock_info_global_cls 形状：标题/内容/发布日期(date对象)/发布时间(time对象)。"""
    return pd.DataFrame(rows, columns=["标题", "内容", "发布日期", "发布时间"])


def _cls_row(title, content="电报正文", day=_DAY, tm=datetime.time(9, 15, 30)):
    return {"标题": title, "内容": content, "发布日期": day, "发布时间": tm}


def _em_df(rows):
    """构造 stock_info_global_em 形状：标题/摘要/发布时间(字符串)/链接。"""
    return pd.DataFrame(rows, columns=["标题", "摘要", "发布时间", "链接"])


def _em_row(title, url, summary="快讯摘要", pub="2026-07-03 09:20:00"):
    return {"标题": title, "摘要": summary, "发布时间": pub, "链接": url}


def _sha1(text):
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


@pytest.fixture
def env(monkeypatch, tmp_path):
    """全隔离环境：两源/OpenSearch/归档/钉钉/词典/时钟全打桩并记录调用。

    cls_data/em_data: DataFrame|Exception|None(None 即空窗口)；
    archived_ids: {(source, date8): set} 控制 load_ids 返回；
    replay_map: {(source, date8): list|Exception} 控制 verify 归档重放。
    断流状态文件真实落 tmp_path（spool_dir 指向之），多轮模拟靠它跨调用持久。
    """
    calls = SimpleNamespace(bulk=[], archived=[], alerts=[], flushes=[],
                            load_ids=[], replays=[])
    e = SimpleNamespace(calls=calls, cls_data=None, em_data=None,
                        archived_ids={}, replay_map={}, tmp_path=tmp_path)

    def fake_fetch_cls():
        if isinstance(e.cls_data, Exception):
            raise e.cls_data
        return e.cls_data if e.cls_data is not None else pd.DataFrame()

    def fake_fetch_em():
        if isinstance(e.em_data, Exception):
            raise e.em_data
        return e.em_data if e.em_data is not None else pd.DataFrame()

    def fake_flush_spool():
        calls.flushes.append(1)
        return 0

    def fake_load_ids(source, date):
        calls.load_ids.append((source, date))
        return set(e.archived_ids.get((source, date), set()))

    def fake_append(source, date, envelopes):
        envelopes = list(envelopes)
        calls.archived.append((source, date, envelopes))
        return len(envelopes)

    def fake_replay(source, date_range):
        calls.replays.append((source, date_range))
        value = e.replay_map.get((source, date_range[0]), [])
        if isinstance(value, Exception):
            raise value
        return iter(value)

    def fake_bulk_create(client, docs):
        calls.bulk.append(list(docs))
        return len(docs), 0

    monkeypatch.setattr(flash, "_fetch_cls", fake_fetch_cls)
    monkeypatch.setattr(flash, "_fetch_em", fake_fetch_em)
    monkeypatch.setattr(flash, "_today", lambda: _TODAY)
    monkeypatch.setattr(flash, "get_news_config",
                        lambda: {"verify_days_back": 2, "spool_dir": str(tmp_path)})
    monkeypatch.setattr(flash, "send_dingtalk", lambda msg: calls.alerts.append(msg))
    monkeypatch.setattr(flash.news_archive, "flush_spool", fake_flush_spool)
    monkeypatch.setattr(flash.news_archive, "load_ids", fake_load_ids)
    monkeypatch.setattr(flash.news_archive, "append", fake_append)
    monkeypatch.setattr(flash.news_archive, "replay", fake_replay)
    monkeypatch.setattr(flash.osu, "get_client", lambda: object())
    monkeypatch.setattr(flash.osu, "bulk_create", fake_bulk_create)
    monkeypatch.setattr(flash.nn, "load_name_dict", lambda: {"贵州茅台": "600519.SH"})
    return e


# ---------- run：双活合并与信封契约 ----------

def test_run_dual_source_merge_and_envelope_contract(env):
    env.cls_data = _cls_df([
        _cls_row("贵州茅台电报<b>一</b>"),                       # 含 HTML：验证清洗
        _cls_row("电报二", tm=datetime.time(10, 0, 0)),
    ])
    env.em_data = _em_df([
        _em_row("东财快讯一", "https://finance.eastmoney.com/a/1.html"),
        _em_row("东财快讯二", "https://finance.eastmoney.com/a/2.html",
                pub="2026-07-03 09:25:00"),
    ])

    summary = flash.run()

    # 归档按源分目录 append（cls/em 各一次，各 2 条**含 raw_* 的完整信封**）
    assert [(s, d, len(envs)) for s, d, envs in env.calls.archived] == \
        [("cls", "20260703", 2), ("em", "20260703", 2)]
    cls_envs = env.calls.archived[0][2]
    assert cls_envs[0]["raw_title"] == "贵州茅台电报<b>一</b>"   # 原文进归档
    assert cls_envs[0]["title"] == "贵州茅台电报一"              # clean 后版本并存

    # bulk 收全部 4 条（两源合并；入库幂等由 create-only 409→dup 保证）
    assert len(env.calls.bulk) == 1
    docs = env.calls.bulk[0]
    assert len(docs) == 4
    for doc in docs:
        assert doc["channel"] == "flash"
        assert doc["vec_status"] == "pending"
        # 入库 doc 剥除只进归档的键（防动态 mapping 长出计划外字段）
        assert "raw_title" not in doc and "raw_content" not in doc
        assert "time_estimated" not in doc

    cls_docs = [d for d in docs if d["source"] == "cls"]
    em_docs = [d for d in docs if d["source"] == "em"]
    assert len(cls_docs) == 2 and len(em_docs) == 2
    assert all("url" not in d for d in cls_docs)                # cls 无 url 不写空键
    assert em_docs[0]["url"] == "https://finance.eastmoney.com/a/1.html"
    assert cls_docs[0]["pub_time"] == "2026-07-03 09:15:30"     # date+time 原生对象组合
    assert em_docs[0]["pub_time"] == "2026-07-03 09:20:00"
    assert cls_docs[0]["stocks"] == ["600519.SH"]               # 清洗后 title 词典打标

    # 摘要含两源窗口条数与入库计数
    assert "cls 2条(新2)" in summary and "em 2条(新2)" in summary
    assert "入库新增 4" in summary and "重复 0" in summary
    assert env.calls.flushes == [1]                             # run 开头搬运 spool


def test_id_determinism_em_url_cls_composite(env):
    url = "https://finance.eastmoney.com/a/xyz.html"
    env.cls_data = _cls_df([_cls_row("电报标题")])
    env.em_data = _em_df([_em_row("东财标题", url)])

    flash.run()

    docs = env.calls.bulk[0]
    em_doc = next(d for d in docs if d["source"] == "em")
    cls_doc = next(d for d in docs if d["source"] == "cls")
    assert em_doc["_id"] == _sha1(url)                          # 二级 sha1(url)
    # cls 无 url/native_id → 三级 sha1(source|pub_time|title)，source 语义参与拼串
    assert cls_doc["_id"] == _sha1("cls|2026-07-03 09:15:30|电报标题")
    assert cls_doc["_id"] == nn.make_id(
        {"source": "cls", "pub_time": "2026-07-03 09:15:30",
         "title": "电报标题", "content": "电报正文"})

    # 同输入重跑 → 同 _id（决定论；create-only 幂等的根基）
    first_ids = sorted(d["_id"] for d in docs)
    env.calls.bulk.clear()
    flash.run()
    assert sorted(d["_id"] for d in env.calls.bulk[0]) == first_ids


def test_batch_internal_dedup_keeps_first(env):
    env.em_data = _em_df([
        _em_row("同一条", "https://finance.eastmoney.com/a/dup.html"),
        _em_row("同一条", "https://finance.eastmoney.com/a/dup.html"),  # 滚动窗内重复行
        _em_row("另一条", "https://finance.eastmoney.com/a/other.html"),
    ])

    summary = flash.run()

    assert len(env.calls.bulk[0]) == 2            # 批内重复行只入一条（保首见）
    _, _, archived = env.calls.archived[0]
    assert len(archived) == 2                     # 归档同样只收一条
    assert "em 2条(新2)" in summary


def test_run_archive_appends_only_new_ids(env):
    """归档 append 只收 load_ids 之外的新 _id；bulk 仍收全量（409→dup 兜幂等）。"""
    url_known = "https://finance.eastmoney.com/a/known.html"
    env.em_data = _em_df([
        _em_row("已归档快讯", url_known),
        _em_row("新快讯", "https://finance.eastmoney.com/a/new.html"),
    ])
    env.archived_ids[("em", "20260703")] = {_sha1(url_known)}

    summary = flash.run()

    _, _, archived = env.calls.archived[0]
    assert [e["title"] for e in archived] == ["新快讯"]   # 只归档新条目
    assert len(env.calls.bulk[0]) == 2                    # 入库仍全量
    assert "em 2条(新1)" in summary


# ---------- 源级隔离 ----------

def test_single_source_failure_isolated(env):
    env.cls_data = RuntimeError("cls 接口抽风")
    env.em_data = _em_df([_em_row("东财快讯", "https://finance.eastmoney.com/a/1.html")])

    summary = flash.run()                          # 单源失败不 raise

    assert len(env.calls.bulk) == 1
    assert [d["source"] for d in env.calls.bulk[0]] == ["em"]   # em 正常入库
    assert "失败源[cls]" in summary
    assert "cls 失败" in summary and "em 1条(新1)" in summary


def test_both_sources_failed_raises(env):
    env.cls_data = RuntimeError("cls 抽风")
    env.em_data = ConnectionError("em 网络故障")

    with pytest.raises(RuntimeError, match="全部源"):
        flash.run()
    assert env.calls.bulk == []


def test_missing_column_counts_source_failed(env):
    """源改版丢列 → 该源计失败（.get 逐行取会静默产出空信封坍缩 _id，比失败更糟）。"""
    env.cls_data = pd.DataFrame([{"标题": "只剩标题"}])   # 缺 内容/发布日期/发布时间
    env.em_data = _em_df([_em_row("东财快讯", "https://finance.eastmoney.com/a/1.html")])

    summary = flash.run()

    assert "失败源[cls]" in summary
    assert [d["source"] for d in env.calls.bulk[0]] == ["em"]


# ---------- 溢出检测 ----------

def test_overflow_alert_when_window_all_new(env):
    env.cls_data = _cls_df([
        _cls_row("全新电报一"),
        _cls_row("全新电报二", tm=datetime.time(10, 0, 0)),
    ])
    env.em_data = _em_df([_em_row("东财旧闻", "https://finance.eastmoney.com/a/1.html")])
    # cls 当日归档非空且与本轮零重叠 → 疑似溢出；em 归档含本轮条目 → 正常
    env.archived_ids[("cls", "20260703")] = {"some-old-archived-id"}
    env.archived_ids[("em", "20260703")] = {_sha1("https://finance.eastmoney.com/a/1.html")}

    summary = flash.run()

    overflow_alerts = [a for a in env.calls.alerts if "溢出" in a]
    assert len(overflow_alerts) == 1 and "cls" in overflow_alerts[0]
    assert "溢出[cls]" in summary
    assert "em 1条(新0)" in summary                # em 全部已归档 → 新 0


def test_overflow_cold_start_exempt(env):
    """冷启动（涉及各日归档全空）整窗必然全新 → 仅记日志，不告警不标记。"""
    env.cls_data = _cls_df([_cls_row("首轮电报")])

    summary = flash.run()

    assert all("溢出" not in a for a in env.calls.alerts)
    assert "溢出" not in summary


# ---------- 断流状态机 ----------

def test_silence_alert_after_n_zero_rounds_then_recovery(env):
    """em 连续 _SILENCE_ROUNDS 轮 0 条新数据 → 恰在第 N 轮发一条告警；
    再多轮不重发；恢复轮发恢复通知并复位（状态文件跨调用持久驱动）。"""
    env.cls_data = _cls_df([_cls_row("电报")])     # cls 正常，保双源不全挂
    env.em_data = None                             # em 持续空窗口

    for _ in range(flash._SILENCE_ROUNDS - 1):
        flash.run()
    assert [a for a in env.calls.alerts if "疑似断流" in a] == []   # 第 N-1 轮尚未告警

    flash.run()                                    # 第 N 轮 → 告警恰好一条
    silence = [a for a in env.calls.alerts if "疑似断流" in a]
    assert len(silence) == 1 and "em" in silence[0]

    flash.run()                                    # 第 N+1 轮不重发
    assert len([a for a in env.calls.alerts if "疑似断流" in a]) == 1

    env.em_data = _em_df([_em_row("恢复了", "https://finance.eastmoney.com/a/r.html")])
    flash.run()                                    # 恢复轮 → 恢复通知 + 状态复位
    recovered = [a for a in env.calls.alerts if "断流恢复" in a]
    assert len(recovered) == 1 and "em" in recovered[0]

    state = json.loads(
        (env.tmp_path / ".flash_source_state.json").read_text(encoding="utf-8"))
    assert state["em"]["consecutive_zero"] == 0
    assert state["em"]["alerted"] is False


def test_silence_keys_off_window_not_archived_new(env):
    """断流判定用窗口条数而非归档新增：某源持续返回**非空**窗口但整窗已归档（0 新）
    → 视为存活，**不**累计断流轮数（回归修复：原按归档新增会把健康源误判断流）。"""
    url = "https://finance.eastmoney.com/a/stale.html"
    env.cls_data = _cls_df([_cls_row("电报")])          # cls 正常，保双源不全挂
    env.em_data = _em_df([_em_row("同一条老快讯", url)])  # em 窗口恒 1 条
    env.archived_ids[("em", "20260703")] = {_sha1(url)}  # 且该条已归档 → 归档新增恒 0

    # 连续多轮（远超阈值）：em 归档新增始终 0，但窗口始终 1 条 → 不应判断流
    for _ in range(flash._SILENCE_ROUNDS + 2):
        summary = flash.run()

    assert "em 1条(新0)" in summary                     # 确属"窗口非空但归档 0 新"情形
    assert [a for a in env.calls.alerts if "疑似断流" in a] == []   # 关键：从不告警
    state = json.loads(
        (env.tmp_path / ".flash_source_state.json").read_text(encoding="utf-8"))
    assert state["em"]["consecutive_zero"] == 0         # 窗口有条目 → 计数器不累计


def test_silence_empty_window_increments_counter(env):
    """真·空窗口（源返回 0 条）确实累计断流计数器——与"窗口非空但 0 新"划清边界。"""
    env.cls_data = _cls_df([_cls_row("电报")])          # cls 正常
    env.em_data = None                                  # em 真空窗口

    rounds = 3
    for _ in range(rounds):
        flash.run()

    state = json.loads(
        (env.tmp_path / ".flash_source_state.json").read_text(encoding="utf-8"))
    assert state["em"]["consecutive_zero"] == rounds    # 空窗口逐轮累计
    assert state["cls"]["consecutive_zero"] == 0        # cls 有窗口条目 → 不累计


def test_silence_failed_source_increments_counter(env):
    """失败源（采集异常，窗口条数计 0）同样累计断流计数器（断流候选）。"""
    env.cls_data = _cls_df([_cls_row("电报")])          # cls 正常，保双源不全挂
    env.em_data = RuntimeError("em 接口抽风")           # em 采集失败

    rounds = 3
    for _ in range(rounds):
        summary = flash.run()

    assert "失败源[em]" in summary
    state = json.loads(
        (env.tmp_path / ".flash_source_state.json").read_text(encoding="utf-8"))
    assert state["em"]["consecutive_zero"] == rounds    # 失败源逐轮累计
    assert state["cls"]["consecutive_zero"] == 0


# ---------- 跨午夜分组 ----------

def test_cross_midnight_groups_by_source_day(env):
    env.cls_data = _cls_df([
        _cls_row("午夜前", day=datetime.date(2026, 7, 2), tm=datetime.time(23, 59, 0)),
        _cls_row("午夜后", day=datetime.date(2026, 7, 3), tm=datetime.time(0, 1, 0)),
    ])

    flash.run()

    # 两日各查一次归档 _id、各 append 一次（(source, day) 粒度）
    cls_loads = [(s, d) for s, d in env.calls.load_ids if s == "cls"]
    assert cls_loads == [("cls", "20260702"), ("cls", "20260703")]
    assert [(s, d, [e["title"] for e in envs]) for s, d, envs in env.calls.archived] == [
        ("cls", "20260702", ["午夜前"]),
        ("cls", "20260703", ["午夜后"]),
    ]


# ---------- pub_time 解析失败回退 ----------

def test_pub_time_parse_failure_falls_back_to_fetch_time(env):
    env.em_data = _em_df([
        _em_row("时间异常快讯", "https://finance.eastmoney.com/a/t.html", pub="待定时间")])
    env.cls_data = _cls_df([
        {"标题": "无时间电报", "内容": "正文", "发布日期": None, "发布时间": None}])

    flash.run()

    # 归档信封：pub_time 回退 fetch_time + time_estimated 标记
    for _, _, archived in env.calls.archived:
        for envelope in archived:
            assert envelope["time_estimated"] is True
            assert envelope["pub_time"] == envelope["fetch_time"]

    docs = env.calls.bulk[0]
    em_doc = next(d for d in docs if d["source"] == "em")
    cls_doc = next(d for d in docs if d["source"] == "cls")
    for doc in (em_doc, cls_doc):
        # 入库 doc 无 time_estimated/raw_* 键（只进归档）
        assert "time_estimated" not in doc
        assert "raw_title" not in doc and "raw_content" not in doc
    # _id 不受解析失败影响仍决定论：em 走 sha1(url)；cls 以空 pub_time 参与三级拼串
    # （若用回退的 fetch_time 参与，同一条目每轮产出新 _id，滚动窗残留期内反复重复入库）
    assert em_doc["_id"] == _sha1("https://finance.eastmoney.com/a/t.html")
    assert cls_doc["_id"] == _sha1("cls||无时间电报")


# ---------- run_verify：归档重放对账 ----------

def test_verify_replays_window_including_today(env, monkeypatch):
    """窗口 [today-N, today] 自然日含今天；忽略传入交易日窗口；剥净入库；409 计数。"""
    def dup_bulk(client, docs):
        env.calls.bulk.append(list(docs))
        return len(docs) - 1, 1                    # 模拟每批 1 条已存在（409→dup）

    monkeypatch.setattr(flash.osu, "bulk_create", dup_bulk)
    env.replay_map[("cls", "20260705")] = [
        {"_id": "a1", "title": "归档一", "raw_title": "归档<b>一</b>", "raw_content": "原文",
         "pub_time": "2026-07-05 09:00:00", "source": "cls", "channel": "flash"},
        {"_id": "a2", "title": "归档二", "raw_title": "x", "raw_content": "y",
         "pub_time": "2026-07-05 10:00:00", "source": "cls", "channel": "flash",
         "time_estimated": True},
    ]
    env.replay_map[("em", "20260706")] = [
        {"_id": "b1", "title": "东财归档", "raw_title": "r", "raw_content": "c",
         "pub_time": "2026-07-06 08:00:00", "source": "em", "channel": "flash",
         "url": "https://finance.eastmoney.com/a/1.html"},
    ]

    summary = flash.run_verify("20200101", "20200102")    # 框架传入的窗口须被忽略

    # verify_days_back=2 → [20260704~20260706] 含今天 20260706，两源逐日重放
    expected_days = ["20260704", "20260705", "20260706"]
    assert env.calls.replays == \
        [("cls", (d, d)) for d in expected_days] + [("em", (d, d)) for d in expected_days]

    # 只有非空 (source,day) 触发 bulk；重放 doc 剥净 raw_*/time_estimated
    assert len(env.calls.bulk) == 2
    for batch in env.calls.bulk:
        for doc in batch:
            assert "raw_title" not in doc and "raw_content" not in doc
            assert "time_estimated" not in doc
    assert {doc["_id"] for doc in env.calls.bulk[0]} == {"a1", "a2"}
    assert {doc["_id"] for doc in env.calls.bulk[1]} == {"b1"}
    assert "重放 3 条" in summary
    assert "新入库 1" in summary                   # (2-1) + (1-1)
    assert "重复 2" in summary                     # 每批 1 dup × 2 批
    assert "失败 0" in summary


def test_verify_replay_error_isolated_then_raises(env):
    """单 (source,day) 重放异常隔离：其余照旧重放入库；末尾 raise 保留框架失败语义。"""
    env.replay_map[("cls", "20260705")] = EOFError("gzip 断写损坏")
    env.replay_map[("em", "20260706")] = [
        {"_id": "b1", "title": "东财归档", "pub_time": "2026-07-06 08:00:00",
         "source": "em", "channel": "flash"}]

    with pytest.raises(RuntimeError) as excinfo:
        flash.run_verify("20200101", "20200102")

    msg = str(excinfo.value)
    assert len(env.calls.replays) == 6             # 出错后其余 (source,day) 仍被处理
    assert len(env.calls.bulk) == 1                # em 当日照常入库
    assert "重放 1 条" in msg                      # 摘要经异常消息可见
    assert "失败 1" in msg and "cls/20260705" in msg


def test_verify_does_not_fetch_or_alert(env):
    """verify 只重放归档：不调源（快讯无源头回补）、不做溢出/断流判定。"""
    env.cls_data = RuntimeError("verify 不应触源")
    env.em_data = RuntimeError("verify 不应触源")

    summary = flash.run_verify("20200101", "20200102")

    assert env.calls.alerts == []
    assert env.calls.archived == []
    assert "重放 0 条" in summary


# ---------- 失败传播（回归锁） ----------

def test_run_bulk_error_propagates(env, monkeypatch):
    """bulk_create 抛错必须上抛（入库失败不能被摘要吞掉，框架 retry 契约）。"""
    def boom(client, docs):
        raise RuntimeError("bulk 写入失败 3 条")

    monkeypatch.setattr(flash.osu, "bulk_create", boom)
    env.em_data = _em_df([_em_row("快讯", "https://finance.eastmoney.com/a/1.html")])

    with pytest.raises(RuntimeError, match="bulk"):
        flash.run()


# ---------- 源级硬超时（2026-07-08 实撞回归锁） ----------
# cls 源挂起（akshare 内部 requests 无超时）→ 240s 子进程被硬杀×2 → 整任务失败，
# em 健康源没机会跑——源级隔离只兜"异常"不兜"挂起"，必须给单源拉取加硬超时。

def test_fetch_with_timeout_cuts_hanging_fetch(monkeypatch):
    import time as _time
    monkeypatch.setattr(flash, "_FETCH_TIMEOUT_SECONDS", 0.2)
    t0 = _time.time()
    with pytest.raises(TimeoutError, match="源挂起"):
        flash._fetch_with_timeout(lambda: _time.sleep(3))
    assert _time.time() - t0 < 2          # 远早于 sleep(3) 结束即返回


def test_fetch_with_timeout_passthrough(monkeypatch):
    """正常返回原样透传；内部异常原样上抛（保持源级隔离语义不变）。"""
    assert flash._fetch_with_timeout(lambda: "df") == "df"
    with pytest.raises(ValueError, match="源改版"):
        flash._fetch_with_timeout(
            lambda: (_ for _ in ()).throw(ValueError("源改版")))


# ---------- 三备源 sina + config 驱动源列表（cls 挂死事故的机动能力） ----------

def _sina_df(rows):
    """构造 stock_info_global_sina 形状：时间/内容（无标题无链接）。"""
    return pd.DataFrame(rows, columns=["时间", "内容"])


def test_sina_envelope_contract():
    row = pd.Series({"时间": "2026-07-08 10:30:00", "内容": "新浪快讯：贵州茅台上涨"})
    env = flash._row_to_envelope("sina", row, "2026-07-08 15:00:00",
                                 {"贵州茅台": "600519.SH"})
    assert env["source"] == "sina" and env["channel"] == "flash"
    assert env["pub_time"] == "2026-07-08 10:30:00"
    assert "url" not in env                              # sina 无链接（同 cls 惯例）
    assert env["_id"] == _sha1("sina|2026-07-08 10:30:00|新浪快讯：贵州茅台上涨")
    assert env["content"] == "新浪快讯:贵州茅台上涨"      # 清洗（全角冒号→半角）


def test_active_sources_config_driven(monkeypatch):
    monkeypatch.setattr(flash, "get_news_config",
                        lambda: {"flash_sources": ["em", "sina"]})
    assert flash._active_sources() == ("em", "sina")
    monkeypatch.setattr(flash, "get_news_config", lambda: {})
    assert flash._active_sources() == ("cls", "em")      # 缺省双活不变
    monkeypatch.setattr(flash, "get_news_config",
                        lambda: {"flash_sources": ["em", "typo_src"]})
    with pytest.raises(ValueError, match="未知源"):
        flash._active_sources()                          # 配置手误 fail-fast


def test_run_with_sina_in_sources(env, monkeypatch):
    """config 切 [em, sina] → run 只采这两源（cls 不再被碰），sina 正常入库。"""
    monkeypatch.setattr(flash, "get_news_config",
                        lambda: {"verify_days_back": 2,
                                 "spool_dir": str(env.tmp_path),
                                 "flash_sources": ["em", "sina"]})
    monkeypatch.setattr(flash, "_fetch_cls",
                        lambda: pytest.fail("cls 不在源列表，不应被调用"))
    monkeypatch.setattr(flash, "_fetch_sina", lambda: _sina_df([
        {"时间": "2026-07-03 10:00:00", "内容": "新浪快讯一"}]))
    env.em_data = _em_df([_em_row("东财快讯", "https://finance.eastmoney.com/a/1.html")])

    summary = flash.run()

    archived_sources = {a[0] for a in env.calls.archived}
    assert archived_sources == {"em", "sina"}
    assert "sina 1条" in summary and "cls" not in summary


def test_run_isolates_hanging_source(env, monkeypatch):
    """cls 挂起 → 按失败源隔离，em 正常采集入库（双活兜底真正生效）。"""
    import time as _time
    monkeypatch.setattr(flash, "_FETCH_TIMEOUT_SECONDS", 0.2)
    monkeypatch.setattr(flash, "_fetch_cls", lambda: _time.sleep(3))
    env.em_data = _em_df([_em_row("东财快讯", "https://finance.eastmoney.com/a/1.html")])

    summary = flash.run()

    assert "cls 失败" in summary and "失败源[cls]" in summary
    archived_sources = {a[0] for a in env.calls.archived}
    assert archived_sources == {"em"}     # em 照常归档
    assert len(env.calls.bulk) == 1       # em 照常入库
