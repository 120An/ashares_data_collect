"""news_embed 单元测试：全 mock——不加载真模型、不连 OpenSearch、不触网。

覆盖（计划 T8 + 设计决策）：
- **显式 `_id` 协议（核心锚点）**：写回严格限定快照内的 (_index, _id)——快照后
  "新插入"的 pending（积压检查轮才可见）绝不被触碰（这是"绝不 update_by_query
  盲改"的行为锚点）；写回用 hit 自带物理索引（跨年文档写别名有歧义）；
- 快照查询形状：term vec_status=pending / size=_BATCH_LIMIT / fetch_time asc /
  _source 三字段；
- 向量下标对齐：快照顺序 ↔ encode 输出行序（假 Embedder 第 i 行返回全 i 向量，
  锁"错位=向量写错文档"）；content_vec 为 Python float 列表（.tolist() 契约，
  np.float32 不可 JSON 序列化）；vec_status 与 content_vec 同一 update 原子写；
- 空文本跳过编码但仍置 done（无 content_vec；pending 卡死比无向量更糟）；
  全空文本轮完全不加载模型；
- 0 pending 早退：不加载模型、不 bulk、不做积压检查；
- 快照查询 404/异常 → 视为 0 条（冷启动别名未建，同 T6/T7 模式）；
- 积压告警：总数 > _BACKLOG_COUNT 或最老 > _BACKLOG_AGE_HOURS 小时 → 告警一条；
  恰在阈值上（严格大于语义）不告警；积压查询失败不反噬已成功的写回；
- 失败传播（回归锁）：encode 抛错 → 上抛且**不写回**（全量留 pending 待重试）；
  bulk_update 抛错 → 上抛（框架 retry 契约）。
"""

import copy
import datetime
from types import SimpleNamespace

import numpy as np
import pytest

from data_collect.jobs import news_embed as ne

_NOW = datetime.datetime(2026, 7, 3, 14, 37, 0)


def _hit(index, doc_id, title="标题", content="正文", fetch_time="2026-07-03 13:00:00",
         body=None):
    """构造 search 命中项（hit 自带物理 _index——写回路由的关键）。"""
    source = {"title": title, "content": content, "fetch_time": fetch_time}
    if body is not None:
        source["body"] = body
    return {"_index": index, "_id": doc_id, "_source": source}


def _resp(hits, total=None):
    """构造 search 响应（total 缺省即命中数；积压场景可显式放大）。"""
    return {"hits": {"total": {"value": len(hits) if total is None else total},
                     "hits": hits}}


@pytest.fixture
def env(monkeypatch):
    """全隔离环境：OpenSearch 检索/写回、Embedder、钉钉、时钟全打桩并记录调用。

    search_responses: 按调用顺序 pop 的响应队列（dict|Exception）；
    假 Embedder 第 i 行返回全 i 向量（[i,i,i,i]），锁快照↔向量下标对齐。
    """
    calls = SimpleNamespace(searches=[], bulk_updates=[], alerts=[],
                            encodes=[], embedder_gets=[])
    e = SimpleNamespace(calls=calls, search_responses=[],
                        encode_exc=None, bulk_exc=None)

    def fake_search_raw(client, body, index=ne.osu.ALIAS):
        calls.searches.append(copy.deepcopy(body))
        if not e.search_responses:
            raise AssertionError("search_raw 调用次数超出预置响应")
        resp = e.search_responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp

    def fake_bulk_update(client, updates):
        calls.bulk_updates.append(copy.deepcopy(list(updates)))
        if e.bulk_exc is not None:
            raise e.bulk_exc
        return len(updates)

    class _FakeEmbedder:
        def encode(self, texts):
            calls.encodes.append(list(texts))
            if e.encode_exc is not None:
                raise e.encode_exc
            return np.stack([np.full(4, float(i), dtype=np.float32)
                             for i in range(len(texts))])

    def fake_get_embedder():
        calls.embedder_gets.append(1)
        return _FakeEmbedder()

    monkeypatch.setattr(ne.osu, "get_client", lambda: object())
    monkeypatch.setattr(ne.osu, "search_raw", fake_search_raw)
    monkeypatch.setattr(ne.osu, "bulk_update", fake_bulk_update)
    monkeypatch.setattr(ne.embedding, "get_embedder", fake_get_embedder)
    monkeypatch.setattr(ne, "send_dingtalk", lambda msg: calls.alerts.append(msg))
    monkeypatch.setattr(ne, "_now", lambda: _NOW)
    # 默认向量启用（不依赖真 config；禁用/默认用例各自覆盖）
    monkeypatch.setattr(ne, "get_news_config", lambda: {"embedding": {"enabled": True}})
    return e


# ---------- 显式 _id 协议（核心锚点） ----------

def test_explicit_id_protocol_only_snapshot_written(env):
    """快照 A/B 被写回；快照后"新插入"的 pending C（积压轮可见）绝不被触碰。"""
    env.search_responses = [
        _resp([_hit("news-2026", "idA", title="标题A", content="正文A"),
               _hit("news-2027", "idB", title="标题B", content="正文B")]),
        # 积压检查轮：扫描后新插入的 C 仍 pending——只准出现在计数里，不准被写
        _resp([_hit("news-2026", "idC", fetch_time="2026-07-03 14:30:00")], total=1),
    ]

    summary = ne.run()

    # 写回恰一批、恰快照两条、顺序即快照顺序（fetch_time asc）
    assert len(env.calls.bulk_updates) == 1
    updates = env.calls.bulk_updates[0]
    assert [(u["_index"], u["_id"]) for u in updates] == \
        [("news-2026", "idA"), ("news-2027", "idB")]   # hit 自带物理索引直达
    touched_ids = {u["_id"] for batch in env.calls.bulk_updates for u in batch}
    assert "idC" not in touched_ids                    # 绝不 update_by_query 盲改

    # 同一 update 原子写两字段；向量按快照下标对齐（第 i 行全 i）
    assert updates[0]["doc"] == {"vec_status": "done", "content_vec": [0.0] * 4}
    assert updates[1]["doc"] == {"vec_status": "done", "content_vec": [1.0] * 4}
    # .tolist() 契约：Python float（np.float32 不是 float 子类，无法 JSON 序列化）
    assert all(isinstance(v, float) for v in updates[0]["doc"]["content_vec"])

    # 编码文本为 title\ncontent 拼接
    assert env.calls.encodes == [["标题A\n正文A", "标题B\n正文B"]]
    assert "编码 2 条" in summary and "写回 2" in summary
    assert "剩余 pending 1" in summary


def test_snapshot_query_shape(env):
    """快照查询：term pending / size=_BATCH_LIMIT / fetch_time asc / _source 三字段。"""
    env.search_responses = [_resp([])]

    ne.run()

    body = env.calls.searches[0]
    assert body["query"] == {"term": {"vec_status": "pending"}}
    assert body["size"] == ne._BATCH_LIMIT
    assert body["sort"] == [{"fetch_time": {"order": "asc"}}]
    assert set(body["_source"]) == {"title", "content", "fetch_time", "body"}


# ---------- 0 pending / 快照查询异常 ----------

def test_no_pending_returns_early_without_model(env):
    """0 条 pending：不加载模型、不 bulk、不做积压检查（只查一次）。"""
    env.search_responses = [_resp([])]

    summary = ne.run(run_date="20260703", extra_kw=True)   # 框架签名兼容顺带锁定

    assert "无待编码" in summary
    assert env.calls.embedder_gets == []       # lazy 意义所在：空轮绝不加载模型
    assert env.calls.bulk_updates == []
    assert len(env.calls.searches) == 1        # 早退：无积压检查轮
    assert env.calls.alerts == []


def test_snapshot_query_error_treated_as_zero(env):
    """别名不存在/404（冷启动尚无采集）→ 视为 0 条，不 raise（同 T6/T7 模式）。"""
    env.search_responses = [RuntimeError("index_not_found_exception: no such index [news]")]

    summary = ne.run()

    assert "无待编码" in summary
    assert env.calls.bulk_updates == []


# ---------- 空文本跳过 ----------

def test_empty_text_skipped_but_marked_done(env):
    """strip 后为空的文档跳过编码但仍置 done（无 content_vec），不占向量下标。"""
    env.search_responses = [
        _resp([_hit("news-2026", "idA", title="标题A", content="正文A"),
               _hit("news-2026", "idEmpty", title="", content="  "),
               _hit("news-2026", "idC", title="", content="仅正文")]),
        _resp([], total=0),
    ]

    summary = ne.run()

    # 只编码两条非空文本（空 title 拼接后 strip，不带前导换行的悬空）
    assert env.calls.encodes == [["标题A\n正文A", "仅正文"]]
    updates = env.calls.bulk_updates[0]
    assert [u["_id"] for u in updates] == ["idA", "idEmpty", "idC"]   # 三条都写回
    assert updates[0]["doc"] == {"vec_status": "done", "content_vec": [0.0] * 4}
    assert updates[1]["doc"] == {"vec_status": "done"}                # 空文本：无向量、仍 done
    assert updates[2]["doc"] == {"vec_status": "done", "content_vec": [1.0] * 4}  # 下标不错位
    assert "编码 2 条" in summary and "跳过空文本 1" in summary and "写回 3" in summary


def test_all_empty_text_marks_done_without_model(env):
    """整轮全空文本：完全不加载模型，仍全部置 done 防 pending 卡死。"""
    env.search_responses = [
        _resp([_hit("news-2026", "idE1", title="", content=""),
               _hit("news-2026", "idE2", title=" ", content="")]),
        _resp([], total=0),
    ]

    summary = ne.run()

    assert env.calls.embedder_gets == []
    assert [u["doc"] for u in env.calls.bulk_updates[0]] == \
        [{"vec_status": "done"}, {"vec_status": "done"}]
    assert "编码 0 条" in summary and "跳过空文本 2" in summary


# ---------- 积压告警 ----------

def test_backlog_count_alert(env):
    """写回后剩余 pending 总数 > _BACKLOG_COUNT → 钉钉告警一条（含总数）。"""
    env.search_responses = [
        _resp([_hit("news-2026", "idA")]),
        _resp([_hit("news-2026", "idNew", fetch_time="2026-07-03 14:30:00")], total=2001),
    ]

    summary = ne.run()

    backlog_alerts = [a for a in env.calls.alerts if "积压" in a]
    assert len(backlog_alerts) == 1 and "2001" in backlog_alerts[0]
    assert "剩余 pending 2001" in summary


def test_backlog_age_alert(env):
    """最老 pending 距今 > _BACKLOG_AGE_HOURS 小时 → 告警（总数未超也告）。"""
    env.search_responses = [
        _resp([_hit("news-2026", "idA")]),
        # 最老 11:37 → 距 _NOW(14:37) 180 分钟 > 120
        _resp([_hit("news-2026", "idOld", fetch_time="2026-07-03 11:37:00")], total=5),
    ]

    summary = ne.run()

    assert len([a for a in env.calls.alerts if "积压" in a]) == 1
    assert "最老 180 分钟" in summary


def test_backlog_at_threshold_no_alert(env):
    """恰在阈值上（=2000 条、=120 分钟）不告警：阈值语义为严格大于。"""
    env.search_responses = [
        _resp([_hit("news-2026", "idA")]),
        _resp([_hit("news-2026", "idOld", fetch_time="2026-07-03 12:37:00")], total=2000),
    ]

    summary = ne.run()

    assert env.calls.alerts == []
    assert "剩余 pending 2000" in summary and "最老 120 分钟" in summary


def test_backlog_query_failure_does_not_fail_run(env):
    """积压查询失败：监控职责不反噬已成功的写回——不 raise、不告警、摘要标注。"""
    env.search_responses = [
        _resp([_hit("news-2026", "idA")]),
        RuntimeError("search phase execution exception"),
    ]

    summary = ne.run()

    assert len(env.calls.bulk_updates) == 1        # 写回已完成
    assert env.calls.alerts == []
    assert "剩余 pending 查询失败" in summary


# ---------- 向量延迟开关（用户 2026-07-08：暂不需要向量，省储存，日后补算） ----------

def test_embedding_disabled_skips_everything(env, monkeypatch):
    """news.embedding.enabled=false → 立即返回，不扫库/不编码/不告警（pending 留存）。"""
    monkeypatch.setattr(ne, "get_news_config",
                        lambda: {"embedding": {"enabled": False}})
    summary = ne.run()
    assert "已禁用" in summary
    assert env.calls.searches == []          # 没扫库
    assert env.calls.encodes == []           # 没编码
    assert env.calls.alerts == []            # 不告积压警（pending 是有意留存）


def test_embedding_enabled_default_true(env, monkeypatch):
    """未配置 enabled → 默认开启（行为不变，向后兼容）。"""
    monkeypatch.setattr(ne, "get_news_config", lambda: {"embedding": {}})
    env.search_responses = [_resp([]), _resp([], total=0)]
    ne.run()
    assert len(env.calls.searches) >= 1      # 照常扫库


# ---------- 失败传播（回归锁） ----------

def test_encode_error_propagates_without_writeback(env):
    """encode 抛错必须上抛（框架 retry + 通知），且不写回——全量留 pending 待重试。"""
    env.search_responses = [_resp([_hit("news-2026", "idA")])]
    env.encode_exc = RuntimeError("模型加载失败: CUDA out of memory")

    with pytest.raises(RuntimeError, match="模型加载失败"):
        ne.run()
    assert env.calls.bulk_updates == []


def test_bulk_update_error_propagates(env):
    """bulk_update 抛错必须上抛（写回失败不能被摘要吞掉，框架 retry 契约）。"""
    env.search_responses = [_resp([_hit("news-2026", "idA")])]
    env.bulk_exc = RuntimeError("bulk 更新失败 1 条")

    with pytest.raises(RuntimeError, match="bulk 更新失败"):
        ne.run()


# ---------- body 优先编码（⑥ Phase 2 全文向量） ----------

def test_prefers_body_when_present(env):
    """有 body（全文已抽取）：编码 title\\nbody，非摘要 content——向量落全文。"""
    env.search_responses = [
        _resp([_hit("news-2026", "idA", title="标题A", content="摘要A",
                    body="这是全文正文A" * 20)]),
        _resp([], total=0),
    ]
    ne.run()
    assert env.calls.encodes == [["标题A\n" + "这是全文正文A" * 20]]


def test_falls_back_to_content_without_body(env):
    """无 body（未抽取/抽取失败）：回退 title\\ncontent（摘要）——两类全覆盖无遗漏。"""
    env.search_responses = [
        _resp([_hit("news-2026", "idA", title="标题A", content="摘要A")]),
        _resp([], total=0),
    ]
    ne.run()
    assert env.calls.encodes == [["标题A\n摘要A"]]


def test_encode_text_truncated_to_context_budget(env):
    """回归锚点（清积压实撞）：body 可达 5 万字，而 Ollama bge-m3 ctx=4096 token——
    编码文本必须截断到 _EMBED_MAX_CHARS，否则 /api/embed 400 使整轮补向量失败。"""
    env.search_responses = [
        _resp([_hit("news-2026", "idA", title="标题A", body="长" * 60_000)]),
        _resp([], total=0),
    ]
    ne.run()
    sent = env.calls.encodes[0][0]
    assert len(sent) == ne._EMBED_MAX_CHARS
    assert sent.startswith("标题A\n长")                    # 截断保留开头（标题+正文头）
