"""news_report 单元测试：全 mock——不连 OpenSearch、不触网、不发真钉钉。

覆盖（计划 T10a + 设计决策）：
- 正常日报：channel agg buckets → flash/cctv/marker 各数字正确、其他 channel 追加、
  总数取 track_total_hits；pending 积压总数 + 最老滞留分钟（_now/_today 可 patch 固定时钟）；
  send_dingtalk 恰调一次且发送串 == 返回串；
- 索引未建（search_raw 抛 NotFoundError / 返回带 error）→ 摘要标"索引未建"、
  其余字段 0/None，**仍发钉钉**（日报必达）；
- agg 缺某 channel（只有 flash bucket）→ cctv/marker 记 0；
- pending total=0 → "最老 - 分钟"（最老分钟 None 的渲染，锁定契约）；
- send_dingtalk 抛异常 → job 不失败（guarded，返回摘要正常）。
"""

import datetime
from types import SimpleNamespace

import pytest
from opensearchpy.exceptions import NotFoundError

from data_collect.jobs import news_report as nr


_TODAY = datetime.date(2026, 7, 3)                     # 固定"今天"
_NOW = datetime.datetime(2026, 7, 3, 20, 40, 0)        # 固定时钟（20:40 日报触发时刻）


def _agg_resp(buckets, total):
    """构造 channel 聚合响应：buckets=[(channel, count), ...]，total=命中总数。"""
    return {
        "hits": {"total": {"value": total, "relation": "eq"}},
        "aggregations": {
            "by_channel": {"buckets": [{"key": k, "doc_count": c} for k, c in buckets]}
        },
    }


def _pending_resp(total, oldest_fetch_time):
    """构造 pending 积压响应（size=1 + fetch_time asc + track_total_hits）。"""
    hits = ([{"_source": {"fetch_time": oldest_fetch_time}}]
            if oldest_fetch_time is not None else [])
    return {"hits": {"total": {"value": total, "relation": "eq"}, "hits": hits}}


@pytest.fixture
def env(monkeypatch):
    """全隔离环境：OpenSearch 检索、钉钉、时钟全打桩并记录调用。

    search_responses: 按调用顺序 pop 的响应队列（dict|Exception）——第 1 次
    是 channel 聚合、第 2 次是 pending 积压；send_exc 置位时 send_dingtalk 抛错。
    """
    calls = SimpleNamespace(searches=[], sent=[])
    e = SimpleNamespace(calls=calls, search_responses=[], send_exc=None)

    def fake_search_raw(client, body, index=nr.osu.ALIAS):
        calls.searches.append(body)
        if not e.search_responses:
            raise AssertionError("search_raw 调用次数超出预置响应")
        resp = e.search_responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp

    def fake_send(msg):
        calls.sent.append(msg)
        if e.send_exc is not None:
            raise e.send_exc

    monkeypatch.setattr(nr.osu, "get_client", lambda: object())
    monkeypatch.setattr(nr.osu, "search_raw", fake_search_raw)
    monkeypatch.setattr(nr, "send_dingtalk", fake_send)
    monkeypatch.setattr(nr, "_today", lambda: _TODAY)
    monkeypatch.setattr(nr, "_now", lambda: _NOW)
    # 默认向量启用（不依赖真 config；vec_deferred 用例各自覆盖）
    monkeypatch.setattr(nr, "get_news_config", lambda: {"embedding": {"enabled": True}})
    return e


# ---------- 正常日报 ----------

def test_normal_report(env):
    """agg 返回 flash/cctv/marker + pending total=5 最老 3 分钟 → 各数字正确、
    send_dingtalk 恰一次、返回串 == 发送串。"""
    env.search_responses = [
        _agg_resp([("flash", 120), ("cctv", 15), ("marker", 2)], total=137),
        # 最老 20:37 → 距 _NOW(20:40) 3 分钟
        _pending_resp(total=5, oldest_fetch_time="2026-07-03 20:37:00"),
    ]

    summary = nr.run()

    assert "新闻日报 2026-07-03" in summary
    assert "总 137" in summary
    assert "flash 120" in summary and "cctv 15" in summary and "marker 2" in summary
    assert "pending 5" in summary
    assert "最老 3 分钟" in summary
    # 日报必达：恰发一次，且发送串 == 返回串
    assert len(env.calls.sent) == 1
    assert env.calls.sent[0] == summary


def test_vec_deferred_note(env, monkeypatch):
    """向量禁用（news.embedding.enabled=false）→ pending 段标"向量已禁用,留待补算"
    （防大额 pending 被误读为积压故障；用户 2026-07-08 决定省储存延迟向量）。"""
    monkeypatch.setattr(nr, "get_news_config",
                        lambda: {"embedding": {"enabled": False}})
    env.search_responses = [
        _agg_resp([("flash", 3)], total=3),
        _pending_resp(total=99999, oldest_fetch_time="2026-07-03 20:37:00"),
    ]

    summary = nr.run()

    assert "向量已禁用" in summary and "pending 99999" in summary
    assert "最老" not in summary


def test_fixed_channels_include_announcement_and_stock(env):
    """回归锚点（审核#8）：announcement/stock 属固定上报——bucket 缺失补 0 可见，
    否则 news_announcement/news_stock 整条管线静默挂掉时日报无任何异常信号
    （stock 22:00 才入库、按 pub_time 口径当日恒近零，更需要 0 行兜底可见）。"""
    env.search_responses = [
        _agg_resp([("flash", 120)], total=120),          # 其余 channel 全缺
        _pending_resp(total=0, oldest_fetch_time=None),
    ]

    summary = nr.run()

    assert "announcement 0" in summary and "stock 0" in summary
    assert "cctv 0" in summary and "marker 0" in summary


def test_agg_query_shape_and_date_window(env):
    """channel 聚合查询：pub_time range 当日 [00:00:00, 23:59:59]、by_channel terms、
    size=0、track_total_hits。"""
    env.search_responses = [
        _agg_resp([("flash", 1)], total=1),
        _pending_resp(total=0, oldest_fetch_time=None),
    ]

    nr.run()

    agg_body = env.calls.searches[0]
    rng = agg_body["query"]["range"]["pub_time"]
    assert rng["gte"] == "2026-07-03 00:00:00"
    assert rng["lte"] == "2026-07-03 23:59:59"
    assert agg_body["aggs"]["by_channel"]["terms"]["field"] == "channel"
    assert agg_body["size"] == 0
    assert agg_body["track_total_hits"] is True


def test_run_date_explicit(env):
    """显式 run_date（YYYYMMDD）→ 摘要日期与查询窗口按该日，不用 _today。"""
    env.search_responses = [
        _agg_resp([("flash", 3)], total=3),
        _pending_resp(total=0, oldest_fetch_time=None),
    ]

    summary = nr.run(run_date="20260701")

    assert "新闻日报 2026-07-01" in summary
    assert env.calls.searches[0]["query"]["range"]["pub_time"]["gte"] == "2026-07-01 00:00:00"


# ---------- 索引未建 ----------

def test_index_not_built_notfound_still_sends(env):
    """channel 聚合 search_raw 抛 NotFoundError（别名/索引未建）→ 摘要标"索引未建"、
    字段 0/None，仍发钉钉（日报必达）。"""
    env.search_responses = [NotFoundError(404, "index_not_found_exception", {})]

    summary = nr.run()

    assert "索引未建" in summary
    assert "总 0" in summary
    assert "flash 0" in summary and "cctv 0" in summary and "marker 0" in summary
    # 索引未建也发钉钉
    assert len(env.calls.sent) == 1
    assert env.calls.sent[0] == summary
    # 索引未建时不应再打 pending 查询（聚合已判定索引缺失）
    assert len(env.calls.searches) == 1


def test_index_not_built_error_payload_still_sends(env):
    """search_raw 返回带 error 载荷（部分部署以 200 + error 回）→ 同样"索引未建"仍发。"""
    env.search_responses = [{"error": {"type": "index_not_found_exception"}}]

    summary = nr.run()

    assert "索引未建" in summary
    assert len(env.calls.sent) == 1


# ---------- 非索引缺失的查询异常：不误报"索引未建" ----------

def test_non_notfound_error_not_labeled_index_missing(env):
    """query①（channel 聚合）正常，query②（pending）抛非 NotFound 错误（集群超时/查询错）
    → 摘要**不**含"索引未建"、标"查询异常"，DingTalk 仍必达（日报不能因查询故障静默）。"""
    env.search_responses = [
        _agg_resp([("flash", 10)], total=10),          # query① 成功
        RuntimeError("cluster read timeout"),          # query② 非 NotFound 失败
    ]

    summary = nr.run()   # 不抛

    assert "索引未建" not in summary                   # 关键：不误报索引缺失
    assert "查询异常" in summary
    assert "RuntimeError" in summary                   # 摘要带异常类型名便于定位
    # 查询故障日报仍必达
    assert len(env.calls.sent) == 1
    assert env.calls.sent[0] == summary


def test_pending_notfound_still_index_missing(env):
    """query① 成功、query②（pending）抛 NotFoundError（别名/索引确已缺失）→ 仍归"索引未建"
    （NotFoundError 分支覆盖第二个查询，锁定与非 NotFound 分支的边界）。"""
    env.search_responses = [
        _agg_resp([("flash", 10)], total=10),
        NotFoundError(404, "index_not_found_exception", {}),
    ]

    summary = nr.run()

    assert "索引未建" in summary
    assert "查询异常" not in summary
    assert len(env.calls.sent) == 1


# ---------- agg 缺 channel ----------

def test_missing_channel_counts_zero(env):
    """agg 只有 flash bucket → cctv/marker 记 0（缺失 channel 补 0，marker 也报）。"""
    env.search_responses = [
        _agg_resp([("flash", 42)], total=42),
        _pending_resp(total=0, oldest_fetch_time=None),
    ]

    summary = nr.run()

    assert "flash 42" in summary
    assert "cctv 0" in summary
    assert "marker 0" in summary


def test_extra_channel_appended(env):
    """出现固定三类之外的 channel（如 policy）→ 追加进摘要（不丢数据）。"""
    env.search_responses = [
        _agg_resp([("flash", 5), ("cctv", 3), ("marker", 1), ("policy", 7)], total=16),
        _pending_resp(total=0, oldest_fetch_time=None),
    ]

    summary = nr.run()

    assert "policy 7" in summary


# ---------- pending 边界 ----------

def test_pending_zero_oldest_dash(env):
    """pending total=0 → 最老分钟 None 渲染为 "-"（锁定契约）。"""
    env.search_responses = [
        _agg_resp([("flash", 1)], total=1),
        _pending_resp(total=0, oldest_fetch_time=None),
    ]

    summary = nr.run()

    assert "pending 0" in summary
    assert "最老 - 分钟" in summary


def test_pending_malformed_fetch_time_dash(env):
    """pending 最老 fetch_time 异形（解析失败）→ 最老分钟记 None → "-"。"""
    env.search_responses = [
        _agg_resp([("flash", 1)], total=1),
        _pending_resp(total=3, oldest_fetch_time="not-a-time"),
    ]

    summary = nr.run()

    assert "pending 3" in summary
    assert "最老 - 分钟" in summary


# ---------- send 异常 guarded ----------

def test_send_failure_does_not_fail_job(env):
    """send_dingtalk 抛异常 → job 不失败（guarded），返回摘要正常。"""
    env.search_responses = [
        _agg_resp([("flash", 1)], total=1),
        _pending_resp(total=0, oldest_fetch_time=None),
    ]
    env.send_exc = RuntimeError("钉钉 webhook 超时")

    summary = nr.run()   # 不抛

    assert "新闻日报 2026-07-03" in summary
    assert len(env.calls.sent) == 1   # 尝试发过（虽然抛错）
