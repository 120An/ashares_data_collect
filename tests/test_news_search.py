"""news_search 单元测试：全 mock（search_raw/embedder/transport）——不连 OpenSearch、不加载模型。

覆盖（计划 T9 + 设计决策）：
- rrf_fuse 纯函数：融合序 / 分数 Σ1/(k+rank) / k 参数生效 / 仅单路命中 / 入参不改写；
- 各 mode query body 构造：bm25 multi_match 字段与 boost、过滤注入 bool 内部、
  marker 默认排除、highlight/collapse；knn 向量与 K 下限、filter **内嵌 knn 子句**
  （不是 post_filter/顶层 filter）；hybrid 两路子查询各自带 filter + search_pipeline
  请求参数透传；
- rrf 模式：两次 search、窗口 size=max(top_k,50)、融合截 top_k、collapse 只作用 bm25 路；
- date_range 规范化（"YYYYMMDD" 与 "YYYY-MM-DD" 等价；单边 None）；
- channel 列表 → terms；显式 channel="marker" 不加 must_not；
- bm25 是降级路径：绝不触发 Embedder 加载；
- 清洗后空查询 / 未知 mode → ValueError；查询串繁→简与索引侧对齐；
- 返回结构：total dict/int 兼容、hit 字段映射（url 缺省 None、highlight 拼接）、
  冷启动 404 按空结果；
- ensure_hybrid_pipeline：PUT body 形状；403 → 带管理员指引的 RuntimeError；
  hybrid 查询遇 pipeline 未建置 → RuntimeError 提示建置或改 mode="rrf"。
"""

from types import SimpleNamespace

import numpy as np
import pytest
from opensearchpy.exceptions import (
    AuthorizationException,
    NotFoundError,
    RequestError,
)

from data_collect.utils import news_search as ns

# float32 精确可表示的假查询向量（tolist 后与源值全等，免 approx）
_VEC = [0.5, 0.25, -1.0]


def _hit(_id, score=1.0, src=None, highlight=None):
    hit = {"_id": _id, "_score": score,
           "_source": src if src is not None else {
               "title": f"标题{_id}", "source": "cls", "channel": "flash",
               "pub_time": "2026-07-01 09:00:00", "url": f"http://x/{_id}"}}
    if highlight is not None:
        hit["highlight"] = highlight
    return hit


def _resp(hits, total=None):
    if total is None:
        total = {"value": len(hits), "relation": "eq"}
    return {"hits": {"total": total, "hits": hits}}


@pytest.fixture
def env(monkeypatch):
    """全隔离：search_raw 捕获 (body, index, params) 并按队列回放响应；假 Embedder 记录编码。"""
    calls = SimpleNamespace(searches=[], encodes=[])
    e = SimpleNamespace(calls=calls, responses=[], search_exc=None)

    def fake_search_raw(client, body, index="news", params=None):
        calls.searches.append(SimpleNamespace(body=body, index=index, params=params))
        if e.search_exc is not None:
            raise e.search_exc
        if e.responses:
            return e.responses.pop(0)
        return _resp([])

    class _FakeEmbedder:
        def encode(self, texts):
            calls.encodes.append(list(texts))
            return np.asarray([_VEC] * len(texts), dtype=np.float32)

    monkeypatch.setattr(ns.osu, "get_client", lambda: object())
    monkeypatch.setattr(ns.osu, "search_raw", fake_search_raw)
    monkeypatch.setattr(ns, "get_embedder", lambda: _FakeEmbedder())
    # 默认向量启用（不依赖真 config；降级用例各自覆盖）
    monkeypatch.setattr(ns, "get_news_config", lambda: {"embedding": {"enabled": True}})
    return e


def test_vector_disabled_downgrades_to_bm25(env, monkeypatch):
    """向量禁用 → hybrid/knn/rrf 均降级 bm25（不静默返回空；不加载 Embedder）。"""
    monkeypatch.setattr(ns, "get_news_config",
                        lambda: {"embedding": {"enabled": False}})
    for mode in ("hybrid", "knn", "rrf"):
        env.calls.searches.clear()
        env.calls.encodes.clear()
        env.responses = [_resp([_hit("A")])]
        ns.search_news("茅台", mode=mode)
        assert env.calls.encodes == []                      # 未加载/未编码向量
        body_str = str(env.calls.searches[0].body)
        assert "knn" not in body_str and "hybrid" not in body_str


# ---------- rrf_fuse 纯函数 ----------

def test_rrf_fuse_order_and_scores():
    """已知两路 → 融合序正确；分数 = Σ 1/(k+rank)，rank 从 1 起；仅单路命中也计分。"""
    bm25 = [_hit("A"), _hit("B"), _hit("C")]
    knn = [_hit("A"), _hit("C"), _hit("D")]   # D 仅 knn 命中
    fused = ns.rrf_fuse(bm25, knn, k=60)
    assert [h["_id"] for h in fused] == ["A", "C", "B", "D"]
    assert fused[0]["_rrf_score"] == pytest.approx(1 / 61 + 1 / 61)
    assert fused[1]["_rrf_score"] == pytest.approx(1 / 63 + 1 / 62)
    assert fused[2]["_rrf_score"] == pytest.approx(1 / 62)   # B 仅 bm25 第 2 名
    assert fused[3]["_rrf_score"] == pytest.approx(1 / 63)   # D 仅 knn 第 3 名


def test_rrf_fuse_k_param():
    """k 参数生效：分数分母随 k 变。"""
    assert ns.rrf_fuse([_hit("A")], [], k=1)[0]["_rrf_score"] == pytest.approx(1 / 2)
    assert ns.rrf_fuse([_hit("A")], [], k=99)[0]["_rrf_score"] == pytest.approx(1 / 100)


def test_rrf_fuse_keeps_first_hit_and_no_mutation():
    """同 _id 保留先见（bm25）hit：highlight 不丢；入参 dict 不被写入 _rrf_score。"""
    bm_hit = _hit("A", highlight={"title": ["<em>甲</em>"]})
    knn_hit = _hit("A")
    fused = ns.rrf_fuse([bm_hit], [knn_hit])
    assert fused[0]["highlight"] == {"title": ["<em>甲</em>"]}
    assert "_rrf_score" not in bm_hit and "_rrf_score" not in knn_hit   # 纯函数不改入参


# ---------- bm25 body ----------

def test_bm25_body_shape(env):
    ns.search_news("贵州茅台", top_k=7, channel="flash", stocks=["600519.SH"],
                   date_range=("20260101", "20260630"), mode="bm25",
                   collapse="title.raw")
    call, = env.calls.searches
    assert call.index == "news"                       # 读别名
    assert call.params is None                        # 非 hybrid 不带 search_pipeline
    body = call.body
    assert body["size"] == 7
    assert body["collapse"] == {"field": "title.raw"}
    assert body["highlight"] == {"fields": {
        "title": {}, "content": {"fragment_size": 80, "number_of_fragments": 2}}}
    bool_q = body["query"]["bool"]
    assert bool_q["must"] == [{"multi_match": {
        "query": "贵州茅台", "fields": ["title^2", "content"]}}]
    assert {"term": {"channel": "flash"}} in bool_q["filter"]
    assert {"terms": {"stocks": ["600519.SH"]}} in bool_q["filter"]
    assert {"range": {"pub_time": {"gte": "2026-01-01 00:00:00",
                                   "lte": "2026-06-30 23:59:59"}}} in bool_q["filter"]
    assert bool_q["must_not"] == [{"term": {"channel": "marker"}}]   # 默认排除标记文档
    assert "post_filter" not in body                  # 过滤注入查询内部，绝不 post_filter


def test_bm25_body_no_filters(env):
    """无过滤条件：不产出空 filter 键；marker 排除仍在。"""
    ns.search_news("政策", mode="bm25")
    bool_q = env.calls.searches[0].body["query"]["bool"]
    assert "filter" not in bool_q
    assert bool_q["must_not"] == [{"term": {"channel": "marker"}}]
    assert env.calls.searches[0].body["size"] == 10   # top_k 默认 10
    assert "collapse" not in env.calls.searches[0].body


# ---------- knn body ----------

def test_knn_body_shape(env):
    ns.search_news("新能源政策", top_k=5, channel="policy", mode="knn")
    call, = env.calls.searches
    body = call.body
    assert list(body["query"].keys()) == ["knn"]      # 单路 knn，无顶层 bool/filter 包裹
    clause = body["query"]["knn"]["content_vec"]
    assert clause["vector"] == _VEC                   # 查询向量来自 Embedder
    assert clause["k"] == 50                          # K 下限 _KNN_K_FLOOR
    inner = clause["filter"]["bool"]                  # 过滤内嵌 knn 子句（非 post_filter）
    assert inner["filter"] == [{"term": {"channel": "policy"}}]
    assert inner["must_not"] == [{"term": {"channel": "marker"}}]
    assert "post_filter" not in body
    assert "highlight" not in body                    # knn 单路无词项，不带高亮
    assert body["size"] == 5
    assert env.calls.encodes == [["新能源政策"]]      # 编码的是清洗后的查询串


def test_knn_k_follows_large_top_k(env):
    ns.search_news("查询", top_k=80, mode="knn")
    body = env.calls.searches[0].body
    assert body["query"]["knn"]["content_vec"]["k"] == 80   # top_k > 下限时取 top_k
    assert body["size"] == 80


# ---------- hybrid body ----------

def test_hybrid_body_and_pipeline_param(env):
    ns.search_news("茅台", top_k=6, channel="flash", mode="hybrid")
    call, = env.calls.searches
    assert call.params == {"search_pipeline": "news-hybrid"}   # 归一化 pipeline 请求参数
    body = call.body
    assert body["size"] == 6
    assert "highlight" in body
    queries = body["query"]["hybrid"]["queries"]
    assert len(queries) == 2
    bm25_q, knn_q = queries
    # 两路子查询各自带 filter（不依赖顶层公共 filter / post_filter）
    assert bm25_q["bool"]["filter"] == [{"term": {"channel": "flash"}}]
    assert bm25_q["bool"]["must_not"] == [{"term": {"channel": "marker"}}]
    inner = knn_q["knn"]["content_vec"]["filter"]["bool"]
    assert inner["filter"] == [{"term": {"channel": "flash"}}]
    assert inner["must_not"] == [{"term": {"channel": "marker"}}]
    assert knn_q["knn"]["content_vec"]["vector"] == _VEC
    assert knn_q["knn"]["content_vec"]["k"] == 50


def test_hybrid_pipeline_missing_wrapped(env):
    """pipeline 未建置的报错（400 RequestError）→ RuntimeError 指引先建置或改用 rrf。"""
    env.search_exc = RequestError(
        400, "illegal_argument_exception",
        {"error": {"root_cause": [{"type": "illegal_argument_exception",
                                   "reason": "Pipeline news-hybrid could not be found"}]}})
    with pytest.raises(RuntimeError, match="rrf"):
        ns.search_news("茅台", mode="hybrid")


def test_hybrid_pipeline_missing_404_not_swallowed(env):
    """pipeline 缺失若以 404 NotFoundError 冒出 → 仍给建置指引 RuntimeError，
    **不被 _search 的 404→空 静默吞没**（Fix 3：hybrid 直调 search_raw 绕过吞没）。"""
    env.search_exc = NotFoundError(
        404, "search_phase_execution_exception",
        {"error": {"reason": "Pipeline [news-hybrid] is not found"}})
    with pytest.raises(RuntimeError, match="ensure_hybrid_pipeline"):
        ns.search_news("茅台", mode="hybrid")
    # 且指引须提到 rrf 替代路径
    env.search_exc = NotFoundError(
        404, "search_phase_execution_exception",
        {"error": {"reason": "Pipeline [news-hybrid] is not found"}})
    with pytest.raises(RuntimeError, match="rrf"):
        ns.search_news("茅台", mode="hybrid")


def test_hybrid_unrelated_404_returns_empty(env):
    """与 pipeline 无关的 404（别名/索引确未建，冷启动）→ 仍按空结果，
    与 bm25/knn/rrf 容错语义一致（Fix 3 只拦 pipeline 相关 404，不改冷启动语义）。"""
    env.search_exc = NotFoundError(404, "index_not_found_exception", {})
    assert ns.search_news("茅台", mode="hybrid") == {"total": 0, "hits": []}


def test_hybrid_unrelated_error_reraised(env):
    """与 pipeline 无关的错误原样上抛，不误包装。"""
    env.search_exc = RequestError(400, "parsing_exception", {})
    with pytest.raises(RequestError):
        ns.search_news("茅台", mode="hybrid")


# ---------- rrf 模式 ----------

def test_rrf_mode_two_searches_fuse_truncate(env):
    env.responses = [
        _resp([_hit("A", 9.0), _hit("B", 8.0)], total={"value": 120}),   # bm25 路
        _resp([_hit("C", 0.9), _hit("A", 0.8)], total={"value": 50}),    # knn 路
    ]
    out = ns.search_news("茅台", top_k=2, mode="rrf", collapse="title.raw")
    bm25_call, knn_call = env.calls.searches                # 恰两次检索：bm25 先、knn 后
    assert bm25_call.body["size"] == 50                     # 窗口 max(top_k, 50)
    assert knn_call.body["size"] == 50
    assert knn_call.body["query"]["knn"]["content_vec"]["k"] == 50
    assert bm25_call.params is None and knn_call.params is None   # 客户端融合不走 pipeline
    assert bm25_call.body["collapse"] == {"field": "title.raw"}   # collapse 只作用 bm25 路
    assert "collapse" not in knn_call.body
    assert "highlight" in bm25_call.body and "highlight" not in knn_call.body
    # 融合：A 两路第1/第2名居首；截断 top_k=2
    assert [h["title"] for h in out["hits"]] == ["标题A", "标题C"]
    assert out["hits"][0]["score"] == pytest.approx(1 / 61 + 1 / 62)   # score=RRF 融合分
    assert out["total"] == 120                              # 两路 total 较大者


# ---------- 过滤器构造细节 ----------

def test_date_range_formats_equivalent(env):
    """"20260101" 与 "2026-01-01" 规范化结果等价。"""
    ns.search_news("q1", date_range=("20260101", "20260630"), mode="bm25")
    ns.search_news("q2", date_range=("2026-01-01", "2026-06-30"), mode="bm25")
    range_of = lambda call: next(
        f for f in call.body["query"]["bool"]["filter"] if "range" in f)
    assert range_of(env.calls.searches[0]) == range_of(env.calls.searches[1])
    assert range_of(env.calls.searches[0])["range"]["pub_time"] == {
        "gte": "2026-01-01 00:00:00", "lte": "2026-06-30 23:59:59"}


def test_date_range_single_sided(env):
    ns.search_news("q", date_range=(None, "20260630"), mode="bm25")
    ns.search_news("q", date_range=("20260101", None), mode="bm25")
    f1 = env.calls.searches[0].body["query"]["bool"]["filter"]
    f2 = env.calls.searches[1].body["query"]["bool"]["filter"]
    assert f1 == [{"range": {"pub_time": {"lte": "2026-06-30 23:59:59"}}}]
    assert f2 == [{"range": {"pub_time": {"gte": "2026-01-01 00:00:00"}}}]


def test_date_range_bad_format_raises(env):
    with pytest.raises(ValueError, match="date_range"):
        ns.search_news("q", date_range=("2026/01/01", None), mode="bm25")


def test_channel_list_terms(env):
    ns.search_news("q", channel=["flash", "cctv"], mode="bm25")
    bool_q = env.calls.searches[0].body["query"]["bool"]
    assert bool_q["filter"] == [{"terms": {"channel": ["flash", "cctv"]}}]
    assert bool_q["must_not"] == [{"term": {"channel": "marker"}}]


def test_stocks_str_treated_as_single_code(env):
    """stocks 误传 str（AI 调用方常见）→ 单代码 terms，不按字符拆散静默零命中。"""
    ns.search_news("q", stocks="600519.SH", mode="bm25")
    bool_q = env.calls.searches[0].body["query"]["bool"]
    assert {"terms": {"stocks": ["600519.SH"]}} in bool_q["filter"]


def test_explicit_marker_channel_not_excluded(env):
    """显式要 marker（str 或 list 包含）→ 不加 must_not，允许查标记文档。"""
    ns.search_news("空日", channel="marker", mode="bm25")
    ns.search_news("空日", channel=["marker", "cctv"], mode="bm25")
    for call in env.calls.searches:
        assert "must_not" not in call.body["query"]["bool"]


# ---------- 降级路径与入参校验 ----------

def test_bm25_never_loads_embedder(env, monkeypatch):
    """bm25 是模型不可用时的降级路径：绝不触发 Embedder 加载。"""
    def _boom():
        raise AssertionError("bm25 不应触碰 Embedder")
    monkeypatch.setattr(ns, "get_embedder", _boom)
    out = ns.search_news("茅台", mode="bm25")
    assert out == {"total": 0, "hits": []}


def test_empty_query_after_clean_raises(env):
    with pytest.raises(ValueError, match="为空"):
        ns.search_news("<p>   </p>")
    assert env.calls.searches == []                   # 未触发任何检索


def test_unknown_mode_raises(env):
    with pytest.raises(ValueError, match="mode"):
        ns.search_news("茅台", mode="fuzzy")
    assert env.calls.searches == []


def test_query_cleaned_traditional_to_simplified(env):
    """查询串繁→简与索引侧 clean_text 对齐：查"乾照光电"须检"干照光电"。"""
    ns.search_news("乾照光电", mode="bm25")
    must = env.calls.searches[0].body["query"]["bool"]["must"]
    assert must[0]["multi_match"]["query"] == "干照光电"


# ---------- 返回结构 ----------

def test_total_int_compat_and_hit_mapping(env):
    env.responses = [_resp(
        [_hit("A", 3.5, src={"title": "甲", "source": "em", "channel": "flash",
                             "pub_time": "2026-07-01 09:00:00"},   # url 缺省
              highlight={"title": ["<em>甲</em>"], "content": ["片1", "片2"]}),
         _hit("B", 2.0)],
        total=7)]                                     # 旧式 int total
    out = ns.search_news("甲", mode="bm25")
    assert out["total"] == 7
    first = out["hits"][0]
    assert first == {"title": "甲", "source": "em", "channel": "flash",
                     "pub_time": "2026-07-01 09:00:00", "url": None,
                     "score": 3.5,
                     "highlight": ["<em>甲</em>", "片1", "片2"]}   # title+content 拼接
    assert out["hits"][1]["highlight"] is None        # 无高亮 → None
    assert out["hits"][1]["url"] == "http://x/B"


def test_search_404_returns_empty(env):
    """冷启动（别名/索引未建）404 → 空结果，不炸 AI 消费方。"""
    env.search_exc = NotFoundError(404, "index_not_found_exception", {})
    assert ns.search_news("茅台", mode="bm25") == {"total": 0, "hits": []}


# ---------- ensure_hybrid_pipeline ----------

class _FakeTransport:
    def __init__(self, exc=None):
        self.calls = []
        self._exc = exc

    def perform_request(self, method, url, body=None, **kw):
        self.calls.append((method, url, body))
        if self._exc is not None:
            raise self._exc
        return {"acknowledged": True}


class _FakePipelineClient:
    def __init__(self, exc=None):
        self.transport = _FakeTransport(exc)


def test_ensure_hybrid_pipeline_body():
    client = _FakePipelineClient()
    ns.ensure_hybrid_pipeline(client)
    (method, url, body), = client.transport.calls
    assert method == "PUT"
    assert url == "/_search/pipeline/news-hybrid"
    assert body == {"phase_results_processors": [{"normalization-processor": {
        "normalization": {"technique": "min_max"},
        "combination": {"technique": "arithmetic_mean"}}}]}


def test_ensure_hybrid_pipeline_403_guidance():
    """news_writer 无集群级权限（§5.6）→ RuntimeError 带管理员建置指引。"""
    client = _FakePipelineClient(AuthorizationException(403, "security_exception", {}))
    with pytest.raises(RuntimeError, match="管理员") as excinfo:
        ns.ensure_hybrid_pipeline(client)
    assert "news-hybrid" in str(excinfo.value)


# ================= 集成测试（连真实 9.12，默认跳过） =================

@pytest.mark.integration
def test_integration_bm25_smoke():
    """bm25 对别名 news 真实查询：无数据/无别名亦不抛，返回结构正确。

    不测 hybrid 端到端（search pipeline 属建置动作，可能尚未建）。
    """
    out = ns.search_news("政策", top_k=3, mode="bm25")
    assert set(out.keys()) == {"total", "hits"}
    assert isinstance(out["total"], int)
    for hit in out["hits"]:
        assert set(hit.keys()) == {"title", "source", "channel", "pub_time",
                                   "url", "score", "highlight"}
