"""opensearch_utils 单元测试（mock，不连库）+ 集成测试（@integration，连 9.12）。"""

import pytest
from opensearchpy.exceptions import AuthorizationException, RequestError

from data_collect.utils import opensearch_utils as osu


# ---------- 按年路由 ----------

def test_index_name_for_by_pub_year():
    assert osu.index_name_for({"pub_time": "2026-06-30 09:00:00"}) == "news-2026"


def test_index_name_for_fallback_fetch_time():
    assert osu.index_name_for({"fetch_time": "2027-01-01 00:00:00"}) == "news-2027"


def test_index_name_for_bad_time_raises():
    with pytest.raises(ValueError):
        osu.index_name_for({"title": "no time"})


# ---------- create-only 动作构造 ----------

def test_build_actions_create_only_and_routing():
    docs = [{"_id": "x1", "pub_time": "2026-05-01 10:00:00", "title": "a"},
            {"_id": "x2", "pub_time": "2027-05-01 10:00:00", "title": "b"}]
    actions = osu._build_actions(docs)
    assert actions[0]["_op_type"] == "create"          # 不可变，防向量回退
    assert actions[0]["_index"] == "news-2026"
    assert actions[0]["_id"] == "x1"
    assert "_id" not in actions[0]["_source"]          # _id 不重复进 _source
    assert actions[0]["_source"]["title"] == "a"
    assert actions[1]["_index"] == "news-2027"


def test_build_actions_missing_id_raises():
    with pytest.raises(ValueError, match="_id"):
        osu._build_actions([{"pub_time": "2026-05-01 10:00:00", "title": "无id"}])


# ---------- analyzer 探测 ----------

class _FakeCatClient:
    def __init__(self, components):
        self._components = components
        self.cat = self  # client.cat.plugins

    def plugins(self, format=None):
        return [{"component": c} for c in self._components]


def test_probe_analyzer_prefers_ik():
    assert osu.probe_analyzer(_FakeCatClient(["analysis-ik", "analysis-smartcn"])) == "ik_max_word"


def test_probe_analyzer_smartcn():
    assert osu.probe_analyzer(_FakeCatClient(["analysis-smartcn"])) == "smartcn"


def test_probe_analyzer_fallback_standard():
    assert osu.probe_analyzer(_FakeCatClient(["opensearch-knn"])) == "standard"


# ---------- ensure_index（fake client，覆盖三分支） ----------

class _FakeIndices:
    def __init__(self, create_exc=None):
        self._create_exc = create_exc
        self.create_calls = []
        self.put_alias_calls = []

    def create(self, index=None, body=None):
        self.create_calls.append((index, body))
        if self._create_exc is not None:
            raise self._create_exc
        return {"acknowledged": True}

    def put_alias(self, index=None, name=None):
        self.put_alias_calls.append((index, name))
        return {"acknowledged": True}


class _FakeIndexClient:
    """带 indices/cat.plugins 的桩，覆盖 ensure_index 的建索引+挂别名路径。"""

    def __init__(self, components=("analysis-smartcn",), create_exc=None):
        self.indices = _FakeIndices(create_exc)
        self.cat = self  # client.cat.plugins
        self._components = list(components)

    def plugins(self, format=None):
        return [{"component": c} for c in self._components]


def test_ensure_index_creates_with_probed_analyzer(monkeypatch):
    monkeypatch.setattr(osu, "get_news_config",
                        lambda: {"embedding": {"model": "BAAI/bge-m3"}})
    client = _FakeIndexClient(["analysis-smartcn"])
    assert osu.ensure_index(client, "2026") == "news-2026"
    (index, body), = client.indices.create_calls
    assert index == "news-2026"
    props = body["mappings"]["properties"]
    assert props["title"]["analyzer"] == "smartcn"     # analyzer 注入 title/content
    assert props["content"]["analyzer"] == "smartcn"
    assert body["mappings"]["_meta"] == {"analyzer": "smartcn",
                                         "embedding_model": "BAAI/bge-m3"}
    assert client.indices.put_alias_calls == [("news-2026", "news")]   # 别名挂载


def test_ensure_index_idempotent_when_exists(monkeypatch):
    monkeypatch.setattr(osu, "get_news_config", lambda: {})
    exc = RequestError(400, "resource_already_exists_exception", {})
    client = _FakeIndexClient(create_exc=exc)
    assert osu.ensure_index(client, "2026") == "news-2026"             # 已存在→静默幂等
    assert client.indices.put_alias_calls == [("news-2026", "news")]   # 仍确保别名


def test_ensure_index_reraises_other_request_error(monkeypatch):
    monkeypatch.setattr(osu, "get_news_config", lambda: {})
    client = _FakeIndexClient(create_exc=RequestError(400, "mapper_parsing_exception", {}))
    with pytest.raises(RequestError):                                  # 关键防线：其他错误必须上抛
        osu.ensure_index(client, "2026")


def test_ensure_index_raises_friendly_runtime_on_403(monkeypatch):
    """403（AuthorizationException，非 RequestError）→ 转可操作的 RuntimeError，
    不让裸 403 traceback 在采集首日硬失败（news_writer 缺 news-* create 权限场景）。"""
    monkeypatch.setattr(osu, "get_news_config", lambda: {})
    exc = AuthorizationException(403, "security_exception", {"error": "no permissions"})
    client = _FakeIndexClient(create_exc=exc)
    with pytest.raises(RuntimeError, match=r"403|权限"):
        osu.ensure_index(client, "2026")


def test_ensure_index_bad_year_raises():
    with pytest.raises(ValueError):
        osu.ensure_index(object(), "20xx")     # 校验先于任何客户端调用


# ---------- bulk_create dup/error 分类 ----------

def test_bulk_create_counts_dup(monkeypatch):
    monkeypatch.setattr(osu, "probe_analyzer", lambda client: "standard")
    ensured = []
    monkeypatch.setattr(osu, "ensure_index",
                        lambda client, year, analyzer=None: ensured.append(year) or f"news-{year}")
    def fake_bulk(client, actions, **kw):
        return 1, [{"create": {"status": 409, "_id": "dup1"}}]   # 1 成功 + 1 已存在
    monkeypatch.setattr(osu, "_bulk_helper", fake_bulk)
    ok, dup = osu.bulk_create(object(), [
        {"_id": "a", "pub_time": "2026-01-01 00:00:00"},
        {"_id": "dup1", "pub_time": "2026-01-01 00:00:00"}])
    assert ok == 1 and dup == 1
    assert ensured == ["2026"]                                   # 写前确保目标索引


def test_bulk_create_raises_on_real_error(monkeypatch):
    monkeypatch.setattr(osu, "probe_analyzer", lambda client: "standard")
    monkeypatch.setattr(osu, "ensure_index", lambda client, year, analyzer=None: f"news-{year}")
    def fake_bulk(client, actions, **kw):
        return 0, [{"create": {"status": 400, "error": "mapper_parsing"}}]
    monkeypatch.setattr(osu, "_bulk_helper", fake_bulk)
    with pytest.raises(RuntimeError):
        osu.bulk_create(object(), [{"_id": "a", "pub_time": "2026-01-01 00:00:00"}])


def test_bulk_create_ensures_indices_probe_once(monkeypatch):
    """跨年 bulk：每个目标索引各 ensure 一次，_cat/plugins 仅探测一次（结果复用）。"""
    probes = []
    monkeypatch.setattr(osu, "probe_analyzer", lambda client: probes.append(1) or "smartcn")
    ensured = []
    monkeypatch.setattr(osu, "ensure_index",
                        lambda client, year, analyzer=None: ensured.append((year, analyzer)) or f"news-{year}")
    monkeypatch.setattr(osu, "_bulk_helper", lambda client, actions, **kw: (2, []))
    ok, dup = osu.bulk_create(object(), [
        {"_id": "a", "pub_time": "2026-01-01 00:00:00"},
        {"_id": "b", "pub_time": "2027-01-01 00:00:00"}])
    assert ok == 2 and dup == 0
    assert ensured == [("2026", "smartcn"), ("2027", "smartcn")]   # 排序确定 + analyzer 复用
    assert len(probes) == 1                                        # 只探测一次


def test_bulk_create_empty():
    assert osu.bulk_create(object(), []) == (0, 0)


# ---------- bulk_update 显式 (_index, _id) 局部更新 ----------

def test_bulk_update_action_shape(monkeypatch):
    """update 动作形状：_op_type=update + 显式物理 _index/_id + doc 局部字段。"""
    captured = {}

    def fake_bulk(client, actions, **kw):
        captured["actions"] = list(actions)
        captured["kw"] = kw
        return len(captured["actions"]), []

    monkeypatch.setattr(osu, "_bulk_helper", fake_bulk)
    ok = osu.bulk_update(object(), [
        {"_index": "news-2026", "_id": "a",
         "doc": {"vec_status": "done", "content_vec": [0.1, 0.2]}},
        {"_index": "news-2027", "_id": "b", "doc": {"vec_status": "done"}},
    ])
    assert ok == 2
    first = captured["actions"][0]
    assert first["_op_type"] == "update"
    assert first["_index"] == "news-2026" and first["_id"] == "a"
    assert first["doc"] == {"vec_status": "done", "content_vec": [0.1, 0.2]}
    # 显式物理索引直达（跨年文档写别名有歧义），逐条各带各的 _index
    assert captured["actions"][1]["_index"] == "news-2027"
    assert captured["kw"] == {"raise_on_error": False, "stats_only": False}


def test_bulk_update_raises_on_error(monkeypatch):
    """错误项（如文档不存在 404）→ RuntimeError（截断列出示例），不静默吞。"""
    def fake_bulk(client, actions, **kw):
        return 1, [{"update": {"status": 404, "_id": "gone"}}]

    monkeypatch.setattr(osu, "_bulk_helper", fake_bulk)
    with pytest.raises(RuntimeError, match="更新失败"):
        osu.bulk_update(object(), [
            {"_index": "news-2026", "_id": "ok1", "doc": {"vec_status": "done"}},
            {"_index": "news-2026", "_id": "gone", "doc": {"vec_status": "done"}},
        ])


def test_bulk_update_empty():
    assert osu.bulk_update(object(), []) == 0   # 空列表直返，不触库


# ---------- search_raw 请求参数透传 ----------

def test_search_raw_params_passthrough():
    """params 透传 client.search（hybrid 检索 search_pipeline 参数依赖）；缺省 None。"""
    class _FakeSearchClient:
        def __init__(self):
            self.calls = []

        def search(self, index=None, body=None, params=None):
            self.calls.append((index, body, params))
            return {"hits": {}}

    client = _FakeSearchClient()
    osu.search_raw(client, {"size": 0})
    osu.search_raw(client, {"size": 1}, params={"search_pipeline": "news-hybrid"})
    assert client.calls == [
        ("news", {"size": 0}, None),
        ("news", {"size": 1}, {"search_pipeline": "news-hybrid"}),
    ]


# ---------- mapping 模板：公告字段扩展 ----------

def test_index_template_has_announcement_fields():
    props = osu._INDEX_BODY_TEMPLATE["mappings"]["properties"]
    assert props["ann_type"] == {"type": "keyword"}
    assert props["pdf_status"] == {"type": "keyword"}
    assert props["body"]["type"] == "text"


def test_render_index_body_sets_body_analyzer():
    body = osu._render_index_body("smartcn", "BAAI/bge-m3")
    props = body["mappings"]["properties"]
    # body 与 title/content 同享探测到的 analyzer（中文分词）
    assert props["body"]["analyzer"] == "smartcn"
    assert props["title"]["analyzer"] == "smartcn"


def test_index_template_has_body_status():
    props = osu._INDEX_BODY_TEMPLATE["mappings"]["properties"]
    assert props["body_status"] == {"type": "keyword"}   # 个股新闻 Phase2 全文状态


# ================= 集成测试（连真实 9.12，默认跳过） =================

def _cleanup_news_2099(client):
    """best-effort 清理测试索引：news_writer 设计上无删索引权限（403），
    故删文档+摘别名近似"删除"；indices.delete 尽力而为（留空壳可由管理员删）。"""
    for i in range(3):
        try:
            client.delete(index="news-2099", id=f"itest-{i}")
        except Exception:
            pass
    try:
        client.indices.delete_alias(index="news-2099", name="news")
    except Exception:
        pass
    try:
        client.indices.delete(index="news-2099")
    except Exception:
        pass


@pytest.mark.integration
def test_integration_ensure_create_idempotent():
    client = osu.get_client()
    idx = osu.ensure_index(client, "2099")            # 测试年份
    assert idx == "news-2099"
    docs = [{"_id": f"itest-{i}", "pub_time": "2099-01-01 00:00:00", "fetch_time": "2099-01-01 00:00:00",
             "channel": "cctv", "source": "inttest", "title": f"标题{i}", "content": "正文",
             "stocks": [], "vec_status": "pending"} for i in range(3)]
    try:
        ok, dup = osu.bulk_create(client, docs)
        assert ok == 3
        # 注：两次 bulk 间不 refresh——news_writer 无 indices:admin/refresh 权限（403，实测），
        # 且 create-only 的 409 判定是引擎级按 _id 实时判存在，不依赖 refresh（本集群实测成立）
        ok2, dup2 = osu.bulk_create(client, docs)      # 再写全部已存在
        assert dup2 == 3 and ok2 == 0                  # create-only：零覆盖
    finally:
        _cleanup_news_2099(client)
