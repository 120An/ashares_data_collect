"""embedding 单元测试（假模型/mock requests，不下载模型不发网络）+ 真实模型集成测试（@integration）。"""

import numpy as np
import pytest

from data_collect.utils import embedding


@pytest.fixture(autouse=True)
def _clean_singleton():
    """每个用例前后清空模块级单例，避免用例间串扰。"""
    embedding._reset_embedder()
    yield
    embedding._reset_embedder()


class _FakeModel:
    """假 sentence-transformers 模型：记录调用参数，返回**未归一**定长向量。

    返回未归一向量是关键——用于证明 encode 出口统一 L2 归一化生效
    （而非依赖真实模型的 normalize_embeddings 参数）。
    """

    def __init__(self, dim=1024):
        self.dim = dim
        self.calls = []

    def encode(self, texts, **kwargs):
        self.calls.append((list(texts), kwargs))
        n = len(texts)
        # 每行值不同、范数远离 1（未归一）、dtype float64（验证出口转 float32）
        return (np.arange(n * self.dim, dtype=np.float64).reshape(n, self.dim) + 1.0)


# ---------- local 后端：归一/维度/dtype ----------

def test_encode_normalizes_shape_dtype():
    emb = embedding.Embedder({"backend": "local", "model": "fake", "batch_size": 4})
    emb._model = _FakeModel(dim=1024)          # 注入假模型，绕过加载
    out = emb.encode(["a", "b", "c"])
    assert out.shape == (3, 1024)              # 批量形状
    assert out.dtype == np.float32
    assert np.allclose(np.linalg.norm(out, axis=1), 1.0, atol=1e-5)   # 出口归一 L2≈1


def test_encode_passes_batch_size_to_model():
    emb = embedding.Embedder({"backend": "local", "model": "fake", "batch_size": 7})
    fake = _FakeModel()
    emb._model = fake
    emb.encode(["x", "y"])
    (texts, kwargs), = fake.calls
    assert texts == ["x", "y"]
    assert kwargs["batch_size"] == 7
    assert kwargs["normalize_embeddings"] is True
    assert kwargs["convert_to_numpy"] is True
    assert kwargs["show_progress_bar"] is False


def test_encode_zero_vector_row_preserved():
    """零向量守卫：全零行保持原样（不除零不产生 NaN），其余行正常归一。"""
    class _ZeroRowModel:
        def encode(self, texts, **kwargs):
            return np.array([[0.0, 0.0, 0.0, 0.0],
                             [3.0, 4.0, 0.0, 0.0]], dtype=np.float64)

    emb = embedding.Embedder({"backend": "local", "model": "fake"})
    emb._model = _ZeroRowModel()
    out = emb.encode(["z", "n"])
    assert out.dtype == np.float32
    assert np.allclose(out[0], 0.0)                                   # 零行保持零
    assert np.allclose(out[1], [0.6, 0.8, 0.0, 0.0], atol=1e-6)       # 非零行归一
    assert not np.any(np.isnan(out))


def test_encode_empty_returns_zero_shape_without_loading(monkeypatch):
    def boom(self):
        raise AssertionError("空列表不应触发模型加载")
    monkeypatch.setattr(embedding.Embedder, "_load_model", boom)
    emb = embedding.Embedder({"backend": "local", "model": "fake"})
    out = emb.encode([])
    assert out.shape == (0, 0)
    assert out.dtype == np.float32
    assert emb._model is None


def test_encode_lazy_loads_model_once(monkeypatch):
    fake = _FakeModel()
    loads = []
    monkeypatch.setattr(embedding.Embedder, "_load_model",
                        lambda self: loads.append(1) or fake)
    emb = embedding.Embedder({"backend": "local", "model": "fake"})
    assert emb._model is None                  # 构造不加载（lazy）
    emb.encode(["a"])
    emb.encode(["b"])
    assert len(loads) == 1                     # 首次 encode 加载且仅一次


# ---------- ollama 后端 ----------

class _FakeResp:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text

    def json(self):
        return self._payload


def test_ollama_chunks_payload_and_normalizes(monkeypatch):
    calls = []

    def fake_post(url, json=None, timeout=None):
        calls.append((url, json, timeout))
        n = len(json["input"])
        return _FakeResp(payload={"embeddings": [[1.0, 2.0, 2.0, 4.0]] * n})  # 未归一（范数 5）

    monkeypatch.setattr(embedding.requests, "post", fake_post)
    emb = embedding.Embedder({"backend": "ollama", "model": "bge-m3",
                              "batch_size": 2, "ollama_url": "http://h:11434"})
    out = emb.encode(["t1", "t2", "t3", "t4", "t5"])
    assert len(calls) == 3                                        # 5 条 batch_size=2 → 3 次
    assert calls[0][0] == "http://h:11434/api/embed"
    assert calls[0][1] == {"model": "bge-m3", "input": ["t1", "t2"]}
    assert calls[1][1]["input"] == ["t3", "t4"]
    assert calls[2][1]["input"] == ["t5"]
    assert all(timeout == 120 for _, _, timeout in calls)
    assert out.shape == (5, 4)                                    # 分块拼接形状
    assert out.dtype == np.float32
    assert np.allclose(np.linalg.norm(out, axis=1), 1.0, atol=1e-5)   # 出口归一


def test_ollama_embedding_count_mismatch_raises(monkeypatch):
    """条数校验：返回条数 != 请求条数必须报错——下游按位置对齐 _id，错位=静默数据损坏。"""
    def fake_post(url, json=None, timeout=None):
        n = len(json["input"]) - 1                                    # 少返回一条
        return _FakeResp(payload={"embeddings": [[1.0, 0.0]] * n})

    monkeypatch.setattr(embedding.requests, "post", fake_post)
    emb = embedding.Embedder({"backend": "ollama", "model": "bge-m3",
                              "batch_size": 4, "ollama_url": "http://h:11434"})
    with pytest.raises(RuntimeError, match="返回 1 条.*请求 2 条"):
        emb.encode(["t1", "t2"])


def test_ollama_non_json_response_raises(monkeypatch):
    """200 但非 JSON（如网关返回 HTML）→ RuntimeError 带响应片段。"""
    class _HtmlResp:
        status_code = 200
        text = "<html>gateway</html>"

        def json(self):
            raise ValueError("Expecting value")

    monkeypatch.setattr(embedding.requests, "post",
                        lambda url, json=None, timeout=None: _HtmlResp())
    emb = embedding.Embedder({"backend": "ollama", "model": "bge-m3",
                              "ollama_url": "http://h:11434"})
    with pytest.raises(RuntimeError, match="非 JSON"):
        emb.encode(["t"])


def test_ollama_non_200_raises(monkeypatch):
    monkeypatch.setattr(embedding.requests, "post",
                        lambda url, json=None, timeout=None: _FakeResp(status_code=500, text="boom"))
    emb = embedding.Embedder({"backend": "ollama", "model": "bge-m3",
                              "ollama_url": "http://h:11434"})
    with pytest.raises(RuntimeError, match="500"):
        emb.encode(["t"])


def test_ollama_missing_embeddings_key_raises(monkeypatch):
    monkeypatch.setattr(embedding.requests, "post",
                        lambda url, json=None, timeout=None: _FakeResp(payload={"error": "x"}))
    emb = embedding.Embedder({"backend": "ollama", "model": "bge-m3",
                              "ollama_url": "http://h:11434"})
    with pytest.raises(RuntimeError, match="embeddings"):
        emb.encode(["t"])


def test_ollama_missing_url_raises():
    with pytest.raises(ValueError, match="ollama_url"):
        embedding.Embedder({"backend": "ollama", "model": "bge-m3"})


# ---------- 配置校验 / 单例 ----------

def test_unknown_backend_raises():
    with pytest.raises(ValueError, match="backend"):
        embedding.Embedder({"backend": "wat"})


def test_get_embedder_singleton(monkeypatch):
    monkeypatch.setattr(embedding, "get_news_config",
                        lambda: {"embedding": {"backend": "local", "model": "m"}})
    e1 = embedding.get_embedder()
    e2 = embedding.get_embedder()
    assert e1 is e2                            # 模块级单例
    assert e1.model_name == "m"
    assert e1.batch_size == 32                 # 缺省值


# ================= 集成测试（真实模型，首次下载慢，默认跳过） =================

@pytest.mark.integration
def test_integration_real_model_roundtrip():
    emb = embedding.Embedder({"backend": "local", "model": "BAAI/bge-m3",
                              "device": "cpu", "batch_size": 4})
    out = emb.encode(["新闻测试文本"])
    assert out.shape == (1, 1024)
    assert out.dtype == np.float32
    assert np.allclose(np.linalg.norm(out, axis=1), 1.0, atol=1e-3)
