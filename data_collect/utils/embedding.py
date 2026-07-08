"""文本向量化：BGE-M3 嵌入编码（local / ollama 双后端）。

新闻系统向量层（docs/superpowers/specs/2026-07-02-news-opensearch-plan.md）：
- Embedder 构造只存配置**不加载模型**（lazy）：sentence-transformers 加载 bge-m3
  约 1~2GB、需 10~30s，15 分钟级任务在 import/构造期加载不可承受，
  故 encode() 首次调用才加载，且顶层绝不 import sentence_transformers；
- encode 出口统一按行 L2 归一化（backend 无关），向量点积即余弦相似度；
- get_embedder() 模块级单例：同进程复用同一实例，模型只加载一次。

backend（config.yaml news.embedding.backend）：
- local：sentence-transformers 本地推理，model 为 HuggingFace 名（如 "BAAI/bge-m3"）；
- ollama：调 Ollama REST /api/embed，model 为 Ollama 模型名（如 "bge-m3"，
  与 local 的 HF 名不同），需配 ollama_url。
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List

import numpy as np
import requests

from data_collect.config import get_news_config

logger = logging.getLogger(__name__)

# 模块级单例缓存（get_embedder / _reset_embedder）
_embedder: Embedder | None = None


def _l2_normalize(arr: np.ndarray) -> np.ndarray:
    """按行 L2 归一化；范数 < 1e-12 的行（零向量）保持原样，防除零。"""
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    safe = np.where(norms < 1e-12, 1.0, norms)
    return arr / safe


class Embedder:
    """文本向量编码器（backend 抽象：local=sentence-transformers | ollama=REST）。

    构造时只读配置不加载模型；encode() 首次调用才加载（lazy，动机见模块 docstring）。
    测试可注入：monkeypatch `Embedder._load_model` 或直接给实例 `_model` 赋假模型。
    """

    def __init__(self, cfg: Dict[str, Any] | None = None):
        """cfg 缺省读 config.yaml 的 news.embedding 段（测试可显式传入覆盖）。"""
        if cfg is None:
            cfg = get_news_config().get("embedding") or {}
        self.backend = cfg.get("backend", "local")
        if self.backend not in ("local", "ollama"):
            raise ValueError(f"未知 embedding backend: {self.backend!r}（支持 local/ollama）")
        self.model_name = cfg.get("model", "BAAI/bge-m3")
        self.device = cfg.get("device", "cpu")
        self.batch_size = int(cfg.get("batch_size", 32))
        self.ollama_url = cfg.get("ollama_url")
        if self.backend == "ollama" and not self.ollama_url:
            raise ValueError("backend=ollama 需在 news.embedding.ollama_url 配置服务地址")
        self._model = None  # lazy：encode 首次调用才加载

    def _load_model(self):
        """加载 sentence-transformers 模型（仅 local 后端，encode 首次调用触发）。"""
        # 未设置时指向国内镜像，避免 HuggingFace 直连下载超时；已设置则尊重现值
        os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")
        from sentence_transformers import SentenceTransformer  # lazy import，动机见模块 docstring

        logger.info(f"加载 embedding 模型 {self.model_name}（device={self.device}）...")
        return SentenceTransformer(self.model_name, device=self.device)

    def encode(self, texts: List[str]) -> np.ndarray:
        """批量编码文本 -> (N, dim) float32（bge-m3 为 1024 维），每行 L2 归一（范数≈1）。

        空列表直接返回 np.zeros((0, 0))、**不触发模型加载**——调用方应先判空。
        """
        if not texts:
            return np.zeros((0, 0), dtype=np.float32)
        if self.backend == "ollama":
            vecs = self._encode_ollama(texts)
        else:
            vecs = self._encode_local(texts)
        # 出口统一归一（backend 无关的保证；local 的 normalize_embeddings 只是双保险）；
        # 入口已转 float32，_l2_normalize 保持 dtype
        return _l2_normalize(np.asarray(vecs, dtype=np.float32))

    def _encode_local(self, texts: List[str]) -> np.ndarray:
        if self._model is None:
            self._model = self._load_model()
        return self._model.encode(
            texts,
            batch_size=self.batch_size,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )

    def _encode_ollama(self, texts: List[str]) -> List[List[float]]:
        """按 batch_size 分块调 Ollama /api/embed（限单请求 payload），拼接结果。

        逐块校验返回条数==请求条数：下游按位置 zip 回文档 _id，
        条数错位=错误向量写错文档（静默数据损坏），必须 fail-fast。
        """
        all_vecs: List[List[float]] = []
        for i in range(0, len(texts), self.batch_size):
            chunk = texts[i:i + self.batch_size]
            resp = requests.post(
                f"{self.ollama_url}/api/embed",
                json={"model": self.model_name, "input": chunk},
                timeout=120,
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Ollama /api/embed HTTP {resp.status_code}: {resp.text[:200]}"
                )
            try:
                data = resp.json()
            except ValueError:  # 网关/代理返回 200 + HTML 等非 JSON 场景
                raise RuntimeError(f"Ollama 非 JSON 响应: {resp.text[:200]}")
            if "embeddings" not in data:
                raise RuntimeError(f"Ollama 响应缺少 embeddings 键: {str(data)[:200]}")
            embs = data["embeddings"]
            if len(embs) != len(chunk):
                raise RuntimeError(
                    f"Ollama 返回 {len(embs)} 条 embedding，请求 {len(chunk)} 条——拒绝静默错位"
                )
            all_vecs.extend(embs)
        return all_vecs


def get_embedder() -> Embedder:
    """模块级单例：同进程复用同一 Embedder，避免模型重复加载。"""
    global _embedder
    if _embedder is None:
        _embedder = Embedder()
    return _embedder


def _reset_embedder() -> None:
    """清空单例缓存（测试用）。"""
    global _embedder
    _embedder = None
