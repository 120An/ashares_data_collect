"""公告 PDF 正文抽取任务（新闻子系统 ⑤ Phase 2）。

扫 `channel=announcement ∧ pdf_status=pending` → 下载 PDF + PyMuPDF 抽文本
（utils/pdftext）→ 写 body（截断 5 万字）+ pdf_status=done + vec_status=pending
（news_embed 已 body 优先，公告全文向量零改动）；失败 failed（title 兜底不重试）。

核心流程在 `utils/bodyfill.py`（与 ⑥ news_fulltext 共用）；本 job 只做 announcement
参数拼装 + pymupdf 依赖预检 fail-fast。批 200/并发 4：PDF 单篇几百 KB~几 MB 且
全打 static.cninfo.com.cn 单站——收敛并发礼貌抓取。
"""

from __future__ import annotations

import logging

from data_collect.utils import bodyfill
from data_collect.utils import opensearch_utils as osu
from data_collect.utils import pdftext

logger = logging.getLogger(__name__)

_BATCH_LIMIT = 200        # 单轮快照上限（200×~2-3s/4并发 ≈ 2-5min，timeout 1800 余量足）
_MAX_WORKERS = 4          # 单站（static.cninfo.com.cn）礼貌并发
_BODY_MAX_CHARS = 50_000  # 年报级正文截断（检索意义上前 5 万字足够；向量只吃前 ~8k token）


def _pymupdf_available() -> bool:
    """pymupdf 是否可用（run 起始预检；pymupdf 的导入名是 fitz）。"""
    import importlib.util
    return importlib.util.find_spec("fitz") is not None


# ======== Pipeline 标准接口 ========

def run(run_date: str | None = None, **kwargs) -> str:
    """每轮任务：公告 PDF 正文抽取（参数化调用 bodyfill 共享核心）。

    `run_date` 仅签名兼容——抽取与日期无关，扫全库 announcement pending。
    依赖预检：pymupdf 缺失立即 raise（框架通知），绝不进抽取——否则 ImportError
    被 best-effort 吞掉，整批公告被永久错标 failed（同 trafilatura 教训）。
    """
    if not _pymupdf_available():
        raise RuntimeError(
            "pymupdf 未安装，news_announce_pdf 终止（不标 failed）；pip install pymupdf")
    client = osu.get_client()
    return bodyfill.fill_bodies(
        client, channel="announcement", status_field="pdf_status",
        extract_fn=pdftext.fetch_pdf_text, batch_limit=_BATCH_LIMIT,
        max_workers=_MAX_WORKERS, body_max_chars=_BODY_MAX_CHARS, label="公告PDF")
