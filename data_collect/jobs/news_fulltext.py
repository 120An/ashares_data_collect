"""全文抽取任务（新闻子系统 ⑥ Phase 2）：抓个股新闻正文写 body、触发全文重编码。

核心流程（扫 pending → 并发抽 → 写回）在共享核心 `utils/bodyfill.py`（⑤ 公告 PDF
同调）；本 job 只负责 stock 参数拼装 + trafilatura 依赖预检 fail-fast（缺依赖时
绝不把整批错标 failed）。抽取不依赖 GPU。
"""

from __future__ import annotations

import logging

from data_collect.utils import bodyfill
from data_collect.utils import fulltext
from data_collect.utils import opensearch_utils as osu

logger = logging.getLogger(__name__)

_BATCH_LIMIT = 500        # 单轮快照上限（bounded：防超大积压把子进程拖过 timeout）
_MAX_WORKERS = 6          # 并发抓取度（多站点分散；适度以不压高占比站点）
_BODY_MAX_CHARS = 50_000  # body 截断上限（新闻正文极少触及；与公告统一契约）


def _trafilatura_available() -> bool:
    """trafilatura 是否可用（run 起始预检，独立函数便于单测 monkeypatch）。"""
    import importlib.util
    return importlib.util.find_spec("trafilatura") is not None


# ======== Pipeline 标准接口 ========

def run(run_date: str | None = None, **kwargs) -> str:
    """每轮任务：stock 全文抽取（参数化调用 bodyfill 共享核心）。

    `run_date` 仅签名兼容——抽取与日期无关，扫全库 stock pending。
    依赖预检：trafilatura 缺失立即 raise（框架通知），绝不进抽取——否则
    ImportError 被 best-effort 吞掉，整批被永久错标 failed（failed 不再被扫）。
    """
    if not _trafilatura_available():
        raise RuntimeError(
            "trafilatura 未安装，news_fulltext 终止（不标 failed）；pip install trafilatura")
    client = osu.get_client()
    return bodyfill.fill_bodies(
        client, channel="stock", status_field="body_status",
        extract_fn=fulltext.fetch_article, batch_limit=_BATCH_LIMIT,
        max_workers=_MAX_WORKERS, body_max_chars=_BODY_MAX_CHARS, label="全文抽取")
