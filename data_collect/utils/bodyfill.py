"""全文回填共享核心（新闻子系统 ⑥ 个股全文 / ⑤ 公告 PDF 共用）。

`fill_bodies`：扫 `channel ∧ {status_field}=pending` 显式 (_index,_id,url) 快照 →
线程池并发 extract_fn(url) → clean_text → 截断 body_max_chars → bulk_update 写
`{body, {status_field}: done, vec_status: pending}` / 失败 `{status_field: failed}`。

状态机幂等（done/failed 不再被扫）；`body` 走 create-only 下 sanctioned bulk_update
（显式 (_index,_id)，绝不 update_by_query——同 news_embed 协议）。extract_fn 约定：
`(url) -> str | None`，内部吞异常（本核心再兜一层，单篇失败绝不拖垮整轮）。
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Tuple

from data_collect.utils import news_normalize as nn
from data_collect.utils import opensearch_utils as osu

logger = logging.getLogger(__name__)

# 分段写回粒度：每 N 篇完成即 bulk_update 一段——子进程超时硬杀最多丢一段进度，
# 已写回的 done/failed 使下轮快照缩小（否则整轮末尾一次性写回，慢源场景下
# "整批重下→再超时"活锁）
_FLUSH_EVERY = 50


def _snapshot_pending(client, channel: str, status_field: str,
                      batch_limit: int) -> List[Tuple[str, str, str]]:
    """扫 channel ∧ {status_field}=pending 快照 → [(物理索引, _id, url), ...]。

    fetch_time asc（先入库先抽）；`_index` 取 hit 自带值（写回直达物理索引）；
    查询异常（别名未建/404）→ 视为 0 条本轮空转（同 news_embed 容错模式）。
    """
    body = {
        "query": {"bool": {"must": [
            {"term": {"channel": channel}},
            {"term": {status_field: "pending"}}]}},
        "size": batch_limit,
        "sort": [{"fetch_time": {"order": "asc"}}],
        "_source": ["url"],
    }
    try:
        resp = osu.search_raw(client, body)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"{channel} 全文 pending 快照查询失败（按 0 条，下轮自愈）: {exc!r}")
        return []
    hits = ((resp or {}).get("hits") or {}).get("hits") or []
    return [(h["_index"], h["_id"], (h.get("_source") or {}).get("url") or "")
            for h in hits]


def fill_bodies(client, *, channel: str, status_field: str,
                extract_fn: Callable[[str], str | None], batch_limit: int,
                max_workers: int, body_max_chars: int, label: str) -> str:
    """全文回填一轮：扫 pending → 并发抽取 → 写回；返回摘要串。

    bulk_update 整体失败上抛（框架 retry；快照未置 done/failed 下轮重扫，幂等）。
    """
    snapshot = _snapshot_pending(client, channel, status_field, batch_limit)
    if not snapshot:
        return f"{label}: 无待抓 pending"

    def _one(url: str) -> str | None:
        """抽取+清洗单篇；**全体入 try**——clean_text 对毒文档抛错若逃逸，
        fut.result() 会崩掉整轮且毒文档永不置态（每轮重下重崩，队头卡死）。"""
        try:
            if not url:
                return None
            text = extract_fn(url)
            if not text:
                return None
            return nn.clean_text(text) or None
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"{label} 抽取/清洗失败 {url}: {exc!r}")
            return None

    updates: List[Dict[str, Any]] = []
    done = failed = written = 0

    def _flush() -> None:
        nonlocal written
        if updates:
            written += osu.bulk_update(client, updates)
            updates.clear()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        fut_map = {executor.submit(_one, url): (index_name, doc_id)
                   for index_name, doc_id, url in snapshot}
        for fut in as_completed(fut_map):
            index_name, doc_id = fut_map[fut]
            text = fut.result()                    # _one 已吞异常
            if text:
                doc: Dict[str, Any] = {"body": text[:body_max_chars],
                                       status_field: "done",
                                       "vec_status": "pending"}
                if len(text) > body_max_chars:
                    doc["body_truncated"] = True   # 超长留痕（boolean 动态映射无害）
                updates.append({"_index": index_name, "_id": doc_id, "doc": doc})
                done += 1
            else:
                updates.append({"_index": index_name, "_id": doc_id,
                                "doc": {status_field: "failed"}})
                failed += 1
            if len(updates) >= _FLUSH_EVERY:       # 分段写回（见 _FLUSH_EVERY 注释）
                _flush()

    _flush()
    return f"{label}: 成功 {done} 失败 {failed}, 写回 {written}"
