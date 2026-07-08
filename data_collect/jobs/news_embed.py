"""小时级补向量任务（新闻系统架构 §7.2 "入库先行、小时级补向量"）。

采集 job 只写文本（`vec_status=pending`）秒级完成、BM25 立即可搜；本 job 每小时
批量补 `content_vec` 并置 `done`（模型加载 10~30s，只有小时级批处理摊得平）。

**显式 `_id` 协议**（本 job 的核心正确性约束）：

1. 扫 pending 取**显式 (_index, _id) 快照**（bounded ≤ _BATCH_LIMIT，超出留待下轮）；
2. 批量 encode（快照顺序 ↔ 向量行序严格对齐）；
3. **按快照逐条 update 写回**——`content_vec` 与 `vec_status=done` 同一 update
   原子写；**绝不 `update_by_query` 盲改**：扫描与写回之间新插入的 pending 若被
   盲改置 done，就永远漏掉向量化（无向量却不再被扫到）。写回用 hit 自带的
   **物理 `_index`**（跨年文档分属不同物理索引，写别名有歧义）。

积压兜底：写回后重查 pending，总数 > _BACKLOG_COUNT 或最老滞留 > _BACKLOG_AGE_HOURS
小时 → 钉钉告警（调度跟不上产出/模型服务故障的监控信号，spec §6）。
编码/写回异常一律上抛（框架 retry + 失败通知）。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

from data_collect.utils import embedding
from data_collect.utils import notify
from data_collect.utils import opensearch_utils as osu
from data_collect.utils.news_common import age_minutes as _age_minutes
from data_collect.utils.news_common import now as _now
from data_collect.utils.notify import send_dingtalk

logger = logging.getLogger(__name__)

_BATCH_LIMIT = 2000        # 单轮快照上限（bounded：防一次吃超大积压把子进程拖过 timeout）
# 编码文本截断预算：body 可达 5 万字，而 Ollama bge-m3 ctx=4096 token（中文 XLM-R
# ≈1 字/token）——超限 /api/embed 直接 400 使整轮失败。3000 字 ≈3200 token 留余量；
# 向量取文首（标题+正文开头语义代表性最强，与检索场景一致）
_EMBED_MAX_CHARS = 3000
_BACKLOG_COUNT = 2000      # 积压告警阈值：写回后剩余 pending 总数（严格大于才告）
_BACKLOG_AGE_HOURS = 2     # 积压告警阈值：最老 pending 滞留小时数（严格大于才告）


def _alert(message: str) -> bool:
    """guarded 钉钉：发送失败仅记日志，绝不阻塞补向量主流程；返回是否发送成功。

    委托 notify.guarded_send，注入本模块级 send_dingtalk（保留其 monkeypatch 点）。
    """
    return notify.guarded_send(message, send=send_dingtalk)


def _snapshot_pending(client) -> List[Tuple[str, str, dict]]:
    """扫 pending 快照：返回 [(物理索引, _id, _source), ...]，最多 _BATCH_LIMIT 条。

    - `_index` 取 hit 自带值（写回直达物理索引的依据，见模块 docstring）；
    - fetch_time asc：先入库先编码，积压时最老的先清（与积压年龄告警口径一致）；
    - 查询异常（含冷启动别名尚未建、404）→ 视为 0 条本轮空转，下轮自愈
      （同 news_cctv._day_has_data 的容错模式）。
    """
    body = {
        "query": {"term": {"vec_status": "pending"}},
        "size": _BATCH_LIMIT,
        "sort": [{"fetch_time": {"order": "asc"}}],
        "_source": ["title", "content", "body", "fetch_time"],
    }
    try:
        resp = osu.search_raw(client, body)
    except Exception as exc:
        logger.warning(f"pending 快照查询失败（按 0 条处理，下轮自愈）: {exc!r}")
        return []
    hits = ((resp or {}).get("hits") or {}).get("hits") or []
    return [(h["_index"], h["_id"], h.get("_source") or {}) for h in hits]


def _pending_backlog(client) -> Tuple[int | None, str | None]:
    """写回后重查积压：返回 (pending 总数, 最老 pending 的 fetch_time)。

    一次查询同时取 total 与最老 hit（size=1 + fetch_time asc + track_total_hits，
    总数不被 10000 上限截断）。查询失败返回 (None, None)——积压检查是监控职责，
    不得反噬已成功的写回（本轮成果已落库，失败重跑反而重复编码）。

    已知噪声（接受不修）：OpenSearch 近实时，刚写回的 done 在 refresh（默认 1s）
    前仍可能被本查询计为 pending（news_writer 无 refresh 权限无法强刷）——大积压
    **恰好清完的那一轮**可能多发一条告警，下轮自愈；用 must_not ids 排除快照
    可根治但查询体膨胀 ~2000 id，不值得。
    """
    body = {
        "query": {"term": {"vec_status": "pending"}},
        "size": 1,
        "sort": [{"fetch_time": {"order": "asc"}}],
        "_source": ["fetch_time"],
        "track_total_hits": True,
    }
    try:
        resp = osu.search_raw(client, body)
    except Exception as exc:
        logger.warning(f"pending 积压查询失败（跳过本轮积压检查）: {exc!r}")
        return None, None
    hits_obj = (resp or {}).get("hits") or {}
    total = hits_obj.get("total", 0)
    if isinstance(total, dict):                       # 新式 {"value": N, "relation": ...}
        total = total.get("value", 0)
    hits = hits_obj.get("hits") or []
    oldest = (hits[0].get("_source") or {}).get("fetch_time") if hits else None
    return int(total or 0), oldest


# ======== Pipeline 标准接口 ========

def run(run_date: str | None = None, **kwargs) -> str:
    """每小时任务：扫 pending 快照 → 批量编码 → 按显式 (_index, _id) 写回置 done。

    `run_date` 仅为框架签名兼容——补向量与日期无关，扫的是全库 pending。
    本轮快照之后新增的 pending 不写回、留待下轮（显式 `_id` 协议，见模块 docstring）；
    编码/写回异常上抛（写回前抛则全量留 pending，重试幂等无重复向量风险）。
    """
    client = osu.get_client()
    snapshot = _snapshot_pending(client)
    if not snapshot:
        return "补向量: 无待编码 pending"          # 早退：不加载模型（lazy 的意义所在）

    # 拼接编码文本；strip 后为空的跳过编码但**仍置 done**：空文本无向量意义，
    # 留在 pending 会每轮重扫重跳、永久卡住积压告警——置 done（无 content_vec）终止
    updates: List[Dict[str, Any]] = []
    texts: List[str] = []
    vec_docs: List[Dict[str, Any]] = []               # 与 texts 一一对应的 doc 引用
    for index_name, doc_id, source in snapshot:
        doc: Dict[str, Any] = {"vec_status": "done"}
        # body 优先（⑥ Phase2 全文向量）：有 body 编码全文，否则回退摘要 content
        body_or_summary = source.get("body") or source.get("content") or ""
        text = f"{source.get('title') or ''}\n{body_or_summary}".strip()[:_EMBED_MAX_CHARS]
        if text:
            texts.append(text)
            vec_docs.append(doc)
        updates.append({"_index": index_name, "_id": doc_id, "doc": doc})

    if texts:
        vectors = embedding.get_embedder().encode(texts)
        if len(vectors) != len(texts):                # 双保险：错位=向量写错文档
            raise RuntimeError(
                f"encode 返回 {len(vectors)} 条向量，输入 {len(texts)} 条——拒绝错位写回")
        for doc, vec in zip(vec_docs, vectors):
            doc["content_vec"] = vec.tolist()         # numpy → Python float 列表（JSON 可序列化）

    ok = osu.bulk_update(client, updates)             # content_vec+done 同一 update 原子写

    # 积压检查（写回后重查——快照期间新插入的 pending 计入下轮工作量与积压口径）
    total, oldest_fetch = _pending_backlog(client)
    oldest_minutes = _age_minutes(oldest_fetch, _now())
    if total is not None:
        over_count = total > _BACKLOG_COUNT
        over_age = oldest_minutes is not None and oldest_minutes > _BACKLOG_AGE_HOURS * 60
        if over_count or over_age:
            _alert(
                f"补向量积压告警：剩余 pending {total} 条（阈值 {_BACKLOG_COUNT}），"
                f"最老已滞留 {oldest_minutes if oldest_minutes is not None else '?'} 分钟"
                f"（阈值 {_BACKLOG_AGE_HOURS * 60}）；请检查模型/编码吞吐或加密调度频率"
            )

    skipped = len(snapshot) - len(texts)
    summary = (f"补向量: 编码 {len(texts)} 条(跳过空文本 {skipped}), 写回 {ok}, "
               f"剩余 pending {total if total is not None else '查询失败'}")
    if oldest_minutes is not None:
        summary += f", 最老 {oldest_minutes} 分钟"
    return summary
