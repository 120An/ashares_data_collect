"""OpenSearch 公共工具：客户端、自适应建索引、按年路由、create-only 批量写入。

新闻系统存储层（docs/superpowers/specs/2026-07-02-news-opensearch-plan.md §5）：
- 物理索引按年 news-{year}，统一挂别名 news——写用物理索引名，读用别名；
- ensure_index 先探测 _cat/plugins，按 ik_max_word > smartcn > standard 自适应渲染
  mapping，杜绝"引用未装插件建索引直接失败"；
- 写入 create-only（op_type=create，已存在 409 计 dup）：文档不可变，
  防双活/窗口重叠把 vec_status=done 打回 pending 并抹掉 content_vec。
"""

from __future__ import annotations

import copy
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from opensearchpy import OpenSearch, helpers
from opensearchpy.exceptions import AuthorizationException, RequestError

from data_collect.config import get_news_config, get_opensearch_config
from data_collect.news_model.compat import (
    build_compatibility_projection,
    read_canonical_news,
)
from data_collect.news_model.opensearch_schema import PHASE1_NEWS_ADDITIVE_PROPERTIES

logger = logging.getLogger(__name__)

# 检索别名（读用别名，写用物理索引名）
ALIAS = "news"

# Phase 1 compatibility write gate.  Existing callers omit the parameter and
# therefore retain the pre-Step-6 legacy behavior.
WRITE_MODE_LEGACY = "legacy"
WRITE_MODE_SHADOW = "shadow"
WRITE_MODE_DUAL = "dual"
_WRITE_MODES = frozenset({WRITE_MODE_LEGACY, WRITE_MODE_SHADOW, WRITE_MODE_DUAL})


class NewsWriteConsistencyError(ValueError):
    """A new document contains conflicting canonical and legacy facts."""

# 年份合法界限：新闻数据最早 2016（联播历史下限），下界收紧到 2000
# 以拦截上游时间解析事故（如 epoch 毫秒串 "1750..." 被误当年份）
_YEAR_MIN, _YEAR_MAX = 2000, 2100

# 模块级引用，便于测试 monkeypatch
_bulk_helper = helpers.bulk

# The only fields sanctioned for post-create enrichment.  Identity, source,
# routing and original evidence fields are intentionally absent.
ENRICHMENT_UPDATE_FIELDS = frozenset({
    "body",
    "body_status",
    "body_truncated",
    "pdf_status",
    "content_vec",
    "vec_status",
    "embedding_model_version",
    "updated_at",
    "raw_archive_uri",
})
_ENRICHMENT_UPDATE_KEYS = frozenset({"_index", "_id", "doc", "archive_receipt"})

# Mapping readiness is an explicit deployment decision.  The default preserves
# pre-Step-7 production updates and therefore cannot emit newly mapped metadata.
ENRICHMENT_MODE_LEGACY = "legacy"
ENRICHMENT_MODE_PHASE1 = "phase1"
_ENRICHMENT_MODES = frozenset({ENRICHMENT_MODE_LEGACY, ENRICHMENT_MODE_PHASE1})
_PHASE1_ONLY_ENRICHMENT_FIELDS = frozenset({
    "embedding_model_version",
    "updated_at",
    "raw_archive_uri",
})

# mapping 模板：analyzer 由 probe_analyzer 探测结果注入（见 _render_index_body）
_INDEX_BODY_TEMPLATE: Dict[str, Any] = {
    "settings": {"index.knn": True, "number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "_meta": {},  # 渲染时写入 analyzer / embedding_model 备查
        "properties": {
            "title": {
                "type": "text",
                "fields": {"raw": {"type": "keyword", "ignore_above": 256}},
            },
            "content": {"type": "text"},
            "summary": {"type": "text"},
            "pub_time": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyyMMdd||epoch_millis"},
            "fetch_time": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyyMMdd||epoch_millis"},
            "source": {"type": "keyword"},
            "channel": {"type": "keyword"},
            "url": {"type": "keyword"},
            "stocks": {"type": "keyword"},
            "vec_status": {"type": "keyword"},
            "ann_type": {"type": "keyword"},       # 公告类型（Phase1 预留，巨潮 category 现为 null）
            "pdf_status": {"type": "keyword"},      # 公告 PDF 正文处理状态（Phase2 扫 pending）
            "body_status": {"type": "keyword"},     # 个股新闻全文抓取状态（Phase2 扫 pending）
            "body": {"type": "text"},               # 公告 PDF 正文 / 个股新闻全文（Phase2；analyzer 见 _render_index_body）
            "content_vec": {
                "type": "knn_vector",
                "dimension": 1024,
                "method": {
                    "name": "hnsw",
                    "engine": "lucene",
                    "space_type": "cosinesimil",
                    "parameters": {"m": 16, "ef_construction": 128},
                },
            },
            # Step 5 is the single source of truth for Phase 1 field mappings.
            # Existing indices are not changed here; this only prepares future
            # news-{year} indices created by ensure_index().
            **copy.deepcopy(PHASE1_NEWS_ADDITIVE_PROPERTIES),
        },
    },
}


def get_client() -> OpenSearch:
    """按 config.yaml 的 opensearch 段创建客户端（传输层自带重试）。"""
    cfg = get_opensearch_config()
    return OpenSearch(
        hosts=[{"host": cfg["host"], "port": cfg["port"]}],
        http_auth=(cfg["user"], cfg["password"]),
        use_ssl=cfg.get("use_ssl", True),
        verify_certs=cfg.get("verify_certs", False),
        ssl_show_warn=False,  # 内网自签证书，不刷 InsecureRequestWarning
        timeout=30,
        max_retries=3,
        retry_on_timeout=True,
    )


def probe_analyzer(client) -> str:
    """探测已装分词插件，按 ik > smartcn > standard 优先级返回 analyzer 名。"""
    plugins = client.cat.plugins(format="json") or []
    components = [p.get("component", "") for p in plugins]
    if any("analysis-ik" in c for c in components):
        return "ik_max_word"
    if any("analysis-smartcn" in c for c in components):
        return "smartcn"
    return "standard"


def _render_index_body(analyzer: str, embedding_model: str) -> Dict[str, Any]:
    """渲染 mapping：注入探测到的 analyzer，_meta 记录选定值备查。"""
    body = copy.deepcopy(_INDEX_BODY_TEMPLATE)
    mappings = body["mappings"]
    mappings["_meta"] = {"analyzer": analyzer, "embedding_model": embedding_model}
    mappings["properties"]["title"]["analyzer"] = analyzer
    mappings["properties"]["content"]["analyzer"] = analyzer
    mappings["properties"]["body"]["analyzer"] = analyzer
    return body


def _is_valid_year(year_str: str) -> bool:
    """年份字符串合法性：全数字且在 _YEAR_MIN~_YEAR_MAX 内（index_name_for/ensure_index 共用）。"""
    return year_str.isdigit() and _YEAR_MIN <= int(year_str) <= _YEAR_MAX


def ensure_index(client, year, analyzer: str | None = None) -> str:
    """确保 news-{year} 物理索引与别名 news 存在（幂等），返回物理索引名。

    - 幂等实现为"直接创建、已存在则跳过"：news_writer 运行时账号无
      indices:admin/exists 权限（HEAD /index 403，实测；权限模型见架构 §5.6），
      且 try-create 原子、无"预检查-创建"竞态；
    - 别名无条件 put_alias：幂等操作，省 exists_alias 往返与别名读权限依赖；
    - analyzer 可传入以复用探测结果（bulk_create 多索引场景），缺省自动探测。
    """
    year_str = str(year)
    if not _is_valid_year(year_str):
        raise ValueError(f"非法年份: {year!r}（应为 {_YEAR_MIN}~{_YEAR_MAX} 的数字）")
    index_name = f"news-{year_str}"
    if analyzer is None:
        analyzer = probe_analyzer(client)
    model = (get_news_config().get("embedding") or {}).get("model", "BAAI/bge-m3")
    try:
        client.indices.create(index=index_name, body=_render_index_body(analyzer, model))
        logger.info(f"创建索引 {index_name}（analyzer={analyzer}, embedding_model={model}）")
    except AuthorizationException as exc:
        # 403：news_writer 缺 news-* 的 create 权限（AuthorizationException 是 TransportError
        # 子类、非 RequestError），裸 403 traceback 会在采集首日硬失败且信息无用——转为
        # 可操作的中文报错，直指权限模型
        raise RuntimeError(
            f"ensure_index 建索引 {index_name} 被拒（403）：news_writer 账号缺 news-* 的 "
            f"create 权限，见架构 §5.6 权限模型；建索引/别名等建置动作需具备 index-level create 权限"
        ) from exc
    except RequestError as exc:
        if exc.error != "resource_already_exists_exception":
            raise
    client.indices.put_alias(index=index_name, name=ALIAS)
    return index_name


def index_name_for(doc: Dict[str, Any]) -> str:
    """按 year(pub_time) 路由物理索引名，缺失回退 fetch_time（跨年 _id 幂等的关键）。"""
    time_value = doc.get("pub_time") or doc.get("fetch_time")
    if not time_value:
        raise ValueError(f"文档缺少 pub_time/fetch_time，无法按年路由: _id={doc.get('_id')}")
    year = str(time_value)[:4]
    if not _is_valid_year(year):
        raise ValueError(f"无法从时间值解析年份: {time_value!r}")
    return f"news-{year}"


def _validate_write_mode(compatibility_mode: str) -> str:
    if compatibility_mode not in _WRITE_MODES:
        allowed = ", ".join(sorted(_WRITE_MODES))
        raise ValueError(
            f"未知 compatibility_mode={compatibility_mode!r}，允许值: {allowed}"
        )
    return compatibility_mode


def _build_actions(
    docs: List[Dict[str, Any]], *, compatibility_mode: str = WRITE_MODE_LEGACY
) -> List[Dict[str, Any]]:
    """构造 create-only bulk 动作，每条自带 _index（跨年分组天然完成）。

    ``legacy`` 完全沿用旧写入；``shadow`` 运行兼容投影校验但仍发送旧文档；
    ``dual`` 才发送旧字段与规范字段并存的投影副本。三个模式都使用原始旧字段
    路由，且都不会修改调用者文档。
    """
    compatibility_mode = _validate_write_mode(compatibility_mode)
    actions = []
    for doc in docs:
        if "_id" not in doc:
            raise ValueError(
                f"文档缺少 _id（幂等键，见架构 §5.2）: "
                f"title={doc.get('title')!r} url={doc.get('url')!r}"
            )

        source_doc = doc
        if compatibility_mode != WRITE_MODE_LEGACY:
            canonical_view = read_canonical_news(doc, hit_id=doc["_id"])
            if canonical_view.has_mismatches:
                if compatibility_mode == WRITE_MODE_DUAL:
                    details = ", ".join(
                        f"{item.field_name}:{item.mismatch_type.value}"
                        for item in canonical_view.mismatches
                    )
                    raise NewsWriteConsistencyError(
                        "dual write rejected conflicting compatibility fields: "
                        f"_id={doc['_id']!r}, mismatches={details}"
                    )
                for item in canonical_view.mismatches:
                    logger.warning(
                        "compatibility shadow mismatch: "
                        f"_id={doc['_id']!r}, field_name={item.field_name}, "
                        f"mismatch_type={item.mismatch_type.value}"
                    )

            projected = build_compatibility_projection(doc, hit_id=doc["_id"])
            # compat already treats this as a hard error; retain an explicit
            # create-only boundary assertion so future compat changes cannot
            # detach the OpenSearch _id from news_id.
            if projected["news_id"] != doc["_id"]:
                raise ValueError(
                    "compatibility projection news_id must equal OpenSearch _id"
                )
            if compatibility_mode == WRITE_MODE_DUAL:
                source_doc = projected

        actions.append(
            {
                "_op_type": "create",  # 文档不可变：已存在 409 计 dup，防 done→pending 回退
                # Keep the established pub_time/fetch_time routing contract;
                # canonical publish_time never changes the target year here.
                "_index": index_name_for(doc),
                "_id": doc["_id"],
                "_source": {k: v for k, v in source_doc.items() if k != "_id"},
            }
        )
    return actions


def bulk_create(
    client,
    docs: List[Dict[str, Any]],
    *,
    compatibility_mode: str = WRITE_MODE_LEGACY,
) -> Tuple[int, int]:
    """
    create-only 批量写入（写物理索引名）。

    - 写前对全部目标索引 ensure_index（保证：绝不触发 auto_create_index 动态建出
      坏索引——无 knn 设置/无中文分词/无 title.raw/不挂别名，数据写入后静默不可见）；
      analyzer 仅探测一次、多索引复用；
    - 返回 (成功条数, 已存在跳过条数)；bulk 响应内非 409 错误抛 RuntimeError；
    - 传输层异常（连接失败/超时重试耗尽等）由 opensearchpy 原样上抛，不转 RuntimeError；
    - 混合场景可能部分成功后抛错——create-only 语义下重跑安全（已写入的变 dup）。
    """
    compatibility_mode = _validate_write_mode(compatibility_mode)
    if not docs:
        return 0, 0

    actions = _build_actions(docs, compatibility_mode=compatibility_mode)
    analyzer = probe_analyzer(client)  # 一次探测，多目标索引复用
    for index_name in sorted({action["_index"] for action in actions}):
        ensure_index(client, index_name.removeprefix("news-"), analyzer=analyzer)

    ok, errors = _bulk_helper(client, actions, raise_on_error=False, stats_only=False)

    dup = 0
    real_errors = []
    for err in errors:
        detail = next(iter(err.values()), {}) if isinstance(err, dict) else {}
        if isinstance(detail, dict) and detail.get("status") == 409:
            dup += 1  # 已存在，幂等跳过
        else:
            real_errors.append(err)
    if real_errors:
        raise RuntimeError(f"bulk 写入失败 {len(real_errors)} 条，示例: {real_errors[:3]}")

    logger.debug(f"bulk_create: 写入 {ok} 条，跳过已存在 {dup} 条")
    return ok, dup


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _validate_aware_iso(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} 必须是带时区的 ISO 8601 字符串")
    raw = value.strip()
    if raw.endswith(("Z", "z")):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{field_name} 必须是带时区的 ISO 8601 字符串") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} 必须包含时区")
    return value


def _validate_receipt_update(update: Dict[str, Any], doc: Dict[str, Any]) -> None:
    receipt = update.get("archive_receipt")
    if "raw_archive_uri" not in doc:
        if receipt is not None:
            raise ValueError("archive_receipt 只能与 raw_archive_uri 同时用于受控更新")
        return
    if receipt is None:
        raise ValueError("raw_archive_uri 只能来自真实 ArchiveReceipt，不接受裸字符串")

    # Lazy import keeps ordinary vector/body updates independent of archive I/O.
    from data_collect.utils.news_archive import ArchiveReceipt

    if not isinstance(receipt, ArchiveReceipt):
        raise ValueError("raw_archive_uri 更新必须携带 ArchiveReceipt")
    if not receipt.is_verified or not receipt.success or receipt.status != "archived":
        raise ValueError("ArchiveReceipt 未证明归档成功")
    if receipt.news_id != update.get("_id"):
        raise ValueError("ArchiveReceipt.news_id 与更新 _id 不一致")
    if receipt.archive_uri != doc.get("raw_archive_uri"):
        raise ValueError("raw_archive_uri 与 ArchiveReceipt.archive_uri 不一致")


def _validate_enrichment_mode(enrichment_mode: str) -> str:
    if enrichment_mode not in _ENRICHMENT_MODES:
        raise ValueError(
            f"未知 enrichment_mode={enrichment_mode!r}；"
            f"应为 {sorted(_ENRICHMENT_MODES)}"
        )
    return enrichment_mode


def _build_enrichment_actions(
    updates: List[Dict[str, Any]],
    *,
    enrichment_mode: str = ENRICHMENT_MODE_LEGACY,
) -> List[Dict[str, Any]]:
    """Validate updates and return independent OpenSearch action copies."""

    enrichment_mode = _validate_enrichment_mode(enrichment_mode)
    if not updates:
        return []
    phase1_enabled = enrichment_mode == ENRICHMENT_MODE_PHASE1
    generated_updated_at = _utc_now_iso() if phase1_enabled else None
    actions: List[Dict[str, Any]] = []
    for position, update in enumerate(updates):
        if not isinstance(update, dict):
            raise ValueError(f"enrichment update[{position}] 必须是 dict")
        unexpected_keys = set(update) - _ENRICHMENT_UPDATE_KEYS
        if unexpected_keys:
            raise ValueError(
                f"enrichment update[{position}] 含未知控制字段: {sorted(unexpected_keys)}"
            )
        if not update.get("_index") or not update.get("_id"):
            raise ValueError("enrichment update 必须显式提供物理 _index 和 _id")
        raw_doc = update.get("doc")
        if not isinstance(raw_doc, dict) or not raw_doc:
            raise ValueError("enrichment update.doc 必须是非空 dict")

        forbidden_fields = set(raw_doc) - ENRICHMENT_UPDATE_FIELDS
        if forbidden_fields:
            raise ValueError(
                "enrichment update 试图修改不可变/未授权字段: "
                f"{sorted(forbidden_fields)}"
            )

        phase1_only_fields = set(raw_doc) & _PHASE1_ONLY_ENRICHMENT_FIELDS
        if not phase1_enabled and phase1_only_fields:
            raise ValueError(
                "legacy enrichment_mode 下禁止写入 mapping 尚未确认 ready 的字段: "
                f"{sorted(phase1_only_fields)}"
            )
        if not phase1_enabled and update.get("archive_receipt") is not None:
            raise ValueError(
                "archive_receipt 仅允许在显式 phase1 enrichment_mode 下使用"
            )
        if phase1_enabled and set(raw_doc) == {"updated_at"}:
            raise ValueError("不得用无实际 enrichment 变化的 update 单独刷新 updated_at")

        doc = copy.deepcopy(raw_doc)
        if phase1_enabled:
            if "updated_at" in doc:
                _validate_aware_iso(doc["updated_at"], "updated_at")
            else:
                doc["updated_at"] = generated_updated_at

        has_vector = "content_vec" in doc
        has_model = "embedding_model_version" in doc
        if phase1_enabled:
            if has_vector != has_model:
                raise ValueError(
                    "content_vec 与 embedding_model_version 必须在同一 enrichment update 中"
                )
            if has_vector and doc.get("vec_status") != "done":
                raise ValueError(
                    "content_vec 成功写入时 vec_status 必须在同一 update 中为 done"
                )
            if has_model and (
                not isinstance(doc["embedding_model_version"], str)
                or not doc["embedding_model_version"].strip()
            ):
                raise ValueError("embedding_model_version 必须是非空真实模型标识")
            _validate_receipt_update(update, doc)
        actions.append({
            "_op_type": "update",
            "_index": update["_index"],
            "_id": update["_id"],
            "doc": doc,
        })
    return actions


def bulk_update(
    client,
    updates: List[Dict[str, Any]],
    *,
    enrichment_mode: str = ENRICHMENT_MODE_LEGACY,
) -> int:
    """按显式 (_index, _id) 批量局部更新（update op），返回成功条数。

    - updates: ``[{"_index": 物理索引, "_id": 文档id, "doc": {局部字段}}, ...]``——
      **必须带 hit 自带的物理 _index**（跨年文档分属不同物理索引，写别名有歧义）；
    - 与 create-only 不可变契约的关系：本函数是后处理字段的唯一豁免通道，只允许
      ``ENRICHMENT_UPDATE_FIELDS``；身份、来源、时间、标题、URL、股票等原始事实禁改；
    - 默认 ``legacy``：保持 Step 7 前的 body/PDF/vector update 形状，不产生
      ``updated_at``，并拒绝 mapping 尚未确认 ready 的 Step 7 元数据；
    - 显式 ``phase1``：每个真实非空 update 自动补带时区 ``updated_at``；只刷新时间
      的空更新被拒绝；``content_vec`` 与 ``embedding_model_version`` 必须同批，且
      ``raw_archive_uri`` 必须与同一 update 携带的真实 ``ArchiveReceipt`` 严格一致；
    - 空列表直返 0；bulk 响应内错误项（如 404）→ RuntimeError（截断列出前几条）；
    - 传输层异常由 opensearchpy 原样上抛。update 目标必已存在（来自 search 命中），
      无需 ensure_index。
    """
    enrichment_mode = _validate_enrichment_mode(enrichment_mode)
    if not updates:
        return 0
    actions = _build_enrichment_actions(
        updates,
        enrichment_mode=enrichment_mode,
    )
    ok, errors = _bulk_helper(client, actions, raise_on_error=False, stats_only=False)
    if errors:
        raise RuntimeError(f"bulk 更新失败 {len(errors)} 条，示例: {errors[:3]}")
    logger.debug(f"bulk_update: 更新 {ok} 条")
    return ok


def search_raw(
    client, body: Dict[str, Any], index: str = ALIAS, params: Dict[str, Any] | None = None
) -> Dict[str, Any]:
    """检索薄封装：默认读别名 news（写物理索引、读别名）。

    params 为请求级查询参数透传（opensearch-py `client.search(..., params=...)`），
    hybrid 检索用它带 `search_pipeline`（news_search）；缺省 None 行为不变。
    """
    return client.search(index=index, body=body, params=params)


def hits_total(resp) -> int:
    """取检索响应 hits.total.value（兼容旧式 int 形态；缺失按 0）。"""
    total = ((resp or {}).get("hits") or {}).get("total", 0)
    if isinstance(total, dict):
        total = total.get("value", 0)
    return int(total or 0)
