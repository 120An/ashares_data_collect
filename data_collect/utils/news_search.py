"""新闻混合检索封装：AI 智能体的消费入口（架构 §5.3/§9）。

`search_news()` 四种 mode：
- **hybrid**（默认）：OpenSearch 3.x 原生 `hybrid` query（BM25 + kNN 两路），
  依赖**预建**的 search pipeline（normalization min_max + arithmetic_mean 归一融合，
  见 `ensure_hybrid_pipeline`）；
- **rrf**：客户端两路 topN + RRF 融合（`rrf_fuse` 纯函数）——质量对比基线（§8.4），
  也是 pipeline 未建置时的替代路径；
- **bm25 / knn**：单路。bm25 **绝不触发 Embedder 加载**——embedding 模型不可用时
  的降级路径必须始终可用。

关键设计：
- **过滤注入子查询内部**：channel/date_range/stocks 过滤器一处构造，注入 BM25 路
  `bool.filter` 与 kNN 路 knn 子句内嵌 `filter`（lucene engine 支持，§5.1）。
  **不用 post_filter**——那是"kNN 取回 k 条后再过滤"，会把窗口内本可命中的
  相关结果直接滤丢（§5.3）；
- 查询串先 `clean_text`（NFKC + 繁→简），与索引侧清洗对齐——索引里存的是简体
  规范文本（如"乾照光电"入库时已成"干照光电"），查询不同步转换会检不到；
- 默认排除 `channel=marker`（联播空日标记等系统文档，见 news_cctv）；
- search pipeline 属**建置期一次性**动作（集群级权限，§5.6 权限分离）：
  运行时账号 news_writer 调 `ensure_hybrid_pipeline` 会 403，故 search_news
  不自动创建，由管理员建置期跑一次。
"""

from __future__ import annotations

import datetime
import json
import logging
from typing import Any, Dict, List, Sequence, Tuple

from opensearchpy.exceptions import AuthorizationException, NotFoundError

from data_collect.utils import opensearch_utils as osu
from data_collect.utils.embedding import get_embedder
from data_collect.utils.news_normalize import clean_text

logger = logging.getLogger(__name__)

_MODES = ("hybrid", "rrf", "bm25", "knn")

# kNN 召回窗口下限：过滤在 knn 子句内部执行，但 k 仍决定候选窗口大小，k 太小
# 会让过滤后可返回的近邻不足。50 为经验起点，质量基线调优（计划 T11）以此为参照。
_KNN_K_FLOOR = 50

# RRF rank 常数（业界默认 60）：越大，头部排名之间的分差越平缓
_RRF_K = 60

# hybrid 归一化 search pipeline 名（建置期一次性预建，见 ensure_hybrid_pipeline）
PIPELINE_NAME = "news-hybrid"

_PIPELINE_BODY: Dict[str, Any] = {
    "phase_results_processors": [
        {
            "normalization-processor": {
                "normalization": {"technique": "min_max"},
                "combination": {"technique": "arithmetic_mean"},
            }
        }
    ]
}

def _is_pipeline_error(exc: Exception) -> bool:
    """判定异常是否 search pipeline 缺失/相关（跨 400/404 稳健识别）。

    opensearchpy 的 `str(exc)` 只在 body 带 `error.root_cause[].reason` 时才把
    reason 提到字符串里（典型 400 RequestError 如此，"pipeline" 可见）；而 404
    NotFoundError 若 body 仅 `error.reason`（无 root_cause），reason 只留在
    `exc.info`、`str(exc)` 看不到——只查 str 会漏判 404，被后续当空结果吞掉。
    故 str 与序列化后的 `exc.info`（响应体）双查，任一含 "pipeline" 即判定。
    """
    if "pipeline" in str(exc).lower():
        return True
    info = getattr(exc, "info", None)
    if info is not None:
        try:
            return "pipeline" in json.dumps(info, ensure_ascii=False).lower()
        except (TypeError, ValueError):
            return "pipeline" in str(info).lower()
    return False


# 高亮：title 整段、content 80 字 × 2 片段（bm25/hybrid 及 rrf 的 bm25 路带；
# knn 单路无词项匹配，不带高亮）
_HIGHLIGHT: Dict[str, Any] = {
    "fields": {"title": {}, "content": {"fragment_size": 80, "number_of_fragments": 2}}
}

# 只取回消费方需要的字段：content_vec（1024 浮点/条）与 content 全文不进响应，省带宽
_SOURCE_FIELDS = ["title", "source", "channel", "pub_time", "url"]

# date_range 端点接受的格式
_DATE_FORMATS = ("%Y%m%d", "%Y-%m-%d")


# ---------- 过滤器构造（一处构造，注入两路） ----------

def _normalize_date(value, *, is_end: bool) -> str:
    """date_range 端点规范化："YYYYMMDD"/"YYYY-MM-DD" → pub_time range 边界串。

    起点补 " 00:00:00"、止点补 " 23:59:59"（pub_time 存"YYYY-MM-DD HH:MM:SS"）。
    无法解析抛 ValueError。
    """
    text = str(value).strip()
    for fmt in _DATE_FORMATS:
        try:
            day = datetime.datetime.strptime(text, fmt)
        except ValueError:
            continue
        return day.strftime("%Y-%m-%d") + (" 23:59:59" if is_end else " 00:00:00")
    raise ValueError(f"date_range 端点无法解析（支持 YYYYMMDD / YYYY-MM-DD）: {value!r}")


def _build_filters(channel, date_range, stocks) -> Tuple[List[dict], List[dict]]:
    """构造结构化过滤，返回 (filter 子句列表, must_not 子句列表)。

    各 mode 共用：BM25 路注入 `bool.filter/must_not`，kNN 路注入 knn 子句内嵌
    `filter`（包一层 bool）。marker 排除：`channel=marker` 是系统标记文档（联播
    空日标记等），默认 must_not 排除；调用方**显式**要 marker（str 等于或 list
    包含）则不排——那是自找的，允许。
    """
    filters: List[dict] = []
    if channel:
        if isinstance(channel, str):
            filters.append({"term": {"channel": channel}})
        else:
            filters.append({"terms": {"channel": list(channel)}})
    if stocks:
        # str 视为单代码（与 channel 的 str/list 语义对称）：AI 调用方误传
        # "600519.SH" 若直接 list() 会按字符拆散，静默零命中
        stock_list = [stocks] if isinstance(stocks, str) else list(stocks)
        filters.append({"terms": {"stocks": stock_list}})
    if date_range is not None:
        start, end = date_range   # 单边 None 允许
        bounds: Dict[str, str] = {}
        if start is not None:
            bounds["gte"] = _normalize_date(start, is_end=False)
        if end is not None:
            bounds["lte"] = _normalize_date(end, is_end=True)
        if bounds:
            filters.append({"range": {"pub_time": bounds}})

    explicit = [channel] if isinstance(channel, str) else list(channel or [])
    must_not = [] if "marker" in explicit else [{"term": {"channel": "marker"}}]
    return filters, must_not


# ---------- 两路子查询 ----------

def _bm25_query(query_text: str, filters: List[dict], must_not: List[dict]) -> Dict[str, Any]:
    """BM25 路：multi_match(title^2, content)，过滤注入 bool 内部（非 post_filter）。"""
    bool_clause: Dict[str, Any] = {
        "must": [{"multi_match": {"query": query_text, "fields": ["title^2", "content"]}}]
    }
    if filters:
        bool_clause["filter"] = filters
    if must_not:
        bool_clause["must_not"] = must_not
    return {"bool": bool_clause}


def _knn_query(vector: List[float], top_k: int,
               filters: List[dict], must_not: List[dict]) -> Dict[str, Any]:
    """kNN 路：过滤**内嵌** knn 子句（lucene engine 高效过滤，§5.1/§5.3）。

    绝不用 post_filter / 顶层 bool 包裹——那是"先取 k 条再过滤"，会把 kNN 窗口内
    本可命中的相关结果滤丢。k 取 max(top_k, _KNN_K_FLOOR) 保证召回窗口。
    """
    clause: Dict[str, Any] = {"vector": vector, "k": max(top_k, _KNN_K_FLOOR)}
    inner: Dict[str, Any] = {}
    if filters:
        inner["filter"] = filters
    if must_not:
        inner["must_not"] = must_not
    if inner:
        clause["filter"] = {"bool": inner}
    return {"knn": {"content_vec": clause}}


def _encode_query(cleaned: str) -> List[float]:
    """查询向量：Embedder 单条编码（lazy 单例，首次调用加载模型）。"""
    return get_embedder().encode([cleaned])[0].tolist()


# ---------- RRF 融合（纯函数） ----------

def rrf_fuse(bm25_hits: Sequence[dict], knn_hits: Sequence[dict],
             k: int = _RRF_K) -> List[dict]:
    """RRF（Reciprocal Rank Fusion）融合两路命中：score(_id) = Σ 1/(k + rank)。

    - rank 从 1 起；仅单路命中的文档同样计分（另一路无贡献）；
    - 同 `_id` 保留**先见**的 hit dict（bm25 路在前，自带 highlight）；
    - 纯函数：不改写入参，融合分写入副本的 `_rrf_score`；
    - 返回按融合分降序（同分保持先见顺序——sorted 稳定，bm25 序优先）。
    """
    scores: Dict[str, float] = {}
    first_hit: Dict[str, dict] = {}
    for hits in (bm25_hits, knn_hits):
        for rank, hit in enumerate(hits, start=1):
            doc_id = hit["_id"]
            scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank)
            first_hit.setdefault(doc_id, hit)
    fused: List[dict] = []
    for doc_id in sorted(scores, key=scores.__getitem__, reverse=True):
        hit = dict(first_hit[doc_id])
        hit["_rrf_score"] = scores[doc_id]
        fused.append(hit)
    return fused


# ---------- search pipeline 建置（一次性，集群级权限） ----------

def ensure_hybrid_pipeline(client) -> None:
    """预建 hybrid 归一化 search pipeline（PUT 幂等，重复执行安全）。

    **建置期一次性动作，需集群级权限**（§5.6 权限分离）：运行时账号 news_writer
    仅有 news* 索引级权限，调本函数会 403——故 search_news 不自动调用，
    由管理员在建置阶段执行一次。
    """
    try:
        client.transport.perform_request(
            "PUT", f"/_search/pipeline/{PIPELINE_NAME}", body=_PIPELINE_BODY)
    except AuthorizationException as exc:
        raise RuntimeError(
            "建 search pipeline 是集群级动作，当前账号无权限（news_writer 属预期，"
            "架构 §5.6 权限分离）。建置动作需管理员执行一次：PUT /_search/pipeline/"
            f"{PIPELINE_NAME} body={json.dumps(_PIPELINE_BODY, ensure_ascii=False)}"
        ) from exc
    logger.info(f"search pipeline {PIPELINE_NAME} 已建置（min_max + arithmetic_mean）")


# ---------- 响应整形 ----------

def _resp_hits(resp) -> List[dict]:
    return ((resp or {}).get("hits") or {}).get("hits") or []


def _join_highlight(highlight) -> List[str] | None:
    """title/content 高亮片段拼成单列表；无高亮返回 None。"""
    if not highlight:
        return None
    fragments = list(highlight.get("title") or []) + list(highlight.get("content") or [])
    return fragments or None


def _format_hit(hit: dict, score) -> Dict[str, Any]:
    """OpenSearch hit → 消费方 hit（溯源三件套 url/source/pub_time；url 可缺省 None）。"""
    src = hit.get("_source") or {}
    return {
        "title": src.get("title"),
        "source": src.get("source"),
        "channel": src.get("channel"),
        "pub_time": src.get("pub_time"),
        "url": src.get("url"),
        "score": score,
        "highlight": _join_highlight(hit.get("highlight")),
    }


def _format_response(resp) -> Dict[str, Any]:
    """单路/hybrid 响应 → {"total", "hits"}（score 取 _score）。"""
    return {"total": osu.hits_total(resp),
            "hits": [_format_hit(h, h.get("_score")) for h in _resp_hits(resp)]}


def _search(client, body: Dict[str, Any], params: Dict[str, Any] | None = None):
    """检索并容忍冷启动 404（别名/索引尚未建）：按空响应返回，不炸 AI 消费方。"""
    try:
        return osu.search_raw(client, body, params=params)
    except NotFoundError as exc:
        logger.warning(f"news 检索 404（别名/索引未建，按空结果处理）: {exc!r}")
        return None


# ---------- 消费入口 ----------

def search_news(query, top_k: int = 10, channel=None, date_range=None, stocks=None,
                mode: str = "hybrid", collapse: str | None = None) -> Dict[str, Any]:
    """新闻混合检索（AI 智能体消费入口，架构 §5.3/§9）。

    Args:
        query: 查询串。先 clean_text（NFKC + 繁→简）与索引侧对齐——否则查
            "乾照光电"检不到索引里的"干照光电"；清洗后为空抛 ValueError。
        top_k: 返回条数（默认 10）。
        channel: 频道过滤，str 或 list（flash/cctv/policy/...）。默认排除
            channel=marker 系统标记文档；显式传 "marker" 则允许查到。
        date_range: (start, end) 按 pub_time 过滤，端点 "YYYYMMDD" 或
            "YYYY-MM-DD"，单边可 None。
        stocks: 股票代码列表（如 ["600519.SH"]），terms 过滤。
        mode: hybrid（默认，需预建 search pipeline，见 ensure_hybrid_pipeline）/
            rrf（客户端 RRF 融合，无 pipeline 依赖）/ bm25 / knn。
            bm25 绝不加载 embedding 模型（模型不可用时的降级路径）。
        collapse: 传 "title.raw" 按标题跨源同题去重；局限：rrf 模式只作用于
            bm25 路（kNN 路不加），融合结果仍可能残留同题。

    Returns:
        ``{"total": N, "hits": [{title, source, channel, pub_time, url, score,
        highlight}, ...]}``——total 供消费方判断召回充分性（rrf 取两路 total 较
        大者）；highlight 为 title+content 片段列表（无则 None）；score 单路/
        hybrid 为 `_score`、rrf 为 RRF 融合分。

    冷启动容错：别名/索引尚未建（404）按空结果返回。hybrid 遇 pipeline 未建置
    → RuntimeError 提示先 ensure_hybrid_pipeline()（管理员）或改用 mode="rrf"
    （无论 pipeline 缺失报 400 还是 404，都给指引而非静默空结果——故 hybrid 直调
    search_raw 绕过 _search 的 404 吞没，仅对与 pipeline 无关的 404 才按空结果）。
    """
    cleaned = clean_text(query)
    if not cleaned:
        raise ValueError(f"查询串清洗后为空: {query!r}")
    if mode not in _MODES:
        raise ValueError(f"未知 mode: {mode!r}（支持 {'/'.join(_MODES)}）")

    filters, must_not = _build_filters(channel, date_range, stocks)
    client = osu.get_client()

    if mode == "bm25":
        body = {"query": _bm25_query(cleaned, filters, must_not),
                "size": top_k, "_source": _SOURCE_FIELDS, "highlight": _HIGHLIGHT}
        if collapse:
            body["collapse"] = {"field": collapse}
        return _format_response(_search(client, body))

    if mode == "knn":
        vector = _encode_query(cleaned)
        body = {"query": _knn_query(vector, top_k, filters, must_not),
                "size": top_k, "_source": _SOURCE_FIELDS}
        if collapse:
            body["collapse"] = {"field": collapse}
        return _format_response(_search(client, body))

    if mode == "hybrid":
        vector = _encode_query(cleaned)
        body = {
            "query": {"hybrid": {"queries": [
                _bm25_query(cleaned, filters, must_not),
                _knn_query(vector, top_k, filters, must_not),
            ]}},
            "size": top_k,
            "_source": _SOURCE_FIELDS,
            "highlight": _HIGHLIGHT,
        }
        if collapse:
            body["collapse"] = {"field": collapse}
        # 关键：hybrid 直调 osu.search_raw，**绕过 _search 的 NotFoundError→空 吞没**。
        # pipeline 未建置在 OS 3.x 一般报 400 RequestError，但也可能以 404
        # NotFoundError 冒出——若走 _search 会被当冷启动空索引静默吞掉，用户只见"无
        # 结果"而拿不到"去建 pipeline"的指引。直调后由下方 except 统一按消息判别。
        try:
            resp = osu.search_raw(client, body, params={"search_pipeline": PIPELINE_NAME})
        except Exception as exc:
            # pipeline 未建置：OpenSearch 报 "Pipeline [news-hybrid] ..."（400 或 404），
            # 按消息判别并给出可执行指引，优先于"索引缺失按空"——否则 pipeline 缺失被
            # 误当空结果，用户拿不到建置指引。_is_pipeline_error 双查 str+body 稳健识别
            # （404 的 reason 只在 body、str 看不到，详见该函数）
            if _is_pipeline_error(exc):
                raise RuntimeError(
                    f"hybrid 检索失败：search pipeline [{PIPELINE_NAME}] 疑似未建置——"
                    "请管理员先执行一次 ensure_hybrid_pipeline()（建置期动作，架构 "
                    f"§5.6），或改用 mode='rrf'（客户端融合，无 pipeline 依赖）。"
                    f"原始错误: {exc!r}"
                ) from exc
            if isinstance(exc, NotFoundError):
                # 与 pipeline 无关的 404（别名/索引确未建，冷启动）：仍按空结果，
                # 与 bm25/knn/rrf 经 _search 的容错语义一致
                logger.warning(f"news 检索 404（别名/索引未建，按空结果处理）: {exc!r}")
                return _format_response(None)
            raise
        return _format_response(resp)

    # mode == "rrf"：两路各取 window 条，客户端融合（质量基线 & pipeline 替代路径）
    window = max(top_k, _KNN_K_FLOOR)
    vector = _encode_query(cleaned)
    bm25_body: Dict[str, Any] = {"query": _bm25_query(cleaned, filters, must_not),
                                 "size": window, "_source": _SOURCE_FIELDS,
                                 "highlight": _HIGHLIGHT}
    if collapse:
        bm25_body["collapse"] = {"field": collapse}   # 局限：只去重 bm25 路（见 docstring）
    knn_body: Dict[str, Any] = {"query": _knn_query(vector, window, filters, must_not),
                                "size": window, "_source": _SOURCE_FIELDS}

    bm25_resp = _search(client, bm25_body)
    knn_resp = _search(client, knn_body)
    fused = rrf_fuse(_resp_hits(bm25_resp), _resp_hits(knn_resp))[:top_k]
    return {
        # 两路 total 较大者：bm25 total 是全量匹配数，knn total 受 k 窗口截断，
        # 取 max 作"至少有这么多相关文档"的召回充分性信号
        "total": max(osu.hits_total(bm25_resp), osu.hits_total(knn_resp)),
        "hits": [_format_hit(h, h["_rrf_score"]) for h in fused],
    }
