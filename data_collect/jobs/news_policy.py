"""政策/宏观新闻采集任务（新闻子系统 ③：注册政策源 + 东财财经早餐）。

四源单 job（source 即归档目录名）：
- ``govcn_policy``/``stats``：官方 RSS（channel=policy），feedparser 解析；
- ``govcn_gwy``：中国政府网国务院政策文件库 JSON API（channel=policy）；
- 上述注册政策源均由 sources.yaml 驱动，采集协议由 adapter 明确分派；
- ``em_cjzc``：东财财经早餐（akshare，channel=media）。

每小时轮询（news_hourly 首位），create-only 幂等；复用快讯全套健壮性模式：
`fetch_with_timeout` 硬超时（cls 挂起事故教训标配）、源级隔离+单源失败 guarded
告警、归档先行（RSS 仅近期窗口→归档为主存档；官网列表页长存可人工深回补）、
verify 归档重放补库（自然日语义）。无断流轮计数（RSS 更新慢/周末可 0 新，会误报）。
"""

from __future__ import annotations

import datetime
import json
import logging
import re
from typing import Dict, List, Mapping

from data_collect.config import get_news_config
from data_collect.news_model import source_health as source_health_shadow
from data_collect.utils import news_archive
from data_collect.utils import news_normalize as nn
from data_collect.utils import source_adapters as sa
from data_collect.utils import source_registry
from data_collect.utils import notify
from data_collect.utils import opensearch_utils as osu
from data_collect.utils.news_common import cell as _cell
from data_collect.utils.news_common import fetch_with_timeout
from data_collect.utils.news_common import flush_spool_safe as _flush_spool_safe
from data_collect.utils.news_common import now as _now
from data_collect.utils.news_common import strip_raw as _strip_raw
from data_collect.utils.news_common import today as _today
from data_collect.utils.news_common import verify_dates as _verify_dates

logger = logging.getLogger(__name__)

# Shadow monitoring is null by default and never writes OpenSearch.  Tests or
# an explicit deployment boundary may inject a local/in-memory sink.
_SOURCE_HEALTH_SINK = source_health_shadow.NullShadowSink()

_FETCH_TIMEOUT_SECONDS = 60   # 单源拉取硬超时地板（源挂起按失败源隔离，见 news_common）
_TZ_BEIJING = datetime.timezone(datetime.timedelta(hours=8))

_CJZC_SOURCE = "em_cjzc"                       # 东财财经早餐（channel=media）
_CJZC_COLUMNS = ("标题", "摘要", "发布时间", "链接")
_GOVCN_GWY_SOURCE = "govcn_gwy"
_GOVCN_GWY_API_PARAMS = {
    "t": "zhengcelibrary_gw",
    "q": "",
    "sort": "pubtime",
    "sortType": 1,
    "searchfield": "title:content:summary",
    "p": 1,
    "n": 50,
}
_GOVCN_DATE_RE = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")
_SUPPORTED_REGISTRY_ADAPTERS = {"rss", "api"}


def _policy_sources() -> Dict[str, source_registry.Source]:
    """注册表 → {source: Source}（RSS/API 采集事实均已解析）。

    函数化 = 热生效：子进程每轮重读 sources.yaml；测试 monkeypatch 本函数，
    与真实注册表/config 解耦（fixture 耦合教训）。
    """
    sources = source_registry.load_sources("news_policy")
    unsupported = [s.id for s in sources
                   if s.adapter not in _SUPPORTED_REGISTRY_ADAPTERS]
    if unsupported:
        raise ValueError(f"news_policy 注册源 adapter 未实现: {unsupported}")
    return {s.id: s for s in sources}


def _source_names() -> tuple:
    """本 job 全部源名（注册政策源 + 代码内 akshare 源；顺序即摘要顺序）。
    em_cjzc（akshare）一期仍在代码（spec §11 二期登记）。"""
    return tuple(_policy_sources()) + (_CJZC_SOURCE,)


def _alert(message: str) -> bool:
    """钉钉告警（guarded：通知失败不影响采集主流程）。"""
    return notify.guarded_send(message)


def _emit_source_observation(**kwargs) -> None:
    """Construct and emit one source fact without affecting collection."""

    try:
        observation = source_health_shadow.SourceObservation(**kwargs)
        source_health_shadow.emit_shadow(
            _SOURCE_HEALTH_SINK, observation, logger=logger
        )
    except Exception as exc:  # noqa: BLE001 - monitoring is strictly fail-open
        logger.warning(
            "政策 SourceHealth observation 构造失败（已 fail-open）: %r", exc
        )


def _source_health_context(
    job_name: str, kwargs: Dict[str, object]
) -> tuple[str, int]:
    """Use pipeline's internal context when valid, otherwise stay direct-call safe."""

    candidate_id = kwargs.get(source_health_shadow.JOB_RUN_ID_CONTEXT_KEY)
    candidate_attempt = kwargs.get(source_health_shadow.ATTEMPT_NO_CONTEXT_KEY)
    if candidate_id is None and candidate_attempt is None:
        started_at = source_health_shadow.utc_now()
        return source_health_shadow.make_job_run_id(job_name, started_at), 1
    if (
        isinstance(candidate_id, str)
        and candidate_id.strip().startswith("jrun_")
        and isinstance(candidate_attempt, int)
        and not isinstance(candidate_attempt, bool)
        and candidate_attempt >= 1
    ):
        return candidate_id.strip(), candidate_attempt

    logger.warning("政策 SourceHealth context 无效，回退 direct-run context")
    started_at = source_health_shadow.utc_now()
    return source_health_shadow.make_job_run_id(job_name, started_at), 1


# ======== 取数层 ========

# feed 抓取单一实现在适配器层（注册表三期）；别名保留原私有名供测试 monkeypatch
_fetch_rss_entries = sa.fetch_feed


def _fetch_govcn_gwy_api(source: source_registry.Source) -> List[Mapping]:
    """拉取国务院政策文件库 API；仅在此请求边界隔离 ambient proxy。

    ``Source.proxy_url`` 非空时显式使用该代理；否则这个临时 Session 不继承
    HTTP_PROXY/HTTPS_PROXY。Security/TLS 校验保持 requests 默认值且不关闭证书
    验证。返回结构缺失不是“0 条”，而是明确的源失败。
    """
    import requests

    session = requests.Session()
    session.trust_env = False
    request_kwargs = {
        "params": dict(_GOVCN_GWY_API_PARAMS),
        "headers": dict(source.headers),
        "timeout": source.timeout,
    }
    if source.proxy_url:
        request_kwargs["proxies"] = {
            "http": source.proxy_url,
            "https": source.proxy_url,
        }
    try:
        response = session.get(source.url, **request_kwargs)
        if not 200 <= response.status_code < 300:
            raise RuntimeError(
                f"govcn_gwy API HTTP {response.status_code}: {source.url}"
            )
        try:
            payload = response.json()
        except (TypeError, ValueError) as exc:
            raise RuntimeError("govcn_gwy API JSON 解析失败") from exc
    finally:
        session.close()

    if not isinstance(payload, Mapping):
        raise RuntimeError("govcn_gwy API JSON 顶层应为 mapping")
    if "code" in payload:
        business_code = payload["code"]
        success = (
            isinstance(business_code, int)
            and not isinstance(business_code, bool)
            and business_code == 200
        ) or (
            isinstance(business_code, str)
            and business_code.strip() == "200"
        )
        if not success:
            raise RuntimeError(
                f"govcn_gwy API 业务状态失败: code={business_code!r}"
            )
    search_vo = payload.get("searchVO")
    if not isinstance(search_vo, Mapping):
        raise RuntimeError("govcn_gwy API 缺失或无效 searchVO")
    if "listVO" not in search_vo:
        raise RuntimeError("govcn_gwy API 缺失 searchVO.listVO")
    rows = search_vo["listVO"]
    if not isinstance(rows, list):
        raise RuntimeError("govcn_gwy API searchVO.listVO 应为 list")
    if any(not isinstance(row, Mapping) for row in rows):
        raise RuntimeError("govcn_gwy API searchVO.listVO 含非 mapping 条目")
    return rows


def _fetch_cjzc():
    """东财财经早餐（lazy import akshare，同快讯惯例）。"""
    import akshare as ak

    return ak.stock_info_cjzc_em()


# ======== 信封层 ========

def _rss_pub_time(entry) -> str | None:
    """RSS 发布时间 → 北京时间串；无时间字段返回 None（调用方回退+打标）。

    feedparser 把 pubDate 统一解析为 UTC struct_time（published_parsed），源带
    +0800 也已换算——转回北京时间即源语义。
    """
    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if not parsed:
        return None
    dt = datetime.datetime(*parsed[:6], tzinfo=datetime.timezone.utc)
    return dt.astimezone(_TZ_BEIJING).strftime("%Y-%m-%d %H:%M:%S")


def _entry_to_envelope(source: str, channel: str, entry, fetch_time: str,
                       name_dict: Dict[str, str]) -> dict:
    """RSS entry → 统一信封（契约同快讯 _row_to_envelope，决定论同款）。

    `_id`：guid（feedparser 的 entry.id）为一级 native_id；缺则二级 sha1(link)；
    原始 title/summary 参与三级兜底计算（清洗规则调整不产生新 `_id`）；
    pub_time 缺失不参与 `_id`（否则每轮新 `_id` 反复重复入库），信封回退
    fetch_time 并标 `time_estimated`（仅归档）。
    """
    raw_title = str(entry.get("title") or "").strip()
    raw_summary = str(entry.get("summary") or entry.get("description") or "").strip()
    url = str(entry.get("link") or "").strip()
    guid = str(entry.get("id") or "").strip()
    pub_time = _rss_pub_time(entry)

    title = nn.clean_text(raw_title)
    content = nn.clean_text(raw_summary) or title    # 摘要空回退标题（content 不空）
    envelope = {
        "_id": nn.make_id({"source": source, "native_id": guid or None, "url": url,
                           "pub_time": pub_time, "title": raw_title,
                           "content": raw_summary}),
        "title": title,
        "content": content,
        "raw_title": raw_title,
        "raw_content": json.dumps(
            {k: entry.get(k) for k in ("title", "summary", "link", "id", "published")},
            ensure_ascii=False, default=str),
        "pub_time": pub_time or fetch_time,
        "fetch_time": fetch_time,
        "source": source,
        "channel": channel,
        "stocks": nn.tag_stocks(f"{title} {content}", name_dict),
        "vec_status": "pending",
    }
    if url:
        envelope["url"] = url
    if pub_time is None:
        envelope["time_estimated"] = True
    return envelope


def _govcn_gwy_pub_time(item: Mapping) -> str | None:
    """政策库日期 → 北京时间 legacy 串；日期精度仍由 time_estimated 保留。

    ``pubtimeStr`` 的当前证据只有 YYYY.MM.DD，补零点只是 legacy ``pub_time``
    的存储兼容形态，不代表来源给出了时分秒。若该字段不可用，才保守尝试原始
    ``pubtime``，但仍不宣称它是官方精确生效时刻。
    """
    raw_date = str(item.get("pubtimeStr") or "").strip()
    if _GOVCN_DATE_RE.fullmatch(raw_date):
        try:
            parsed = datetime.datetime.strptime(raw_date, "%Y.%m.%d")
        except ValueError:
            return None
        return parsed.strftime("%Y-%m-%d 00:00:00")
    return nn.normalize_time(item.get("pubtime"), source=_GOVCN_GWY_SOURCE)


def _govcn_gwy_to_envelope(item: Mapping, fetch_time: str,
                           name_dict: Dict[str, str]) -> dict:
    """国务院政策文件库条目 → legacy 新闻信封（日期精度不伪装）。"""
    raw_title = str(item.get("title") or "").strip()
    raw_summary = str(item.get("summary") or "").strip()
    url = str(item.get("url") or "").strip()
    native_value = item.get("id")
    native_id = "" if native_value is None else str(native_value).strip()
    pub_time = _govcn_gwy_pub_time(item)

    title = nn.clean_text(raw_title)
    content = nn.clean_text(raw_summary) or title
    envelope = {
        "_id": nn.make_id({
            "source": _GOVCN_GWY_SOURCE,
            "native_id": native_id or None,
            "url": url,
            "pub_time": pub_time,
            "title": raw_title,
            "content": raw_summary,
        }),
        "title": title,
        "content": content,
        "raw_title": raw_title,
        "raw_content": json.dumps(dict(item), ensure_ascii=False, default=str),
        "pub_time": pub_time or fetch_time,
        "fetch_time": fetch_time,
        "source": _GOVCN_GWY_SOURCE,
        "channel": "policy",
        "stocks": nn.tag_stocks(f"{title} {content}", name_dict),
        "vec_status": "pending",
        # 来源仅给出日期（或缺失）；00:00:00 是 legacy 兼容占位，不是精确时刻。
        "time_estimated": True,
    }
    if url:
        envelope["url"] = url
    return envelope


def _cjzc_to_envelope(row, fetch_time: str, name_dict: Dict[str, str]) -> dict:
    """财经早餐单行 → 信封（channel=media；`_id` 二级 sha1(链接)）。"""
    raw_title = _cell(row.get("标题")).strip()
    raw_summary = _cell(row.get("摘要")).strip()
    url = _cell(row.get("链接")).strip()
    pub_time = nn.normalize_time(row.get("发布时间"), source=_CJZC_SOURCE)

    title = nn.clean_text(raw_title)
    content = nn.clean_text(raw_summary) or title
    envelope = {
        "_id": nn.make_id({"source": _CJZC_SOURCE, "url": url,
                           "pub_time": pub_time, "title": raw_title,
                           "content": raw_summary}),
        "title": title,
        "content": content,
        "raw_title": raw_title,
        "raw_content": raw_summary,
        "pub_time": pub_time or fetch_time,
        "fetch_time": fetch_time,
        "source": _CJZC_SOURCE,
        "channel": "media",
        "stocks": nn.tag_stocks(f"{title} {content}", name_dict),
        "vec_status": "pending",
    }
    if url:
        envelope["url"] = url
    if pub_time is None:
        envelope["time_estimated"] = True
    return envelope


# ======== 采集与归档 ========

def _load_name_dict() -> Dict[str, str]:
    """简称词典（降级契约同快讯：失败退空典仅正则打标，不阻塞采集）。"""
    try:
        return nn.load_name_dict()
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"政策简称词典加载失败，本轮打标降级: {exc!r}")
        return {}


def _collect_source(source: str, fetch_time: str, name_dict: Dict[str, str],
                    source_map: Dict[str, source_registry.Source]) -> List[dict]:
    """拉取单源并信封化（批内 `_id` 去重保首见）；异常由调用方按源隔离。

    source_map 由 run() 一次加载传入（勿在此重读——每次=一次磁盘读+last-good
    重拷，评审 S1）。em_cjzc（akshare）不走 source_map。
    """
    if source == _CJZC_SOURCE:
        df = fetch_with_timeout(_fetch_cjzc, _FETCH_TIMEOUT_SECONDS,
                                f"政策源 {source} 拉取")
        if df is None or len(df) == 0:
            logger.info(f"政策源 {source} 本轮 0 条")
            return []
        missing = [c for c in _CJZC_COLUMNS if c not in df.columns]
        if missing:   # 缺列防御：静默空信封比失败更糟（同快讯）
            raise ValueError(f"财经早餐返回缺列 {missing}，实际列: {list(df.columns)}")
        rows = [_cjzc_to_envelope(row, fetch_time, name_dict)
                for _, row in df.iterrows()]
    else:
        src = source_map[source]
        if src.adapter == "api":
            if src.id != _GOVCN_GWY_SOURCE:
                raise ValueError(f"news_policy API 源未实现: {src.id}")
            items = fetch_with_timeout(
                lambda: _fetch_govcn_gwy_api(src),
                sa.hard_timeout(src.timeout, _FETCH_TIMEOUT_SECONDS),
                f"政策源 {source} 拉取",
            )
            rows = [_govcn_gwy_to_envelope(item, fetch_time, name_dict)
                    for item in (items or [])]
        elif src.adapter == "rss":
            entries = fetch_with_timeout(
                lambda: _fetch_rss_entries(src.url, headers=src.headers,
                                           proxy=src.proxy_url,
                                           timeout=src.timeout,
                                           label=f"RSS {source}"),
                sa.hard_timeout(src.timeout, _FETCH_TIMEOUT_SECONDS),
                f"政策源 {source} 拉取")
            rows = [_entry_to_envelope(source, src.channel, entry, fetch_time,
                                       name_dict)
                    for entry in (entries or [])]
        else:  # _policy_sources 已先拒绝；保留边界防直接单测/误调用绕过。
            raise ValueError(f"news_policy adapter 未实现: {src.adapter}")

    envelopes: List[dict] = []
    seen: set = set()
    for env in rows:
        if env["_id"] in seen:
            continue
        seen.add(env["_id"])
        envelopes.append(env)
    logger.info(f"政策源 {source} 本轮 {len(envelopes)} 条")
    return envelopes


def _archive_source(source: str, envelopes: List[dict]) -> int:
    """按 pub_time 日期分组归档新信封，返回新增条数。

    load_ids 异常（归档损坏等）→ 放弃当日去重继续采集（同快讯降级契约，
    归档重复可容忍、丢采不可容忍）。
    """
    by_date: Dict[str, List[dict]] = {}
    for env in envelopes:
        date8 = str(env.get("pub_time", ""))[:10].replace("-", "")
        if len(date8) == 8:
            by_date.setdefault(date8, []).append(env)

    new_total = 0
    for date8, day_envelopes in sorted(by_date.items()):
        try:
            existing = news_archive.load_ids(source, date8)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"归档 load_ids({source},{date8}) 失败，放弃当日去重: {exc!r}")
            existing = set()
        fresh = [env for env in day_envelopes if env["_id"] not in existing]
        if fresh:
            new_total += news_archive.append(source, date8, fresh)
    return new_total


# ======== Pipeline 标准接口 ========

def run(run_date: str | None = None, **kwargs) -> str:
    """每小时任务：四源采集 → 归档 → create-only 入库。

    单源失败（异常/挂起超时）按源隔离并 guarded 告警一次；全源失败 raise
    （框架 retry + 通知）。`run_date` 仅签名兼容（滚动窗无日期参数）。
    """
    _flush_spool_safe()
    fetch_time = _now().strftime("%Y-%m-%d %H:%M:%S")
    name_dict = _load_name_dict()

    source_map = _policy_sources()            # 每轮加载一次（子进程模型=天然热生效）
    sources = tuple(source_map) + (_CJZC_SOURCE,)
    job_run_id, attempt_no = _source_health_context("news_policy", kwargs)
    envelopes_by_source: Dict[str, List[dict]] = {}
    successful_timings: Dict[str, tuple[datetime.datetime, datetime.datetime]] = {}
    failed_sources: List[str] = []
    errors: List[str] = []
    for source in sources:
        source_started_at = source_health_shadow.utc_now()
        try:
            envelopes_by_source[source] = _collect_source(
                source, fetch_time, name_dict, source_map)
            successful_timings[source] = (
                source_started_at,
                source_health_shadow.utc_now(),
            )
        except Exception as exc:  # noqa: BLE001
            source_finished_at = source_health_shadow.utc_now()
            failed_sources.append(source)
            errors.append(f"{source}: {exc!r}")
            logger.warning(f"政策源 {source} 采集失败（源级隔离，继续其余源）: {exc!r}")
            _emit_source_observation(
                job_run_id=job_run_id,
                source_id=source,
                observation_type=source_health_shadow.ObservationType.COLLECT,
                started_at=source_started_at,
                finished_at=source_finished_at,
                attempt_no=attempt_no,
                outcome=(
                    source_health_shadow.ObservationOutcome.TIMEOUT
                    if isinstance(exc, TimeoutError)
                    else source_health_shadow.ObservationOutcome.FAILURE
                ),
                error_code=source_health_shadow.classify_error(exc),
                error_summary=source_health_shadow.error_summary(exc),
                incomplete=isinstance(exc, TimeoutError),
            )
    if len(failed_sources) == len(sources):
        raise RuntimeError(f"政策源全部采集失败: {'; '.join(errors)}")
    if failed_sources:
        _alert(f"政策源 {','.join(failed_sources)} 本轮采集失败（其余源正常）: "
               f"{'; '.join(errors)[:300]}")

    archive_new_counts: Dict[str, int] = {}
    for source, envelopes in envelopes_by_source.items():
        started_at, finished_at = successful_timings[source]
        try:
            archive_new_count = _archive_source(source, envelopes)
        except Exception:
            # Collection itself did succeed; archive/new count is simply not
            # evidenced.  Preserve the original archive exception semantics.
            _emit_source_observation(
                job_run_id=job_run_id,
                source_id=source,
                observation_type=source_health_shadow.ObservationType.COLLECT,
                started_at=started_at,
                finished_at=finished_at,
                attempt_no=attempt_no,
                outcome=source_health_shadow.ObservationOutcome.SUCCESS,
                collected_item_count=len(envelopes),
                new_item_count=None,
                empty_success=len(envelopes) == 0,
                parse_failure_count=0,
                last_item_publish_time=(
                    source_health_shadow.latest_item_publish_time(envelopes)
                ),
            )
            raise
        archive_new_counts[source] = archive_new_count
        _emit_source_observation(
            job_run_id=job_run_id,
            source_id=source,
            observation_type=source_health_shadow.ObservationType.COLLECT,
            started_at=started_at,
            finished_at=finished_at,
            attempt_no=attempt_no,
            outcome=source_health_shadow.ObservationOutcome.SUCCESS,
            collected_item_count=len(envelopes),
            # The job archives per source, but OpenSearch create-only writes a
            # merged batch.  Per-source newly indexed counts are not knowable.
            new_item_count=None,
            empty_success=len(envelopes) == 0,
            parse_failure_count=0,
            last_item_publish_time=(
                source_health_shadow.latest_item_publish_time(envelopes)
            ),
            completeness_info={
                "archive_new_item_count": archive_new_count,
            },
        )

    all_envelopes = [env for source in sources
                     for env in envelopes_by_source.get(source, [])]
    ok = dup = 0
    if all_envelopes:
        ok, dup = osu.bulk_create(osu.get_client(),
                                  [_strip_raw(env) for env in all_envelopes])

    parts = []
    for source in sources:
        if source in failed_sources:
            parts.append(f"{source} 失败")
        else:
            parts.append(f"{source} {len(envelopes_by_source[source])}条"
                         f"(新{archive_new_counts[source]})")
    summary = f"政策: {' '.join(parts)}, 入库新增 {ok} 重复 {dup}"
    if failed_sources:
        summary += f", 失败源[{','.join(failed_sources)}]"
    logger.info(summary)
    return summary


def run_verify(start_date: str, end_date: str, **kwargs) -> str:
    """查漏补缺：归档重放 → create-only 补库（补"已归档未入库"缺口）。

    自然日语义——**忽略框架传入窗口**（交易日推算漏周末，政策周末照发），
    读 config `news.verify_days_back` 回溯 [today-N, today]（重放廉价含今天）。
    RSS 滚动窗不可深回补（同快讯），归档即事实源。
    """
    days_back = int(get_news_config().get("verify_days_back", 3))
    dates = _verify_dates(days_back, _today(), include_today=True)
    window = (dates[0], dates[-1])

    job_run_id, attempt_no = _source_health_context(
        "news_policy_verify", kwargs
    )
    replayed: List[dict] = []
    for source in _source_names():
        source_started_at = source_health_shadow.utc_now()
        try:
            source_replayed = list(news_archive.replay(source, window))
            replayed.extend(source_replayed)
            source_finished_at = source_health_shadow.utc_now()
            _emit_source_observation(
                job_run_id=job_run_id,
                source_id=source,
                observation_type=source_health_shadow.ObservationType.VERIFY,
                started_at=source_started_at,
                finished_at=source_finished_at,
                attempt_no=attempt_no,
                outcome=source_health_shadow.ObservationOutcome.SUCCESS,
                collected_item_count=len(source_replayed),
                completeness_status=source_health_shadow.CompletenessStatus.UNKNOWN,
                completeness_info={
                    "archive_replay_succeeded": True,
                    "replayed_item_count": len(source_replayed),
                    "window_start": window[0],
                    "window_end": window[1],
                },
            )
        except Exception as exc:  # noqa: BLE001
            source_finished_at = source_health_shadow.utc_now()
            logger.warning(f"政策 verify 重放 {source} 失败（跳过该源）: {exc!r}")
            _emit_source_observation(
                job_run_id=job_run_id,
                source_id=source,
                observation_type=source_health_shadow.ObservationType.VERIFY,
                started_at=source_started_at,
                finished_at=source_finished_at,
                attempt_no=attempt_no,
                outcome=source_health_shadow.ObservationOutcome.FAILURE,
                error_code=source_health_shadow.classify_error(exc),
                error_summary=source_health_shadow.error_summary(exc),
                completeness_status=source_health_shadow.CompletenessStatus.UNKNOWN,
                completeness_info={
                    "archive_replay_succeeded": False,
                    "window_start": window[0],
                    "window_end": window[1],
                },
            )

    ok = dup = 0
    if replayed:
        ok, dup = osu.bulk_create(osu.get_client(),
                                  [_strip_raw(env) for env in replayed])
    summary = (f"政策 verify({window[0]}~{window[1]}): 重放 {len(replayed)} 条, "
               f"补录 {ok} 重复 {dup}")
    logger.info(summary)
    return summary
