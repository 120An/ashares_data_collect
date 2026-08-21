"""Pure-Python SourceHealth shadow observations and projections.

This module has no database, OpenSearch, network, or import-time file effects.
It records runtime evidence only; it never mutates SourceRecord or news data.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence

from data_collect.news_model.contracts import (
    CompletenessStatus,
    HealthStatus,
    SourceHealth,
    validate_source_id,
)


SHADOW_HEALTH_POLICY_VERSION = "source_health_shadow_v1"
JOB_RUN_ID_CONTEXT_KEY = "_source_health_job_run_id"
ATTEMPT_NO_CONTEXT_KEY = "_source_health_attempt_no"
SHADOW_JSONL_PATH_CONTEXT_KEY = "_source_health_shadow_jsonl_path"
_BEIJING = timezone(timedelta(hours=8), "Asia/Shanghai")
_SUMMARY_LIMIT = 500


class ObservationType(str, Enum):
    COLLECT = "collect"
    VERIFY = "verify"
    TASK = "task"


class ObservationOutcome(str, Enum):
    STARTED = "started"
    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    RETRY = "retry"
    UNKNOWN = "unknown"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _aware_datetime(value: datetime | str, field_name: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str) and value.strip():
        raw = value.strip()
        if raw.endswith(("Z", "z")):
            raw = raw[:-1] + "+00:00"
        try:
            result = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(f"{field_name} 必须是 ISO 8601 时间") from exc
    else:
        raise ValueError(f"{field_name} 必须是 datetime 或 ISO 8601 字符串")
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{field_name} 必须包含时区")
    return result


def _optional_aware_datetime(
    value: datetime | str | None, field_name: str
) -> datetime | None:
    if value is None:
        return None
    return _aware_datetime(value, field_name)


def _coerce_enum(value: Enum | str, enum_type: type[Enum], field_name: str) -> Enum:
    try:
        return value if isinstance(value, enum_type) else enum_type(value)
    except (TypeError, ValueError) as exc:
        allowed = [item.value for item in enum_type]
        raise ValueError(f"{field_name} 必须是 {allowed} 之一") from exc


def _optional_non_negative_int(value: int | None, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} 必须是非负整数或 None")
    return value


def _stable_digest(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _identity_time(value: datetime) -> str:
    """Canonical instant representation for stable identity digests."""

    return value.astimezone(timezone.utc).isoformat()


def make_job_run_id(job_name: str, started_at: datetime | str) -> str:
    name = str(job_name or "").strip()
    if not name:
        raise ValueError("job_name 不能为空")
    started = _aware_datetime(started_at, "started_at")
    return "jrun_" + _stable_digest({
        "job_name": name,
        "started_at": _identity_time(started),
    })


def make_source_health_id(
    source_id: str,
    window_start: datetime | str,
    window_end: datetime | str,
    policy_version: str = SHADOW_HEALTH_POLICY_VERSION,
) -> str:
    source_id = validate_source_id(source_id)
    start = _aware_datetime(window_start, "window_start")
    end = _aware_datetime(window_end, "window_end")
    if start > end:
        raise ValueError("window_start 不得晚于 window_end")
    return "shealth_" + _stable_digest({
        "source_id": source_id,
        "window_start": _identity_time(start),
        "window_end": _identity_time(end),
        "policy_version": policy_version,
    })


@dataclass(frozen=True, slots=True, kw_only=True)
class SourceObservation:
    job_run_id: str
    source_id: str | None
    observation_type: ObservationType | str
    started_at: datetime | str
    finished_at: datetime | str | None
    attempt_no: int
    outcome: ObservationOutcome | str

    latency_ms: int | None = None
    collected_item_count: int | None = None
    new_item_count: int | None = None
    empty_success: bool | None = None
    parse_failure_count: int | None = None
    last_item_publish_time: datetime | str | None = None
    error_code: str | None = None
    error_summary: str | None = None
    completeness_status: CompletenessStatus | str | None = None
    completeness_info: Mapping[str, Any] = field(default_factory=dict)
    retry_scheduled: bool = False
    incomplete: bool = False
    observation_id: str = field(init=False)

    def __post_init__(self) -> None:
        job_run_id = str(self.job_run_id or "").strip()
        if not job_run_id.startswith("jrun_"):
            raise ValueError("job_run_id 必须使用 jrun_ 稳定命名空间")
        object.__setattr__(self, "job_run_id", job_run_id)
        if self.source_id is not None:
            object.__setattr__(self, "source_id", validate_source_id(self.source_id))

        observation_type = _coerce_enum(
            self.observation_type, ObservationType, "observation_type"
        )
        outcome = _coerce_enum(self.outcome, ObservationOutcome, "outcome")
        if (
            observation_type in {ObservationType.COLLECT, ObservationType.VERIFY}
            and self.source_id is None
        ):
            raise ValueError("collect/verify observation 必须包含 source_id")
        object.__setattr__(self, "observation_type", observation_type)
        object.__setattr__(self, "outcome", outcome)

        started_at = _aware_datetime(self.started_at, "started_at")
        finished_at = _optional_aware_datetime(self.finished_at, "finished_at")
        if finished_at is None and outcome is not ObservationOutcome.STARTED:
            raise ValueError("只有 started observation 可以没有 finished_at")
        if finished_at is not None and finished_at < started_at:
            raise ValueError("finished_at 不得早于 started_at")
        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "finished_at", finished_at)

        if (
            isinstance(self.attempt_no, bool)
            or not isinstance(self.attempt_no, int)
            or self.attempt_no < 1
        ):
            raise ValueError("attempt_no 必须是正整数")
        for field_name in (
            "latency_ms", "collected_item_count", "new_item_count",
            "parse_failure_count",
        ):
            object.__setattr__(self, field_name, _optional_non_negative_int(
                getattr(self, field_name), field_name
            ))
        if self.latency_ms is None and finished_at is not None:
            object.__setattr__(
                self,
                "latency_ms",
                max(0, int((finished_at - started_at).total_seconds() * 1000)),
            )
        if self.new_item_count is not None:
            if self.collected_item_count is None:
                raise ValueError(
                    "new_item_count 有值时 collected_item_count 也必须有值"
                )
            if self.new_item_count > self.collected_item_count:
                raise ValueError("new_item_count 不得大于 collected_item_count")
        if self.empty_success is not None and not isinstance(self.empty_success, bool):
            raise ValueError("empty_success 必须是 boolean 或 None")
        if self.empty_success and outcome is not ObservationOutcome.SUCCESS:
            raise ValueError("empty_success 只能用于成功 observation")
        if self.empty_success and self.collected_item_count != 0:
            raise ValueError("empty_success=true 时 collected_item_count 必须为 0")

        object.__setattr__(self, "last_item_publish_time", _optional_aware_datetime(
            self.last_item_publish_time, "last_item_publish_time"
        ))
        if self.completeness_status is not None:
            object.__setattr__(self, "completeness_status", _coerce_enum(
                self.completeness_status,
                CompletenessStatus,
                "completeness_status",
            ))
        object.__setattr__(self, "completeness_info", dict(self.completeness_info))
        if not isinstance(self.retry_scheduled, bool) or not isinstance(
            self.incomplete, bool
        ):
            raise ValueError("retry_scheduled/incomplete 必须是 boolean")
        for field_name in ("error_code", "error_summary"):
            value = getattr(self, field_name)
            if value is not None:
                value = str(value).strip()
                if not value:
                    value = None
                if field_name == "error_summary" and value is not None:
                    value = value[:_SUMMARY_LIMIT]
                object.__setattr__(self, field_name, value)

        identity = {
            "job_run_id": job_run_id,
            "source_id": self.source_id,
            "observation_type": observation_type.value,
            "started_at": _identity_time(started_at),
            "finished_at": _identity_time(finished_at) if finished_at else None,
            "attempt_no": self.attempt_no,
            "outcome": outcome.value,
            "latency_ms": self.latency_ms,
            "collected_item_count": self.collected_item_count,
            "new_item_count": self.new_item_count,
            "empty_success": self.empty_success,
            "parse_failure_count": self.parse_failure_count,
            "last_item_publish_time": (
                _identity_time(self.last_item_publish_time)
                if self.last_item_publish_time else None
            ),
            "error_code": self.error_code,
            "error_summary": self.error_summary,
            "completeness_status": (
                self.completeness_status.value
                if self.completeness_status else None
            ),
            "completeness_info": self.completeness_info,
            "retry_scheduled": self.retry_scheduled,
            "incomplete": self.incomplete,
        }
        object.__setattr__(self, "observation_id", "obs_" + _stable_digest(identity))


def classify_error(exc: Exception, *, task_level: bool = False) -> str:
    if isinstance(exc, TimeoutError):
        return "timeout"
    if isinstance(exc, (ConnectionError, OSError)):
        return "network_error"
    if task_level:
        return "task_error"
    return "unknown_error"


def error_summary(exc: Exception) -> str:
    text = f"{type(exc).__name__}: {exc}"
    return text.replace("\r", " ").replace("\n", " ")[:_SUMMARY_LIMIT]


def latest_item_publish_time(
    documents: Iterable[Mapping[str, Any]],
) -> datetime | None:
    """Return latest evidenced publish time without mutating input documents."""

    values: list[datetime] = []
    for document in documents:
        canonical = document.get("publish_time")
        legacy = document.get("pub_time")
        try:
            if canonical is not None:
                values.append(_aware_datetime(canonical, "publish_time"))
            elif isinstance(legacy, str) and legacy.strip():
                parsed = datetime.fromisoformat(legacy.strip())
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=_BEIJING)
                values.append(parsed)
        except (TypeError, ValueError):
            continue
    return max(values) if values else None


def _completed_collects(
    source_id: str,
    observations: Iterable[SourceObservation],
    window_start: datetime,
    window_end: datetime,
) -> list[SourceObservation]:
    terminal = {
        ObservationOutcome.SUCCESS,
        ObservationOutcome.FAILURE,
        ObservationOutcome.TIMEOUT,
    }
    return sorted(
        (
            item for item in observations
            if item.source_id == source_id
            and item.observation_type is ObservationType.COLLECT
            and item.outcome in terminal
            and item.finished_at is not None
            and window_start <= item.finished_at <= window_end
        ),
        key=lambda item: (
            item.started_at, item.finished_at, item.attempt_no, item.observation_id
        ),
    )


def build_source_health(
    *,
    source_id: str,
    observations: Sequence[SourceObservation],
    window_start: datetime | str,
    window_end: datetime | str,
    observed_at: datetime | str | None = None,
    policy_version: str = SHADOW_HEALTH_POLICY_VERSION,
    is_current: bool = False,
    previous_snapshot: SourceHealth | None = None,
) -> SourceHealth:
    """Aggregate one window while carrying only frozen latest-state fields."""

    source_id = validate_source_id(source_id)
    start = _aware_datetime(window_start, "window_start")
    end = _aware_datetime(window_end, "window_end")
    if start > end:
        raise ValueError("window_start 不得晚于 window_end")
    if previous_snapshot is not None:
        if previous_snapshot.source_id != source_id:
            raise ValueError("previous_snapshot.source_id 必须与 source_id 一致")
        if previous_snapshot.window_end > start:
            raise ValueError(
                "previous_snapshot.window_end 不得晚于当前 window_start"
            )
    observed = _aware_datetime(observed_at or utc_now(), "observed_at")
    collects = _completed_collects(source_id, observations, start, end)
    successes = [item for item in collects if item.outcome is ObservationOutcome.SUCCESS]

    consecutive_failures = (
        previous_snapshot.consecutive_failures if previous_snapshot else 0
    )
    for item in collects:
        if item.outcome is ObservationOutcome.SUCCESS:
            consecutive_failures = 0
        else:
            consecutive_failures += 1

    latest_collect = max(
        collects,
        key=lambda item: (item.started_at, item.finished_at, item.observation_id),
        default=None,
    )
    last_success = max(
        (item.finished_at for item in successes),
        default=(previous_snapshot.last_success_at if previous_snapshot else None),
    )
    publish_candidates = [
        item.last_item_publish_time for item in collects
        if item.last_item_publish_time is not None
    ]
    if previous_snapshot and previous_snapshot.last_item_publish_time is not None:
        publish_candidates.append(previous_snapshot.last_item_publish_time)
    last_publish = max(publish_candidates, default=None)
    data_delay = None
    if last_publish is not None and last_publish <= observed:
        data_delay = int((observed - last_publish).total_seconds())

    verify_items = sorted(
        (
            item for item in observations
            if item.source_id == source_id
            and item.observation_type is ObservationType.VERIFY
            and item.finished_at is not None
            and start <= item.finished_at <= end
            and item.completeness_status is not None
        ),
        key=lambda item: (item.finished_at, item.observation_id),
    )
    latest_verify = verify_items[-1] if verify_items else None
    completeness_status = (
        latest_verify.completeness_status
        if latest_verify else CompletenessStatus.UNKNOWN
    )

    metrics: dict[str, Any] = {
        "policy_kind": "shadow_no_scoring",
        "unknown_collected_attempt_count": sum(
            item.collected_item_count is None for item in collects
        ),
        "unknown_new_item_attempt_count": sum(
            item.new_item_count is None for item in collects
        ),
        "unknown_parse_failure_attempt_count": sum(
            item.parse_failure_count is None for item in collects
        ),
    }
    if latest_verify:
        metrics["latest_verify"] = dict(latest_verify.completeness_info)

    latest_error = (
        latest_collect
        if latest_collect and latest_collect.outcome is not ObservationOutcome.SUCCESS
        else None
    )
    return SourceHealth(
        source_health_id=make_source_health_id(
            source_id, start, end, policy_version
        ),
        source_id=source_id,
        observed_at=observed,
        window_start=start,
        window_end=end,
        health_status=HealthStatus.UNKNOWN,
        last_success_at=last_success,
        # V1.1 defines this as the time the latest collection attempt began,
        # not when it happened to finish.
        last_attempt_at=(
            latest_collect.started_at
            if latest_collect else (
                previous_snapshot.last_attempt_at if previous_snapshot else None
            )
        ),
        consecutive_failures=consecutive_failures,
        latency_ms=(
            latest_collect.latency_ms
            if latest_collect else (
                previous_snapshot.latency_ms if previous_snapshot else None
            )
        ),
        attempt_count=len(collects),
        success_count=len(successes),
        collected_item_count=sum(
            item.collected_item_count or 0 for item in collects
        ),
        new_item_count=sum(item.new_item_count or 0 for item in collects),
        empty_success_count=sum(item.empty_success is True for item in successes),
        parse_failure_count=sum(
            item.parse_failure_count or 0 for item in collects
        ),
        last_item_publish_time=last_publish,
        data_delay_seconds=data_delay,
        completeness_status=completeness_status,
        completeness_metrics=metrics,
        last_error_code=latest_error.error_code if latest_error else None,
        last_error_summary=latest_error.error_summary if latest_error else None,
        health_policy_version=policy_version,
        is_current=is_current,
        created_at=observed,
    )


def project_current(
    snapshots: Iterable[SourceHealth],
) -> dict[str, SourceHealth]:
    """Select the latest window per source without mutating history snapshots."""

    selected: dict[str, SourceHealth] = {}
    for snapshot in snapshots:
        current = selected.get(snapshot.source_id)
        key = (snapshot.window_end, snapshot.observed_at, snapshot.source_health_id)
        if current is None or key > (
            current.window_end, current.observed_at, current.source_health_id
        ):
            selected[snapshot.source_id] = snapshot
    return {
        source_id: replace(snapshot, is_current=True)
        for source_id, snapshot in selected.items()
    }


class ShadowSink(Protocol):
    def emit(self, record: SourceObservation | SourceHealth) -> None: ...


class NullShadowSink:
    def emit(self, record: SourceObservation | SourceHealth) -> None:
        return None


class InMemoryShadowSink:
    def __init__(self) -> None:
        self.records: list[SourceObservation | SourceHealth] = []

    def emit(self, record: SourceObservation | SourceHealth) -> None:
        self.records.append(record)


def _jsonable_record(record: SourceObservation | SourceHealth) -> dict[str, Any]:
    payload = asdict(record)
    for key, value in list(payload.items()):
        if isinstance(value, datetime):
            payload[key] = value.isoformat()
        elif isinstance(value, Enum):
            payload[key] = value.value
    payload["record_type"] = (
        "observation" if isinstance(record, SourceObservation) else "source_health"
    )
    return payload


class JsonlShadowSink:
    """Explicit local UTF-8 JSONL sink; constructing/importing it writes nothing."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def emit(self, record: SourceObservation | SourceHealth) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(
                _jsonable_record(record), ensure_ascii=False, sort_keys=True
            ) + "\n")


def emit_shadow(
    sink: ShadowSink,
    record: SourceObservation | SourceHealth,
    *,
    logger: logging.Logger | None = None,
) -> bool:
    """Fail-open emission: monitoring can never fail collection or scheduling."""

    try:
        sink.emit(record)
        return True
    except Exception as exc:  # noqa: BLE001 - fail-open boundary by design
        (logger or logging.getLogger(__name__)).warning(
            "SourceHealth shadow sink 写入失败（已 fail-open）: %r", exc
        )
        return False


__all__ = [
    "ATTEMPT_NO_CONTEXT_KEY",
    "InMemoryShadowSink",
    "JOB_RUN_ID_CONTEXT_KEY",
    "JsonlShadowSink",
    "NullShadowSink",
    "ObservationOutcome",
    "ObservationType",
    "SHADOW_HEALTH_POLICY_VERSION",
    "SHADOW_JSONL_PATH_CONTEXT_KEY",
    "SourceObservation",
    "build_source_health",
    "classify_error",
    "emit_shadow",
    "error_summary",
    "latest_item_publish_time",
    "make_job_run_id",
    "make_source_health_id",
    "project_current",
    "utc_now",
]
