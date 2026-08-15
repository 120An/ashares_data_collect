"""Phase 1 news-field compatibility helpers.

This module is deliberately pure Python and side-effect free.  It reads the
legacy and V1.1 field pairs, exposes a canonical in-memory view, and can build
a new compatibility projection without mutating its input.

It does not create news IDs, infer entities, consult source governance, or
construct a FINAL :class:`NewsDocument`.  In particular, a missing archive
receipt is left missing rather than replaced with a fabricated URI.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from data_collect.news_model.contracts import (
    ContractValidationError,
    PublishTimePrecision,
    validate_news_id,
    validate_source_id,
    validate_stock_code,
)


NEWS_DOCUMENT_SCHEMA_VERSION = "news_document_v1"

_BEIJING_TIMEZONE = timezone(timedelta(hours=8), name="Asia/Shanghai")
_LEGACY_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class NewsCompatibilityError(ContractValidationError):
    """Base error for an invalid or incomplete compatibility input."""


class NewsIdentityMismatchError(NewsCompatibilityError):
    """Raised when ``news_id`` and an actual OpenSearch ID disagree."""


class MissingCompatibilityFieldError(NewsCompatibilityError):
    """Raised when neither side of a required legacy/new field pair exists."""


class MismatchType(str, Enum):
    """Kinds of non-fatal mismatch reported by the compatibility reader."""

    VALUE_MISMATCH = "value_mismatch"
    TIME_INSTANT_MISMATCH = "time_instant_mismatch"


@dataclass(frozen=True, slots=True)
class MismatchDiagnostic:
    """A readable, non-mutating report for one legacy/new field conflict."""

    field_name: str
    legacy_value: Any
    canonical_value: Any
    mismatch_type: MismatchType
    message: str

    @property
    def new_value(self) -> Any:
        """Alias for callers that refer to the preferred value as new_value."""

        return self.canonical_value


@dataclass(frozen=True, slots=True)
class CanonicalNewsView:
    """The Phase 1 canonical view of compatibility-safe document fields."""

    news_id: str
    publish_time: datetime
    collect_time: datetime
    source_id: str
    stock_codes: tuple[str, ...]
    publish_time_precision: PublishTimePrecision
    publish_time_is_estimated: bool | None
    mismatches: tuple[MismatchDiagnostic, ...]

    @property
    def diagnostics(self) -> tuple[MismatchDiagnostic, ...]:
        """A terminology alias for ``mismatches``."""

        return self.mismatches

    @property
    def has_mismatches(self) -> bool:
        return bool(self.mismatches)


def _is_provided(document: Mapping[str, Any], field_name: str) -> bool:
    """Treat explicit invalid values as present, while allowing null fallback."""

    return field_name in document and document[field_name] is not None


def _copy_diagnostic_value(value: Any) -> Any:
    """Keep diagnostics independent from caller-owned mutable containers."""

    return deepcopy(value)


def _identity_mismatch(left_name: str, left: str, right_name: str, right: str) -> None:
    raise NewsIdentityMismatchError(
        "news identity mismatch: "
        f"{left_name}={left!r} does not equal {right_name}={right!r}"
    )


def _validate_exact_news_id(value: Any, field_name: str) -> str:
    """Validate identity without silently trimming or otherwise rewriting it."""

    validated = validate_news_id(value)
    if value != validated:
        raise NewsCompatibilityError(
            f"{field_name} must already be a canonical news ID; "
            "the compatibility layer does not rewrite identities"
        )
    return validated


def _read_news_id(document: Mapping[str, Any], hit_id: str | None) -> str:
    canonical_id = None
    embedded_hit_id = None
    external_hit_id = None

    if _is_provided(document, "news_id"):
        canonical_id = _validate_exact_news_id(document["news_id"], "news_id")
    if _is_provided(document, "_id"):
        embedded_hit_id = _validate_exact_news_id(document["_id"], "_id")
    if hit_id is not None:
        external_hit_id = _validate_exact_news_id(hit_id, "hit_id")

    if (
        embedded_hit_id is not None
        and external_hit_id is not None
        and embedded_hit_id != external_hit_id
    ):
        _identity_mismatch("_id", embedded_hit_id, "hit_id", external_hit_id)

    actual_id = external_hit_id if external_hit_id is not None else embedded_hit_id
    actual_name = "hit_id" if external_hit_id is not None else "_id"
    if canonical_id is not None and actual_id is not None and canonical_id != actual_id:
        _identity_mismatch("news_id", canonical_id, actual_name, actual_id)

    selected_id = canonical_id if canonical_id is not None else actual_id
    if selected_id is None:
        raise MissingCompatibilityFieldError(
            "news identity is missing: provide news_id, hit_id, or _id; "
            "the compatibility layer never generates an ID"
        )
    return selected_id


def _parse_canonical_time(value: Any, field_name: str) -> datetime:
    parsed = value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            raise NewsCompatibilityError(
                f"{field_name} must be a non-empty timezone-aware ISO 8601 value"
            )
        if raw.endswith(("Z", "z")):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise NewsCompatibilityError(
                f"{field_name} must be a valid timezone-aware ISO 8601 value: {value!r}"
            ) from exc

    if not isinstance(parsed, datetime):
        raise NewsCompatibilityError(
            f"{field_name} must be a timezone-aware datetime or ISO 8601 string"
        )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise NewsCompatibilityError(f"{field_name} must include a timezone")
    return parsed


def _parse_legacy_time(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise NewsCompatibilityError(
            f"{field_name} must remain a non-empty legacy datetime string"
        )
    try:
        parsed = datetime.strptime(value.strip(), _LEGACY_DATETIME_FORMAT)
    except ValueError as exc:
        raise NewsCompatibilityError(
            f"{field_name} must use legacy format YYYY-MM-DD HH:MM:SS: {value!r}"
        ) from exc
    return parsed.replace(tzinfo=_BEIJING_TIMEZONE)


def _same_instant(left: datetime, right: datetime) -> bool:
    return left.astimezone(timezone.utc) == right.astimezone(timezone.utc)


def _read_time_pair(
    document: Mapping[str, Any],
    canonical_field: str,
    legacy_field: str,
    mismatches: list[MismatchDiagnostic],
) -> datetime:
    has_canonical = _is_provided(document, canonical_field)
    has_legacy = _is_provided(document, legacy_field)

    if not has_canonical and not has_legacy:
        raise MissingCompatibilityFieldError(
            f"missing time: provide {canonical_field} or {legacy_field}"
        )

    canonical_time = (
        _parse_canonical_time(document[canonical_field], canonical_field)
        if has_canonical
        else None
    )
    legacy_time = (
        _parse_legacy_time(document[legacy_field], legacy_field)
        if has_legacy
        else None
    )

    if (
        canonical_time is not None
        and legacy_time is not None
        and not _same_instant(canonical_time, legacy_time)
    ):
        mismatches.append(
            MismatchDiagnostic(
                field_name=canonical_field,
                legacy_value=_copy_diagnostic_value(document[legacy_field]),
                canonical_value=_copy_diagnostic_value(document[canonical_field]),
                mismatch_type=MismatchType.TIME_INSTANT_MISMATCH,
                message=(
                    f"{canonical_field} and {legacy_field} represent different instants; "
                    f"{canonical_field} was selected"
                ),
            )
        )

    if canonical_time is not None:
        return canonical_time
    if legacy_time is not None:
        return legacy_time
    raise MissingCompatibilityFieldError(
        f"missing time: provide {canonical_field} or {legacy_field}"
    )


def _read_source_id(
    document: Mapping[str, Any], mismatches: list[MismatchDiagnostic]
) -> str:
    has_canonical = _is_provided(document, "source_id")
    has_legacy = _is_provided(document, "source")
    if not has_canonical and not has_legacy:
        raise MissingCompatibilityFieldError("missing source: provide source_id or source")

    canonical_source = (
        validate_source_id(document["source_id"]) if has_canonical else None
    )
    legacy_source = validate_source_id(document["source"]) if has_legacy else None

    if (
        canonical_source is not None
        and legacy_source is not None
        and canonical_source != legacy_source
    ):
        mismatches.append(
            MismatchDiagnostic(
                field_name="source_id",
                legacy_value=_copy_diagnostic_value(document["source"]),
                canonical_value=_copy_diagnostic_value(document["source_id"]),
                mismatch_type=MismatchType.VALUE_MISMATCH,
                message="source_id and source differ; source_id was selected",
            )
        )

    if canonical_source is not None:
        return canonical_source
    if legacy_source is not None:
        return legacy_source
    raise MissingCompatibilityFieldError("missing source: provide source_id or source")


def _coerce_stock_codes(value: Any, field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise NewsCompatibilityError(f"{field_name} must be a sequence of stock codes")
    return tuple(validate_stock_code(code) for code in value)


def _read_stock_codes(
    document: Mapping[str, Any], mismatches: list[MismatchDiagnostic]
) -> tuple[str, ...]:
    has_canonical = _is_provided(document, "stock_codes")
    has_legacy = _is_provided(document, "stocks")

    canonical_codes = (
        _coerce_stock_codes(document["stock_codes"], "stock_codes")
        if has_canonical
        else None
    )
    legacy_codes = (
        _coerce_stock_codes(document["stocks"], "stocks") if has_legacy else None
    )

    if (
        canonical_codes is not None
        and legacy_codes is not None
        and canonical_codes != legacy_codes
    ):
        mismatches.append(
            MismatchDiagnostic(
                field_name="stock_codes",
                legacy_value=_copy_diagnostic_value(document["stocks"]),
                canonical_value=_copy_diagnostic_value(document["stock_codes"]),
                mismatch_type=MismatchType.VALUE_MISMATCH,
                message="stock_codes and stocks differ; stock_codes was selected without merging",
            )
        )

    if canonical_codes is not None:
        return canonical_codes
    if legacy_codes is not None:
        return legacy_codes
    return ()


def _read_publish_time_precision(document: Mapping[str, Any]) -> PublishTimePrecision:
    value = document.get("publish_time_precision")
    if value is None:
        return PublishTimePrecision.UNKNOWN
    try:
        return PublishTimePrecision(value)
    except (TypeError, ValueError) as exc:
        allowed = ", ".join(item.value for item in PublishTimePrecision)
        raise NewsCompatibilityError(
            f"publish_time_precision must be one of: {allowed}; got {value!r}"
        ) from exc


def _require_boolean(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise NewsCompatibilityError(f"{field_name} must be boolean when provided")
    return value


def _read_publish_time_is_estimated(
    document: Mapping[str, Any], mismatches: list[MismatchDiagnostic]
) -> bool | None:
    has_canonical = _is_provided(document, "publish_time_is_estimated")
    has_legacy = _is_provided(document, "time_estimated")

    canonical_value = (
        _require_boolean(
            document["publish_time_is_estimated"], "publish_time_is_estimated"
        )
        if has_canonical
        else None
    )
    legacy_value = (
        _require_boolean(document["time_estimated"], "time_estimated")
        if has_legacy
        else None
    )

    if (
        canonical_value is not None
        and legacy_value is not None
        and canonical_value != legacy_value
    ):
        mismatches.append(
            MismatchDiagnostic(
                field_name="publish_time_is_estimated",
                legacy_value=legacy_value,
                canonical_value=canonical_value,
                mismatch_type=MismatchType.VALUE_MISMATCH,
                message=(
                    "publish_time_is_estimated and time_estimated differ; "
                    "publish_time_is_estimated was selected"
                ),
            )
        )

    return canonical_value if canonical_value is not None else legacy_value


def read_canonical_news(
    document: Mapping[str, Any], *, hit_id: str | None = None
) -> CanonicalNewsView:
    """Read legacy/new field pairs into a canonical, timezone-aware view.

    ``news_id`` conflicts are hard errors because selecting either identity
    could corrupt create-only semantics.  Other conflicts are returned in
    ``CanonicalNewsView.mismatches`` while the canonical field wins.
    """

    if not isinstance(document, Mapping):
        raise NewsCompatibilityError("document must be a mapping")

    mismatches: list[MismatchDiagnostic] = []
    news_id = _read_news_id(document, hit_id)
    publish_time = _read_time_pair(
        document, "publish_time", "pub_time", mismatches
    )
    collect_time = _read_time_pair(
        document, "collect_time", "fetch_time", mismatches
    )
    source_id = _read_source_id(document, mismatches)
    stock_codes = _read_stock_codes(document, mismatches)
    publish_time_precision = _read_publish_time_precision(document)
    publish_time_is_estimated = _read_publish_time_is_estimated(
        document, mismatches
    )

    return CanonicalNewsView(
        news_id=news_id,
        publish_time=publish_time,
        collect_time=collect_time,
        source_id=source_id,
        stock_codes=stock_codes,
        publish_time_precision=publish_time_precision,
        publish_time_is_estimated=publish_time_is_estimated,
        mismatches=tuple(mismatches),
    )


def build_compatibility_projection(
    document: Mapping[str, Any], *, hit_id: str | None = None
) -> dict[str, Any]:
    """Return a deep-copied Phase 1 projection; never mutate ``document``.

    Canonical times are serialized as ISO 8601 strings with explicit UTC
    offsets.  Legacy fields remain untouched in the returned copy.  The
    projection only mirrors deterministic compatibility fields and never
    invents IDs, archive URIs, authority values, entities, or relations.
    """

    view = read_canonical_news(document, hit_id=hit_id)
    projected = deepcopy(dict(document))
    projected["news_id"] = view.news_id
    projected["schema_version"] = NEWS_DOCUMENT_SCHEMA_VERSION
    projected["publish_time"] = view.publish_time.isoformat()
    projected["collect_time"] = view.collect_time.isoformat()
    projected["source_id"] = view.source_id
    projected["stock_codes"] = list(view.stock_codes)
    projected["publish_time_precision"] = view.publish_time_precision.value
    if view.publish_time_is_estimated is not None:
        projected["publish_time_is_estimated"] = view.publish_time_is_estimated
    return projected


__all__ = [
    "CanonicalNewsView",
    "MismatchDiagnostic",
    "MismatchType",
    "MissingCompatibilityFieldError",
    "NEWS_DOCUMENT_SCHEMA_VERSION",
    "NewsCompatibilityError",
    "NewsIdentityMismatchError",
    "build_compatibility_projection",
    "read_canonical_news",
]
