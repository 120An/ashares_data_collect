"""Validated, versioned snapshots of the official A-share universe.

Capture is an explicit local-file boundary.  Loading never trusts stored PASS
labels: the frozen stock-code, exchange, count, Unicode and sample gates are
recomputed from the serialized records.  Importing this module performs no
network, database or filesystem operation.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import tempfile
from typing import Any
from uuid import uuid4

from data_collect.master_data.official_exchanges import (
    BSE_SOURCE_ID,
    DOMESTIC_NETWORK_MODE,
    INHERITED_ENV_PROXY,
    PROVIDER_MODE,
    SSE_SOURCE_ID,
    SZSE_SOURCE_ID,
    OfficialAShareUniverse,
    OfficialInstrumentRecord,
    OfficialProviderResult,
)
from data_collect.master_data.public_instruments import InstrumentSchemaInspection
from data_collect.news_model.contracts import (
    ContractValidationError,
    exchange_for_stock_code,
    validate_stock_code,
)


SNAPSHOT_SCHEMA_VERSION = "official_a_share_universe_snapshot_v1"
SNAPSHOT_STATUS_VALID = "VALID"
SNAPSHOT_STATUS_STALE = "STALE"
DEFAULT_MAX_SNAPSHOT_AGE = timedelta(hours=24)
MINIMUM_SNAPSHOT_RECORDS = 3_000
REQUIRED_SAMPLE_NAMES = {
    "000001.SZ": "平安银行",
    "600519.SH": "贵州茅台",
}
_SOURCE_BY_EXCHANGE = {
    "SSE": SSE_SOURCE_ID,
    "SZSE": SZSE_SOURCE_ID,
    "BSE": BSE_SOURCE_ID,
}
_LEGACY_EXCHANGE = {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}
_ANOMALY_FIELDS = (
    "duplicate_code_count",
    "name_conflict_count",
    "cross_exchange_conflict_count",
    "invalid_code_count",
    "empty_name_count",
    "question_mark_name_count",
    "replacement_char_name_count",
    "security_type_uncertain_count",
)
_ROOT_FIELDS = {
    "schema_version",
    "created_at",
    "fetched_at",
    "provider_mode",
    "domestic_network_mode",
    "inherited_env_proxy",
    "source_ids",
    "providers",
    "authoritative_raw_total",
    "authoritative_unique_total",
    "exchange_counts",
    "anomaly_counts",
    "universe_status",
    "completeness_status",
    "records",
    "content_sha256",
    "snapshot_sha256",
}
_RECORD_FIELDS = {
    "stock_code",
    "InstrumentName",
    "ExchangeID",
    "canonical_exchange",
    "source_id",
    "source_record_type",
    "listing_presence",
    "source_security_type",
    "classification_basis",
    "provenance",
}
_CONTENT_HASH_EXCLUDED_FIELDS = {
    "created_at",
    "fetched_at",
    "content_sha256",
    "snapshot_sha256",
}
_SNAPSHOT_HASH_EXCLUDED_FIELDS = {"snapshot_sha256"}


class OfficialSnapshotError(RuntimeError):
    """Base error for capture or validation failures."""


class SnapshotCaptureError(OfficialSnapshotError):
    """A snapshot could not be safely captured."""


class SnapshotValidationError(OfficialSnapshotError):
    """A snapshot is malformed, stale or inconsistent."""


@dataclass(frozen=True)
class OfficialSnapshotCapture:
    path: Path
    content_sha256: str
    snapshot_sha256: str
    fetched_at: datetime
    source_total: int
    exchange_counts: Mapping[str, int]


@dataclass(frozen=True)
class ValidatedOfficialSnapshot:
    path: Path
    content_sha256: str
    snapshot_sha256: str
    fetched_at: datetime
    snapshot_status: str
    universe: OfficialAShareUniverse


def _invalid(message: str, *, status: str = "SNAPSHOT_INVALID") -> SnapshotValidationError:
    return SnapshotValidationError(f"{status}: {message}")


def _require_aware(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise SnapshotCaptureError(f"{field_name} must be timezone-aware")
    return value


def _parse_aware(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise _invalid(f"{field_name} must be a timezone-aware ISO 8601 string")
    raw = value.strip()
    if raw.endswith(("Z", "z")):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise _invalid(f"{field_name} is not valid ISO 8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _invalid(f"{field_name} must include timezone offset")
    return parsed


def _canonical_hash_payload(
    payload: Mapping[str, Any], excluded_fields: set[str]
) -> dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if key not in excluded_fields
    }


def canonical_content_sha256(payload: Mapping[str, Any]) -> str:
    """Hash stable content, excluding capture timestamps and the hash itself."""

    canonical = json.dumps(
        _canonical_hash_payload(payload, _CONTENT_HASH_EXCLUDED_FIELDS),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def snapshot_envelope_sha256(payload: Mapping[str, Any]) -> str:
    """Hash one exact capture envelope, excluding only this digest itself."""

    canonical = json.dumps(
        _canonical_hash_payload(payload, _SNAPSHOT_HASH_EXCLUDED_FIELDS),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _record_payload(record: OfficialInstrumentRecord) -> dict[str, Any]:
    return {
        "stock_code": record.stock_code,
        "InstrumentName": record.instrument_name,
        "ExchangeID": record.exchange_id,
        "canonical_exchange": record.canonical_exchange,
        "source_id": record.source_id,
        "source_record_type": record.source_record_type,
        "listing_presence": record.listing_presence,
        "source_security_type": record.source_security_type,
        "classification_basis": record.classification_basis,
        "provenance": {
            "source_id": record.source_id,
            "captured_from": PROVIDER_MODE,
        },
    }


def snapshot_payload(
    universe: OfficialAShareUniverse,
    *,
    fetched_at: datetime,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    """Build the deterministic serializable payload for one PASS universe."""

    fetched = _require_aware(fetched_at, "fetched_at")
    created = _require_aware(created_at or fetched, "created_at")
    if universe.universe_status != "PASS" or universe.completeness_status != "PASS":
        raise SnapshotCaptureError(
            "snapshot capture requires universe_status=PASS and completeness_status=PASS"
        )
    records = [
        _record_payload(record)
        for record in sorted(universe.records, key=lambda item: item.stock_code)
    ]
    payload: dict[str, Any] = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "created_at": created.isoformat(timespec="seconds"),
        "fetched_at": fetched.isoformat(timespec="seconds"),
        "provider_mode": universe.provider_mode,
        "domestic_network_mode": universe.domestic_network_mode,
        "inherited_env_proxy": universe.inherited_env_proxy,
        "source_ids": [SSE_SOURCE_ID, SZSE_SOURCE_ID, BSE_SOURCE_ID],
        "providers": {
            "sse": {
                "source_id": universe.sse.provider_id,
                "raw_main_count": int(universe.sse.raw_part_counts.get("main", 0)),
                "raw_star_count": int(universe.sse.raw_part_counts.get("star", 0)),
                "excluded_cdr_codes": list(universe.sse.excluded_cdr_codes),
                "excluded_cdr_count": universe.sse.excluded_cdr_count,
                "ordinary_count": universe.sse.ordinary_stock_count,
            },
            "szse": {
                "source_id": universe.szse.provider_id,
                "raw_count": universe.szse.raw_count,
                "ordinary_count": universe.szse.ordinary_stock_count,
            },
            "bse": {
                "source_id": universe.bse.provider_id,
                "expected_total": universe.bse.expected_total,
                "fetched_total": universe.bse.fetched_total,
                "total_pages": universe.bse.total_pages,
                "ordinary_count": universe.bse.ordinary_stock_count,
            },
        },
        "authoritative_raw_total": universe.authoritative_raw_total,
        "authoritative_unique_total": universe.authoritative_unique_total,
        "exchange_counts": dict(universe.exchange_counts),
        "anomaly_counts": {
            field_name: int(getattr(universe, field_name))
            for field_name in _ANOMALY_FIELDS
        },
        "universe_status": universe.universe_status,
        "completeness_status": universe.completeness_status,
        "records": records,
    }
    payload["content_sha256"] = canonical_content_sha256(payload)
    payload["snapshot_sha256"] = snapshot_envelope_sha256(payload)
    return payload


def capture_official_snapshot(
    universe: OfficialAShareUniverse,
    output_dir: str | Path,
    *,
    fetched_at: datetime | None = None,
) -> OfficialSnapshotCapture:
    """Atomically write one validated PASS snapshot without overwriting history."""

    captured_at = fetched_at or datetime.now().astimezone()
    payload = snapshot_payload(universe, fetched_at=captured_at)
    try:
        validate_snapshot_payload(
            payload,
            path="<capture-preflight>",
            now=captured_at,
        )
    except SnapshotValidationError as exc:
        raise SnapshotCaptureError(f"snapshot capture gate failed: {exc}") from exc
    destination_dir = Path(output_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)
    stamp = captured_at.strftime("%Y%m%dT%H%M%S%f%z")
    content_digest = str(payload["content_sha256"])
    snapshot_digest = str(payload["snapshot_sha256"])
    final_path = destination_dir / (
        f"official_a_share_universe_{stamp}_{snapshot_digest[:12]}_{uuid4().hex[:8]}.json"
    )
    if final_path.exists():
        raise SnapshotCaptureError(f"snapshot destination already exists: {final_path}")

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            prefix=".official_a_share_universe_",
            suffix=".tmp",
            dir=destination_dir,
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.rename(temporary_path, final_path)
        temporary_path = None
    except Exception as exc:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
        if isinstance(exc, OfficialSnapshotError):
            raise
        raise SnapshotCaptureError(f"atomic snapshot write failed: {exc}") from exc

    return OfficialSnapshotCapture(
        path=final_path,
        content_sha256=content_digest,
        snapshot_sha256=snapshot_digest,
        fetched_at=captured_at,
        source_total=universe.authoritative_unique_total,
        exchange_counts=dict(universe.exchange_counts),
    )


def _require_mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _invalid(f"{field_name} must be an object")
    return value


def _require_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _invalid(f"{field_name} must be an integer")
    return value


def _provider_result(payload: Mapping[str, Any], exchange: str) -> OfficialProviderResult:
    if exchange == "sse":
        excluded = payload.get("excluded_cdr_codes")
        if not isinstance(excluded, list) or any(not isinstance(item, str) for item in excluded):
            raise _invalid("providers.sse.excluded_cdr_codes must be a string list")
        excluded_count = _require_int(
            payload.get("excluded_cdr_count"), "providers.sse.excluded_cdr_count"
        )
        if excluded_count != len(excluded):
            raise _invalid("providers.sse excluded CDR count mismatch")
        main = _require_int(payload.get("raw_main_count"), "providers.sse.raw_main_count")
        star = _require_int(payload.get("raw_star_count"), "providers.sse.raw_star_count")
        ordinary = _require_int(payload.get("ordinary_count"), "providers.sse.ordinary_count")
        if ordinary != main + star - excluded_count:
            raise _invalid("providers.sse ordinary count is inconsistent")
        return OfficialProviderResult(
            provider_id=str(payload.get("source_id", "")),
            candidates=(),
            raw_count=main + star,
            ordinary_stock_count=ordinary,
            raw_part_counts={"main": main, "star": star},
            excluded_cdr_codes=tuple(excluded),
        )
    if exchange == "szse":
        raw = _require_int(payload.get("raw_count"), "providers.szse.raw_count")
        ordinary = _require_int(payload.get("ordinary_count"), "providers.szse.ordinary_count")
        if raw != ordinary:
            raise _invalid("providers.szse raw and ordinary counts differ")
        return OfficialProviderResult(
            provider_id=str(payload.get("source_id", "")),
            candidates=(),
            raw_count=raw,
            ordinary_stock_count=ordinary,
            raw_part_counts={"a_share_tab1": raw},
        )
    expected = _require_int(payload.get("expected_total"), "providers.bse.expected_total")
    fetched = _require_int(payload.get("fetched_total"), "providers.bse.fetched_total")
    pages = _require_int(payload.get("total_pages"), "providers.bse.total_pages")
    ordinary = _require_int(payload.get("ordinary_count"), "providers.bse.ordinary_count")
    if expected != fetched or fetched != ordinary or pages <= 0:
        raise _invalid("providers.bse pagination/count facts are inconsistent")
    return OfficialProviderResult(
        provider_id=str(payload.get("source_id", "")),
        candidates=(),
        raw_count=fetched,
        ordinary_stock_count=ordinary,
        raw_part_counts={"listed_company": fetched},
        expected_total=expected,
        fetched_total=fetched,
        total_pages=pages,
    )


def _validate_records(raw_records: Any) -> tuple[tuple[OfficialInstrumentRecord, ...], dict[str, int]]:
    if not isinstance(raw_records, list):
        raise _invalid("records must be a list")
    records: list[OfficialInstrumentRecord] = []
    seen: set[str] = set()
    counts = {"SSE": 0, "SZSE": 0, "BSE": 0}
    names: dict[str, str] = {}
    for position, raw in enumerate(raw_records):
        row = _require_mapping(raw, f"records[{position}]")
        if set(row) != _RECORD_FIELDS:
            raise _invalid(f"records[{position}] fields do not match snapshot schema")
        try:
            code = validate_stock_code(str(row.get("stock_code", "")))
        except ContractValidationError as exc:
            raise _invalid(f"records[{position}] has invalid stock_code") from exc
        if code != row.get("stock_code"):
            raise _invalid(f"records[{position}] stock_code is not canonical")
        if code in seen:
            raise _invalid(f"duplicate stock_code: {code}")
        seen.add(code)
        canonical_exchange = exchange_for_stock_code(code).value
        if row.get("canonical_exchange") != canonical_exchange:
            raise _invalid(f"{code} canonical_exchange mismatch")
        if row.get("ExchangeID") != _LEGACY_EXCHANGE[canonical_exchange]:
            raise _invalid(f"{code} ExchangeID mismatch")
        if row.get("source_id") != _SOURCE_BY_EXCHANGE[canonical_exchange]:
            raise _invalid(f"{code} source_id mismatch")
        name = row.get("InstrumentName")
        if not isinstance(name, str) or not name.strip():
            raise _invalid(f"{code} has empty InstrumentName")
        compact_name = "".join(character for character in name if not character.isspace())
        if compact_name and all(character in {"?", "？"} for character in compact_name):
            raise _invalid(f"{code} has question-mark InstrumentName")
        if "\ufffd" in name:
            raise _invalid(f"{code} has replacement character in InstrumentName")
        for field_name in (
            "source_record_type",
            "listing_presence",
            "classification_basis",
        ):
            if not isinstance(row.get(field_name), str) or not str(row[field_name]).strip():
                raise _invalid(f"{code} has empty {field_name}")
        if row.get("source_security_type") != "ordinary_a_share":
            raise _invalid(f"{code} is not explicitly ordinary_a_share")
        provenance = _require_mapping(row.get("provenance"), f"{code}.provenance")
        if provenance.get("source_id") != row.get("source_id"):
            raise _invalid(f"{code} provenance source mismatch")
        counts[canonical_exchange] += 1
        names[code] = name
        records.append(
            OfficialInstrumentRecord(
                stock_code=code,
                instrument_name=name,
                exchange_id=str(row["ExchangeID"]),
                canonical_exchange=canonical_exchange,
                source_id=str(row["source_id"]),
                source_record_type=str(row["source_record_type"]),
                listing_presence=str(row["listing_presence"]),
                source_security_type=str(row["source_security_type"]),
                classification_basis=str(row["classification_basis"]),
                provenance=dict(provenance),
            )
        )
    if len(records) < MINIMUM_SNAPSHOT_RECORDS:
        raise _invalid(f"record total is below {MINIMUM_SNAPSHOT_RECORDS}")
    if any(counts[exchange] <= 0 for exchange in counts):
        raise _invalid("SSE/SZSE/BSE records must all be present")
    for code, expected_name in REQUIRED_SAMPLE_NAMES.items():
        if names.get(code) != expected_name:
            raise _invalid(f"required sample mismatch: {code}")
    if tuple(item.stock_code for item in records) != tuple(sorted(seen)):
        raise _invalid("records must use deterministic stock_code ordering")
    return tuple(records), counts


def validate_snapshot_payload(
    payload: Mapping[str, Any],
    *,
    path: str | Path,
    now: datetime | None = None,
    max_age: timedelta = DEFAULT_MAX_SNAPSHOT_AGE,
) -> ValidatedOfficialSnapshot:
    if set(payload) != _ROOT_FIELDS:
        raise _invalid("root fields do not match snapshot schema")
    if payload.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        raise _invalid("unknown schema_version")
    stored_snapshot_hash = payload.get("snapshot_sha256")
    if not isinstance(stored_snapshot_hash, str) or len(stored_snapshot_hash) != 64:
        raise _invalid("snapshot_sha256 is invalid")
    computed_snapshot_hash = snapshot_envelope_sha256(payload)
    if not hmac.compare_digest(stored_snapshot_hash, computed_snapshot_hash):
        raise _invalid("snapshot_sha256 mismatch")
    stored_content_hash = payload.get("content_sha256")
    if not isinstance(stored_content_hash, str) or len(stored_content_hash) != 64:
        raise _invalid("content_sha256 is invalid")
    computed_content_hash = canonical_content_sha256(payload)
    if not hmac.compare_digest(stored_content_hash, computed_content_hash):
        raise _invalid("content_sha256 mismatch")
    created_at = _parse_aware(payload.get("created_at"), "created_at")
    fetched_at = _parse_aware(payload.get("fetched_at"), "fetched_at")
    if created_at < fetched_at - timedelta(minutes=5):
        raise _invalid("created_at precedes fetched_at")
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    age = current.astimezone(timezone.utc) - fetched_at.astimezone(timezone.utc)
    if age < -timedelta(minutes=5):
        raise _invalid("fetched_at is unexpectedly in the future")
    if age > max_age:
        raise _invalid(
            f"snapshot age {age} exceeds {max_age}", status=SNAPSHOT_STATUS_STALE
        )
    if payload.get("provider_mode") != PROVIDER_MODE:
        raise _invalid("provider_mode mismatch")
    if payload.get("domestic_network_mode") != DOMESTIC_NETWORK_MODE:
        raise _invalid("domestic_network_mode must be DIRECT")
    if payload.get("inherited_env_proxy") is not INHERITED_ENV_PROXY:
        raise _invalid("inherited_env_proxy must be false")
    expected_sources = [SSE_SOURCE_ID, SZSE_SOURCE_ID, BSE_SOURCE_ID]
    if payload.get("source_ids") != expected_sources:
        raise _invalid("source_ids mismatch")
    if payload.get("universe_status") != "PASS" or payload.get("completeness_status") != "PASS":
        raise _invalid("stored PASS labels are absent")

    providers = _require_mapping(payload.get("providers"), "providers")
    if set(providers) != {"sse", "szse", "bse"}:
        raise _invalid("providers must contain exactly sse/szse/bse")
    sse = _provider_result(_require_mapping(providers["sse"], "providers.sse"), "sse")
    szse = _provider_result(_require_mapping(providers["szse"], "providers.szse"), "szse")
    bse = _provider_result(_require_mapping(providers["bse"], "providers.bse"), "bse")
    if (sse.provider_id, szse.provider_id, bse.provider_id) != tuple(expected_sources):
        raise _invalid("provider source_id mismatch")

    records, recomputed_counts = _validate_records(payload.get("records"))
    unique_total = _require_int(
        payload.get("authoritative_unique_total"), "authoritative_unique_total"
    )
    if unique_total != len(records):
        raise _invalid("records count does not equal authoritative_unique_total")
    stored_counts = _require_mapping(payload.get("exchange_counts"), "exchange_counts")
    if dict(stored_counts) != recomputed_counts:
        raise _invalid("exchange_counts do not match records")
    provider_counts = {
        "SSE": sse.ordinary_stock_count,
        "SZSE": szse.ordinary_stock_count,
        "BSE": bse.ordinary_stock_count,
    }
    if provider_counts != recomputed_counts:
        raise _invalid("provider ordinary counts do not match records")
    raw_total = _require_int(payload.get("authoritative_raw_total"), "authoritative_raw_total")
    if raw_total != sse.raw_count + szse.raw_count + bse.raw_count:
        raise _invalid("authoritative_raw_total does not match provider facts")
    anomaly_counts = _require_mapping(payload.get("anomaly_counts"), "anomaly_counts")
    if set(anomaly_counts) != set(_ANOMALY_FIELDS):
        raise _invalid("anomaly_counts fields do not match snapshot schema")
    for field_name in _ANOMALY_FIELDS:
        if _require_int(anomaly_counts.get(field_name), f"anomaly_counts.{field_name}") != 0:
            raise _invalid(f"anomaly gate is nonzero: {field_name}")

    universe = OfficialAShareUniverse(
        provider_mode=PROVIDER_MODE,
        domestic_network_mode=DOMESTIC_NETWORK_MODE,
        inherited_env_proxy=INHERITED_ENV_PROXY,
        sse=sse,
        szse=szse,
        bse=bse,
        records=records,
        authoritative_raw_total=raw_total,
        authoritative_unique_total=unique_total,
        exchange_counts=recomputed_counts,
        duplicate_code_count=0,
        name_conflict_count=0,
        cross_exchange_conflict_count=0,
        invalid_code_count=0,
        empty_name_count=0,
        question_mark_name_count=0,
        replacement_char_name_count=0,
        security_type_uncertain_count=0,
        universe_status="PASS",
        completeness_status="PASS",
        apply_allowed=False,
        future_apply_prerequisites=("controlled_database_apply_not_enabled",),
        blockers=(),
        schema_inspection=InstrumentSchemaInspection(inspected=False),
    )
    return ValidatedOfficialSnapshot(
        path=Path(path),
        content_sha256=stored_content_hash,
        snapshot_sha256=stored_snapshot_hash,
        fetched_at=fetched_at,
        snapshot_status=SNAPSHOT_STATUS_VALID,
        universe=universe,
    )


def load_official_snapshot(
    path: str | Path,
    *,
    now: datetime | None = None,
    max_age: timedelta = DEFAULT_MAX_SNAPSHOT_AGE,
) -> ValidatedOfficialSnapshot:
    snapshot_path = Path(path)
    try:
        raw = snapshot_path.read_text(encoding="utf-8")
        payload = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise _invalid(f"malformed or unreadable JSON: {snapshot_path}") from exc
    if not isinstance(payload, Mapping):
        raise _invalid("snapshot root must be an object")
    return validate_snapshot_payload(
        payload,
        path=snapshot_path,
        now=now,
        max_age=max_age,
    )


__all__ = [
    "DEFAULT_MAX_SNAPSHOT_AGE",
    "MINIMUM_SNAPSHOT_RECORDS",
    "OfficialSnapshotCapture",
    "OfficialSnapshotError",
    "SNAPSHOT_SCHEMA_VERSION",
    "SNAPSHOT_STATUS_STALE",
    "SNAPSHOT_STATUS_VALID",
    "SnapshotCaptureError",
    "SnapshotValidationError",
    "ValidatedOfficialSnapshot",
    "canonical_content_sha256",
    "capture_official_snapshot",
    "load_official_snapshot",
    "snapshot_envelope_sha256",
    "snapshot_payload",
    "validate_snapshot_payload",
]
