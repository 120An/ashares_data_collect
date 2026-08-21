"""Deterministic instrument_info bootstrap write-plan (dry-run only).

This module deliberately implements BOOTSTRAP planning, not recurring REFRESH
and not database apply.  PostgreSQL is a read-only boundary.  The plan never
deletes legacy rows and never invents values for QMT-only columns.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime
import hashlib
import json
import re
from typing import Any

from data_collect.master_data.official_exchanges import OfficialAShareUniverse
from data_collect.news_model.contracts import (
    ContractValidationError,
    validate_stock_code,
)


PLAN_MODE = "BOOTSTRAP"
CONTROLLED_APPLY_PREREQUISITE = (
    "explicit_snapshot_and_expected_plan_sha256_required"
)
INSTRUMENT_WRITE_FIELDS = ("stock_code", "InstrumentName", "ExchangeID")
CHANGELOG_FIELDS = (
    "stock_code",
    "changed_at",
    "field_name",
    "old_value",
    "new_value",
)


class InstrumentBootstrapError(RuntimeError):
    """Base error for an unsafe or unreadable bootstrap plan."""


@dataclass(frozen=True)
class BootstrapReadSnapshot:
    existing_instruments: tuple[Mapping[str, Any], ...]
    relevant_changelog: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class InstrumentBootstrapAction:
    stock_code: str
    action_type: str
    change_kinds: tuple[str, ...]
    old_instrument_name: str | None
    new_instrument_name: str
    old_exchange_id: str | None
    new_exchange_id: str
    planned_values: Mapping[str, Any]


@dataclass(frozen=True)
class InstrumentChangelogPlan:
    stock_code: str
    changed_at: date
    field_name: str
    old_value: str
    new_value: str


@dataclass(frozen=True)
class InstrumentBootstrapPlan:
    plan_mode: str
    plan_date: date
    source_content_sha256: str | None
    source_snapshot_sha256: str | None
    database_baseline_sha256: str
    plan_sha256: str
    source_total: int
    existing_total: int
    would_insert_count: int
    would_update_count: int
    would_repair_corrupted_name_count: int
    would_change_exchange_count: int
    would_unchanged_count: int
    would_insert_changelog_count: int
    would_delete_count: int
    existing_not_in_official_count: int
    insert_codes: tuple[str, ...]
    update_codes: tuple[str, ...]
    repair_codes: tuple[str, ...]
    unchanged_codes: tuple[str, ...]
    existing_not_in_official_codes: tuple[str, ...]
    actions: tuple[InstrumentBootstrapAction, ...]
    changelog_entries: tuple[InstrumentChangelogPlan, ...]
    allowed_instrument_write_fields: tuple[str, ...]
    allowed_changelog_fields: tuple[str, ...]
    schema_compatible: bool
    universe_status: str
    plan_status: str
    apply_allowed: bool
    future_apply_prerequisites: tuple[str, ...]
    blockers: tuple[str, ...]

    @property
    def repair_samples(self) -> tuple[InstrumentBootstrapAction, ...]:
        return tuple(
            action
            for action in self.actions
            if action.action_type == "corrupted_name_repair"
        )[:10]

    @property
    def update_samples(self) -> tuple[InstrumentBootstrapAction, ...]:
        return tuple(
            action
            for action in self.actions
            if action.action_type == "existing_fact_difference"
        )[:10]


def _question_mark_only(value: str) -> bool:
    compact = "".join(character for character in value if not character.isspace())
    return bool(compact) and all(character in {"?", "？"} for character in compact)


def corrupted_name_reason(value: Any) -> str | None:
    if value is None or not str(value).strip():
        return "empty_name"
    text = str(value)
    if _question_mark_only(text):
        return "question_mark_name"
    if "\ufffd" in text:
        return "replacement_character_name"
    return None


def _date_key(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value).strip()


def _canonical_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _sha256_json(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def database_baseline_sha256(
    existing_rows: Sequence[Mapping[str, Any]],
    changelog_rows: Sequence[Mapping[str, Any]],
) -> str:
    """Hash the exact three-field snapshot and relevant legacy changelog facts."""

    instruments = sorted(
        (
            {
                "stock_code": _canonical_scalar(row.get("stock_code")),
                "InstrumentName": _canonical_scalar(row.get("InstrumentName")),
                "ExchangeID": _canonical_scalar(row.get("ExchangeID")),
            }
            for row in existing_rows
        ),
        key=lambda row: (
            str(row["stock_code"]),
            str(row["InstrumentName"]),
            str(row["ExchangeID"]),
        ),
    )
    changelog = sorted(
        (
            {
                "stock_code": _canonical_scalar(row.get("stock_code")),
                "changed_at": _date_key(row.get("changed_at")),
                "field_name": _canonical_scalar(row.get("field_name")),
                "old_value": _canonical_scalar(row.get("old_value")),
                "new_value": _canonical_scalar(row.get("new_value")),
            }
            for row in changelog_rows
        ),
        key=lambda row: (
            str(row["stock_code"]),
            str(row["changed_at"]),
            str(row["field_name"]),
            str(row["old_value"]),
            str(row["new_value"]),
        ),
    )
    return _sha256_json(
        {
            "schema_version": "instrument_bootstrap_database_baseline_v1",
            "instrument_info": instruments,
            "instrument_changelog": changelog,
        }
    )


def instrument_bootstrap_plan_sha256(plan: InstrumentBootstrapPlan) -> str:
    """Hash the full actionable plan, not only its aggregate counts."""

    payload = {
        "schema_version": "instrument_bootstrap_plan_v2",
        "plan_mode": plan.plan_mode,
        "plan_date": plan.plan_date.isoformat(),
        "source_content_sha256": plan.source_content_sha256,
        "source_snapshot_sha256": plan.source_snapshot_sha256,
        "database_baseline_sha256": plan.database_baseline_sha256,
        "source_total": plan.source_total,
        "existing_total": plan.existing_total,
        "counts": {
            "insert": plan.would_insert_count,
            "update": plan.would_update_count,
            "repair_corrupted_name": plan.would_repair_corrupted_name_count,
            "change_exchange": plan.would_change_exchange_count,
            "unchanged": plan.would_unchanged_count,
            "insert_changelog": plan.would_insert_changelog_count,
            "delete": plan.would_delete_count,
            "existing_not_in_official": plan.existing_not_in_official_count,
        },
        "actions": [
            {
                "stock_code": action.stock_code,
                "action_type": action.action_type,
                "change_kinds": list(action.change_kinds),
                "old_instrument_name": action.old_instrument_name,
                "new_instrument_name": action.new_instrument_name,
                "old_exchange_id": action.old_exchange_id,
                "new_exchange_id": action.new_exchange_id,
                "planned_values": dict(action.planned_values),
            }
            for action in plan.actions
        ],
        "changelog_entries": [
            {
                "stock_code": entry.stock_code,
                "changed_at": entry.changed_at.isoformat(),
                "field_name": entry.field_name,
                "old_value": entry.old_value,
                "new_value": entry.new_value,
            }
            for entry in plan.changelog_entries
        ],
        "allowed_instrument_write_fields": list(plan.allowed_instrument_write_fields),
        "allowed_changelog_fields": list(plan.allowed_changelog_fields),
        "schema_compatible": plan.schema_compatible,
        "universe_status": plan.universe_status,
        "plan_status": plan.plan_status,
        "apply_allowed": plan.apply_allowed,
        "future_apply_prerequisites": list(plan.future_apply_prerequisites),
        "blockers": list(plan.blockers),
    }
    return _sha256_json(payload)


def read_bootstrap_inputs_from_cursor(cursor: Any) -> BootstrapReadSnapshot:
    """Read the protected baseline using an existing transaction cursor."""

    cursor.execute(
        """
        SELECT "stock_code", "InstrumentName", "ExchangeID"
        FROM "instrument_info"
        ORDER BY "stock_code"
        """
    )
    instrument_rows = tuple(
        {
            "stock_code": row[0],
            "InstrumentName": row[1],
            "ExchangeID": row[2],
        }
        for row in cursor.fetchall()
    )
    cursor.execute(
        """
        SELECT "stock_code", "changed_at", "field_name", "old_value", "new_value"
        FROM "instrument_changelog"
        WHERE "field_name" IN ('InstrumentName', 'ExchangeID')
        ORDER BY "stock_code", "changed_at", "field_name"
        """
    )
    changelog_rows = tuple(
        {
            "stock_code": row[0],
            "changed_at": row[1],
            "field_name": row[2],
            "old_value": row[3],
            "new_value": row[4],
        }
        for row in cursor.fetchall()
    )
    return BootstrapReadSnapshot(
        existing_instruments=instrument_rows,
        relevant_changelog=changelog_rows,
    )


def read_bootstrap_inputs_from_postgres(
    connection_factory: Callable[[], Any] | None = None,
) -> BootstrapReadSnapshot:
    """Read only the three authoritative fields and relevant legacy changelog."""

    if connection_factory is None:
        from data_collect.utils.db import get_connection

        connection_factory = get_connection
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            return read_bootstrap_inputs_from_cursor(cursor)
    finally:
        try:
            connection.rollback()
        finally:
            connection.close()


def _failed_plan(
    universe: OfficialAShareUniverse,
    existing_rows: Sequence[Mapping[str, Any]],
    changelog_rows: Sequence[Mapping[str, Any]],
    plan_date: date,
    blockers: Sequence[str],
    source_content_sha256: str | None,
    source_snapshot_sha256: str | None,
) -> InstrumentBootstrapPlan:
    baseline_hash = database_baseline_sha256(existing_rows, changelog_rows)
    plan = InstrumentBootstrapPlan(
        plan_mode=PLAN_MODE,
        plan_date=plan_date,
        source_content_sha256=source_content_sha256,
        source_snapshot_sha256=source_snapshot_sha256,
        database_baseline_sha256=baseline_hash,
        plan_sha256="",
        source_total=universe.authoritative_unique_total,
        existing_total=len(existing_rows),
        would_insert_count=0,
        would_update_count=0,
        would_repair_corrupted_name_count=0,
        would_change_exchange_count=0,
        would_unchanged_count=0,
        would_insert_changelog_count=0,
        would_delete_count=0,
        existing_not_in_official_count=0,
        insert_codes=(),
        update_codes=(),
        repair_codes=(),
        unchanged_codes=(),
        existing_not_in_official_codes=(),
        actions=(),
        changelog_entries=(),
        allowed_instrument_write_fields=INSTRUMENT_WRITE_FIELDS,
        allowed_changelog_fields=CHANGELOG_FIELDS,
        schema_compatible=universe.schema_inspection.compatible,
        universe_status=universe.universe_status,
        plan_status="FAIL",
        apply_allowed=False,
        future_apply_prerequisites=(CONTROLLED_APPLY_PREREQUISITE,),
        blockers=tuple(blockers),
    )
    return replace(plan, plan_sha256=instrument_bootstrap_plan_sha256(plan))


def build_instrument_bootstrap_plan(
    universe: OfficialAShareUniverse,
    existing_rows: Sequence[Mapping[str, Any]],
    changelog_rows: Sequence[Mapping[str, Any]],
    *,
    plan_date: date,
    source_content_sha256: str | None = None,
    source_snapshot_sha256: str | None = None,
) -> InstrumentBootstrapPlan:
    """Build a deterministic, non-deleting BOOTSTRAP plan without database writes."""

    existing_snapshot = tuple(existing_rows)
    changelog_snapshot = tuple(changelog_rows)
    baseline_hash = database_baseline_sha256(existing_snapshot, changelog_snapshot)
    preflight_blockers: list[str] = []
    if universe.universe_status != "PASS":
        preflight_blockers.append("official universe status is not PASS")
    if universe.completeness_status != "PASS":
        preflight_blockers.append("official completeness status is not PASS")
    if not universe.schema_inspection.inspected:
        preflight_blockers.append("instrument_info schema was not inspected")
    elif not universe.schema_inspection.compatible:
        preflight_blockers.append("instrument_info schema is not compatible")
    if universe.authoritative_unique_total < 3_000:
        preflight_blockers.append("official source total is below 3000")
    if len(universe.records) != universe.authoritative_unique_total:
        preflight_blockers.append(
            "official record count does not equal authoritative unique total"
        )
    for exchange_id in ("SSE", "SZSE", "BSE"):
        if int(universe.exchange_counts.get(exchange_id, 0)) <= 0:
            preflight_blockers.append(f"official {exchange_id} universe is empty")
    anomaly_counts = {
        "duplicate_code_count": universe.duplicate_code_count,
        "name_conflict_count": universe.name_conflict_count,
        "cross_exchange_conflict_count": universe.cross_exchange_conflict_count,
        "invalid_code_count": universe.invalid_code_count,
        "empty_name_count": universe.empty_name_count,
        "question_mark_name_count": universe.question_mark_name_count,
        "replacement_char_name_count": universe.replacement_char_name_count,
        "security_type_uncertain_count": universe.security_type_uncertain_count,
    }
    for field_name, count in anomaly_counts.items():
        if count:
            preflight_blockers.append(f"official {field_name} is not zero: {count}")
    for digest_name, digest_value in (
        ("content", source_content_sha256),
        ("snapshot", source_snapshot_sha256),
    ):
        if digest_value is not None and not re.fullmatch(r"[0-9a-f]{64}", digest_value):
            preflight_blockers.append(f"source {digest_name} SHA256 is invalid")
    if preflight_blockers:
        return _failed_plan(
            universe,
            existing_snapshot,
            changelog_snapshot,
            plan_date,
            preflight_blockers,
            source_content_sha256,
            source_snapshot_sha256,
        )

    existing_by_code: dict[str, Mapping[str, Any]] = {}
    existing_validation_blockers: list[str] = []
    for position, row in enumerate(existing_snapshot):
        if not isinstance(row, Mapping):
            existing_validation_blockers.append(
                f"instrument_info row {position} is not a mapping"
            )
            continue
        try:
            code = validate_stock_code(str(row.get("stock_code", "")))
        except ContractValidationError as exc:
            existing_validation_blockers.append(
                f"invalid existing stock_code at row {position}: {exc}"
            )
            continue
        if code in existing_by_code:
            existing_validation_blockers.append(
                f"duplicate existing instrument_info stock_code: {code}"
            )
            continue
        existing_by_code[code] = row
    if existing_validation_blockers:
        return _failed_plan(
            universe,
            existing_snapshot,
            changelog_snapshot,
            plan_date,
            existing_validation_blockers,
            source_content_sha256,
            source_snapshot_sha256,
        )

    existing_changelog: dict[tuple[str, str, str], tuple[str, str]] = {}
    for row in changelog_snapshot:
        if not isinstance(row, Mapping):
            continue
        key = (
            str(row.get("stock_code", "")).strip().upper(),
            _date_key(row.get("changed_at")),
            str(row.get("field_name", "")).strip(),
        )
        existing_changelog[key] = (
            str(row.get("old_value")),
            str(row.get("new_value")),
        )

    actions: list[InstrumentBootstrapAction] = []
    changelog_entries: list[InstrumentChangelogPlan] = []
    planning_blockers: list[str] = []
    source_codes = {record.stock_code for record in universe.records}
    existing_not_in_official = tuple(sorted(set(existing_by_code) - source_codes))

    def plan_changelog(code: str, field_name: str, old_value: Any, new_value: Any) -> None:
        old_text = str(old_value)
        new_text = str(new_value)
        key = (code, plan_date.isoformat(), field_name)
        already = existing_changelog.get(key)
        if already is None:
            changelog_entries.append(
                InstrumentChangelogPlan(
                    stock_code=code,
                    changed_at=plan_date,
                    field_name=field_name,
                    old_value=old_text,
                    new_value=new_text,
                )
            )
        elif already != (old_text, new_text):
            planning_blockers.append(
                f"instrument_changelog key conflict for {code} {plan_date} {field_name}"
            )

    for official in sorted(universe.records, key=lambda item: item.stock_code):
        existing = existing_by_code.get(official.stock_code)
        if existing is None:
            actions.append(
                InstrumentBootstrapAction(
                    stock_code=official.stock_code,
                    action_type="insert",
                    change_kinds=("new_official_baseline",),
                    old_instrument_name=None,
                    new_instrument_name=official.instrument_name,
                    old_exchange_id=None,
                    new_exchange_id=official.exchange_id,
                    planned_values={
                        "stock_code": official.stock_code,
                        "InstrumentName": official.instrument_name,
                        "ExchangeID": official.exchange_id,
                    },
                )
            )
            continue

        raw_old_name = existing.get("InstrumentName")
        old_name = None if raw_old_name is None else str(raw_old_name)
        raw_old_exchange = existing.get("ExchangeID")
        old_exchange = None if raw_old_exchange is None else str(raw_old_exchange)
        name_difference = old_name != official.instrument_name
        exchange_difference = (
            "" if old_exchange is None else old_exchange.strip().upper()
        ) != official.exchange_id
        corruption = corrupted_name_reason(old_name)
        change_kinds: list[str] = []
        planned_values: dict[str, Any] = {}

        if corruption is not None:
            change_kinds.append("corrupted_name_repair")
            planned_values["InstrumentName"] = official.instrument_name
        elif name_difference:
            change_kinds.append("verified_current_name_difference")
            planned_values["InstrumentName"] = official.instrument_name
            plan_changelog(
                official.stock_code,
                "InstrumentName",
                old_name,
                official.instrument_name,
            )
        if exchange_difference:
            change_kinds.append("exchange_id_difference")
            planned_values["ExchangeID"] = official.exchange_id
            plan_changelog(
                official.stock_code,
                "ExchangeID",
                old_exchange,
                official.exchange_id,
            )

        if corruption is not None:
            action_type = "corrupted_name_repair"
        elif change_kinds:
            action_type = "existing_fact_difference"
        else:
            action_type = "unchanged"
        actions.append(
            InstrumentBootstrapAction(
                stock_code=official.stock_code,
                action_type=action_type,
                change_kinds=tuple(change_kinds),
                old_instrument_name=old_name,
                new_instrument_name=official.instrument_name,
                old_exchange_id=old_exchange,
                new_exchange_id=official.exchange_id,
                planned_values=planned_values,
            )
        )

    actions_tuple = tuple(actions)
    insert_codes = tuple(
        item.stock_code for item in actions_tuple if item.action_type == "insert"
    )
    update_codes = tuple(
        item.stock_code
        for item in actions_tuple
        if item.action_type == "existing_fact_difference"
    )
    repair_codes = tuple(
        item.stock_code
        for item in actions_tuple
        if item.action_type == "corrupted_name_repair"
    )
    unchanged_codes = tuple(
        item.stock_code for item in actions_tuple if item.action_type == "unchanged"
    )
    change_exchange_count = sum(
        "exchange_id_difference" in item.change_kinds for item in actions_tuple
    )
    conservation_total = (
        len(insert_codes) + len(update_codes) + len(repair_codes) + len(unchanged_codes)
    )
    if conservation_total != universe.authoritative_unique_total:
        planning_blockers.append(
            f"plan conservation failed: actions={conservation_total}, "
            f"source={universe.authoritative_unique_total}"
        )
    for action in actions_tuple:
        allowed = set(INSTRUMENT_WRITE_FIELDS)
        if not set(action.planned_values).issubset(allowed):
            planning_blockers.append(
                f"action {action.stock_code} contains fields outside write whitelist"
            )

    sorted_changelog = tuple(
        sorted(
            changelog_entries,
            key=lambda item: (item.stock_code, item.changed_at, item.field_name),
        )
    )
    plan = InstrumentBootstrapPlan(
        plan_mode=PLAN_MODE,
        plan_date=plan_date,
        source_content_sha256=source_content_sha256,
        source_snapshot_sha256=source_snapshot_sha256,
        database_baseline_sha256=baseline_hash,
        plan_sha256="",
        source_total=universe.authoritative_unique_total,
        existing_total=len(existing_snapshot),
        would_insert_count=len(insert_codes),
        would_update_count=len(update_codes),
        would_repair_corrupted_name_count=len(repair_codes),
        would_change_exchange_count=change_exchange_count,
        would_unchanged_count=len(unchanged_codes),
        would_insert_changelog_count=len(sorted_changelog),
        would_delete_count=0,
        existing_not_in_official_count=len(existing_not_in_official),
        insert_codes=insert_codes,
        update_codes=update_codes,
        repair_codes=repair_codes,
        unchanged_codes=unchanged_codes,
        existing_not_in_official_codes=existing_not_in_official,
        actions=actions_tuple,
        changelog_entries=sorted_changelog,
        allowed_instrument_write_fields=INSTRUMENT_WRITE_FIELDS,
        allowed_changelog_fields=CHANGELOG_FIELDS,
        schema_compatible=universe.schema_inspection.compatible,
        universe_status=universe.universe_status,
        plan_status="PASS" if not planning_blockers else "FAIL",
        apply_allowed=False,
        future_apply_prerequisites=(CONTROLLED_APPLY_PREREQUISITE,),
        blockers=tuple(planning_blockers),
    )
    return replace(plan, plan_sha256=instrument_bootstrap_plan_sha256(plan))


__all__ = [
    "BootstrapReadSnapshot",
    "CHANGELOG_FIELDS",
    "CONTROLLED_APPLY_PREREQUISITE",
    "INSTRUMENT_WRITE_FIELDS",
    "InstrumentBootstrapAction",
    "InstrumentBootstrapError",
    "InstrumentBootstrapPlan",
    "InstrumentChangelogPlan",
    "PLAN_MODE",
    "build_instrument_bootstrap_plan",
    "corrupted_name_reason",
    "database_baseline_sha256",
    "instrument_bootstrap_plan_sha256",
    "read_bootstrap_inputs_from_cursor",
    "read_bootstrap_inputs_from_postgres",
]
