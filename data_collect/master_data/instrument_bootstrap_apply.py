"""Explicit transactional apply boundary for an audited instrument bootstrap plan.

This module never runs implicitly.  A write requires a fresh validated snapshot
and the exact deterministic plan SHA-256 emitted by a preceding dry-run.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date
import hmac
from pathlib import Path
import re
from typing import Any

from data_collect.master_data.instrument_bootstrap import (
    CHANGELOG_FIELDS,
    INSTRUMENT_WRITE_FIELDS,
    InstrumentBootstrapAction,
    InstrumentBootstrapPlan,
    build_instrument_bootstrap_plan,
    read_bootstrap_inputs_from_cursor,
)
from data_collect.master_data.official_snapshot import load_official_snapshot
from data_collect.master_data.public_instruments import (
    InstrumentSchemaInspection,
    inspect_instrument_info_schema,
)


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UPDATE_FIELDS = frozenset({"InstrumentName", "ExchangeID"})


class InstrumentBootstrapApplyError(RuntimeError):
    """Base fail-closed apply error; every raised instance is uncommitted."""

    committed = False


class ApplyConfigurationError(InstrumentBootstrapApplyError):
    """Required explicit apply authority was absent or malformed."""


class PlanChangedSinceDryRun(InstrumentBootstrapApplyError):
    """The protected database baseline no longer produces the reviewed plan."""


class PostWriteVerificationError(InstrumentBootstrapApplyError):
    """DML results did not exactly reproduce the authoritative snapshot."""


@dataclass(frozen=True)
class InstrumentBootstrapApplyResult:
    content_sha256: str
    snapshot_sha256: str
    database_baseline_sha256: str
    plan_sha256: str
    source_total: int
    inserted_count: int
    updated_count: int
    repaired_count: int
    changelog_inserted_count: int
    deleted_count: int
    post_total: int
    missing_count: int
    mismatch_count: int
    committed: bool


def _require_apply_authority(
    snapshot_path: str | Path | None,
    expected_plan_sha256: str | None,
) -> tuple[Path, str]:
    if snapshot_path is None or not str(snapshot_path).strip():
        raise ApplyConfigurationError("--snapshot is required for controlled apply")
    if expected_plan_sha256 is None or not _SHA256_RE.fullmatch(expected_plan_sha256):
        raise ApplyConfigurationError(
            "--expect-plan-sha256 must be an exact lowercase SHA-256"
        )
    return Path(snapshot_path), expected_plan_sha256


def _assert_plan_is_writable(
    plan: InstrumentBootstrapPlan,
    *,
    content_sha256: str,
    snapshot_sha256: str,
) -> None:
    blockers: list[str] = []
    if plan.plan_status != "PASS":
        blockers.append("plan_status is not PASS")
    if plan.source_snapshot_sha256 != snapshot_sha256:
        blockers.append("plan is not bound to the validated snapshot SHA-256")
    if plan.source_content_sha256 != content_sha256:
        blockers.append("plan is not bound to the validated content SHA-256")
    if plan.source_total < 3_000:
        blockers.append("source_total is below 3000")
    if plan.would_delete_count != 0:
        blockers.append("plan contains deletes")
    if plan.allowed_instrument_write_fields != INSTRUMENT_WRITE_FIELDS:
        blockers.append("instrument write whitelist differs from frozen bootstrap fields")
    if plan.allowed_changelog_fields != CHANGELOG_FIELDS:
        blockers.append("changelog write whitelist differs from frozen legacy fields")
    for action in plan.actions:
        allowed = (
            set(INSTRUMENT_WRITE_FIELDS)
            if action.action_type == "insert"
            else _UPDATE_FIELDS
        )
        if not set(action.planned_values).issubset(allowed):
            blockers.append(f"{action.stock_code} contains non-whitelisted fields")
    if blockers:
        raise InstrumentBootstrapApplyError("; ".join(blockers))


def _insert_instruments(cursor: Any, actions: tuple[InstrumentBootstrapAction, ...]) -> int:
    inserts = tuple(action for action in actions if action.action_type == "insert")
    if not inserts:
        return 0
    codes = [action.stock_code for action in inserts]
    names = [action.new_instrument_name for action in inserts]
    exchanges = [action.new_exchange_id for action in inserts]
    cursor.execute(
        """
        INSERT INTO "instrument_info" ("stock_code", "InstrumentName", "ExchangeID")
        SELECT source.stock_code, source.instrument_name, source.exchange_id
        FROM UNNEST(%s::text[], %s::text[], %s::text[])
             AS source(stock_code, instrument_name, exchange_id)
        RETURNING "stock_code"
        """,
        (codes, names, exchanges),
    )
    returned = tuple(str(row[0]) for row in cursor.fetchall())
    if len(returned) != len(inserts) or set(returned) != set(codes):
        raise PostWriteVerificationError(
            f"instrument INSERT count/identity mismatch: expected={len(inserts)}, "
            f"actual={len(returned)}"
        )
    return len(returned)


def _update_instruments(cursor: Any, actions: tuple[InstrumentBootstrapAction, ...]) -> int:
    updates = tuple(
        action
        for action in actions
        if action.action_type in {"existing_fact_difference", "corrupted_name_repair"}
    )
    affected = 0
    for action in updates:
        values = dict(action.planned_values)
        if not values or not set(values).issubset(_UPDATE_FIELDS):
            raise InstrumentBootstrapApplyError(
                f"{action.stock_code} update has invalid planned_values"
            )
        assignments = []
        parameters: list[Any] = []
        for field_name in ("InstrumentName", "ExchangeID"):
            if field_name in values:
                assignments.append(f'"{field_name}" = %s')
                parameters.append(values[field_name])
        parameters.append(action.stock_code)
        cursor.execute(
            f'UPDATE "instrument_info" SET {", ".join(assignments)} '
            'WHERE "stock_code" = %s',
            tuple(parameters),
        )
        if cursor.rowcount != 1:
            raise PostWriteVerificationError(
                f"instrument UPDATE count mismatch for {action.stock_code}: "
                f"actual={cursor.rowcount}"
            )
        affected += 1
    return affected


def _insert_changelog(cursor: Any, plan: InstrumentBootstrapPlan) -> int:
    entries = plan.changelog_entries
    if not entries:
        return 0
    cursor.execute(
        """
        INSERT INTO "instrument_changelog"
            ("stock_code", "changed_at", "field_name", "old_value", "new_value")
        SELECT source.stock_code, source.changed_at, source.field_name,
               source.old_value, source.new_value
        FROM UNNEST(%s::text[], %s::date[], %s::text[], %s::text[], %s::text[])
             AS source(stock_code, changed_at, field_name, old_value, new_value)
        RETURNING "stock_code", "changed_at", "field_name"
        """,
        (
            [entry.stock_code for entry in entries],
            [entry.changed_at for entry in entries],
            [entry.field_name for entry in entries],
            [entry.old_value for entry in entries],
            [entry.new_value for entry in entries],
        ),
    )
    returned = tuple(cursor.fetchall())
    returned_keys = {
        (str(row[0]), row[1], str(row[2]))
        for row in returned
    }
    expected_keys = {
        (entry.stock_code, entry.changed_at, entry.field_name)
        for entry in entries
    }
    if len(returned) != len(entries) or returned_keys != expected_keys:
        raise PostWriteVerificationError(
            f"changelog INSERT count/identity mismatch: expected={len(entries)}, "
            f"actual={len(returned)}"
        )
    return len(returned)


def _post_write_verify(cursor: Any, plan: InstrumentBootstrapPlan, universe: Any) -> tuple[int, int, int]:
    expected = {
        record.stock_code: (record.instrument_name, record.exchange_id)
        for record in universe.records
    }
    cursor.execute(
        """
        SELECT "stock_code", "InstrumentName", "ExchangeID"
        FROM "instrument_info"
        WHERE "stock_code" = ANY(%s)
        ORDER BY "stock_code"
        """,
        (list(expected),),
    )
    rows = tuple(cursor.fetchall())
    observed: dict[str, tuple[Any, Any]] = {}
    duplicate_count = 0
    for stock_code, instrument_name, exchange_id in rows:
        code = str(stock_code)
        if code in observed:
            duplicate_count += 1
        observed[code] = (instrument_name, exchange_id)
    missing = tuple(sorted(set(expected) - set(observed)))
    mismatches = tuple(
        code
        for code in sorted(expected)
        if code in observed and observed[code] != expected[code]
    )
    if missing or mismatches or duplicate_count:
        raise PostWriteVerificationError(
            "post-write authoritative coverage mismatch: "
            f"missing={len(missing)}, mismatches={len(mismatches)}, "
            f"duplicates={duplicate_count}"
        )
    cursor.execute('SELECT COUNT(*) FROM "instrument_info"')
    row = cursor.fetchone()
    if row is None:
        raise PostWriteVerificationError("post-write instrument_info count is unavailable")
    post_total = int(row[0])
    expected_post_total = plan.source_total + plan.existing_not_in_official_count
    if post_total != expected_post_total:
        raise PostWriteVerificationError(
            f"post-write total mismatch: expected={expected_post_total}, "
            f"actual={post_total}"
        )
    return post_total, len(missing), len(mismatches) + duplicate_count


def apply_instrument_bootstrap_plan(
    *,
    snapshot_path: str | Path | None,
    expected_plan_sha256: str | None,
    plan_date: date | None = None,
    connection_factory: Callable[[], Any] | None = None,
    schema_inspector: Callable[[], InstrumentSchemaInspection] | None = None,
) -> InstrumentBootstrapApplyResult:
    """Rebuild, verify and apply exactly one reviewed plan in one locked transaction."""

    snapshot_file, expected_hash = _require_apply_authority(
        snapshot_path, expected_plan_sha256
    )
    validated = load_official_snapshot(snapshot_file)
    inspect_schema = schema_inspector or inspect_instrument_info_schema
    schema = inspect_schema()
    if not schema.inspected or not schema.compatible:
        raise InstrumentBootstrapApplyError("instrument_info schema is not compatible")
    if connection_factory is None:
        from data_collect.utils.db import get_connection

        connection_factory = get_connection

    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            cursor.execute(
                'LOCK TABLE "instrument_info", "instrument_changelog" '
                "IN SHARE ROW EXCLUSIVE MODE"
            )
            baseline = read_bootstrap_inputs_from_cursor(cursor)
            universe = replace(validated.universe, schema_inspection=schema)
            plan = build_instrument_bootstrap_plan(
                universe,
                baseline.existing_instruments,
                baseline.relevant_changelog,
                plan_date=plan_date or date.today(),
                source_content_sha256=validated.content_sha256,
                source_snapshot_sha256=validated.snapshot_sha256,
            )
            _assert_plan_is_writable(
                plan,
                content_sha256=validated.content_sha256,
                snapshot_sha256=validated.snapshot_sha256,
            )
            if not hmac.compare_digest(plan.plan_sha256, expected_hash):
                raise PlanChangedSinceDryRun(
                    "PLAN_CHANGED_SINCE_DRY_RUN: expected plan SHA-256 does not match "
                    "the locked current database baseline"
                )

            inserted = _insert_instruments(cursor, plan.actions)
            updated_total = _update_instruments(cursor, plan.actions)
            changelog_inserted = _insert_changelog(cursor, plan)
            expected_updated_total = (
                plan.would_update_count + plan.would_repair_corrupted_name_count
            )
            if inserted != plan.would_insert_count:
                raise PostWriteVerificationError("instrument INSERT total differs from plan")
            if updated_total != expected_updated_total:
                raise PostWriteVerificationError("instrument UPDATE total differs from plan")
            if changelog_inserted != plan.would_insert_changelog_count:
                raise PostWriteVerificationError("changelog INSERT total differs from plan")
            post_total, missing_count, mismatch_count = _post_write_verify(
                cursor, plan, universe
            )
        connection.commit()
        return InstrumentBootstrapApplyResult(
            content_sha256=validated.content_sha256,
            snapshot_sha256=validated.snapshot_sha256,
            database_baseline_sha256=plan.database_baseline_sha256,
            plan_sha256=plan.plan_sha256,
            source_total=plan.source_total,
            inserted_count=inserted,
            updated_count=plan.would_update_count,
            repaired_count=plan.would_repair_corrupted_name_count,
            changelog_inserted_count=changelog_inserted,
            deleted_count=0,
            post_total=post_total,
            missing_count=missing_count,
            mismatch_count=mismatch_count,
            committed=True,
        )
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


__all__ = [
    "ApplyConfigurationError",
    "InstrumentBootstrapApplyError",
    "InstrumentBootstrapApplyResult",
    "PlanChangedSinceDryRun",
    "PostWriteVerificationError",
    "apply_instrument_bootstrap_plan",
]
