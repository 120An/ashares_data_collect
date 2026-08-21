"""Phase 1 PostgreSQL Entity persistence and sync boundary tests."""

from __future__ import annotations

from contextlib import redirect_stdout
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from io import StringIO
from pathlib import Path
import re
import subprocess
import sys
import unittest
from unittest.mock import patch

import manage_news_foundation
from data_collect.news_model.contracts import (
    ContractValidationError,
    EntityAliasType,
    EntityType,
    make_stock_entity_id,
)
from data_collect.news_model.entity_catalog import (
    ShadowInputRows,
    build_entity_catalog,
    make_entity_alias_id,
    normalize_entity_alias,
)
from data_collect.news_model import entity_persistence as persistence


_ROOT = Path(__file__).resolve().parents[1]
_SQL_PATH = _ROOT / "sql" / "012_create_news_entity_foundation.sql"
_TZ = timezone(timedelta(hours=8), "Asia/Shanghai")
_T1 = datetime(2026, 8, 20, 9, 0, tzinfo=_TZ)
_T2 = datetime(2026, 8, 21, 9, 0, tzinfo=_TZ)


def _instrument(name: str = "贵州茅台") -> dict:
    return {
        "stock_code": "600519.SH",
        "InstrumentName": name,
        "ExchangeID": "SH",
    }


def _snapshot(name: str = "贵州茅台", *, observed_at: datetime = _T1):
    return build_entity_catalog([_instrument(name)], observed_at=observed_at)


def _source_rows(name: str = "贵州茅台") -> ShadowInputRows:
    return ShadowInputRows(
        instrument_rows=(_instrument(name),),
        changelog_rows=(),
        sector_rows=(
            {
                "sector_name": "GICS1消费",
                "stock_code": "600519.SH",
                "update_date": "2026-08-21",
            },
        ),
        instrument_columns=("stock_code", "InstrumentName", "ExchangeID"),
    )


def _alias_state_after_plan(current_aliases, plan):
    latest = {alias.entity_alias_id: alias for alias in current_aliases}
    latest.update({alias.entity_alias_id: alias for alias in plan.alias_inserts})
    return tuple(latest[alias_id] for alias_id in sorted(latest))


def _effective_entity(current_entity, plan):
    return plan.entity_inserts[0] if plan.entity_inserts else current_entity


def _db_row(record, columns, confidence):
    return tuple(
        confidence if column == "confidence" else getattr(record, column)
        for column in columns
    )


class _TargetCursor:
    def __init__(
        self,
        *,
        entity_rows=(),
        alias_rows=(),
        fail_alias_insert=False,
        foundation_ready=True,
        entity_primary_key=("entity_id", "entity_revision"),
        alias_primary_key=("entity_alias_id", "revision"),
        latest_index_rows=None,
    ):
        self.entity_rows = list(entity_rows)
        self.alias_rows = list(alias_rows)
        self.fail_alias_insert = fail_alias_insert
        self.foundation_ready = foundation_ready
        self.entity_primary_key = entity_primary_key
        self.alias_primary_key = alias_primary_key
        self.latest_index_rows = latest_index_rows
        self.executed: list[tuple[str, tuple]] = []
        self._rows = []
        self.rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=()):
        normalized = " ".join(sql.split())
        self.executed.append((normalized, params))
        if "FROM information_schema.columns" in normalized:
            self._rows = [] if not self.foundation_ready else [
                    (persistence.ENTITY_TABLE, column)
                    for column in (*persistence.ENTITY_COLUMNS, "is_latest_revision")
                ] + [
                    (persistence.ENTITY_ALIAS_TABLE, column)
                    for column in (*persistence.ENTITY_ALIAS_COLUMNS, "is_latest_revision")
                ]
        elif "FROM information_schema.table_constraints" in normalized:
            self._rows = [] if not self.foundation_ready else [
                *(
                    (persistence.ENTITY_TABLE, column_name, ordinal_position)
                    for ordinal_position, column_name in enumerate(
                        self.entity_primary_key, start=1
                    )
                ),
                *(
                    (persistence.ENTITY_ALIAS_TABLE, column_name, ordinal_position)
                    for ordinal_position, column_name in enumerate(
                        self.alias_primary_key, start=1
                    )
                ),
            ]
        elif "FROM pg_index AS index_meta" in normalized:
            if not self.foundation_ready:
                self._rows = []
            elif self.latest_index_rows is not None:
                self._rows = list(self.latest_index_rows)
            else:
                self._rows = [
                    (
                        persistence.ENTITY_ALIAS_TABLE,
                        "uq_news_entity_alias_latest_revision",
                        True,
                        1,
                        1,
                        "entity_alias_id",
                        "is_latest_revision",
                    ),
                    (
                        persistence.ENTITY_TABLE,
                        "uq_news_entity_latest_revision",
                        True,
                        1,
                        1,
                        "entity_id",
                        "is_latest_revision",
                    ),
                ]
        elif normalized.startswith("SELECT") and persistence.ENTITY_ALIAS_TABLE in normalized:
            self._rows = self.alias_rows
        elif normalized.startswith("SELECT") and persistence.ENTITY_TABLE in normalized:
            self._rows = self.entity_rows
        else:
            self._rows = []
        if (
            self.fail_alias_insert
            and normalized.startswith(f"INSERT INTO {persistence.ENTITY_ALIAS_TABLE}")
        ):
            raise RuntimeError("forced alias insert failure")

    def fetchall(self):
        return list(self._rows)


class _TargetConnection:
    def __init__(self, cursor: _TargetCursor):
        self.cursor_instance = cursor
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def close(self):
        self.closed = True


class EntityMigrationAuditTests(unittest.TestCase):
    def test_sql_only_creates_new_foundation_objects(self):
        audit = persistence.audit_entity_migration_sql(_SQL_PATH)
        self.assertEqual(set(audit.table_names), set(persistence.FOUNDATION_TABLES))
        self.assertTrue(audit.contract_columns_match)
        sql = "\n".join(
            line.split("--", 1)[0]
            for line in _SQL_PATH.read_text(encoding="utf-8").splitlines()
        )
        self.assertNotRegex(
            sql,
            re.compile(r"\b(?:DROP|ALTER|INSERT|UPDATE|DELETE|TRUNCATE)\b", re.I),
        )
        for legacy_table in (
            "instrument_info", "instrument_changelog", "sector_stock", "sector_changelog"
        ):
            self.assertNotRegex(sql, re.compile(rf"\b{legacy_table}\b", re.I))

    def test_sql_is_repeat_safe_and_contract_exact(self):
        first = persistence.audit_entity_migration_sql(_SQL_PATH)
        second = persistence.audit_entity_migration_sql(_SQL_PATH)
        self.assertEqual(first, second)
        self.assertTrue(first.repeat_safe)
        self.assertEqual(first.statement_count, 9)
        self.assertEqual(first.index_count, 7)

    def test_local_migration_preflight_never_connects(self):
        output = StringIO()
        with (
            patch(
                "data_collect.news_model.entity_persistence.inspect_entity_foundation",
                side_effect=AssertionError("database must not be touched"),
            ),
            redirect_stdout(output),
        ):
            code = manage_news_foundation.main(["preflight-entity-migration"])
        self.assertEqual(code, 0)
        self.assertIn("no connection attempted", output.getvalue())

    def test_persistence_import_has_no_database_opensearch_or_network_side_effect(self):
        script = r'''
import builtins
import socket
real_import = builtins.__import__
def guarded_import(name, *args, **kwargs):
    lowered = name.lower()
    if any(token in lowered for token in ("psycopg", "opensearch", "requests")):
        raise AssertionError(f"forbidden import: {name}")
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded_import
socket.socket = lambda *a, **k: (_ for _ in ()).throw(AssertionError("network used"))
import data_collect.news_model.entity_persistence
'''
        result = subprocess.run(
            [sys.executable, "-B", "-c", script],
            cwd=_ROOT,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


class EntityFoundationReadinessTests(unittest.TestCase):
    @staticmethod
    def _inspect(**cursor_kwargs):
        cursor = _TargetCursor(**cursor_kwargs)
        connection = _TargetConnection(cursor)
        status = persistence.inspect_entity_foundation(
            connection_factory=lambda: connection
        )
        return status, connection, cursor

    def test_exact_foundation_structure_is_ready(self):
        status, connection, _ = self._inspect()
        self.assertTrue(status.ready, status.diagnostics)
        self.assertEqual(connection.rollback_count, 1)
        self.assertTrue(connection.closed)

    def test_entity_primary_key_columns_and_order_are_required(self):
        status, _, _ = self._inspect(
            entity_primary_key=("entity_revision", "entity_id")
        )
        self.assertFalse(status.ready)
        self.assertTrue(any(
            "news_entity_revision primary key mismatch" in diagnostic
            for diagnostic in status.diagnostics
        ))

    def test_alias_primary_key_columns_and_order_are_required(self):
        status, _, _ = self._inspect(
            alias_primary_key=("revision", "entity_alias_id")
        )
        self.assertFalse(status.ready)
        self.assertTrue(any(
            "news_entity_alias_revision primary key mismatch" in diagnostic
            for diagnostic in status.diagnostics
        ))

    def test_primary_key_missing_column_is_not_ready(self):
        status, _, _ = self._inspect(entity_primary_key=("entity_id",))
        self.assertFalse(status.ready)
        self.assertTrue(any(
            "expected=('entity_id', 'entity_revision'), actual=('entity_id',)"
            in diagnostic
            for diagnostic in status.diagnostics
        ))

    def test_primary_key_extra_column_is_not_ready(self):
        status, _, _ = self._inspect(
            alias_primary_key=("entity_alias_id", "revision", "unexpected_column")
        )
        self.assertFalse(status.ready)
        self.assertTrue(any(
            "actual=('entity_alias_id', 'revision', 'unexpected_column')"
            in diagnostic
            for diagnostic in status.diagnostics
        ))

    def test_latest_index_with_expected_name_must_be_unique(self):
        status, _, _ = self._inspect(latest_index_rows=[
            (
                persistence.ENTITY_TABLE,
                "uq_news_entity_latest_revision",
                False,
                1,
                1,
                "entity_id",
                "is_latest_revision",
            ),
            (
                persistence.ENTITY_ALIAS_TABLE,
                "uq_news_entity_alias_latest_revision",
                True,
                1,
                1,
                "entity_alias_id",
                "is_latest_revision",
            ),
        ])
        self.assertFalse(status.ready)
        self.assertTrue(any("index must be UNIQUE" in item for item in status.diagnostics))

    def test_latest_index_key_must_match(self):
        status, _, _ = self._inspect(latest_index_rows=[
            (
                persistence.ENTITY_TABLE,
                "uq_news_entity_latest_revision",
                True,
                1,
                1,
                "entity_revision",
                "is_latest_revision",
            ),
            (
                persistence.ENTITY_ALIAS_TABLE,
                "uq_news_entity_alias_latest_revision",
                True,
                1,
                1,
                "entity_alias_id",
                "is_latest_revision",
            ),
        ])
        self.assertFalse(status.ready)
        self.assertTrue(any(
            "key expected=entity_id" in item for item in status.diagnostics
        ))

    def test_latest_index_requires_partial_predicate(self):
        status, _, _ = self._inspect(latest_index_rows=[
            (
                persistence.ENTITY_TABLE,
                "uq_news_entity_latest_revision",
                True,
                1,
                1,
                "entity_id",
                None,
            ),
            (
                persistence.ENTITY_ALIAS_TABLE,
                "uq_news_entity_alias_latest_revision",
                True,
                1,
                1,
                "entity_alias_id",
                "is_latest_revision",
            ),
        ])
        self.assertFalse(status.ready)
        self.assertTrue(any(
            "predicate expected=is_latest_revision" in item
            for item in status.diagnostics
        ))

    def test_missing_latest_index_is_not_ready(self):
        status, _, _ = self._inspect(latest_index_rows=[
            (
                persistence.ENTITY_TABLE,
                "uq_news_entity_latest_revision",
                True,
                1,
                1,
                "entity_id",
                "is_latest_revision",
            ),
        ])
        self.assertFalse(status.ready)
        self.assertIn(
            "missing latest-revision index: uq_news_entity_alias_latest_revision",
            status.diagnostics,
        )

    def test_missing_tables_and_columns_are_not_ready(self):
        status, _, _ = self._inspect(foundation_ready=False)
        self.assertFalse(status.ready)
        self.assertTrue(any("missing table" in item for item in status.diagnostics))

    def test_inspection_executes_only_read_only_or_select_sql(self):
        status, connection, cursor = self._inspect()
        self.assertTrue(status.ready)
        self.assertEqual(connection.commit_count, 0)
        for sql, _ in cursor.executed:
            self.assertRegex(sql.upper(), r"^(SET TRANSACTION READ ONLY|SELECT )")


class EntitySyncPlanningTests(unittest.TestCase):
    def test_first_sync_inserts_one_stock_and_three_aliases(self):
        plan = persistence.plan_entity_sync(_snapshot(), observed_at=_T1)
        self.assertEqual(plan.inserted_entity_revisions, 1)
        self.assertEqual(plan.inserted_alias_revisions, 3)
        self.assertEqual(plan.entity_inserts[0].entity_type, EntityType.STOCK)
        self.assertEqual(plan.entity_inserts[0].entity_id, "ent_stock_600519_sh")

    def test_second_identical_sync_is_idempotent(self):
        snapshot = _snapshot()
        first = persistence.plan_entity_sync(snapshot, observed_at=_T1)
        second = persistence.plan_entity_sync(
            snapshot,
            first.entity_inserts,
            first.alias_inserts,
            observed_at=_T2,
        )
        self.assertEqual(second.inserted_entity_revisions, 0)
        self.assertEqual(second.inserted_alias_revisions, 0)
        self.assertEqual(second.entity_supersedes, ())
        self.assertEqual(second.alias_supersedes, ())

    def test_entity_and_alias_ids_remain_frozen(self):
        snapshot = _snapshot()
        entity = snapshot.stock_entities[0]
        self.assertEqual(entity.entity_id, make_stock_entity_id("600519.SH"))
        qualified = next(alias for alias in snapshot.aliases if alias.alias == "600519.SH")
        expected = make_entity_alias_id(
            entity.entity_id,
            qualified.alias_type,
            qualified.alias,
            fact_key="instrument_info:stock_code:qualified",
        )
        self.assertEqual(qualified.entity_alias_id, expected)

    def test_changed_facts_append_revisions_and_preserve_history(self):
        old = _snapshot("旧简称", observed_at=_T1)
        new = _snapshot("新简称", observed_at=_T2)
        plan = persistence.plan_entity_sync(
            new,
            old.entities,
            old.aliases,
            observed_at=_T2,
        )
        self.assertEqual(plan.entity_inserts[0].entity_revision, 2)
        self.assertEqual(plan.entity_supersedes[0].revision, 1)
        old_name = next(alias for alias in old.aliases if alias.alias == "旧简称")
        retired = next(
            alias
            for alias in plan.alias_inserts
            if alias.entity_alias_id == old_name.entity_alias_id
        )
        self.assertEqual(retired.revision, 2)
        self.assertFalse(retired.is_current)
        self.assertEqual(old_name.revision, 1)
        self.assertTrue(old_name.is_current)

    def test_manual_lock_is_never_overwritten(self):
        snapshot = _snapshot()
        locked = replace(snapshot.aliases[0], confidence=0.75, manual_lock=True)
        plan = persistence.plan_entity_sync(
            snapshot,
            snapshot.entities,
            (locked, *snapshot.aliases[1:]),
            observed_at=_T2,
        )
        self.assertIn(locked.entity_alias_id, plan.locked_alias_ids)
        self.assertNotIn(
            locked.entity_alias_id,
            {alias.entity_alias_id for alias in plan.alias_inserts},
        )
        self.assertNotIn(
            locked.entity_alias_id,
            {pointer.record_id for pointer in plan.alias_supersedes},
        )

    def test_locked_old_name_and_new_name_both_project_to_entity(self):
        old = _snapshot("旧简称", observed_at=_T1)
        new = _snapshot("新简称", observed_at=_T2)
        old_name = next(alias for alias in old.aliases if alias.alias == "旧简称")
        locked_old_name = replace(old_name, manual_lock=True)
        current_aliases = tuple(
            locked_old_name if alias.entity_alias_id == old_name.entity_alias_id else alias
            for alias in old.aliases
        )

        plan = persistence.plan_entity_sync(
            new,
            old.entities,
            current_aliases,
            observed_at=_T2,
        )
        final_aliases = _alias_state_after_plan(current_aliases, plan)
        final_values = tuple(sorted({
            alias.alias for alias in final_aliases if alias.is_current
        }))
        entity = _effective_entity(old.entities[0], plan)

        self.assertIn(old_name.entity_alias_id, plan.locked_alias_ids)
        self.assertIn("旧简称", final_values)
        self.assertIn("新简称", final_values)
        self.assertEqual(entity.aliases, final_values)

        second = persistence.plan_entity_sync(
            new,
            (entity,),
            final_aliases,
            observed_at=_T2,
        )
        self.assertEqual(second.inserted_entity_revisions, 0)
        self.assertEqual(second.inserted_alias_revisions, 0)

    def test_unlocked_old_name_retires_and_leaves_entity_projection(self):
        old = _snapshot("旧简称", observed_at=_T1)
        new = _snapshot("新简称", observed_at=_T2)
        old_name = next(alias for alias in old.aliases if alias.alias == "旧简称")

        plan = persistence.plan_entity_sync(
            new,
            old.entities,
            old.aliases,
            observed_at=_T2,
        )
        final_aliases = _alias_state_after_plan(old.aliases, plan)
        final_values = tuple(sorted({
            alias.alias for alias in final_aliases if alias.is_current
        }))
        entity = _effective_entity(old.entities[0], plan)
        retired = next(
            alias
            for alias in plan.alias_inserts
            if alias.entity_alias_id == old_name.entity_alias_id
        )

        self.assertFalse(retired.is_current)
        self.assertNotIn("旧简称", final_values)
        self.assertIn("新简称", final_values)
        self.assertEqual(entity.aliases, final_values)

    def test_locked_current_alias_missing_from_snapshot_remains_projected(self):
        snapshot = _snapshot()
        template = next(
            alias
            for alias in snapshot.aliases
            if alias.alias_type is EntityAliasType.STOCK_SHORT_NAME
        )
        extra_alias = replace(
            template,
            entity_alias_id=make_entity_alias_id(
                template.entity_id,
                EntityAliasType.OTHER,
                "人工保留名",
                fact_key="manual:retained-name:1",
            ),
            alias="人工保留名",
            normalized_alias=normalize_entity_alias("人工保留名"),
            alias_type=EntityAliasType.OTHER,
            manual_lock=True,
        )
        expected_values = tuple(sorted({
            *snapshot.entities[0].aliases,
            extra_alias.alias,
        }))
        current_entity = replace(snapshot.entities[0], aliases=expected_values)
        current_aliases = (*snapshot.aliases, extra_alias)

        plan = persistence.plan_entity_sync(
            snapshot,
            (current_entity,),
            current_aliases,
            observed_at=_T2,
        )
        final_aliases = _alias_state_after_plan(current_aliases, plan)
        entity = _effective_entity(current_entity, plan)

        self.assertIn(extra_alias.entity_alias_id, plan.locked_alias_ids)
        self.assertNotIn(
            extra_alias.entity_alias_id,
            {pointer.record_id for pointer in plan.alias_supersedes},
        )
        self.assertIn(extra_alias.alias, entity.aliases)
        self.assertEqual(
            entity.aliases,
            tuple(sorted({alias.alias for alias in final_aliases if alias.is_current})),
        )

    def test_historical_alias_is_excluded_from_entity_projection(self):
        snapshot = _snapshot()
        template = next(
            alias
            for alias in snapshot.aliases
            if alias.alias_type is EntityAliasType.STOCK_SHORT_NAME
        )
        historical = replace(
            template,
            entity_alias_id=make_entity_alias_id(
                template.entity_id,
                EntityAliasType.FORMER_NAME,
                "历史简称",
                fact_key="instrument_changelog:historical:test",
            ),
            alias="历史简称",
            normalized_alias=normalize_entity_alias("历史简称"),
            alias_type=EntityAliasType.FORMER_NAME,
            valid_to=_T1,
            is_current=False,
        )
        snapshot_with_history = replace(
            snapshot,
            entities=(replace(
                snapshot.entities[0],
                aliases=(*snapshot.entities[0].aliases, historical.alias),
            ),),
            aliases=(*snapshot.aliases, historical),
        )

        plan = persistence.plan_entity_sync(snapshot_with_history, observed_at=_T2)
        self.assertNotIn("历史简称", plan.entity_inserts[0].aliases)
        self.assertIn(historical.entity_alias_id, {
            alias.entity_alias_id for alias in plan.alias_inserts
        })

    def test_duplicate_alias_values_are_deduplicated_in_deterministic_order(self):
        snapshot = _snapshot()
        template = next(
            alias
            for alias in snapshot.aliases
            if alias.alias_type is EntityAliasType.STOCK_SHORT_NAME
        )
        duplicate = replace(
            template,
            entity_alias_id=make_entity_alias_id(
                template.entity_id,
                EntityAliasType.SHORT_NAME,
                template.alias,
                fact_key="instrument_info:duplicate-name:test",
            ),
            alias_type=EntityAliasType.SHORT_NAME,
        )
        aliases = (*snapshot.aliases, duplicate)
        forward = persistence.plan_entity_sync(
            replace(snapshot, aliases=aliases),
            observed_at=_T1,
        )
        reverse = persistence.plan_entity_sync(
            replace(snapshot, aliases=tuple(reversed(aliases))),
            observed_at=_T1,
        )

        projected = forward.entity_inserts[0].aliases
        self.assertEqual(projected, tuple(sorted(set(projected))))
        self.assertEqual(projected.count(template.alias), 1)
        self.assertEqual(projected, reverse.entity_inserts[0].aliases)

    def test_company_and_industry_entities_are_refused(self):
        snapshot = _snapshot()
        for entity_type in (EntityType.COMPANY, EntityType.INDUSTRY):
            with self.subTest(entity_type=entity_type):
                non_stock = replace(
                    snapshot.entities[0],
                    entity_type=entity_type,
                    stock_code=None,
                    exchange=None,
                )
                invalid_snapshot = replace(snapshot, entities=(non_stock,))
                with self.assertRaises(persistence.EntityPersistenceError):
                    persistence.plan_entity_sync(invalid_snapshot, observed_at=_T1)


class EntitySyncDatabaseBoundaryTests(unittest.TestCase):
    def _run(self, *, apply: bool, fail_alias_insert: bool = False):
        cursor = _TargetCursor(fail_alias_insert=fail_alias_insert)
        connection = _TargetConnection(cursor)
        with patch(
            "data_collect.news_model.entity_persistence.load_shadow_inputs_from_postgres",
            return_value=_source_rows(),
        ):
            result = persistence.synchronize_entities(
                apply=apply,
                observed_at=_T1,
                connection_factory=lambda: connection,
                json_adapter=lambda value: value,
            )
        return result, connection, cursor

    def test_decimal_confidence_is_decoded_for_entity_and_alias(self):
        snapshot = _snapshot()
        entity = snapshot.stock_entities[0]
        alias = snapshot.aliases[0]
        cursor = _TargetCursor(
            entity_rows=(
                _db_row(entity, persistence.ENTITY_COLUMNS, Decimal("1.00000")),
            ),
            alias_rows=(
                _db_row(alias, persistence.ENTITY_ALIAS_COLUMNS, Decimal("1.00000")),
            ),
        )

        entities, aliases = persistence._load_current_records(
            cursor, (entity.entity_id,)
        )

        self.assertEqual(entities[0].confidence, 1.0)
        self.assertIsInstance(entities[0].confidence, float)
        self.assertEqual(aliases[0].confidence, 1.0)
        self.assertIsInstance(aliases[0].confidence, float)

    def test_fractional_decimal_confidence_is_decoded_exactly_at_boundary(self):
        decoded = persistence._decode_db_record(
            ("confidence",), (Decimal("0.98000"),)
        )
        self.assertEqual(decoded["confidence"], 0.98)
        self.assertIsInstance(decoded["confidence"], float)

    def test_int_and_float_confidence_are_not_changed_by_db_decoder(self):
        integer = persistence._decode_db_record(("confidence",), (1,))
        floating = persistence._decode_db_record(("confidence",), (0.75,))
        self.assertIs(type(integer["confidence"]), int)
        self.assertIs(type(floating["confidence"]), float)

    def test_invalid_confidence_still_fails_closed_in_contract(self):
        entity = _snapshot().stock_entities[0]
        cursor = _TargetCursor(
            entity_rows=(
                _db_row(entity, persistence.ENTITY_COLUMNS, "1.0"),
            ),
        )
        with self.assertRaisesRegex(ContractValidationError, "0～1"):
            persistence._load_current_records(cursor, (entity.entity_id,))

    def test_dry_run_never_commits_or_executes_dml(self):
        result, connection, cursor = self._run(apply=False)
        self.assertFalse(result.applied)
        self.assertEqual(connection.commit_count, 0)
        self.assertGreaterEqual(connection.rollback_count, 1)
        for sql, _ in cursor.executed:
            self.assertRegex(sql.upper(), r"^(SET TRANSACTION READ ONLY|SELECT )")

    def test_apply_writes_only_two_foundation_tables(self):
        result, connection, cursor = self._run(apply=True)
        self.assertTrue(result.applied)
        self.assertEqual(connection.commit_count, 1)
        dml = [sql for sql, _ in cursor.executed if re.match(r"^(INSERT|UPDATE|DELETE)", sql, re.I)]
        self.assertTrue(dml)
        for sql in dml:
            self.assertRegex(
                sql,
                re.compile(
                    rf"^(?:INSERT INTO|UPDATE) "
                    rf"(?:{persistence.ENTITY_TABLE}|{persistence.ENTITY_ALIAS_TABLE})\b",
                    re.I,
                ),
            )
            for legacy_table in (
                "instrument_info", "instrument_changelog", "sector_stock", "sector_changelog"
            ):
                self.assertNotIn(legacy_table, sql.lower())

    def test_apply_failure_rolls_back_whole_transaction(self):
        cursor = _TargetCursor(fail_alias_insert=True)
        connection = _TargetConnection(cursor)
        with (
            patch(
                "data_collect.news_model.entity_persistence.load_shadow_inputs_from_postgres",
                return_value=_source_rows(),
            ),
            self.assertRaisesRegex(RuntimeError, "forced alias insert failure"),
        ):
            persistence.synchronize_entities(
                apply=True,
                observed_at=_T1,
                connection_factory=lambda: connection,
                json_adapter=lambda value: value,
            )
        self.assertEqual(connection.commit_count, 0)
        self.assertEqual(connection.rollback_count, 1)

    def test_apply_refuses_missing_foundation_without_dml(self):
        cursor = _TargetCursor(foundation_ready=False)
        connection = _TargetConnection(cursor)
        with (
            patch(
                "data_collect.news_model.entity_persistence.load_shadow_inputs_from_postgres",
                return_value=_source_rows(),
            ),
            self.assertRaises(persistence.EntityFoundationNotReadyError),
        ):
            persistence.synchronize_entities(
                apply=True,
                observed_at=_T1,
                connection_factory=lambda: connection,
                json_adapter=lambda value: value,
            )
        self.assertEqual(connection.commit_count, 0)
        self.assertEqual(connection.rollback_count, 1)
        self.assertFalse(any(
            re.match(r"^(INSERT|UPDATE|DELETE)", sql, re.I)
            for sql, _ in cursor.executed
        ))

    def test_sync_never_generates_company_industry_or_relation_dml(self):
        result, _, cursor = self._run(apply=True)
        self.assertEqual(result.plan.source_stock_count, 1)
        self.assertEqual(result.plan.sector_crosswalk_stock_count, 1)
        serialized_sql = " ".join(sql.lower() for sql, _ in cursor.executed)
        for forbidden in (
            "company_relation", "industry_relation", "stock_relation", "news_event"
        ):
            self.assertNotIn(forbidden, serialized_sql)

    def test_cli_defaults_to_dry_run(self):
        status = persistence.EntityFoundationStatus(
            ready=True,
            table_columns={},
            primary_key_tables=(),
            latest_revision_indexes=(),
            diagnostics=(),
        )
        plan = persistence.EntitySyncPlan(
            foundation_status=status,
            source_stock_count=1,
            sector_crosswalk_stock_count=1,
            entity_inserts=(),
            alias_inserts=(),
            entity_supersedes=(),
            alias_supersedes=(),
            locked_alias_ids=(),
        )
        output = StringIO()
        with (
            patch(
                "data_collect.news_model.entity_persistence.synchronize_entities",
                return_value=persistence.EntitySyncResult(plan=plan, applied=False),
            ) as sync,
            redirect_stdout(output),
        ):
            code = manage_news_foundation.main(["sync-entities"])
        self.assertEqual(code, 0)
        self.assertFalse(sync.call_args.kwargs["apply"])
        self.assertIn("mode: dry-run", output.getvalue())

    def test_cli_apply_requires_explicit_flag(self):
        status = persistence.EntityFoundationStatus(
            ready=True,
            table_columns={},
            primary_key_tables=(),
            latest_revision_indexes=(),
            diagnostics=(),
        )
        plan = persistence.EntitySyncPlan(
            foundation_status=status,
            source_stock_count=0,
            sector_crosswalk_stock_count=0,
            entity_inserts=(),
            alias_inserts=(),
            entity_supersedes=(),
            alias_supersedes=(),
            locked_alias_ids=(),
        )
        with patch(
            "data_collect.news_model.entity_persistence.synchronize_entities",
            return_value=persistence.EntitySyncResult(plan=plan, applied=True),
        ) as sync:
            code = manage_news_foundation.main(["sync-entities", "--apply"])
        self.assertEqual(code, 0)
        self.assertTrue(sync.call_args.kwargs["apply"])

    def test_cli_rejects_apply_with_limit_before_sync(self):
        output = StringIO()
        with (
            patch(
                "data_collect.news_model.entity_persistence.synchronize_entities",
                side_effect=AssertionError("sync must not be called"),
            ) as sync,
            redirect_stdout(output),
        ):
            code = manage_news_foundation.main(
                ["sync-entities", "--apply", "--limit", "1"]
            )
        self.assertNotEqual(code, 0)
        sync.assert_not_called()
        self.assertIn("--limit is dry-run only", output.getvalue())

    def test_direct_apply_with_limit_rejects_before_connection(self):
        connection_factory_called = False

        def connection_factory():
            nonlocal connection_factory_called
            connection_factory_called = True
            raise AssertionError("database connection must not be opened")

        with self.assertRaisesRegex(
            persistence.EntityPersistenceError,
            "--limit is dry-run only",
        ):
            persistence.synchronize_entities(
                apply=True,
                limit=1,
                connection_factory=connection_factory,
            )
        self.assertFalse(connection_factory_called)

    def test_cli_dry_run_with_limit_is_allowed(self):
        status = persistence.EntityFoundationStatus(
            ready=True,
            table_columns={},
            primary_key_tables=(),
            latest_revision_indexes=(),
            diagnostics=(),
        )
        plan = persistence.EntitySyncPlan(
            foundation_status=status,
            source_stock_count=1,
            sector_crosswalk_stock_count=1,
            entity_inserts=(),
            alias_inserts=(),
            entity_supersedes=(),
            alias_supersedes=(),
            locked_alias_ids=(),
        )
        for command in (
            ["sync-entities", "--dry-run", "--limit", "1"],
            ["sync-entities", "--limit", "1"],
        ):
            with self.subTest(command=command), patch(
                "data_collect.news_model.entity_persistence.synchronize_entities",
                return_value=persistence.EntitySyncResult(plan=plan, applied=False),
            ) as sync:
                code = manage_news_foundation.main(command)
                self.assertEqual(code, 0)
                self.assertFalse(sync.call_args.kwargs["apply"])
                self.assertEqual(sync.call_args.kwargs["limit"], 1)


if __name__ == "__main__":
    unittest.main()
