from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
import importlib
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest import mock

import manage_master_data
from data_collect.master_data.instrument_bootstrap import build_instrument_bootstrap_plan
from data_collect.master_data.instrument_bootstrap_apply import (
    ApplyConfigurationError,
    InstrumentBootstrapApplyError,
    InstrumentBootstrapApplyResult,
    PlanChangedSinceDryRun,
    PostWriteVerificationError,
    apply_instrument_bootstrap_plan,
)
from data_collect.master_data.official_snapshot import (
    SnapshotValidationError,
    capture_official_snapshot,
    load_official_snapshot,
)
from data_collect.master_data.public_instruments import InstrumentSchemaInspection
from tests.test_instrument_bootstrap_plan import _universe


PLAN_DATE = date(2026, 8, 22)


def _schema():
    return InstrumentSchemaInspection(
        inspected=True,
        columns=("stock_code", "InstrumentName", "ExchangeID"),
        compatible=True,
    )


@dataclass
class _DatabaseState:
    instruments: dict[str, dict]
    changelog: list[dict]


def _state(rows=(), changelog=()):
    return _DatabaseState(
        instruments={
            str(row["stock_code"]): {
                "InstrumentName": row.get("InstrumentName"),
                "ExchangeID": row.get("ExchangeID"),
            }
            for row in rows
        },
        changelog=[dict(row) for row in changelog],
    )


def _full_state():
    return _state(
        {
            "stock_code": record.stock_code,
            "InstrumentName": record.instrument_name,
            "ExchangeID": record.exchange_id,
        }
        for record in _universe().records
    )


def _baseline_rows(state):
    return tuple(
        {
            "stock_code": code,
            "InstrumentName": values["InstrumentName"],
            "ExchangeID": values["ExchangeID"],
        }
        for code, values in sorted(state.instruments.items())
    )


class _ApplyCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        upper = normalized.upper()
        self.connection.statements.append((normalized, params))
        self.connection.events.append(f"SQL:{upper.split()[0]}")
        self.rows = []
        self.rowcount = -1
        state = self.connection.state

        if upper.startswith("SET TRANSACTION") or upper.startswith("LOCK TABLE"):
            return
        if (
            upper.startswith('SELECT "STOCK_CODE", "INSTRUMENTNAME", "EXCHANGEID"')
            and "WHERE" not in upper
        ):
            self.rows = [
                (code, values["InstrumentName"], values["ExchangeID"])
                for code, values in sorted(state.instruments.items())
            ]
            return
        if upper.startswith('SELECT "STOCK_CODE", "CHANGED_AT"'):
            self.rows = [
                (
                    row["stock_code"],
                    row["changed_at"],
                    row["field_name"],
                    row["old_value"],
                    row["new_value"],
                )
                for row in sorted(
                    state.changelog,
                    key=lambda item: (
                        item["stock_code"],
                        item["changed_at"],
                        item["field_name"],
                    ),
                )
                if row["field_name"] in {"InstrumentName", "ExchangeID"}
            ]
            return
        if upper.startswith('INSERT INTO "INSTRUMENT_INFO"'):
            if self.connection.fail_on == "insert":
                raise RuntimeError("injected instrument INSERT failure")
            codes, names, exchanges = params
            for code, name, exchange in zip(codes, names, exchanges, strict=True):
                if code in state.instruments:
                    raise RuntimeError("duplicate instrument")
                state.instruments[code] = {
                    "InstrumentName": name,
                    "ExchangeID": exchange,
                }
            self.rows = [(code,) for code in codes]
            self.rowcount = len(codes)
            return
        if upper.startswith('UPDATE "INSTRUMENT_INFO"'):
            if self.connection.fail_on == "update":
                raise RuntimeError("injected instrument UPDATE failure")
            code = params[-1]
            if code not in state.instruments:
                self.rowcount = 0
                return
            position = 0
            if '"INSTRUMENTNAME" = %S' in upper:
                state.instruments[code]["InstrumentName"] = params[position]
                position += 1
            if '"EXCHANGEID" = %S' in upper:
                state.instruments[code]["ExchangeID"] = params[position]
            self.rowcount = 1
            return
        if upper.startswith('INSERT INTO "INSTRUMENT_CHANGELOG"'):
            if self.connection.fail_on == "changelog":
                raise RuntimeError("injected changelog INSERT failure")
            codes, dates, fields, old_values, new_values = params
            for values in zip(
                codes, dates, fields, old_values, new_values, strict=True
            ):
                code, changed_at, field_name, old_value, new_value = values
                state.changelog.append(
                    {
                        "stock_code": code,
                        "changed_at": changed_at,
                        "field_name": field_name,
                        "old_value": old_value,
                        "new_value": new_value,
                    }
                )
            self.rows = [
                (code, changed_at, field_name)
                for code, changed_at, field_name in zip(
                    codes, dates, fields, strict=True
                )
            ]
            self.rowcount = len(self.rows)
            return
        if (
            upper.startswith('SELECT "STOCK_CODE", "INSTRUMENTNAME", "EXCHANGEID"')
            and "ANY" in upper
        ):
            if self.connection.fail_on == "post_select":
                raise RuntimeError("injected post-write SELECT failure")
            codes = params[0]
            self.rows = [
                (code, state.instruments[code]["InstrumentName"], state.instruments[code]["ExchangeID"])
                for code in codes
                if code in state.instruments
            ]
            if self.rows and self.connection.fail_on == "post_name_mismatch":
                first = self.rows[0]
                self.rows[0] = (first[0], "错误名称", first[2])
            if self.rows and self.connection.fail_on == "post_exchange_mismatch":
                first = self.rows[0]
                self.rows[0] = (first[0], first[1], "XX")
            if self.rows and self.connection.fail_on == "post_missing":
                self.rows.pop()
            return
        if upper.startswith('SELECT COUNT(*) FROM "INSTRUMENT_INFO"'):
            count = len(state.instruments)
            if self.connection.fail_on == "post_count_low":
                count -= 1
            elif self.connection.fail_on == "post_count_high":
                count += 1
            self.rows = [(count,)]
            return
        raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None


class _ApplyConnection:
    def __init__(self, state, *, fail_on=None):
        self.state = state
        self.original_instruments = deepcopy(state.instruments)
        self.original_changelog = deepcopy(state.changelog)
        self.fail_on = fail_on
        self.statements = []
        self.events = []
        self.commit_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0

    def cursor(self):
        return _ApplyCursor(self)

    def commit(self):
        self.events.append("COMMIT")
        self.commit_calls += 1

    def rollback(self):
        self.events.append("ROLLBACK")
        self.rollback_calls += 1
        self.state.instruments.clear()
        self.state.instruments.update(deepcopy(self.original_instruments))
        self.state.changelog[:] = deepcopy(self.original_changelog)

    def close(self):
        self.events.append("CLOSE")
        self.close_calls += 1


class InstrumentBootstrapApplyTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.capture = capture_official_snapshot(
            _universe(), self.tempdir.name, fetched_at=datetime.now().astimezone()
        )
        self.loaded = load_official_snapshot(self.capture.path)

    def tearDown(self):
        self.tempdir.cleanup()

    def _plan(self, state):
        universe = replace(self.loaded.universe, schema_inspection=_schema())
        return build_instrument_bootstrap_plan(
            universe,
            _baseline_rows(state),
            tuple(state.changelog),
            plan_date=PLAN_DATE,
            source_content_sha256=self.loaded.content_sha256,
            source_snapshot_sha256=self.loaded.snapshot_sha256,
        )

    def _apply(self, state, expected_hash, *, fail_on=None):
        connection = _ApplyConnection(state, fail_on=fail_on)
        result = apply_instrument_bootstrap_plan(
            snapshot_path=self.capture.path,
            expected_plan_sha256=expected_hash,
            plan_date=PLAN_DATE,
            connection_factory=lambda: connection,
            schema_inspector=_schema,
        )
        return result, connection

    def test_apply_without_snapshot_rejects_before_database(self):
        connection_factory = mock.Mock()
        with self.assertRaisesRegex(ApplyConfigurationError, "--snapshot"):
            apply_instrument_bootstrap_plan(
                snapshot_path=None,
                expected_plan_sha256="0" * 64,
                connection_factory=connection_factory,
            )
        connection_factory.assert_not_called()

    def test_apply_without_expected_hash_rejects_before_database(self):
        connection_factory = mock.Mock()
        with self.assertRaisesRegex(ApplyConfigurationError, "expect-plan"):
            apply_instrument_bootstrap_plan(
                snapshot_path=self.capture.path,
                expected_plan_sha256=None,
                connection_factory=connection_factory,
            )
        connection_factory.assert_not_called()

    def test_stale_snapshot_rejects_before_database(self):
        stale = capture_official_snapshot(
            _universe(),
            self.tempdir.name,
            fetched_at=datetime.now().astimezone() - timedelta(hours=25),
        )
        connection_factory = mock.Mock()
        with self.assertRaisesRegex(SnapshotValidationError, "STALE"):
            apply_instrument_bootstrap_plan(
                snapshot_path=stale.path,
                expected_plan_sha256="0" * 64,
                connection_factory=connection_factory,
            )
        connection_factory.assert_not_called()

    def test_invalid_snapshot_hash_rejects_before_database(self):
        payload = json.loads(self.capture.path.read_text(encoding="utf-8"))
        payload["records"][0]["InstrumentName"] = "篡改"
        self.capture.path.write_text(json.dumps(payload), encoding="utf-8")
        connection_factory = mock.Mock()
        with self.assertRaisesRegex(SnapshotValidationError, "sha256 mismatch"):
            apply_instrument_bootstrap_plan(
                snapshot_path=self.capture.path,
                expected_plan_sha256="0" * 64,
                connection_factory=connection_factory,
            )
        connection_factory.assert_not_called()

    def test_incompatible_schema_rejects_before_write_transaction(self):
        connection_factory = mock.Mock()
        with self.assertRaisesRegex(
            InstrumentBootstrapApplyError, "schema is not compatible"
        ):
            apply_instrument_bootstrap_plan(
                snapshot_path=self.capture.path,
                expected_plan_sha256="0" * 64,
                connection_factory=connection_factory,
                schema_inspector=lambda: InstrumentSchemaInspection(
                    inspected=True, compatible=False
                ),
            )
        connection_factory.assert_not_called()

    def test_plan_hash_mismatch_performs_zero_dml_and_rolls_back(self):
        state = _state()
        connection = _ApplyConnection(state)
        with self.assertRaisesRegex(PlanChangedSinceDryRun, "PLAN_CHANGED"):
            apply_instrument_bootstrap_plan(
                snapshot_path=self.capture.path,
                expected_plan_sha256="0" * 64,
                plan_date=PLAN_DATE,
                connection_factory=lambda: connection,
                schema_inspector=_schema,
            )
        self.assertFalse(state.instruments)
        self.assertEqual(connection.rollback_calls, 1)
        self.assertFalse(any(sql.upper().startswith(("INSERT", "UPDATE")) for sql, _ in connection.statements))

    def test_plan_hash_binds_specific_capture_even_when_content_is_unchanged(self):
        state = _state()
        reviewed = self._plan(state)
        second_capture = capture_official_snapshot(
            _universe(),
            self.tempdir.name,
            fetched_at=datetime.now().astimezone() + timedelta(seconds=1),
        )
        self.assertEqual(second_capture.content_sha256, self.capture.content_sha256)
        self.assertNotEqual(second_capture.snapshot_sha256, self.capture.snapshot_sha256)
        connection = _ApplyConnection(state)
        with self.assertRaises(PlanChangedSinceDryRun):
            apply_instrument_bootstrap_plan(
                snapshot_path=second_capture.path,
                expected_plan_sha256=reviewed.plan_sha256,
                plan_date=PLAN_DATE,
                connection_factory=lambda: connection,
                schema_inspector=_schema,
            )
        self.assertEqual(connection.rollback_calls, 1)
        self.assertFalse(
            any(
                sql.upper().startswith(("INSERT", "UPDATE"))
                for sql, _params in connection.statements
            )
        )

    def test_database_change_after_dry_run_changes_plan_hash(self):
        state = _state()
        reviewed = self._plan(state)
        state.instruments["601999.SH"] = {
            "InstrumentName": "额外证券",
            "ExchangeID": "SH",
        }
        connection = _ApplyConnection(state)
        with self.assertRaises(PlanChangedSinceDryRun):
            apply_instrument_bootstrap_plan(
                snapshot_path=self.capture.path,
                expected_plan_sha256=reviewed.plan_sha256,
                plan_date=PLAN_DATE,
                connection_factory=lambda: connection,
                schema_inspector=_schema,
            )
        self.assertIn("601999.SH", state.instruments)

    def test_empty_database_insert_baseline_and_post_verify(self):
        state = _state()
        plan = self._plan(state)
        result, connection = self._apply(state, plan.plan_sha256)
        self.assertEqual(result.inserted_count, 3_000)
        self.assertEqual(result.updated_count, 0)
        self.assertEqual(result.repaired_count, 0)
        self.assertTrue(result.committed)
        self.assertEqual(len(state.instruments), 3_000)
        self.assertEqual(connection.commit_calls, 1)
        self.assertEqual(connection.rollback_calls, 0)

    def test_corrupted_name_repair_has_no_changelog(self):
        state = _full_state()
        state.instruments["000001.SZ"]["InstrumentName"] = "????"
        plan = self._plan(state)
        result, _connection = self._apply(state, plan.plan_sha256)
        self.assertEqual(result.repaired_count, 1)
        self.assertEqual(result.changelog_inserted_count, 0)
        self.assertEqual(state.instruments["000001.SZ"]["InstrumentName"], "平安银行")
        self.assertFalse(state.changelog)

    def test_normal_fact_change_creates_legacy_changelog(self):
        state = _full_state()
        state.instruments["000001.SZ"]["InstrumentName"] = "正常旧简称"
        plan = self._plan(state)
        result, _connection = self._apply(state, plan.plan_sha256)
        self.assertEqual(result.updated_count, 1)
        self.assertEqual(result.repaired_count, 0)
        self.assertEqual(result.changelog_inserted_count, 1)
        self.assertEqual(state.changelog[0]["field_name"], "InstrumentName")

    def test_exact_unchanged_plan_performs_no_dml(self):
        state = _full_state()
        plan = self._plan(state)
        result, connection = self._apply(state, plan.plan_sha256)
        self.assertEqual(result.inserted_count + result.updated_count + result.repaired_count, 0)
        dml = [sql for sql, _ in connection.statements if sql.upper().startswith(("INSERT", "UPDATE"))]
        self.assertFalse(dml)

    def test_existing_extra_is_retained(self):
        state = _full_state()
        state.instruments["601999.SH"] = {
            "InstrumentName": "额外证券",
            "ExchangeID": "SH",
        }
        plan = self._plan(state)
        result, _connection = self._apply(state, plan.plan_sha256)
        self.assertIn("601999.SH", state.instruments)
        self.assertEqual(result.deleted_count, 0)
        self.assertEqual(result.post_total, 3_001)

    def test_sql_never_deletes_or_touches_sector_or_entity(self):
        state = _state()
        plan = self._plan(state)
        _result, connection = self._apply(state, plan.plan_sha256)
        sql = " ".join(statement for statement, _ in connection.statements).upper()
        self.assertNotIn("DELETE", sql)
        self.assertNotIn("TRUNCATE", sql)
        self.assertNotIn("SECTOR_STOCK", sql)
        self.assertNotIn("NEWS_ENTITY", sql)

    def test_instrument_dml_uses_only_frozen_whitelist(self):
        state = _state()
        plan = self._plan(state)
        _result, connection = self._apply(state, plan.plan_sha256)
        insert_sql = next(
            sql for sql, _ in connection.statements if sql.upper().startswith("INSERT INTO \"INSTRUMENT_INFO\"")
        )
        self.assertIn('("stock_code", "InstrumentName", "ExchangeID")', insert_sql)
        self.assertNotIn("updated_at", insert_sql)

    def test_one_serializable_locked_transaction(self):
        state = _state()
        plan = self._plan(state)
        _result, connection = self._apply(state, plan.plan_sha256)
        statements = [sql.upper() for sql, _ in connection.statements]
        self.assertTrue(statements[0].startswith("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))
        self.assertTrue(statements[1].startswith("LOCK TABLE"))
        self.assertIn("SHARE ROW EXCLUSIVE", statements[1])
        self.assertEqual(connection.commit_calls, 1)
        self.assertEqual(connection.close_calls, 1)

    def test_insert_failure_rolls_back(self):
        self._assert_failure_rolls_back(_state(), "insert", RuntimeError)

    def test_repair_update_failure_rolls_back(self):
        state = _full_state()
        state.instruments["000001.SZ"]["InstrumentName"] = "????"
        self._assert_failure_rolls_back(state, "update", RuntimeError)
        self.assertEqual(state.instruments["000001.SZ"]["InstrumentName"], "????")

    def test_changelog_insert_failure_rolls_back(self):
        state = _full_state()
        state.instruments["000001.SZ"]["InstrumentName"] = "正常旧简称"
        self._assert_failure_rolls_back(state, "changelog", RuntimeError)
        self.assertFalse(state.changelog)

    def _assert_failure_rolls_back(self, state, fail_on, error_type):
        plan = self._plan(state)
        connection = _ApplyConnection(state, fail_on=fail_on)
        with self.assertRaises(error_type):
            apply_instrument_bootstrap_plan(
                snapshot_path=self.capture.path,
                expected_plan_sha256=plan.plan_sha256,
                plan_date=PLAN_DATE,
                connection_factory=lambda: connection,
                schema_inspector=_schema,
            )
        self.assertEqual(connection.rollback_calls, 1)
        self.assertEqual(connection.commit_calls, 0)
        self.assertEqual(connection.close_calls, 1)

    def test_post_write_missing_name_and_exchange_mismatches_roll_back(self):
        for fail_on in ("post_missing", "post_name_mismatch", "post_exchange_mismatch"):
            with self.subTest(fail_on=fail_on):
                self._assert_failure_rolls_back(
                    _state(), fail_on, PostWriteVerificationError
                )

    def test_post_write_total_2999_when_expected_3000_rolls_back(self):
        state = _state()
        self._assert_failure_rolls_back(
            state, "post_count_low", PostWriteVerificationError
        )
        self.assertEqual(len(state.instruments), 0)

    def test_post_write_total_3001_when_expected_3000_rolls_back(self):
        state = _state()
        self._assert_failure_rolls_back(
            state, "post_count_high", PostWriteVerificationError
        )
        self.assertEqual(len(state.instruments), 0)

    def test_commit_occurs_only_after_post_write_selects(self):
        state = _state()
        plan = self._plan(state)
        _result, connection = self._apply(state, plan.plan_sha256)
        self.assertEqual(connection.events[-2:], ["COMMIT", "CLOSE"])
        self.assertGreater(connection.events.index("COMMIT"), connection.events.index("SQL:SELECT"))

    def test_repeated_apply_with_old_hash_is_rejected(self):
        state = _state()
        original_plan = self._plan(state)
        self._apply(state, original_plan.plan_sha256)
        connection = _ApplyConnection(state)
        with self.assertRaises(PlanChangedSinceDryRun):
            apply_instrument_bootstrap_plan(
                snapshot_path=self.capture.path,
                expected_plan_sha256=original_plan.plan_sha256,
                plan_date=PLAN_DATE,
                connection_factory=lambda: connection,
                schema_inspector=_schema,
            )
        self.assertEqual(connection.rollback_calls, 1)

    def test_successful_apply_produces_unchanged_next_dry_run(self):
        state = _state()
        original = self._plan(state)
        self._apply(state, original.plan_sha256)
        next_plan = self._plan(state)
        self.assertEqual(next_plan.would_insert_count, 0)
        self.assertEqual(next_plan.would_update_count, 0)
        self.assertEqual(next_plan.would_repair_corrupted_name_count, 0)
        self.assertEqual(next_plan.would_unchanged_count, 3_000)

    def test_snapshot_apply_mode_has_no_exchange_network(self):
        state = _full_state()
        plan = self._plan(state)
        with mock.patch.object(
            socket.socket, "connect", side_effect=AssertionError("network used")
        ):
            result, _connection = self._apply(state, plan.plan_sha256)
        self.assertTrue(result.committed)

    def test_import_has_no_side_effect(self):
        module_name = "data_collect.master_data.instrument_bootstrap_apply"
        module = importlib.import_module(module_name)
        self.assertTrue(hasattr(module, "apply_instrument_bootstrap_plan"))

    def test_cli_requires_both_apply_authority_arguments(self):
        with mock.patch.object(manage_master_data, "apply_instrument_bootstrap_plan") as apply:
            self.assertEqual(manage_master_data.main(["bootstrap-instruments", "--apply"]), 2)
            self.assertEqual(
                manage_master_data.main(
                    ["bootstrap-instruments", "--apply", "--snapshot", "x.json"]
                ),
                2,
            )
        apply.assert_not_called()

    def test_cli_forwards_explicit_snapshot_and_plan_hash(self):
        result = InstrumentBootstrapApplyResult(
            content_sha256="0" * 64,
            snapshot_sha256="1" * 64,
            database_baseline_sha256="2" * 64,
            plan_sha256="3" * 64,
            source_total=3_000,
            inserted_count=3_000,
            updated_count=0,
            repaired_count=0,
            changelog_inserted_count=0,
            deleted_count=0,
            post_total=3_000,
            missing_count=0,
            mismatch_count=0,
            committed=True,
        )
        with mock.patch.object(
            manage_master_data,
            "apply_instrument_bootstrap_plan",
            return_value=result,
        ) as apply:
            exit_code = manage_master_data.main(
                [
                    "bootstrap-instruments",
                    "--apply",
                    "--snapshot",
                    "snapshot.json",
                    "--expect-plan-sha256",
                    "3" * 64,
                ]
            )
        self.assertEqual(exit_code, 0)
        apply.assert_called_once_with(
            snapshot_path="snapshot.json", expected_plan_sha256="3" * 64
        )


if __name__ == "__main__":
    unittest.main()
