from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from datetime import date
import importlib
import io
from pathlib import Path
import socket
import sys
import unittest
from unittest import mock

import manage_master_data
from data_collect.master_data.instrument_bootstrap import (
    BootstrapReadSnapshot,
    INSTRUMENT_WRITE_FIELDS,
    build_instrument_bootstrap_plan,
    database_baseline_sha256,
    read_bootstrap_inputs_from_postgres,
)
from data_collect.master_data.official_exchanges import (
    BSE_SOURCE_ID,
    SSE_SOURCE_ID,
    SZSE_SOURCE_ID,
    OfficialAShareUniverse,
    OfficialInstrumentRecord,
    OfficialProviderResult,
)
from data_collect.master_data.public_instruments import InstrumentSchemaInspection


PLAN_DATE = date(2026, 8, 21)


def _record(code: str, name: str, exchange_id: str) -> OfficialInstrumentRecord:
    canonical = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}[exchange_id]
    return OfficialInstrumentRecord(
        stock_code=code,
        instrument_name=name,
        exchange_id=exchange_id,
        canonical_exchange=canonical,
        source_id={"SSE": SSE_SOURCE_ID, "SZSE": SZSE_SOURCE_ID, "BSE": BSE_SOURCE_ID}[
            canonical
        ],
        source_record_type="ordinary_a_share",
        listing_presence="present_in_current_official_list",
        source_security_type="ordinary_a_share",
        classification_basis="official exchange fixture",
        provenance={"source": canonical},
    )


def _official_records() -> tuple[OfficialInstrumentRecord, ...]:
    rows = []
    for number in range(600_000, 601_000):
        code = f"{number:06d}.SH"
        name = "贵州茅台" if code == "600519.SH" else f"沪股{number:06d}"
        rows.append(_record(code, name, "SH"))
    for number in range(1, 1_501):
        code = f"{number:06d}.SZ"
        name = "平安银行" if code == "000001.SZ" else f"深股{number:06d}"
        rows.append(_record(code, name, "SZ"))
    for number in range(920_001, 920_501):
        code = f"{number:06d}.BJ"
        rows.append(_record(code, f"北股{number:06d}", "BJ"))
    return tuple(rows)


_RECORDS = _official_records()


def _provider(provider_id: str, count: int) -> OfficialProviderResult:
    raw_parts = {
        SSE_SOURCE_ID: {"main": count, "star": 0},
        SZSE_SOURCE_ID: {"a_share_tab1": count},
        BSE_SOURCE_ID: {"listed_company": count},
    }[provider_id]
    return OfficialProviderResult(
        provider_id=provider_id,
        candidates=(),
        raw_count=count,
        ordinary_stock_count=count,
        raw_part_counts=raw_parts,
        expected_total=count if provider_id == BSE_SOURCE_ID else None,
        fetched_total=count if provider_id == BSE_SOURCE_ID else None,
        total_pages=25 if provider_id == BSE_SOURCE_ID else None,
    )


def _universe(**changes) -> OfficialAShareUniverse:
    base = OfficialAShareUniverse(
        provider_mode="official_exchange_union",
        domestic_network_mode="DIRECT",
        inherited_env_proxy=False,
        sse=_provider(SSE_SOURCE_ID, 1_000),
        szse=_provider(SZSE_SOURCE_ID, 1_500),
        bse=_provider(BSE_SOURCE_ID, 500),
        records=_RECORDS,
        authoritative_raw_total=3_000,
        authoritative_unique_total=3_000,
        exchange_counts={"SSE": 1_000, "SZSE": 1_500, "BSE": 500},
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
        future_apply_prerequisites=("database_apply_not_implemented",),
        blockers=(),
        schema_inspection=InstrumentSchemaInspection(
            inspected=True,
            columns=("stock_code", "InstrumentName", "ExchangeID"),
            compatible=True,
        ),
    )
    return replace(base, **changes)


def _existing(code: str, name: str | None, exchange_id: str | None):
    return {
        "stock_code": code,
        "InstrumentName": name,
        "ExchangeID": exchange_id,
    }


class _Cursor:
    def __init__(self, result_sets):
        self.result_sets = list(result_sets)
        self.executed = []
        self._current = ()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self.executed.append((normalized, params))
        if normalized.upper().startswith("SELECT"):
            self._current = self.result_sets.pop(0)

    def fetchall(self):
        return list(self._current)


class _Connection:
    def __init__(self, result_sets):
        self.cursor_obj = _Cursor(result_sets)
        self.rollback_calls = 0
        self.close_calls = 0
        self.commit_calls = 0

    def cursor(self):
        return self.cursor_obj

    def rollback(self):
        self.rollback_calls += 1

    def close(self):
        self.close_calls += 1

    def commit(self):
        self.commit_calls += 1


class InstrumentBootstrapPlanTests(unittest.TestCase):
    def test_empty_database_bootstrap_plans_only_inserts_without_changelog(self):
        plan = build_instrument_bootstrap_plan(_universe(), (), (), plan_date=PLAN_DATE)
        self.assertEqual(plan.plan_status, "PASS")
        self.assertEqual(plan.would_insert_count, 3_000)
        self.assertEqual(plan.would_insert_changelog_count, 0)
        self.assertEqual(plan.would_delete_count, 0)
        self.assertFalse(plan.apply_allowed)

    def test_two_question_mark_names_are_repairs_not_renames(self):
        rows = (
            _existing("000001.SZ", "????", "SZ"),
            _existing("600519.SH", "????", "SH"),
        )
        plan = build_instrument_bootstrap_plan(_universe(), rows, (), plan_date=PLAN_DATE)
        self.assertEqual(plan.would_repair_corrupted_name_count, 2)
        self.assertEqual(plan.repair_codes, ("000001.SZ", "600519.SH"))
        self.assertEqual(plan.would_insert_changelog_count, 0)
        self.assertEqual(
            [item.new_instrument_name for item in plan.repair_samples],
            ["平安银行", "贵州茅台"],
        )

    def test_empty_and_replacement_character_names_are_repairs(self):
        rows = (
            _existing("000001.SZ", "", "SZ"),
            _existing("600519.SH", "贵�茅台", "SH"),
        )
        plan = build_instrument_bootstrap_plan(_universe(), rows, (), plan_date=PLAN_DATE)
        self.assertEqual(plan.would_repair_corrupted_name_count, 2)
        self.assertFalse(plan.changelog_entries)

    def test_normal_unchanged_row_is_not_updated(self):
        rows = (_existing("000001.SZ", "平安银行", "SZ"),)
        plan = build_instrument_bootstrap_plan(_universe(), rows, (), plan_date=PLAN_DATE)
        self.assertEqual(plan.would_unchanged_count, 1)
        self.assertEqual(plan.unchanged_codes, ("000001.SZ",))

    def test_normal_name_difference_is_fact_difference_with_truthful_changelog(self):
        rows = (_existing("000001.SZ", "正常旧简称", "SZ"),)
        plan = build_instrument_bootstrap_plan(_universe(), rows, (), plan_date=PLAN_DATE)
        self.assertEqual(plan.would_update_count, 1)
        self.assertEqual(plan.would_insert_changelog_count, 1)
        entry = plan.changelog_entries[0]
        self.assertEqual(entry.field_name, "InstrumentName")
        self.assertEqual((entry.old_value, entry.new_value), ("正常旧简称", "平安银行"))

    def test_exchange_difference_is_separate_and_plans_changelog(self):
        rows = (_existing("000001.SZ", "平安银行", "SH"),)
        plan = build_instrument_bootstrap_plan(_universe(), rows, (), plan_date=PLAN_DATE)
        self.assertEqual(plan.would_update_count, 1)
        self.assertEqual(plan.would_change_exchange_count, 1)
        self.assertEqual(plan.changelog_entries[0].field_name, "ExchangeID")

    def test_existing_extra_is_retained_and_never_deleted(self):
        rows = (_existing("601999.SH", "库内额外证券", "SH"),)
        plan = build_instrument_bootstrap_plan(_universe(), rows, (), plan_date=PLAN_DATE)
        self.assertEqual(plan.existing_not_in_official_codes, ("601999.SH",))
        self.assertEqual(plan.would_delete_count, 0)

    def test_duplicate_existing_code_fails_closed(self):
        rows = (
            _existing("000001.SZ", "平安银行", "SZ"),
            _existing("000001.SZ", "平安银行", "SZ"),
        )
        plan = build_instrument_bootstrap_plan(_universe(), rows, (), plan_date=PLAN_DATE)
        self.assertEqual(plan.plan_status, "FAIL")
        self.assertIn("duplicate existing", " ".join(plan.blockers))

    def test_invalid_existing_code_fails_closed(self):
        plan = build_instrument_bootstrap_plan(
            _universe(),
            (_existing("000001.HK", "非法", "HK"),),
            (),
            plan_date=PLAN_DATE,
        )
        self.assertEqual(plan.plan_status, "FAIL")
        self.assertIn("invalid existing", " ".join(plan.blockers))

    def test_universe_failure_or_schema_incompatibility_produces_no_actions(self):
        failed_universe = replace(_universe(), universe_status="FAIL")
        plan = build_instrument_bootstrap_plan(failed_universe, (), (), plan_date=PLAN_DATE)
        self.assertEqual((plan.plan_status, plan.actions), ("FAIL", ()))
        bad_schema = InstrumentSchemaInspection(inspected=True, compatible=False)
        plan = build_instrument_bootstrap_plan(
            _universe(schema_inspection=bad_schema), (), (), plan_date=PLAN_DATE
        )
        self.assertEqual((plan.plan_status, plan.actions), ("FAIL", ()))

    def test_exchange_and_anomaly_gates_are_not_inferred_from_pass_label(self):
        plan = build_instrument_bootstrap_plan(
            _universe(exchange_counts={"SSE": 1_500, "SZSE": 1_500, "BSE": 0}),
            (),
            (),
            plan_date=PLAN_DATE,
        )
        self.assertEqual(plan.plan_status, "FAIL")
        self.assertIn("BSE universe is empty", " ".join(plan.blockers))
        plan = build_instrument_bootstrap_plan(
            _universe(question_mark_name_count=1), (), (), plan_date=PLAN_DATE
        )
        self.assertEqual(plan.plan_status, "FAIL")

    def test_plan_is_deterministic_and_conserves_official_universe(self):
        rows = (
            _existing("600519.SH", "????", "SH"),
            _existing("000001.SZ", "正常旧简称", "SZ"),
        )
        first = build_instrument_bootstrap_plan(_universe(), rows, (), plan_date=PLAN_DATE)
        second = build_instrument_bootstrap_plan(
            _universe(), tuple(reversed(rows)), (), plan_date=PLAN_DATE
        )
        self.assertEqual(first.actions, second.actions)
        self.assertEqual(first.changelog_entries, second.changelog_entries)
        self.assertEqual(first.database_baseline_sha256, second.database_baseline_sha256)
        self.assertEqual(first.plan_sha256, second.plan_sha256)
        self.assertEqual(
            first.would_insert_count
            + first.would_update_count
            + first.would_repair_corrupted_name_count
            + first.would_unchanged_count,
            first.source_total,
        )
        self.assertEqual(first.allowed_instrument_write_fields, INSTRUMENT_WRITE_FIELDS)
        self.assertTrue(
            all(
                set(action.planned_values).issubset(INSTRUMENT_WRITE_FIELDS)
                for action in first.actions
            )
        )

    def test_baseline_hash_is_order_independent_but_fact_sensitive(self):
        rows = (
            _existing("600519.SH", "贵州茅台", "SH"),
            _existing("000001.SZ", "平安银行", "SZ"),
        )
        changelog = (
            {
                "stock_code": "600519.SH",
                "changed_at": PLAN_DATE,
                "field_name": "ExchangeID",
                "old_value": "X",
                "new_value": "SH",
            },
        )
        first = database_baseline_sha256(rows, changelog)
        second = database_baseline_sha256(tuple(reversed(rows)), changelog)
        changed = database_baseline_sha256(
            (_existing("600519.SH", "不同事实", "SH"), rows[1]), changelog
        )
        self.assertEqual(first, second)
        self.assertNotEqual(first, changed)

    def test_plan_hash_binds_snapshot_sha_and_complete_database_baseline(self):
        rows = (_existing("000001.SZ", "????", "SZ"),)
        first = build_instrument_bootstrap_plan(
            _universe(),
            rows,
            (),
            plan_date=PLAN_DATE,
            source_content_sha256="0" * 64,
            source_snapshot_sha256="1" * 64,
        )
        different_snapshot = build_instrument_bootstrap_plan(
            _universe(),
            rows,
            (),
            plan_date=PLAN_DATE,
            source_content_sha256="0" * 64,
            source_snapshot_sha256="2" * 64,
        )
        different_database = build_instrument_bootstrap_plan(
            _universe(),
            (_existing("000001.SZ", "正常旧简称", "SZ"),),
            (),
            plan_date=PLAN_DATE,
            source_content_sha256="0" * 64,
            source_snapshot_sha256="1" * 64,
        )
        self.assertNotEqual(first.plan_sha256, different_snapshot.plan_sha256)
        self.assertNotEqual(first.plan_sha256, different_database.plan_sha256)
        self.assertEqual(len(first.plan_sha256), 64)

    def test_existing_identical_changelog_is_not_planned_twice(self):
        rows = (_existing("000001.SZ", "正常旧简称", "SZ"),)
        changelog = ({
            "stock_code": "000001.SZ",
            "changed_at": PLAN_DATE,
            "field_name": "InstrumentName",
            "old_value": "正常旧简称",
            "new_value": "平安银行",
        },)
        plan = build_instrument_bootstrap_plan(
            _universe(), rows, changelog, plan_date=PLAN_DATE
        )
        self.assertEqual(plan.plan_status, "PASS")
        self.assertEqual(plan.would_insert_changelog_count, 0)

    def test_read_boundary_is_select_only_and_always_rolls_back(self):
        connection = _Connection(
            [
                [("000001.SZ", "????", "SZ")],
                [("000001.SZ", PLAN_DATE, "InstrumentName", "旧", "新")],
            ]
        )
        snapshot = read_bootstrap_inputs_from_postgres(lambda: connection)
        self.assertIsInstance(snapshot, BootstrapReadSnapshot)
        self.assertEqual(len(snapshot.existing_instruments), 1)
        self.assertEqual(connection.rollback_calls, 1)
        self.assertEqual(connection.close_calls, 1)
        self.assertEqual(connection.commit_calls, 0)
        statements = [sql.upper() for sql, _ in connection.cursor_obj.executed]
        self.assertTrue(
            all(sql.startswith(("SET TRANSACTION READ ONLY", "SELECT")) for sql in statements)
        )
        self.assertFalse(any("SECTOR_STOCK" in sql for sql in statements))

    def test_bootstrap_cli_requires_explicit_inspection_before_any_work(self):
        error = io.StringIO()
        with mock.patch.object(manage_master_data, "run_bootstrap_dry_run") as run, redirect_stderr(error):
            exit_code = manage_master_data.main(["bootstrap-instruments", "--dry-run"])
        self.assertEqual(exit_code, 2)
        run.assert_not_called()
        self.assertIn("requires --inspect-postgres", error.getvalue())

    def test_bootstrap_cli_prints_read_only_plan(self):
        plan = build_instrument_bootstrap_plan(
            _universe(),
            (_existing("000001.SZ", "????", "SZ"),),
            (),
            plan_date=PLAN_DATE,
        )
        output = io.StringIO()
        with mock.patch.object(
            manage_master_data, "run_bootstrap_dry_run", return_value=plan
        ) as run, redirect_stdout(output):
            exit_code = manage_master_data.main(
                ["bootstrap-instruments", "--dry-run", "--inspect-postgres"]
            )
        self.assertEqual(exit_code, 0)
        run.assert_called_once_with(snapshot_path=None)
        text = output.getvalue()
        self.assertIn("plan_status: PASS", text)
        self.assertIn("database_baseline_sha256:", text)
        self.assertIn("plan_sha256:", text)
        self.assertIn("would_delete: 0", text)
        self.assertIn("database_dml_executed: false", text)

    def test_apply_is_rejected_before_network_or_database(self):
        with mock.patch.object(manage_master_data, "run_bootstrap_dry_run") as run:
            exit_code = manage_master_data.main(
                ["bootstrap-instruments", "--apply", "--inspect-postgres"]
            )
        self.assertEqual(exit_code, 2)
        run.assert_not_called()

    def test_import_has_no_network_or_database_side_effect(self):
        module_name = "data_collect.master_data.instrument_bootstrap"
        module = sys.modules.pop(module_name, None)
        db_was_imported = "data_collect.utils.db" in sys.modules
        try:
            with mock.patch.object(
                socket.socket, "connect", side_effect=AssertionError("network used")
            ):
                imported = importlib.import_module(module_name)
            self.assertEqual(imported.PLAN_MODE, "BOOTSTRAP")
            self.assertEqual("data_collect.utils.db" in sys.modules, db_was_imported)
        finally:
            sys.modules.pop(module_name, None)
            if module is not None:
                sys.modules[module_name] = module

    def test_module_does_not_touch_sector_or_entity_sync(self):
        source = Path("data_collect/master_data/instrument_bootstrap.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("sector_stock", source)
        self.assertNotIn("synchronize_entities", source)
        self.assertNotIn("Company", source)
        self.assertNotIn("IndustryRelation", source)


if __name__ == "__main__":
    unittest.main()
