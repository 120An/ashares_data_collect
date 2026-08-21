from __future__ import annotations

import copy
import importlib
import os
from pathlib import Path
import sys
import unittest
from unittest import mock

import manage_master_data
from data_collect.master_data.public_instruments import (
    CompletenessPolicy,
    EastmoneyDirectAShareProvider,
    InstrumentSchemaInspection,
    ProviderResponseError,
    evaluate_public_instruments,
    infer_stock_code,
    inspect_instrument_info_schema,
    normalize_public_instrument,
)


def _row(code: str, name: str, market_id: int, *, security_type: str = "a_share") -> dict:
    return {
        "code": code,
        "name": name,
        "market_id": market_id,
        "security_type": security_type,
    }


def _complete_rows() -> list[dict]:
    return [
        _row("000001", "平安银行", 0),
        _row("600519", "贵州茅台", 1),
        _row("920001", "北交测试", 0),
    ]


def _test_policy(**kwargs) -> CompletenessPolicy:
    values = {
        "minimum_total_records": 3,
        "minimum_valid_ratio": 0.98,
        "require_samples": ("000001.SZ", "600519.SH"),
        "require_secondary_validation_for_apply": True,
    }
    values.update(kwargs)
    return CompletenessPolicy(**values)


def _compatible_schema() -> InstrumentSchemaInspection:
    return InstrumentSchemaInspection(
        inspected=True,
        columns=("stock_code", "InstrumentName", "ExchangeID", "updated_at"),
        compatible=True,
    )


class _FakeResponse:
    def __init__(self, payload, *, status_error: Exception | None = None):
        self.payload = payload
        self.status_error = status_error

    def raise_for_status(self):
        if self.status_error:
            raise self.status_error

    def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.trust_env = True
        self.proxies = {"http": "http://ambient.invalid", "https": "http://ambient.invalid"}
        self.calls = []
        self.closed = False

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def close(self):
        self.closed = True


class PublicProviderNetworkTests(unittest.TestCase):
    def _fetch_with_env(self, env: dict[str, str]) -> _FakeSession:
        response = _FakeResponse(
            {
                "data": {
                    "total": 3,
                    "diff": [
                        {"f12": "000001", "f13": 0, "f14": "平安银行"},
                        {"f12": "600519", "f13": 1, "f14": "贵州茅台"},
                        {"f12": "920001", "f13": 0, "f14": "北交测试"},
                    ],
                }
            }
        )
        session = _FakeSession([response])
        with mock.patch.dict(os.environ, env, clear=False):
            rows = EastmoneyDirectAShareProvider(
                session_factory=lambda: session,
                sleeper=lambda _seconds: None,
            ).fetch()
        self.assertEqual(len(rows), 3)
        return session

    def test_ambient_proxy_is_not_inherited(self):
        session = self._fetch_with_env(
            {
                "HTTP_PROXY": "http://127.0.0.1:7897",
                "HTTPS_PROXY": "http://127.0.0.1:7897",
            }
        )
        self.assertFalse(session.trust_env)
        self.assertEqual(session.proxies, {})
        self.assertNotIn("proxies", session.calls[0][1])
        self.assertTrue(session.closed)

    def test_no_proxy_environment_uses_same_direct_policy(self):
        clean_env = {key: value for key, value in os.environ.items() if key.upper() not in {
            "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"
        }}
        with mock.patch.dict(os.environ, clean_env, clear=True):
            session = self._fetch_with_env({})
        self.assertFalse(session.trust_env)
        self.assertNotIn("proxies", session.calls[0][1])

    def test_provider_closes_session_on_malformed_response(self):
        session = _FakeSession([_FakeResponse({"data": {"total": 1}})])
        with self.assertRaisesRegex(ProviderResponseError, "data.diff"):
            EastmoneyDirectAShareProvider(
                session_factory=lambda: session,
                sleeper=lambda _seconds: None,
            ).fetch()
        self.assertTrue(session.closed)

    def test_transient_request_failure_retries_same_direct_endpoint(self):
        session = _FakeSession(
            [
                OSError("temporary disconnect"),
                _FakeResponse(
                    {
                        "data": {
                            "total": 1,
                            "diff": [{"f12": "600519", "f13": 1, "f14": "贵州茅台"}],
                        }
                    }
                ),
            ]
        )
        rows = EastmoneyDirectAShareProvider(
            session_factory=lambda: session,
            request_attempts=2,
            retry_interval_seconds=0,
            sleeper=lambda _seconds: None,
        ).fetch()
        self.assertEqual(rows[0]["code"], "600519")
        self.assertEqual(len(session.calls), 2)
        self.assertFalse(session.trust_env)


class UniverseNormalizationTests(unittest.TestCase):
    def test_sh_sz_bj_codes_use_frozen_suffixes(self):
        self.assertEqual(infer_stock_code("600519"), ("600519.SH", "SH", 1))
        self.assertEqual(infer_stock_code("000001"), ("000001.SZ", "SZ", 0))
        self.assertEqual(infer_stock_code("920001"), ("920001.BJ", "BJ", 0))

    def test_legacy_exchange_id_and_canonical_exchange_are_both_explicit(self):
        item = normalize_public_instrument(_row("600519", "贵州茅台", 1))
        self.assertEqual(item.exchange_id, "SH")
        self.assertEqual(item.canonical_exchange, "SSE")

    def test_illegal_suffix_or_non_stock_family_is_rejected(self):
        with self.assertRaises(ValueError):
            normalize_public_instrument(_row("600519.HK", "测试", 1))
        with self.assertRaises(ValueError):
            normalize_public_instrument(_row("510300", "ETF", 1))

    def test_source_market_conflict_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "conflicts"):
            normalize_public_instrument(_row("600519", "贵州茅台", 0))

    def test_st_stock_is_preserved(self):
        item = normalize_public_instrument(_row("600001", "*ST测试", 1))
        self.assertEqual(item.instrument_name, "*ST测试")

    def test_inputs_are_not_mutated(self):
        rows = _complete_rows()
        before = copy.deepcopy(rows)
        evaluate_public_instruments(
            rows,
            policy=_test_policy(),
            schema_inspection=_compatible_schema(),
            secondary_validation_passed=True,
        )
        self.assertEqual(rows, before)


class CompletenessGateTests(unittest.TestCase):
    def _evaluate(self, rows, **kwargs):
        return evaluate_public_instruments(
            rows,
            policy=kwargs.pop("policy", _test_policy()),
            schema_inspection=kwargs.pop("schema_inspection", _compatible_schema()),
            secondary_validation_passed=kwargs.pop("secondary_validation_passed", True),
            **kwargs,
        )

    def test_normal_complete_fixture_passes(self):
        report = self._evaluate(_complete_rows())
        self.assertEqual(report.completeness_status, "PASS")
        self.assertTrue(report.apply_allowed)
        self.assertEqual(report.exchange_counts, {"SSE": 1, "SZSE": 1, "BSE": 1})

    def test_duplicate_is_detected_and_fails(self):
        rows = _complete_rows() + [_row("600519", "贵州茅台", 1)]
        report = self._evaluate(rows)
        self.assertEqual(report.duplicate_code_count, 1)
        self.assertEqual(report.valid_stock_records, 4)
        self.assertEqual(report.unique_stock_codes, 3)
        self.assertEqual(report.completeness_status, "FAIL")

    def test_question_mark_only_name_fails(self):
        rows = _complete_rows()
        rows[0]["name"] = "????"
        report = self._evaluate(rows)
        self.assertEqual(report.question_mark_name_count, 1)
        self.assertEqual(report.completeness_status, "FAIL")

    def test_replacement_character_name_fails(self):
        rows = _complete_rows()
        rows[0]["name"] = "平安\ufffd行"
        report = self._evaluate(rows)
        self.assertEqual(report.replacement_char_name_count, 1)
        self.assertEqual(report.completeness_status, "FAIL")

    def test_missing_bse_fails(self):
        report = self._evaluate(_complete_rows()[:2], policy=_test_policy(minimum_total_records=2))
        self.assertEqual(report.exchange_counts["BSE"], 0)
        self.assertEqual(report.completeness_status, "FAIL")

    def test_obviously_small_snapshot_fails(self):
        report = self._evaluate(
            _complete_rows(),
            policy=_test_policy(minimum_total_records=3_000),
        )
        self.assertEqual(report.completeness_status, "FAIL")
        self.assertTrue(any("conservative minimum" in item for item in report.blockers))

    def test_unknown_security_type_fails_closed(self):
        rows = _complete_rows() + [_row("600001", "测试", 1, security_type="unknown")]
        report = self._evaluate(rows)
        self.assertEqual(report.unknown_security_type_count, 1)
        self.assertEqual(report.completeness_status, "FAIL")

    def test_secondary_validation_is_separate_apply_gate(self):
        report = self._evaluate(_complete_rows(), secondary_validation_passed=False)
        self.assertEqual(report.completeness_status, "PASS")
        self.assertFalse(report.apply_allowed)
        self.assertEqual(report.secondary_validation_status, "PENDING")

    def test_uninspected_schema_is_separate_apply_gate(self):
        report = self._evaluate(
            _complete_rows(),
            schema_inspection=InstrumentSchemaInspection(inspected=False),
        )
        self.assertEqual(report.completeness_status, "PASS")
        self.assertFalse(report.apply_allowed)


class _SchemaCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))

    def fetchall(self):
        return list(self.rows)


class _SchemaConnection:
    def __init__(self, rows):
        self.cursor_obj = _SchemaCursor(rows)
        self.rollback_calls = 0
        self.close_calls = 0

    def cursor(self):
        return self.cursor_obj

    def rollback(self):
        self.rollback_calls += 1

    def close(self):
        self.close_calls += 1


class SchemaAndCliBoundaryTests(unittest.TestCase):
    def test_schema_inspection_is_select_only_and_rolls_back(self):
        connection = _SchemaConnection(
            [
                ("stock_code", "character varying", "NO", None),
                ("InstrumentName", "text", "YES", None),
                ("ExchangeID", "text", "YES", None),
                ("updated_at", "timestamp without time zone", "YES", "now()"),
            ]
        )
        result = inspect_instrument_info_schema(lambda: connection)
        self.assertTrue(result.compatible)
        self.assertEqual(connection.rollback_calls, 1)
        self.assertEqual(connection.close_calls, 1)
        statements = [sql.upper() for sql, _params in connection.cursor_obj.executed]
        self.assertTrue(all(sql.startswith(("SELECT", "SET TRANSACTION READ ONLY")) for sql in statements))

    def test_schema_inspection_does_not_rebuild_dynamic_legacy_table(self):
        connection = _SchemaConnection([("stock_code", "text", "NO", None)])
        result = inspect_instrument_info_schema(lambda: connection)
        self.assertFalse(result.compatible)
        self.assertEqual(
            result.missing_minimum_columns,
            ("InstrumentName", "ExchangeID"),
        )

    def test_schema_rejects_extra_required_column_public_provider_cannot_supply(self):
        connection = _SchemaConnection(
            [
                ("stock_code", "character varying", "NO", None),
                ("InstrumentName", "text", "YES", None),
                ("ExchangeID", "text", "YES", None),
                ("legacy_required", "text", "NO", None),
            ]
        )
        result = inspect_instrument_info_schema(lambda: connection)
        self.assertFalse(result.compatible)
        self.assertEqual(result.unsupported_required_columns, ("legacy_required",))

    def test_dry_run_cli_never_connects_or_runs_dml(self):
        report = evaluate_public_instruments(
            _complete_rows(),
            policy=_test_policy(),
            schema_inspection=_compatible_schema(),
            secondary_validation_passed=False,
        )
        with mock.patch.object(manage_master_data, "run_preflight", return_value=report) as run:
            exit_code = manage_master_data.main(["preflight"])
        self.assertEqual(exit_code, 0)
        run.assert_called_once_with(inspect_postgres=False)

    def test_apply_is_rejected_before_provider_or_database_work(self):
        with mock.patch.object(manage_master_data, "run_preflight") as run:
            exit_code = manage_master_data.main(["bootstrap-instruments", "--apply"])
        self.assertEqual(exit_code, 2)
        run.assert_not_called()

    def test_import_has_no_network_or_database_side_effect(self):
        module_name = "data_collect.master_data.public_instruments"
        module = sys.modules.pop(module_name, None)
        db_was_imported = "data_collect.utils.db" in sys.modules
        try:
            with mock.patch("socket.socket.connect", side_effect=AssertionError("network used")):
                imported = importlib.import_module(module_name)
            self.assertEqual(imported.DOMESTIC_NETWORK_MODE, "DIRECT")
            self.assertEqual("data_collect.utils.db" in sys.modules, db_was_imported)
        finally:
            sys.modules.pop(module_name, None)
            if module is not None:
                sys.modules[module_name] = module

    def test_module_does_not_create_phase2_objects_or_touch_sector(self):
        source = Path("data_collect/master_data/public_instruments.py").read_text(encoding="utf-8")
        self.assertNotIn("Entity sync", source)
        self.assertNotIn("sector_stock", source)
        self.assertNotIn("IndustryRelation", source)
        self.assertNotIn("StockRelation", source)


if __name__ == "__main__":
    unittest.main()
