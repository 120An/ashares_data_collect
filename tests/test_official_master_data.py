from __future__ import annotations

from contextlib import redirect_stdout
import importlib
import io
import json
import os
from pathlib import Path
import sys
import unittest
from unittest import mock

import manage_master_data
from data_collect.master_data.official_exchanges import (
    BSEOfficialInstrumentProvider,
    BSE_SOURCE_ID,
    DATABASE_APPLY_BLOCKER,
    OfficialInstrumentCandidate,
    OfficialProviderResponseError,
    OfficialProviderResult,
    OfficialUniversePolicy,
    SSEOfficialInstrumentProvider,
    SSE_SOURCE_ID,
    SZSEOfficialInstrumentProvider,
    SZSE_SOURCE_ID,
    build_official_a_share_universe,
    fetch_official_a_share_universe,
)


class _Response:
    def __init__(
        self,
        *,
        payload=None,
        content: bytes = b"fixture",
        text: str = "",
        status_error: Exception | None = None,
        status_code: int = 200,
    ):
        self.payload = payload
        self.content = content
        self.text = text
        self.status_error = status_error
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_error is not None:
            raise self.status_error

    def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class _Session:
    def __init__(self, *, get_responses=(), post_responses=()):
        self.get_responses = list(get_responses)
        self.post_responses = list(post_responses)
        self.get_calls = []
        self.post_calls = []
        self.trust_env = True
        self.proxies = {"http": "http://ambient.invalid"}
        self.closed = False

    def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        result = self.get_responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def post(self, url, **kwargs):
        self.post_calls.append((url, kwargs))
        result = self.post_responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def close(self):
        self.closed = True


def _sse_response(rows):
    return _Response(payload={"result": rows})


def _bse_page(*, rows, total_pages, total_elements, page):
    payload = [
        {
            "content": rows,
            "totalPages": total_pages,
            "totalElements": total_elements,
            "number": page,
            "size": 20,
        }
    ]
    return _Response(text="callback(" + json.dumps(payload, ensure_ascii=False) + ");")


def _candidate(
    code: str,
    name: str,
    exchange: str,
    *,
    source_id: str | None = None,
    uncertain: bool = False,
) -> OfficialInstrumentCandidate:
    canonical = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}[exchange]
    source = source_id or {"SH": SSE_SOURCE_ID, "SZ": SZSE_SOURCE_ID, "BJ": BSE_SOURCE_ID}[exchange]
    return OfficialInstrumentCandidate(
        raw_code=code,
        instrument_name=name,
        exchange_id=exchange,
        canonical_exchange=canonical,
        source_id=source,
        source_record_type="fixture",
        listing_presence=f"present_in_current_{canonical.lower()}_official_list",
        source_security_type="ordinary_a_share",
        classification_basis="official fixture",
        security_type_uncertain=uncertain,
        raw_evidence={"code": code, "name": name},
    )


def _result(
    provider_id: str,
    candidates,
    *,
    raw_count: int | None = None,
    expected_total: int | None = None,
    fetched_total: int | None = None,
    total_pages: int | None = None,
) -> OfficialProviderResult:
    items = tuple(candidates)
    count = len(items) if raw_count is None else raw_count
    raw_parts = {
        SSE_SOURCE_ID: {"main": count, "star": 0},
        SZSE_SOURCE_ID: {"a_share_tab1": count},
        BSE_SOURCE_ID: {"listed_company": count},
    }[provider_id]
    return OfficialProviderResult(
        provider_id=provider_id,
        candidates=items,
        raw_count=count,
        ordinary_stock_count=len(items),
        raw_part_counts=raw_parts,
        expected_total=expected_total,
        fetched_total=fetched_total,
        total_pages=total_pages,
    )


def _complete_results():
    sse = _result(
        SSE_SOURCE_ID,
        [_candidate("600519", "贵州茅台", "SH"), _candidate("688001", "华兴源创", "SH")],
    )
    szse = _result(SZSE_SOURCE_ID, [_candidate("000001", "平安银行", "SZ")])
    bse = _result(
        BSE_SOURCE_ID,
        [_candidate("920001", "北交测试", "BJ")],
        expected_total=1,
        fetched_total=1,
        total_pages=1,
    )
    return sse, szse, bse


def _build(sse=None, szse=None, bse=None):
    defaults = _complete_results()
    return build_official_a_share_universe(
        sse or defaults[0],
        szse or defaults[1],
        bse or defaults[2],
        policy=OfficialUniversePolicy(minimum_total_records=3),
    )


class SSEOfficialProviderTests(unittest.TestCase):
    def test_transient_connection_failure_retries_with_bounded_backoff(self):
        session = _Session(
            get_responses=[
                ConnectionError("reset"),
                _sse_response(
                    [{"A_STOCK_CODE": "600519", "SEC_NAME_CN": "贵州茅台"}]
                ),
                _sse_response(
                    [{"A_STOCK_CODE": "688001", "SEC_NAME_CN": "华兴源创"}]
                ),
            ]
        )
        sleeps = []
        result = SSEOfficialInstrumentProvider(
            session_factory=lambda: session,
            request_attempts=3,
            sleeper=sleeps.append,
        ).fetch()
        self.assertEqual(result.ordinary_stock_count, 2)
        self.assertEqual(sleeps, [1.0])
        self.assertEqual(len(session.get_calls), 3)

    def test_transient_retry_stops_after_three_attempts(self):
        session = _Session(
            get_responses=[
                ConnectionError("one"),
                ConnectionError("two"),
                ConnectionError("three"),
            ]
        )
        sleeps = []
        with self.assertRaisesRegex(OfficialProviderResponseError, "main request failed"):
            SSEOfficialInstrumentProvider(
                session_factory=lambda: session,
                request_attempts=3,
                sleeper=sleeps.append,
            ).fetch()
        self.assertEqual(sleeps, [1.0, 2.0])
        self.assertEqual(len(session.get_calls), 3)

    def test_http_5xx_retries_but_non_transient_response_parsing_does_not(self):
        session = _Session(
            get_responses=[
                _Response(
                    status_error=RuntimeError("service unavailable"), status_code=503
                ),
                _sse_response(
                    [{"A_STOCK_CODE": "600519", "SEC_NAME_CN": "贵州茅台"}]
                ),
                _sse_response(
                    [{"A_STOCK_CODE": "688001", "SEC_NAME_CN": "华兴源创"}]
                ),
            ]
        )
        sleeps = []
        result = SSEOfficialInstrumentProvider(
            session_factory=lambda: session,
            sleeper=sleeps.append,
        ).fetch()
        self.assertEqual(result.ordinary_stock_count, 2)
        self.assertEqual(sleeps, [1.0])

    def test_main_and_star_are_merged_and_689_cdr_is_excluded(self):
        session = _Session(
            get_responses=[
                _sse_response(
                    [{"A_STOCK_CODE": "600519", "SEC_NAME_CN": "贵州茅台", "raw": "main"}]
                ),
                _sse_response(
                    [
                        {"A_STOCK_CODE": "688001", "SEC_NAME_CN": "华兴源创", "raw": "star"},
                        {"A_STOCK_CODE": "689009", "SEC_NAME_CN": "九号公司", "raw": "cdr"},
                    ]
                ),
            ]
        )
        result = SSEOfficialInstrumentProvider(session_factory=lambda: session).fetch()
        self.assertEqual(result.raw_part_counts, {"main": 1, "star": 2})
        self.assertEqual(result.raw_count, 3)
        self.assertEqual(result.ordinary_stock_count, 2)
        self.assertEqual(result.excluded_cdr_codes, ("689009",))
        self.assertEqual([row.raw_code for row in result.candidates], ["600519", "688001"])
        self.assertEqual(result.candidates[1].raw_evidence["raw"], "star")

    def test_required_sse_sample_and_unicode_survive(self):
        session = _Session(
            get_responses=[
                _sse_response([{"A_STOCK_CODE": "600519", "SEC_NAME_CN": "贵州茅台"}]),
                _sse_response([{"A_STOCK_CODE": "688001", "SEC_NAME_CN": "华兴源创"}]),
            ]
        )
        result = SSEOfficialInstrumentProvider(session_factory=lambda: session).fetch()
        sample = result.candidates[0]
        self.assertEqual(sample.instrument_name, "贵州茅台")
        self.assertEqual(sample.instrument_name.encode("utf-8").hex(), "e8b4b5e5b79ee88c85e58fb0")

    def test_malformed_result_fails_closed(self):
        session = _Session(get_responses=[_Response(payload={"result": {}})])
        with self.assertRaisesRegex(OfficialProviderResponseError, "result"):
            SSEOfficialInstrumentProvider(session_factory=lambda: session).fetch()
        self.assertTrue(session.closed)

    def test_sse_direct_session_ignores_ambient_proxy(self):
        session = _Session(
            get_responses=[_sse_response([]), _sse_response([])]
        )
        with mock.patch.dict(
            os.environ,
            {"HTTP_PROXY": "http://127.0.0.1:7897", "HTTPS_PROXY": "http://127.0.0.1:7897"},
        ):
            SSEOfficialInstrumentProvider(session_factory=lambda: session).fetch()
        self.assertFalse(session.trust_env)
        self.assertEqual(session.proxies, {})
        self.assertTrue(session.closed)
        self.assertEqual(
            [call[1]["params"]["STOCK_TYPE"] for call in session.get_calls],
            ["1", "8"],
        )


class SZSEOfficialProviderTests(unittest.TestCase):
    def test_tab1_a_share_columns_are_used_and_other_columns_are_only_evidence(self):
        rows = [
            {
                "板块": "主板",
                "A股代码": 1,
                "A股简称": "平安银行",
                "B股代码": "200001",
                "CDR代码": "999999",
            },
            {"板块": "创业板", "A股代码": "300001", "A股简称": "特锐德"},
        ]
        session = _Session(get_responses=[_Response(content=b"xlsx")])
        result = SZSEOfficialInstrumentProvider(
            session_factory=lambda: session,
            xlsx_reader=lambda _content: rows,
        ).fetch()
        self.assertEqual([item.raw_code for item in result.candidates], ["000001", "300001"])
        self.assertEqual(result.candidates[0].instrument_name, "平安银行")
        self.assertEqual(result.candidates[0].raw_evidence["B股代码"], "200001")
        self.assertEqual(session.get_calls[0][1]["params"]["TABKEY"], "tab1")
        self.assertEqual(len(session.get_calls), 1)

    def test_szse_unicode_is_preserved(self):
        session = _Session(get_responses=[_Response(content=b"xlsx")])
        result = SZSEOfficialInstrumentProvider(
            session_factory=lambda: session,
            xlsx_reader=lambda _content: [{"A股代码": "000001", "A股简称": "平安银行"}],
        ).fetch()
        name = result.candidates[0].instrument_name
        self.assertEqual(name.encode("utf-8").hex(), "e5b9b3e5ae89e993b6e8a18c")

    def test_malformed_xlsx_fails_closed(self):
        session = _Session(get_responses=[_Response(content=b"bad")])

        def bad_reader(_content):
            raise ValueError("broken workbook")

        with self.assertRaisesRegex(OfficialProviderResponseError, "malformed XLSX"):
            SZSEOfficialInstrumentProvider(
                session_factory=lambda: session,
                xlsx_reader=bad_reader,
            ).fetch()
        self.assertTrue(session.closed)

    def test_szse_direct_session_ignores_ambient_proxy(self):
        session = _Session(get_responses=[_Response(content=b"xlsx")])
        with mock.patch.dict(os.environ, {"HTTP_PROXY": "http://127.0.0.1:7897"}):
            SZSEOfficialInstrumentProvider(
                session_factory=lambda: session,
                xlsx_reader=lambda _content: [],
            ).fetch()
        self.assertFalse(session.trust_env)
        self.assertEqual(session.proxies, {})
        self.assertTrue(session.closed)


class BSEOfficialProviderTests(unittest.TestCase):
    def test_all_pages_are_fetched_and_totals_match(self):
        session = _Session(
            post_responses=[
                _bse_page(
                    rows=[
                        {"xxzqdm": "920001", "xxzqjc": "北交一", "xxzqjb": "T"},
                        {"xxzqdm": "920002", "xxzqjc": "北交二", "xxzqjb": "T"},
                    ],
                    total_pages=2,
                    total_elements=3,
                    page=0,
                ),
                _bse_page(
                    rows=[{"xxzqdm": "920003", "xxzqjc": "北交三", "xxzqjb": "T"}],
                    total_pages=2,
                    total_elements=3,
                    page=1,
                ),
            ]
        )
        result = BSEOfficialInstrumentProvider(session_factory=lambda: session).fetch()
        self.assertEqual(result.expected_total, 3)
        self.assertEqual(result.fetched_total, 3)
        self.assertEqual(result.total_pages, 2)
        self.assertEqual([call[1]["data"]["page"] for call in session.post_calls], ["0", "1"])
        self.assertEqual(result.candidates[0].raw_evidence["xxzqjb"], "T")

    def test_page_failure_fails_entire_provider(self):
        session = _Session(
            post_responses=[
                _bse_page(
                    rows=[{"xxzqdm": "920001", "xxzqjc": "北交一"}],
                    total_pages=2,
                    total_elements=2,
                    page=0,
                ),
                OSError("page unavailable"),
            ]
        )
        with self.assertRaisesRegex(OfficialProviderResponseError, "page 1 request failed"):
            BSEOfficialInstrumentProvider(session_factory=lambda: session).fetch()
        self.assertTrue(session.closed)

    def test_total_mismatch_fails_closed(self):
        session = _Session(
            post_responses=[
                _bse_page(
                    rows=[{"xxzqdm": "920001", "xxzqjc": "北交一"}],
                    total_pages=1,
                    total_elements=2,
                    page=0,
                )
            ]
        )
        with self.assertRaisesRegex(OfficialProviderResponseError, "total mismatch"):
            BSEOfficialInstrumentProvider(session_factory=lambda: session).fetch()

    def test_duplicate_across_pages_fails_closed(self):
        duplicate = {"xxzqdm": "920001", "xxzqjc": "北交一"}
        session = _Session(
            post_responses=[
                _bse_page(rows=[duplicate], total_pages=2, total_elements=2, page=0),
                _bse_page(rows=[duplicate], total_pages=2, total_elements=2, page=1),
            ]
        )
        with self.assertRaisesRegex(OfficialProviderResponseError, "duplicate codes"):
            BSEOfficialInstrumentProvider(session_factory=lambda: session).fetch()

    def test_bse_direct_session_ignores_ambient_proxy(self):
        session = _Session(
            post_responses=[
                _bse_page(rows=[], total_pages=1, total_elements=0, page=0)
            ]
        )
        with mock.patch.dict(os.environ, {"HTTPS_PROXY": "http://127.0.0.1:7897"}):
            BSEOfficialInstrumentProvider(session_factory=lambda: session).fetch()
        self.assertFalse(session.trust_env)
        self.assertEqual(session.proxies, {})
        self.assertTrue(session.closed)


class OfficialUniverseTests(unittest.TestCase):
    def test_complete_official_fixture_passes_but_apply_stays_disabled(self):
        report = _build()
        self.assertEqual(report.universe_status, "PASS")
        self.assertEqual(report.completeness_status, "PASS")
        self.assertEqual(report.exchange_counts, {"SSE": 2, "SZSE": 1, "BSE": 1})
        self.assertEqual(report.authoritative_unique_total, 4)
        self.assertFalse(report.apply_allowed)
        self.assertIn(DATABASE_APPLY_BLOCKER, report.future_apply_prerequisites)
        self.assertFalse(any("Eastmoney" in value for value in report.future_apply_prerequisites))

    def test_duplicate_and_name_conflict_fail(self):
        sse, szse, bse = _complete_results()
        sse = _result(
            SSE_SOURCE_ID,
            [*sse.candidates, _candidate("600519", "错误名称", "SH")],
        )
        report = _build(sse=sse, szse=szse, bse=bse)
        self.assertEqual(report.duplicate_code_count, 1)
        self.assertEqual(report.name_conflict_count, 1)
        self.assertEqual(report.universe_status, "FAIL")

    def test_cross_exchange_bare_code_conflict_fails(self):
        sse, szse, bse = _complete_results()
        szse = _result(
            SZSE_SOURCE_ID,
            [*szse.candidates, _candidate("600519", "冲突证券", "SZ")],
        )
        report = _build(sse=sse, szse=szse, bse=bse)
        self.assertEqual(report.cross_exchange_conflict_count, 1)
        self.assertEqual(report.universe_status, "FAIL")

    def test_empty_question_mark_and_replacement_names_each_fail(self):
        for bad_name, field_name in (
            ("", "empty_name_count"),
            ("????", "question_mark_name_count"),
            ("坏\ufffd名称", "replacement_char_name_count"),
        ):
            with self.subTest(name=bad_name):
                sse, szse, bse = _complete_results()
                sse = _result(
                    SSE_SOURCE_ID,
                    [*sse.candidates, _candidate("600001", bad_name, "SH")],
                )
                report = _build(sse=sse, szse=szse, bse=bse)
                self.assertEqual(getattr(report, field_name), 1)
                self.assertEqual(report.universe_status, "FAIL")

    def test_invalid_code_and_uncertain_security_type_fail(self):
        sse, szse, bse = _complete_results()
        sse = _result(
            SSE_SOURCE_ID,
            [*sse.candidates, _candidate("bad", "非法代码", "SH")],
        )
        bse = _result(
            BSE_SOURCE_ID,
            [*bse.candidates, _candidate("920002", "类型不明", "BJ", uncertain=True)],
            expected_total=2,
            fetched_total=2,
            total_pages=1,
        )
        report = _build(sse=sse, szse=szse, bse=bse)
        self.assertEqual(report.invalid_code_count, 1)
        self.assertEqual(report.security_type_uncertain_count, 1)
        self.assertEqual(report.universe_status, "FAIL")

    def test_missing_exchange_fails(self):
        sse, szse, _bse = _complete_results()
        bse = _result(
            BSE_SOURCE_ID,
            [],
            expected_total=0,
            fetched_total=0,
            total_pages=1,
        )
        report = _build(sse=sse, szse=szse, bse=bse)
        self.assertEqual(report.exchange_counts["BSE"], 0)
        self.assertEqual(report.universe_status, "FAIL")

    def test_required_sample_name_is_exact(self):
        sse, szse, bse = _complete_results()
        szse = _result(SZSE_SOURCE_ID, [_candidate("000001", "错误名称", "SZ")])
        report = _build(sse=sse, szse=szse, bse=bse)
        self.assertTrue(any("sample name mismatch" in value for value in report.blockers))
        self.assertEqual(report.universe_status, "FAIL")

    def test_provider_failure_aborts_union(self):
        class _FailingProvider:
            def fetch(self):
                raise OfficialProviderResponseError("SSE unavailable")

        class _StaticProvider:
            def __init__(self, value):
                self.value = value

            def fetch(self):
                return self.value

        sse, szse, bse = _complete_results()
        with self.assertRaisesRegex(OfficialProviderResponseError, "SSE unavailable"):
            fetch_official_a_share_universe(
                sse_provider=_FailingProvider(),
                szse_provider=_StaticProvider(szse),
                bse_provider=_StaticProvider(bse),
                policy=OfficialUniversePolicy(minimum_total_records=3),
            )


class OfficialBoundaryTests(unittest.TestCase):
    def test_official_cli_is_dry_run_and_returns_success_for_passed_universe(self):
        report = _build()
        output = io.StringIO()
        with mock.patch.object(
            manage_master_data,
            "run_official_preflight",
            return_value=report,
        ) as run, redirect_stdout(output):
            exit_code = manage_master_data.main(["official-preflight"])
        self.assertEqual(exit_code, 0)
        run.assert_called_once_with(inspect_postgres=False)
        self.assertIn("provider_mode: official_exchange_union", output.getvalue())
        self.assertIn("apply_allowed: false", output.getvalue())
        self.assertIn("database_dml_executed: false", output.getvalue())

    def test_import_has_no_network_or_database_side_effect(self):
        module_name = "data_collect.master_data.official_exchanges"
        module = sys.modules.pop(module_name, None)
        db_was_imported = "data_collect.utils.db" in sys.modules
        try:
            with mock.patch("socket.socket.connect", side_effect=AssertionError("network used")):
                imported = importlib.import_module(module_name)
            self.assertEqual(imported.PROVIDER_MODE, "official_exchange_union")
            self.assertEqual("data_collect.utils.db" in sys.modules, db_was_imported)
        finally:
            sys.modules.pop(module_name, None)
            if module is not None:
                sys.modules[module_name] = module

    def test_module_has_no_legacy_sector_or_entity_sync_boundary(self):
        source = Path("data_collect/master_data/official_exchanges.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("sector_stock", source)
        self.assertNotIn("synchronize_entities", source)
        self.assertNotIn("IndustryRelation", source)
        self.assertNotIn("StockRelation", source)

    def test_bootstrap_apply_is_still_rejected_before_official_network(self):
        with mock.patch.object(manage_master_data, "run_official_preflight") as official:
            exit_code = manage_master_data.main(["bootstrap-instruments", "--apply"])
        self.assertEqual(exit_code, 2)
        official.assert_not_called()


if __name__ == "__main__":
    unittest.main()
