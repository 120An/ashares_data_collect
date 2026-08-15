"""Pure Phase 1 Entity / EntityAlias shadow tests."""

from __future__ import annotations

from contextlib import redirect_stdout
from copy import deepcopy
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from io import StringIO
import importlib.util
from pathlib import Path
import re
import sys
import unittest
from unittest.mock import patch

import manage_news_foundation
from data_collect.news_model.contracts import EntityAliasType, EntityType, Exchange
from data_collect.news_model.entity_catalog import (
    AliasMatchStatus,
    EntityCatalogError,
    ExchangeMismatchError,
    build_entity_catalog,
    build_sector_crosswalk,
    load_shadow_inputs_from_postgres,
    make_entity_alias_id,
    match_entity_alias,
    normalize_entity_alias,
    stock_entity_from_instrument_row,
)


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SQL_PATH = _PROJECT_ROOT / "sql" / "012_create_news_entity_foundation.sql"
_TZ = timezone(timedelta(hours=8), "Asia/Shanghai")
_NOW = datetime(2026, 8, 16, 12, 0, tzinfo=_TZ)


def _instrument(
    stock_code: str = "600519.SH",
    name: str = "贵州茅台",
    exchange_id: str | None = "SH",
    **extra,
) -> dict:
    row = {"stock_code": stock_code, "InstrumentName": name}
    if exchange_id is not None:
        row["ExchangeID"] = exchange_id
    row.update(extra)
    return row


def _name_change(stock_code: str, changed_at: str, old: object, new: object) -> dict:
    return {
        "stock_code": stock_code,
        "changed_at": changed_at,
        "field_name": "InstrumentName",
        "old_value": old,
        "new_value": new,
    }


def _sql_table_body(sql: str, table_name: str) -> str:
    match = re.search(
        rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{re.escape(table_name)}\s*\((.*?)\);",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"SQL table not found: {table_name}")
    return match.group(1)


class NewsEntityCatalogTests(unittest.TestCase):
    def test_sh_sz_bj_stock_entities_use_frozen_identity_and_exchange_rules(self):
        cases = (
            ("600519.SH", "贵州茅台", "SH", "ent_stock_600519_sh", Exchange.SSE),
            ("000001.SZ", "平安银行", "SZ", "ent_stock_000001_sz", Exchange.SZSE),
            ("920001.BJ", "北交样本", "BJ", "ent_stock_920001_bj", Exchange.BSE),
        )
        for code, name, exchange_id, expected_id, expected_exchange in cases:
            with self.subTest(code=code):
                projection = stock_entity_from_instrument_row(
                    _instrument(code, name, exchange_id), observed_at=_NOW
                )
                self.assertEqual(projection.entity.entity_id, expected_id)
                self.assertEqual(projection.entity.stock_code, code)
                self.assertIs(projection.entity.exchange, expected_exchange)
                self.assertIs(projection.entity.entity_type, EntityType.STOCK)

    def test_stock_entity_id_is_stable_across_repeated_runs(self):
        row = _instrument()
        first = stock_entity_from_instrument_row(row, observed_at=_NOW)
        second = stock_entity_from_instrument_row(
            row,
            observed_at=datetime(2026, 8, 17, 12, 0, tzinfo=_TZ),
        )
        self.assertEqual(first.entity.entity_id, second.entity.entity_id)

    def test_exchange_id_conflict_never_silently_passes(self):
        with self.assertRaisesRegex(ExchangeMismatchError, "requires SSE"):
            stock_entity_from_instrument_row(
                _instrument("600519.SH", "贵州茅台", "SZ"), observed_at=_NOW
            )

        projection = stock_entity_from_instrument_row(
            _instrument("600519.SH", "贵州茅台", "SZ"),
            observed_at=_NOW,
            strict_exchange=False,
        )
        self.assertIs(projection.entity.exchange, Exchange.SSE)
        self.assertEqual(projection.diagnostics[0].code, "exchange_mismatch")

    def test_instrument_name_creates_current_stock_short_name_alias(self):
        projection = stock_entity_from_instrument_row(_instrument(), observed_at=_NOW)
        aliases = [a for a in projection.aliases if a.alias_type is EntityAliasType.STOCK_SHORT_NAME]
        self.assertEqual(len(aliases), 1)
        self.assertEqual(aliases[0].alias, "贵州茅台")
        self.assertTrue(aliases[0].is_current)

    def test_qualified_stock_code_creates_ticker_alias(self):
        projection = stock_entity_from_instrument_row(_instrument(), observed_at=_NOW)
        aliases = [a.alias for a in projection.aliases if a.alias_type is EntityAliasType.TICKER]
        self.assertIn("600519.SH", aliases)

    def test_bare_stock_code_creates_ticker_alias(self):
        projection = stock_entity_from_instrument_row(_instrument(), observed_at=_NOW)
        aliases = [a.alias for a in projection.aliases if a.alias_type is EntityAliasType.TICKER]
        self.assertIn("600519", aliases)

    def test_qualified_bare_and_short_name_queries_resolve_the_same_stock(self):
        snapshot = build_entity_catalog([_instrument()], observed_at=_NOW)
        for query in ("600519.SH", "600519", "贵州茅台"):
            with self.subTest(query=query):
                result = match_entity_alias(query, snapshot.aliases)
                self.assertIs(result.status, AliasMatchStatus.UNIQUE)
                self.assertEqual(result.entity_ids, ("ent_stock_600519_sh",))

    def test_alias_id_is_stable_for_same_logical_fact(self):
        kwargs = {
            "entity_id": "ent_stock_600519_sh",
            "alias_type": EntityAliasType.FORMER_NAME,
            "alias": "旧名称",
            "fact_key": "instrument_changelog:600519.SH:2020-01-01:InstrumentName:old_value",
        }
        self.assertEqual(make_entity_alias_id(**kwargs), make_entity_alias_id(**kwargs))

    def test_repeated_name_in_disjoint_intervals_has_distinct_alias_ids(self):
        snapshot = build_entity_catalog(
            [_instrument(name="名称丙")],
            [
                _name_change("600519.SH", "2020-01-01", "名称甲", "名称乙"),
                _name_change("600519.SH", "2021-01-01", "名称乙", "名称甲"),
                _name_change("600519.SH", "2022-01-01", "名称甲", "名称丙"),
            ],
            observed_at=_NOW,
        )
        repeated = [a for a in snapshot.historical_aliases if a.alias == "名称甲"]
        self.assertEqual(len(repeated), 2)
        self.assertEqual(len({a.entity_alias_id for a in repeated}), 2)

    def test_returned_current_name_has_a_distinct_occurrence_identity(self):
        snapshot = build_entity_catalog(
            [_instrument(name="名称甲")],
            [
                _name_change("600519.SH", "2020-01-01", "名称甲", "名称乙"),
                _name_change("600519.SH", "2021-01-01", "名称乙", "名称甲"),
            ],
            observed_at=_NOW,
        )
        same_name = [a for a in snapshot.aliases if a.alias == "名称甲"]
        self.assertEqual(len(same_name), 2)
        self.assertEqual(len({a.entity_alias_id for a in same_name}), 2)

    def test_same_name_for_multiple_stocks_is_ambiguous(self):
        snapshot = build_entity_catalog(
            [
                _instrument("600519.SH", "同名证券", "SH"),
                _instrument("000001.SZ", "同名证券", "SZ"),
            ],
            observed_at=_NOW,
        )
        result = match_entity_alias("同名证券", snapshot.aliases)
        self.assertIs(result.status, AliasMatchStatus.AMBIGUOUS)
        self.assertTrue(result.ambiguous)
        self.assertEqual(set(result.entity_ids), {"ent_stock_600519_sh", "ent_stock_000001_sz"})

    def test_ambiguous_match_never_exposes_a_selected_first_candidate(self):
        snapshot = build_entity_catalog(
            [
                _instrument("600519.SH", "同名证券", "SH"),
                _instrument("000001.SZ", "同名证券", "SZ"),
            ],
            observed_at=_NOW,
        )
        result = match_entity_alias("同名证券", reversed(snapshot.aliases))
        self.assertEqual(len(result.entity_ids), 2)
        self.assertNotIn("selected_entity_id", {field.name for field in fields(result)})

    def test_at_time_restricts_historical_alias_half_open_intervals(self):
        snapshot = build_entity_catalog(
            [_instrument(name="名称丙")],
            [
                _name_change("600519.SH", "2020-01-01", "名称甲", "名称乙"),
                _name_change("600519.SH", "2021-01-01", "名称乙", "名称丙"),
            ],
            observed_at=_NOW,
        )
        before = match_entity_alias("名称甲", snapshot.aliases, at_time="2019-06-01T00:00:00+08:00")
        after = match_entity_alias("名称甲", snapshot.aliases, at_time="2020-01-01T00:00:00+08:00")
        middle = match_entity_alias("名称乙", snapshot.aliases, at_time="2020-06-01T00:00:00+08:00")
        current = match_entity_alias("名称丙", snapshot.aliases, at_time="2022-01-01T00:00:00+08:00")
        self.assertIs(before.status, AliasMatchStatus.UNIQUE)
        self.assertIs(after.status, AliasMatchStatus.NO_MATCH)
        self.assertIs(middle.status, AliasMatchStatus.UNIQUE)
        self.assertIs(current.status, AliasMatchStatus.UNIQUE)
        self.assertIs(match_entity_alias("名称甲", snapshot.aliases).status, AliasMatchStatus.NO_MATCH)

    def test_unknown_validity_boundaries_remain_none(self):
        snapshot = build_entity_catalog([_instrument()], observed_at=_NOW)
        for alias in snapshot.aliases:
            self.assertIsNone(alias.valid_from)
            self.assertIsNone(alias.valid_to)

    def test_current_name_without_changelog_has_only_instrument_info_provenance(self):
        snapshot = build_entity_catalog([_instrument()], observed_at=_NOW)
        current_name = next(
            alias
            for alias in snapshot.aliases
            if alias.alias_type is EntityAliasType.STOCK_SHORT_NAME
        )
        self.assertIsNone(current_name.valid_from)
        self.assertEqual(current_name.provenance_source_ids, ("instrument_info",))
        self.assertEqual(len(current_name.provenance_refs), 1)
        self.assertEqual(current_name.provenance_refs[0]["table"], "instrument_info")

    def test_current_name_with_changelog_boundary_preserves_both_provenances(self):
        snapshot = build_entity_catalog(
            [_instrument(name="新名称")],
            [_name_change("600519.SH", "2021-03-04", "旧名称", "新名称")],
            observed_at=_NOW,
        )
        current_name = next(
            alias
            for alias in snapshot.aliases
            if alias.alias_type is EntityAliasType.STOCK_SHORT_NAME
        )
        self.assertEqual(current_name.valid_from, datetime(2021, 3, 4, tzinfo=_TZ))
        self.assertEqual(
            current_name.provenance_source_ids,
            ("instrument_info", "instrument_changelog"),
        )
        refs_by_table = {ref["table"]: ref for ref in current_name.provenance_refs}
        self.assertEqual(set(refs_by_table), {"instrument_info", "instrument_changelog"})
        self.assertEqual(
            refs_by_table["instrument_changelog"]["time_semantics"],
            "system_observation_date",
        )
        self.assertEqual(
            refs_by_table["instrument_changelog"]["value_role"],
            "new_value",
        )

    def test_unique_status_is_candidate_count_not_complete_history_proof(self):
        snapshot = build_entity_catalog([_instrument()], observed_at=_NOW)
        result = match_entity_alias("贵州茅台", snapshot.aliases)
        matching_alias = result.matches[0]
        self.assertIs(result.status, AliasMatchStatus.UNIQUE)
        self.assertIsNone(matching_alias.valid_from)
        self.assertIsNone(matching_alias.valid_to)
        self.assertNotIn("history_complete", {field.name for field in fields(result)})

    def test_changelog_first_historical_start_is_unknown_and_end_is_observed_date(self):
        snapshot = build_entity_catalog(
            [_instrument(name="新名称")],
            [_name_change("600519.SH", "2021-03-04", "旧名称", "新名称")],
            observed_at=_NOW,
        )
        old_alias = next(a for a in snapshot.historical_aliases if a.alias == "旧名称")
        self.assertIsNone(old_alias.valid_from)
        self.assertEqual(old_alias.valid_to, datetime(2021, 3, 4, tzinfo=_TZ))
        self.assertEqual(old_alias.provenance_refs[0]["time_semantics"], "system_observation_date")
        self.assertTrue(any(d.code == "historical_boundary_is_observation_date" for d in snapshot.diagnostics))

    def test_insufficient_changelog_does_not_fabricate_history(self):
        snapshot = build_entity_catalog(
            [_instrument(name="新名称")],
            [_name_change("600519.SH", "2021-03-04", None, "新名称")],
            observed_at=_NOW,
        )
        self.assertEqual(snapshot.historical_aliases, ())
        self.assertTrue(any(d.code == "insufficient_name_changelog" for d in snapshot.diagnostics))

    def test_changelog_input_order_does_not_change_alias_ids(self):
        changes = [
            _name_change("600519.SH", "2020-01-01", "名称甲", "名称乙"),
            _name_change("600519.SH", "2021-01-01", "名称乙", "名称丙"),
        ]
        first = build_entity_catalog([_instrument(name="名称丙")], changes, observed_at=_NOW)
        second = build_entity_catalog([_instrument(name="名称丙")], reversed(changes), observed_at=_NOW)
        self.assertEqual(
            {a.entity_alias_id for a in first.aliases},
            {a.entity_alias_id for a in second.aliases},
        )

    def test_no_company_entity_is_invented_without_proven_issuer_semantics(self):
        snapshot = build_entity_catalog(
            [_instrument(CompanyName="贵州茅台股份有限公司")],
            observed_at=_NOW,
        )
        self.assertEqual(snapshot.company_entities, ())
        self.assertTrue(any(d.code == "unverified_company_field" for d in snapshot.diagnostics))

    def test_sector_stock_builds_only_read_only_crosswalk(self):
        rows = [
            {"sector_name": "GICS1信息技术", "stock_code": "600519.SH", "update_date": "2026-08-16"},
            {"sector_name": "TGN人工智能", "stock_code": "600519.SH", "update_date": "2026-08-16"},
            {"sector_name": "THY1银行", "stock_code": "000001.SZ", "update_date": "2026-08-16"},
        ]
        crosswalk = build_sector_crosswalk(rows)
        self.assertEqual(crosswalk.by_stock_code["600519.SH"], ("GICS1信息技术", "TGN人工智能"))
        self.assertEqual(crosswalk.by_stock_code["000001.SZ"], ("THY1银行",))
        self.assertEqual(crosswalk.classifications["TGN人工智能"].kind_hint, "concept")

    def test_sector_crosswalk_creates_no_industry_entity_or_relation(self):
        crosswalk = build_sector_crosswalk(
            [{"sector_name": "TFG大盘", "stock_code": "600519.SH"}]
        )
        field_names = {field.name for field in fields(crosswalk)}
        self.assertNotIn("industry_entities", field_names)
        self.assertNotIn("industry_relations", field_names)
        self.assertEqual(crosswalk.classifications["TFG大盘"].kind_hint, "style")

    def test_pure_functions_do_not_mutate_rows_collections_or_news_compat_fields(self):
        instruments = [_instrument(stocks=["legacy"], stock_codes=["canonical"])]
        changes = [_name_change("600519.SH", "2021-03-04", "旧名称", "贵州茅台")]
        sectors = [{"sector_name": "GICS1消费", "stock_code": "600519.SH", "stocks": ["x"]}]
        before = deepcopy((instruments, changes, sectors))
        build_entity_catalog(instruments, changes, observed_at=_NOW)
        build_sector_crosswalk(sectors)
        self.assertEqual((instruments, changes, sectors), before)

    def test_alias_normalizer_is_minimal_deterministic_nfkc_casefold(self):
        self.assertEqual(normalize_entity_alias(" ６００５１９．ＳＨ "), "600519.sh")
        self.assertEqual(normalize_entity_alias("  贵州\t茅台  "), "贵州 茅台")

    def test_entity_catalog_import_has_no_db_opensearch_or_network_side_effect(self):
        real_import = __import__

        def guarded_import(name, *args, **kwargs):
            lowered = name.lower()
            if any(token in lowered for token in ("psycopg2", "opensearch", "requests")):
                raise AssertionError(f"forbidden dependency import: {name}")
            return real_import(name, *args, **kwargs)

        module_path = (
            _PROJECT_ROOT / "data_collect" / "news_model" / "entity_catalog.py"
        )
        module_name = "_news_entity_catalog_import_probe"
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        with (
            patch("builtins.__import__", side_effect=guarded_import),
            patch("socket.create_connection", side_effect=AssertionError("network forbidden")),
        ):
            sys.modules[module_name] = module
            try:
                spec.loader.exec_module(module)
            finally:
                sys.modules.pop(module_name, None)

    def test_inspect_entities_default_is_dry_run_without_db_import(self):
        stdout = StringIO()
        real_import = __import__

        def guarded_import(name, *args, **kwargs):
            if name == "data_collect.utils.db" or name.startswith("psycopg2"):
                raise AssertionError("database import forbidden")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=guarded_import), redirect_stdout(stdout):
            exit_code = manage_news_foundation.main(["inspect-entities"])
        self.assertEqual(exit_code, 0)
        self.assertIn("no connection attempted", stdout.getvalue())

    def test_explicit_postgres_boundary_executes_only_read_statements(self):
        executed = []

        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, sql, params=()):
                self.sql = " ".join(sql.split())
                executed.append((self.sql, params))

            def fetchall(self):
                if "information_schema.columns" in self.sql:
                    return [
                        ("instrument_changelog", name)
                        for name in ("stock_code", "changed_at", "field_name", "old_value", "new_value")
                    ] + [
                        ("instrument_info", name)
                        for name in ("stock_code", "InstrumentName", "ExchangeID")
                    ] + [
                        ("sector_stock", name)
                        for name in ("sector_name", "stock_code", "update_date")
                    ]
                if 'FROM "instrument_info"' in self.sql:
                    return [("600519.SH", "贵州茅台", "SH")]
                if "FROM instrument_changelog" in self.sql:
                    return []
                if "FROM sector_stock" in self.sql:
                    return [("GICS1消费", "600519.SH", "2026-08-16")]
                return []

        class FakeConnection:
            def __init__(self):
                self.rollback_count = 0
                self.closed = False

            def cursor(self):
                return FakeCursor()

            def rollback(self):
                self.rollback_count += 1

            def close(self):
                self.closed = True

            def commit(self):
                raise AssertionError("read-only adapter must never commit")

        connection = FakeConnection()
        rows = load_shadow_inputs_from_postgres(
            limit=1,
            connection_factory=lambda: connection,
        )
        self.assertEqual(rows.instrument_rows[0]["stock_code"], "600519.SH")
        self.assertEqual(rows.sector_rows[0]["sector_name"], "GICS1消费")
        self.assertTrue(connection.closed)
        self.assertGreaterEqual(connection.rollback_count, 1)
        for statement, _ in executed:
            self.assertRegex(statement.upper(), r"^(SET TRANSACTION READ ONLY|SELECT )")

    def test_sql_does_not_change_legacy_tables(self):
        sql = _SQL_PATH.read_text(encoding="utf-8")
        statements = "\n".join(line.split("--", 1)[0] for line in sql.splitlines()).upper()
        self.assertNotRegex(statements, r"\bDROP\b")
        self.assertNotRegex(statements, r"\bALTER\b")
        for legacy_table in (
            "INSTRUMENT_INFO",
            "INSTRUMENT_CHANGELOG",
            "SECTOR_STOCK",
            "SECTOR_CHANGELOG",
        ):
            self.assertNotRegex(statements, rf"CREATE\s+TABLE[^;]*\b{legacy_table}\b")

    def test_sql_preserves_entity_alias_revision_and_history(self):
        raw_sql = _SQL_PATH.read_text(encoding="utf-8")
        sql = re.sub(r"\s+", " ", raw_sql.lower())
        alias_body = _sql_table_body(raw_sql, "news_entity_alias_revision")
        required_columns = (
            "entity_alias_id",
            "entity_id",
            "alias",
            "normalized_alias",
            "alias_type",
            "language",
            "valid_from",
            "valid_to",
            "provenance_refs",
            "confidence",
            "derived_by",
            "revision",
            "is_current",
            "manual_lock",
            "created_at",
            "updated_at",
        )
        for column in required_columns:
            self.assertIn(column, sql)
        self.assertRegex(alias_body, r"(?im)^\s*provenance_refs\s+JSONB\b")
        self.assertNotRegex(alias_body, r"(?im)^\s*provenance\s+")
        self.assertIn("primary key (entity_alias_id, revision)", sql)
        self.assertIn("is_latest_revision", sql)
        self.assertIn("where is_latest_revision", sql)
        self.assertIn("primary key (entity_id, entity_revision)", sql)

    def test_manual_lock_exists_only_on_entity_alias_sql(self):
        raw_sql = _SQL_PATH.read_text(encoding="utf-8")
        entity_body = _sql_table_body(raw_sql, "news_entity_revision")
        alias_body = _sql_table_body(raw_sql, "news_entity_alias_revision")
        self.assertNotRegex(entity_body, r"(?im)^\s*manual_lock\s+")
        self.assertRegex(alias_body, r"(?im)^\s*manual_lock\s+BOOLEAN\b")


if __name__ == "__main__":
    unittest.main()
