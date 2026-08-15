"""Tests for the local, side-effect-free V1.1 Source Catalog."""

from __future__ import annotations

import ast
from collections import Counter
from contextlib import redirect_stdout
from dataclasses import fields
from io import StringIO
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import manage_news_foundation
from data_collect.news_model.contracts import (
    AcquisitionType,
    AuthorityStatus,
    SourceHealth,
    SourceRecord,
)
from data_collect.news_model.source_catalog import (
    DEFAULT_GOVERNANCE_PATH,
    DEFAULT_SOURCES_PATH,
    MissingSourceGovernanceError,
    PRODUCTION_UNREGISTERED_SOURCE_IDS,
    SourceAcquisitionFact,
    SourceCatalogConflictError,
    SourceCatalogError,
    build_source_catalog_snapshot,
    get_source_record,
    load_registry_acquisition_facts,
    load_source_catalog,
)


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_REGISTERED_DISABLED = {
    "stats",
    "bbc_world",
    "bbc_business",
    "bbc_tech",
    "npr_news",
    "aljazeera",
}
_TEST_ONLY_SOURCE_IDS = {
    "ghost",
    "x",
    "typo_src",
    "nope",
    "eastmoney",
    "test_source",
    "mock_source",
}
def _assignment_target_names(node: ast.Assign | ast.AnnAssign) -> tuple[str, ...]:
    targets = node.targets if isinstance(node, ast.Assign) else (node.target,)
    return tuple(target.id for target in targets if isinstance(target, ast.Name))


def _discover_production_job_source_ids() -> set[str]:
    """Independently inspect the bounded source choices in production jobs."""

    source_ids = _scan_sources_yaml_ids()
    for path in sorted((_PROJECT_ROOT / "data_collect" / "jobs").glob("news_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                names = _assignment_target_names(node)
                value = node.value
                if any(name.endswith("SOURCE") for name in names):
                    if isinstance(value, ast.Constant) and isinstance(value.value, str):
                        source_ids.add(value.value)
                if path.name == "news_flash.py" and "_REQUIRED_COLUMNS" in names:
                    required_columns = ast.literal_eval(value)
                    source_ids.update(required_columns)
            if isinstance(node, ast.Dict):
                for key, value in zip(node.keys, node.values):
                    if (
                        isinstance(key, ast.Constant)
                        and key.value == "source"
                        and isinstance(value, ast.Constant)
                        and isinstance(value.value, str)
                    ):
                        source_ids.add(value.value)
    return source_ids


def _scan_sources_yaml_ids(path: Path = DEFAULT_SOURCES_PATH) -> set[str]:
    """Read source list IDs without relying on the Catalog parser under test."""

    source_ids: list[str] = []
    in_sources = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line == "sources:":
            in_sources = True
            continue
        if in_sources and line.startswith("  - id: "):
            source_ids.append(line.removeprefix("  - id: ").strip())
    if len(source_ids) != len(set(source_ids)):
        raise AssertionError("sources.yaml contains duplicate source IDs")
    return set(source_ids)


def _minimal_sources(source_id: str = "local_source") -> str:
    return f"""\
version: 1
defaults:
  enabled: true
sources:
  - id: {source_id}
    adapter: rss
    channel: media
    job: news_policy
    url: https://example.invalid/feed.xml
"""


def _minimal_governance(
    source_id: str = "local_source", *, include_source_name: bool = True
) -> str:
    name_line = "    source_name: Local Source\n" if include_source_name else ""
    return f"""\
version: 1
defaults:
  source_revision: 1
  authority_status: unrated
  source_authority: null
  authority_version: source_authority_v1
  authority_effective_from: "2026-08-16T00:00:00+08:00"
  content_license: unknown
  paywall_type: unknown
  created_at: "2026-08-16T00:00:00+08:00"
  updated_at: "2026-08-16T00:00:00+08:00"
sources:
  - source_id: {source_id}
{name_line}    source_category: other
    directness: unknown
    country_region_codes: ["CN"]
    languages: ["zh-CN"]
    source_timezone: Asia/Shanghai
    is_official: false
"""


class NewsSourceCatalogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.snapshot = build_source_catalog_snapshot()
        cls.catalog = dict(cls.snapshot.records)
        cls.registry_facts = load_registry_acquisition_facts()

    def test_every_actual_sources_yaml_id_resolves_to_source_record(self):
        registry_ids = _scan_sources_yaml_ids()
        loader_ids = {fact.source_id for fact in self.registry_facts}

        self.assertEqual(len(registry_ids), 47)
        self.assertEqual(loader_ids, registry_ids)
        self.assertTrue(registry_ids <= set(self.catalog))
        for source_id in registry_ids:
            self.assertIsInstance(self.catalog[source_id], SourceRecord)

    def test_every_production_news_job_source_id_resolves(self):
        discovered = _discover_production_job_source_ids()

        self.assertEqual(discovered, set(self.snapshot.production_source_ids))
        self.assertEqual(discovered - set(self.catalog), set())

    def test_production_source_and_catalog_difference_is_empty(self):
        self.assertEqual(self.snapshot.unresolved_source_ids, ())
        self.assertEqual(
            set(self.snapshot.production_source_ids) - set(self.snapshot.records), set()
        )
        self.assertEqual(self.snapshot.production_source_count, 51)
        self.assertEqual(self.snapshot.catalog_source_count, 51)

    def test_unregistered_production_sources_resolve(self):
        expected = {"cls", "em", "sina", "em_cjzc"}

        self.assertEqual(set(PRODUCTION_UNREGISTERED_SOURCE_IDS), expected)
        self.assertEqual(
            set(self.snapshot.unregistered_production_source_ids), expected
        )
        for source_id in expected:
            self.assertEqual(get_source_record(source_id).source_id, source_id)

    def test_disabled_registry_sources_remain_in_catalog(self):
        disabled_from_facts = {
            fact.source_id for fact in self.registry_facts if not fact.enabled
        }

        self.assertEqual(disabled_from_facts, _REGISTERED_DISABLED)
        for source_id in _REGISTERED_DISABLED:
            self.assertIn(source_id, self.catalog)
            self.assertIs(self.catalog[source_id].enabled, False)

    def test_every_unrated_source_has_no_numeric_authority(self):
        self.assertEqual(len(self.snapshot.unrated_source_ids), 51)
        for record in self.catalog.values():
            self.assertIs(record.authority_status, AuthorityStatus.UNRATED)
            self.assertIsNone(record.source_authority)

    def test_test_fixture_source_ids_are_not_in_production_catalog(self):
        self.assertEqual(set(self.catalog) & _TEST_ONLY_SOURCE_IDS, set())

    def test_source_ids_are_preserved_exactly_and_never_auto_renamed(self):
        for source_id, record in self.catalog.items():
            self.assertEqual(record.source_id, source_id)
        with self.assertRaisesRegex(SourceCatalogError, "never renames"):
            SourceAcquisitionFact(
                source_id="CLS",
                adapter="api",
                default_channel="flash",
                job="news_flash",
                endpoint_url=None,
                endpoint_route=None,
                enabled=True,
                registered_in_sources_yaml=False,
            )

    def test_conflicting_acquisition_fact_for_same_source_id_fails(self):
        conflict = SourceAcquisitionFact(
            source_id="cls",
            adapter="api",
            default_channel="media",
            job="news_flash",
            endpoint_url="https://www.cls.cn/v1/roll/get_roll_list",
            endpoint_route=None,
            enabled=True,
            registered_in_sources_yaml=False,
        )

        with self.assertRaisesRegex(
            SourceCatalogConflictError, "conflicting acquisition facts"
        ):
            build_source_catalog_snapshot(additional_facts=(conflict,))

    def test_duplicate_governance_source_id_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources_path = root / "sources.yaml"
            governance_path = root / "source_governance.yaml"
            sources_path.write_text(_minimal_sources(), encoding="utf-8")
            governance_path.write_text(
                _minimal_governance() + _minimal_governance().split("sources:\n", 1)[1],
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                SourceCatalogConflictError, "duplicate source_id"
            ):
                build_source_catalog_snapshot(
                    sources_path,
                    governance_path,
                    include_builtin_unregistered=False,
                )

    def test_source_record_contains_no_source_health_runtime_fields(self):
        source_record_fields = {field.name for field in fields(SourceRecord)}
        source_health_fields = {field.name for field in fields(SourceHealth)}
        shared_master_fields = {"source_id", "created_at"}

        self.assertEqual(
            source_record_fields & source_health_fields,
            shared_master_fields,
        )
        for record in self.catalog.values():
            self.assertFalse(hasattr(record, "health_status"))
            self.assertFalse(hasattr(record, "consecutive_failures"))

    def test_loading_catalog_has_no_database_opensearch_network_or_write_access(self):
        real_import = __import__
        forbidden_imports = {"psycopg2", "opensearchpy", "requests", "urllib"}

        def guarded_import(name, *args, **kwargs):
            if name.split(".", 1)[0] in forbidden_imports:
                raise AssertionError(f"forbidden external dependency import: {name}")
            return real_import(name, *args, **kwargs)

        sources_before = DEFAULT_SOURCES_PATH.read_bytes()
        governance_before = DEFAULT_GOVERNANCE_PATH.read_bytes()
        with (
            patch("builtins.__import__", side_effect=guarded_import),
            patch.object(Path, "write_text", side_effect=AssertionError("write forbidden")),
            patch.object(Path, "write_bytes", side_effect=AssertionError("write forbidden")),
            patch("socket.create_connection", side_effect=AssertionError("network forbidden")),
        ):
            loaded = load_source_catalog()

        self.assertEqual(len(loaded), 51)
        self.assertEqual(DEFAULT_SOURCES_PATH.read_bytes(), sources_before)
        self.assertEqual(DEFAULT_GOVERNANCE_PATH.read_bytes(), governance_before)

    def test_missing_required_governance_is_an_explicit_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources_path = root / "sources.yaml"
            governance_path = root / "source_governance.yaml"
            sources_path.write_text(_minimal_sources(), encoding="utf-8")
            governance_path.write_text(
                _minimal_governance(include_source_name=False), encoding="utf-8"
            )

            with self.assertRaisesRegex(
                MissingSourceGovernanceError, "source_name"
            ):
                build_source_catalog_snapshot(
                    sources_path,
                    governance_path,
                    include_builtin_unregistered=False,
                )

    def test_source_identity_governance_cannot_be_hidden_in_defaults(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources_path = root / "sources.yaml"
            governance_path = root / "source_governance.yaml"
            sources_path.write_text(_minimal_sources(), encoding="utf-8")
            governance = _minimal_governance(include_source_name=False).replace(
                "  source_revision: 1\n",
                "  source_revision: 1\n  source_name: Fabricated Default\n",
            )
            governance_path.write_text(governance, encoding="utf-8")

            with self.assertRaisesRegex(SourceCatalogError, "source_name"):
                build_source_catalog_snapshot(
                    sources_path,
                    governance_path,
                    include_builtin_unregistered=False,
                )

    def test_orphan_governance_source_is_not_silently_added(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources_path = root / "sources.yaml"
            governance_path = root / "source_governance.yaml"
            sources_path.write_text(_minimal_sources(), encoding="utf-8")
            orphan_entry = _minimal_governance("fixture_only").split(
                "sources:\n", 1
            )[1]
            governance_path.write_text(
                _minimal_governance() + orphan_entry, encoding="utf-8"
            )

            with self.assertRaisesRegex(
                SourceCatalogConflictError,
                "non-production source_ids.*fixture_only",
            ):
                build_source_catalog_snapshot(
                    sources_path,
                    governance_path,
                    include_builtin_unregistered=False,
                )

    def test_health_fields_in_governance_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources_path = root / "sources.yaml"
            governance_path = root / "source_governance.yaml"
            sources_path.write_text(_minimal_sources(), encoding="utf-8")
            governance = _minimal_governance().replace(
                "    is_official: false\n",
                "    is_official: false\n    health_status: healthy\n",
            )
            governance_path.write_text(governance, encoding="utf-8")

            with self.assertRaisesRegex(SourceCatalogError, "health_status"):
                build_source_catalog_snapshot(
                    sources_path,
                    governance_path,
                    include_builtin_unregistered=False,
                )

    def test_adapter_facts_map_to_frozen_acquisition_types(self):
        counts = Counter(record.acquisition_type for record in self.catalog.values())

        self.assertEqual(counts[AcquisitionType.RSS], 18)
        self.assertEqual(counts[AcquisitionType.RSSHUB], 23)
        self.assertEqual(counts[AcquisitionType.WEB], 3)
        self.assertEqual(counts[AcquisitionType.AKSHARE], 5)
        self.assertEqual(counts[AcquisitionType.API], 2)

    def test_config_selectable_flash_source_is_schedulable_not_disabled(self):
        self.assertIs(self.catalog["sina"].enabled, True)
        self.assertIs(self.snapshot.acquisition_facts["sina"].enabled, True)

    def test_code_and_registry_endpoints_are_kept_without_runtime_resolution(self):
        facts = self.snapshot.acquisition_facts

        self.assertIsNone(facts["ndrc"].endpoint_url)
        self.assertEqual(facts["ndrc"].endpoint_route, "/gov/ndrc/xwdt")
        self.assertEqual(facts["ndrc"].endpoint_locator, "/gov/ndrc/xwdt")
        self.assertEqual(
            facts["csrc"].endpoint_url,
            "http://www.csrc.gov.cn/csrc/c100028/common_list.shtml",
        )
        self.assertEqual(
            facts["cninfo"].endpoint_url,
            "http://www.cninfo.com.cn/new/hisAnnouncement/query",
        )
        self.assertEqual(
            facts["cls"].endpoint_url,
            "https://www.cls.cn/v1/roll/get_roll_list",
        )

    def test_validate_sources_command_runs_locally_and_reports_required_counts(self):
        stdout = StringIO()
        with (
            patch("socket.create_connection", side_effect=AssertionError("network forbidden")),
            redirect_stdout(stdout),
        ):
            exit_code = manage_news_foundation.main(["validate-sources"])

        output = stdout.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("catalog sources: 51", output)
        self.assertIn("sources.yaml sources: 47", output)
        self.assertIn("unregistered production sources (4)", output)
        self.assertIn("unrated sources: 51", output)
        self.assertIn("unresolved source_ids (0): none", output)
        self.assertIn("configuration conflicts: 0", output)
        self.assertIn("read-only/dry-run", output)


if __name__ == "__main__":
    unittest.main()
