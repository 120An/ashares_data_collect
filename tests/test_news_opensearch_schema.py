"""Offline tests for the Phase 1 OpenSearch schema and additive diff rules."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import unittest

from data_collect.news_model import opensearch_schema as schema


def _legacy_news_body(analyzer: str = "smartcn") -> dict:
    """The currently deployed ``news-{year}`` template, rendered offline."""

    return {
        "settings": {
            "index.knn": True,
            "number_of_shards": 1,
            "number_of_replicas": 0,
        },
        "mappings": {
            "_meta": {
                "analyzer": analyzer,
                "embedding_model": "BAAI/bge-m3",
            },
            "properties": {
                "title": {
                    "type": "text",
                    "analyzer": analyzer,
                    "fields": {
                        "raw": {"type": "keyword", "ignore_above": 256}
                    },
                },
                "content": {"type": "text", "analyzer": analyzer},
                "summary": {"type": "text"},
                "pub_time": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyyMMdd||epoch_millis",
                },
                "fetch_time": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyyMMdd||epoch_millis",
                },
                "source": {"type": "keyword"},
                "channel": {"type": "keyword"},
                "url": {"type": "keyword"},
                "stocks": {"type": "keyword"},
                "vec_status": {"type": "keyword"},
                "ann_type": {"type": "keyword"},
                "pdf_status": {"type": "keyword"},
                "body_status": {"type": "keyword"},
                "body": {"type": "text", "analyzer": analyzer},
                "content_vec": {
                    "type": "knn_vector",
                    "dimension": 1024,
                    "method": {
                        "name": "hnsw",
                        "engine": "lucene",
                        "space_type": "cosinesimil",
                        "parameters": {"m": 16, "ef_construction": 128},
                    },
                },
            },
        },
    }


class NewsYearMappingTests(unittest.TestCase):
    def test_phase1_fields_are_additive_and_explicit(self):
        existing = _legacy_news_body()
        diff = schema.news_year_additive_diff(existing)

        self.assertTrue(diff.is_additive_compatible)
        self.assertEqual(diff.incompatible_changes, ())
        self.assertEqual(
            {change.path.rsplit(".", 1)[-1] for change in diff.additive_changes},
            set(schema.PHASE1_NEWS_ADDITIVE_PROPERTIES),
        )

    def test_same_name_different_type_is_rejected(self):
        existing = _legacy_news_body()
        existing["mappings"]["properties"]["source_id"] = {"type": "text"}

        with self.assertRaisesRegex(
            schema.MappingCompatibilityError, "field type differs"
        ):
            schema.build_news_year_target_mapping(existing)

    def test_analyzer_change_is_rejected(self):
        existing = _legacy_news_body("smartcn")
        target = deepcopy(existing)
        target["mappings"]["properties"]["title"]["analyzer"] = "standard"

        diff = schema.diff_index_mappings(existing, target)
        self.assertFalse(diff.is_additive_compatible)
        self.assertTrue(any("analyzer" in item.path for item in diff.incompatible_changes))
        with self.assertRaises(schema.MappingCompatibilityError):
            diff.require_compatible()

    def test_adding_analyzer_to_existing_field_is_rejected(self):
        existing = _legacy_news_body()
        target = deepcopy(existing)
        target["mappings"]["properties"]["summary"]["analyzer"] = "standard"

        with self.assertRaises(schema.MappingCompatibilityError):
            schema.require_additive_mapping(existing, target)

    def test_content_vector_dimension_change_is_rejected(self):
        existing = _legacy_news_body()
        target = deepcopy(existing)
        target["mappings"]["properties"]["content_vec"]["dimension"] = 768

        diff = schema.diff_index_mappings(existing, target)
        self.assertTrue(any(
            item.reason == "vector dimension would change"
            for item in diff.incompatible_changes
        ))

    def test_settings_and_bm25_behavior_cannot_change(self):
        existing = _legacy_news_body()
        target = deepcopy(existing)
        target["settings"]["index.similarity"] = {
            "default": {"type": "BM25", "k1": 1.4}
        }

        with self.assertRaisesRegex(
            schema.MappingCompatibilityError, "mapping contains incompatible"
        ):
            schema.require_additive_mapping(existing, target)

    def test_target_keeps_legacy_mapping_and_input_unchanged(self):
        existing = _legacy_news_body()
        before = deepcopy(existing)
        target = schema.build_news_year_target_mapping(existing)

        self.assertEqual(existing, before)
        for name, definition in before["mappings"]["properties"].items():
            self.assertEqual(target["mappings"]["properties"][name], definition)
        self.assertEqual(target["settings"], before["settings"])
        self.assertEqual(
            target["mappings"]["properties"]["content_vec"]["dimension"], 1024
        )

    def test_old_search_compatibility_fields_remain(self):
        target = schema.build_news_year_target_mapping(_legacy_news_body())
        properties = target["mappings"]["properties"]
        for field_name in (
            "title", "content", "pub_time", "fetch_time", "source", "stocks",
            "channel", "content_vec",
        ):
            self.assertIn(field_name, properties)
        self.assertNotIn("news-documents-v1", json.dumps(target))
        self.assertEqual(schema.NEWS_INDEX_PATTERN, "news-{year}")

    def test_additive_patch_contains_only_missing_new_fields(self):
        existing = _legacy_news_body()
        existing["mappings"]["properties"]["news_id"] = {"type": "keyword"}
        patch = schema.build_additive_mapping_patch(existing)

        self.assertNotIn("news_id", patch["properties"])
        self.assertEqual(
            set(patch["properties"]),
            set(schema.PHASE1_NEWS_ADDITIVE_PROPERTIES) - {"news_id"},
        )
        self.assertNotIn("settings", patch)

    def test_single_index_get_mapping_response_is_supported(self):
        response = {"news-2026": {"mappings": _legacy_news_body()["mappings"]}}
        diff = schema.news_year_additive_diff(response)
        self.assertTrue(diff.is_additive_compatible)
        self.assertEqual(len(diff.additive_changes), 8)

    def test_mapping_only_shape_is_supported(self):
        mapping = deepcopy(_legacy_news_body()["mappings"])
        target = schema.build_news_year_target_mapping(mapping)
        self.assertIn("news_id", target["mappings"]["properties"])

    def test_no_future_phase_business_fields_are_introduced(self):
        serialized = json.dumps(
            {
                "news": schema.PHASE1_NEWS_ADDITIVE_PROPERTIES,
                "standalone": schema.build_phase1_standalone_mappings(),
            }
        )
        for forbidden in (
            "event_id", "importance_score", "sentiment", "impact_strength",
            "stock_relations", "industry_relations",
        ):
            self.assertNotIn(forbidden, serialized)


class StandaloneObjectMappingTests(unittest.TestCase):
    def test_expected_standalone_index_names(self):
        self.assertEqual(
            set(schema.build_phase1_standalone_mappings()),
            {
                "news-sources-v1",
                "news-source-health-current-v1",
                "news-entities-v1",
                "news-entity-aliases-v1",
            },
        )

    def test_standalone_roots_are_strict(self):
        for body in schema.build_phase1_standalone_mappings().values():
            self.assertEqual(body["mappings"]["dynamic"], "strict")

    def test_unstable_objects_never_create_dynamic_fields(self):
        mappings = schema.build_phase1_standalone_mappings()
        checks = {
            schema.ENTITY_INDEX: "external_ids",
            schema.ENTITY_ALIAS_INDEX: "provenance_refs",
            schema.SOURCE_HEALTH_INDEX: "completeness_metrics",
        }
        for index_name, field_name in checks.items():
            definition = mappings[index_name]["mappings"]["properties"][field_name]
            self.assertEqual(definition["type"], "object")
            self.assertIs(definition["enabled"], False)
            self.assertIs(definition["dynamic"], False)

    def test_source_health_is_not_mixed_into_source(self):
        source_properties = schema.build_source_mapping()["mappings"]["properties"]
        health_properties = schema.build_source_health_mapping()["mappings"]["properties"]
        health_only = {
            "source_health_id", "health_status", "last_success_at", "last_attempt_at",
            "consecutive_failures", "latency_ms", "last_item_publish_time",
            "data_delay_seconds", "completeness_metrics",
        }
        self.assertTrue(health_only <= set(health_properties))
        self.assertTrue(health_only.isdisjoint(source_properties))

    def test_entity_alias_has_explicit_query_fields(self):
        properties = schema.build_entity_alias_mapping()["mappings"]["properties"]
        expected = {
            "normalized_alias": "keyword",
            "alias_type": "keyword",
            "entity_id": "keyword",
            "valid_from": "date",
            "valid_to": "date",
            "is_current": "boolean",
        }
        self.assertEqual(
            {name: properties[name]["type"] for name in expected}, expected
        )

    def test_builders_return_independent_copies(self):
        first = schema.build_entity_mapping()
        first["mappings"]["properties"]["entity_id"]["type"] = "text"
        second = schema.build_entity_mapping()
        self.assertEqual(
            second["mappings"]["properties"]["entity_id"]["type"], "keyword"
        )


class ImportSafetyTests(unittest.TestCase):
    def test_import_has_no_opensearch_or_network_dependency(self):
        workspace = Path(__file__).resolve().parents[1]
        script = r'''
import builtins
import socket
real_import = builtins.__import__
def guarded_import(name, *args, **kwargs):
    if name == "opensearchpy" or name.startswith("opensearchpy."):
        raise AssertionError("opensearchpy imported")
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded_import
socket.socket = lambda *a, **k: (_ for _ in ()).throw(AssertionError("network used"))
import data_collect.news_model.opensearch_schema as module
assert module.NEWS_INDEX_PATTERN == "news-{year}"
'''
        result = subprocess.run(
            [sys.executable, "-B", "-c", script],
            cwd=workspace,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
