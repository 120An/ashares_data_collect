"""Unit tests for the side-effect-free Phase 1 news compatibility layer."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest

from data_collect.news_model.compat import (
    MismatchType,
    MissingCompatibilityFieldError,
    NEWS_DOCUMENT_SCHEMA_VERSION,
    NewsCompatibilityError,
    NewsIdentityMismatchError,
    build_compatibility_projection,
    read_canonical_news,
)
from data_collect.news_model.contracts import PublishTimePrecision


def _legacy_news(**overrides):
    document = {
        "_id": "cls-20260815-001",
        "pub_time": "2026-08-15 09:30:00",
        "fetch_time": "2026-08-15 09:31:00",
        "source": "cls",
        "stocks": ["600519.SH"],
        "title": "示例快讯",
    }
    document.update(overrides)
    return document


def _canonical_news(**overrides):
    document = {
        "news_id": "cls-20260815-001",
        "publish_time": "2026-08-15T09:30:00+08:00",
        "collect_time": "2026-08-15T09:31:00+08:00",
        "source_id": "cls",
        "stock_codes": ["600519.SH"],
        "publish_time_precision": "minute",
        "title": "示例快讯",
    }
    document.update(overrides)
    return document


class NewsModelCompatibilityTests(unittest.TestCase):
    def test_legacy_only_document_can_be_read(self):
        view = read_canonical_news(_legacy_news())

        self.assertEqual(view.news_id, "cls-20260815-001")
        self.assertEqual(view.publish_time.isoformat(), "2026-08-15T09:30:00+08:00")
        self.assertEqual(view.collect_time.isoformat(), "2026-08-15T09:31:00+08:00")
        self.assertEqual(view.source_id, "cls")
        self.assertEqual(view.stock_codes, ("600519.SH",))
        self.assertFalse(view.has_mismatches)

    def test_canonical_only_document_can_be_read(self):
        view = read_canonical_news(_canonical_news())

        self.assertEqual(view.news_id, "cls-20260815-001")
        self.assertEqual(view.source_id, "cls")
        self.assertEqual(view.stock_codes, ("600519.SH",))
        self.assertIs(view.publish_time_precision, PublishTimePrecision.MINUTE)
        self.assertFalse(view.has_mismatches)

    def test_matching_legacy_and_canonical_fields_have_no_mismatch(self):
        document = _legacy_news(
            news_id="cls-20260815-001",
            publish_time="2026-08-15T09:30:00+08:00",
            collect_time="2026-08-15T09:31:00+08:00",
            source_id="cls",
            stock_codes=["600519.SH"],
        )

        view = read_canonical_news(document, hit_id="cls-20260815-001")

        self.assertEqual(view.mismatches, ())

    def test_equivalent_utc_and_beijing_times_have_no_mismatch(self):
        document = _legacy_news(
            publish_time="2026-08-15T01:30:00Z",
            collect_time="2026-08-15T01:31:00Z",
        )

        view = read_canonical_news(document)

        self.assertEqual(view.publish_time.utcoffset(), timedelta(0))
        self.assertEqual(view.mismatches, ())

    def test_different_publish_instants_prefer_canonical_and_report_mismatch(self):
        document = _legacy_news(publish_time="2026-08-15T01:31:00Z")
        before = deepcopy(document)

        view = read_canonical_news(document)

        self.assertEqual(view.publish_time, datetime(2026, 8, 15, 1, 31, tzinfo=timezone.utc))
        self.assertEqual(document, before)
        self.assertEqual(len(view.mismatches), 1)
        mismatch = view.mismatches[0]
        self.assertEqual(mismatch.field_name, "publish_time")
        self.assertIs(mismatch.mismatch_type, MismatchType.TIME_INSTANT_MISMATCH)
        self.assertEqual(mismatch.legacy_value, "2026-08-15 09:30:00")
        self.assertEqual(mismatch.canonical_value, "2026-08-15T01:31:00Z")

    def test_different_collect_instants_use_the_same_diagnostic_rules(self):
        document = _legacy_news(collect_time="2026-08-15T01:32:00Z")

        view = read_canonical_news(document)

        self.assertEqual(
            view.collect_time, datetime(2026, 8, 15, 1, 32, tzinfo=timezone.utc)
        )
        self.assertEqual(len(view.mismatches), 1)
        self.assertEqual(view.mismatches[0].field_name, "collect_time")
        self.assertIs(
            view.mismatches[0].mismatch_type,
            MismatchType.TIME_INSTANT_MISMATCH,
        )

    def test_source_mismatch_prefers_source_id_and_reports_diagnostic(self):
        document = _legacy_news(source="em", source_id="cls")

        view = read_canonical_news(document)

        self.assertEqual(view.source_id, "cls")
        self.assertEqual(len(view.mismatches), 1)
        mismatch = view.mismatches[0]
        self.assertEqual(mismatch.field_name, "source_id")
        self.assertEqual(mismatch.legacy_value, "em")
        self.assertEqual(mismatch.new_value, "cls")

    def test_stock_mismatch_prefers_stock_codes_without_merging(self):
        document = _legacy_news(
            stocks=["000001.SZ"],
            stock_codes=["600519.SH"],
        )

        view = read_canonical_news(document)

        self.assertEqual(view.stock_codes, ("600519.SH",))
        self.assertNotIn("000001.SZ", view.stock_codes)
        self.assertEqual(len(view.mismatches), 1)
        self.assertEqual(view.mismatches[0].field_name, "stock_codes")
        self.assertIn("without merging", view.mismatches[0].message)

    def test_news_id_mismatch_with_hit_or_embedded_id_is_a_hard_error(self):
        cases = (
            (_canonical_news(_id="different-id"), None),
            (_canonical_news(), "different-id"),
            (_legacy_news(), "different-id"),
        )
        for document, hit_id in cases:
            with self.subTest(document=document, hit_id=hit_id):
                with self.assertRaisesRegex(
                    NewsIdentityMismatchError, "identity mismatch"
                ):
                    read_canonical_news(document, hit_id=hit_id)

    def test_missing_identity_is_not_generated(self):
        document = _legacy_news()
        del document["_id"]

        with self.assertRaisesRegex(MissingCompatibilityFieldError, "never generates"):
            read_canonical_news(document)

    def test_identity_is_never_silently_rewritten(self):
        document = _legacy_news(_id=" cls-20260815-001 ")

        with self.assertRaisesRegex(NewsCompatibilityError, "does not rewrite"):
            read_canonical_news(document)

    def test_external_hit_id_can_supply_missing_document_identity(self):
        document = _legacy_news()
        del document["_id"]

        view = read_canonical_news(document, hit_id="cls-20260815-001")

        self.assertEqual(view.news_id, "cls-20260815-001")

    def test_projection_never_mutates_input_including_nested_values(self):
        document = _legacy_news(metadata={"tags": ["original"]})
        before = deepcopy(document)

        projected = build_compatibility_projection(document)
        projected["metadata"]["tags"].append("projected-only")

        self.assertEqual(document, before)

    def test_projection_preserves_legacy_fields_and_adds_canonical_fields(self):
        document = _legacy_news(time_estimated=True)

        projected = build_compatibility_projection(document)

        for field_name in ("_id", "pub_time", "fetch_time", "source", "stocks"):
            self.assertEqual(projected[field_name], document[field_name])
        self.assertEqual(projected["news_id"], document["_id"])
        self.assertEqual(projected["schema_version"], NEWS_DOCUMENT_SCHEMA_VERSION)
        self.assertEqual(projected["publish_time"], "2026-08-15T09:30:00+08:00")
        self.assertEqual(projected["collect_time"], "2026-08-15T09:31:00+08:00")
        self.assertEqual(projected["source_id"], "cls")
        self.assertEqual(projected["stock_codes"], ["600519.SH"])
        self.assertIs(projected["publish_time_is_estimated"], True)

    def test_stocks_projection_is_an_independent_non_destructive_mirror(self):
        document = _legacy_news(stocks=["600519.SH", "000001.SZ", "920001.BJ"])

        projected = build_compatibility_projection(document)
        projected["stock_codes"].append("688001.SH")

        self.assertEqual(
            document["stocks"], ["600519.SH", "000001.SZ", "920001.BJ"]
        )
        self.assertEqual(
            projected["stock_codes"],
            ["600519.SH", "000001.SZ", "920001.BJ", "688001.SH"],
        )

    def test_legacy_time_is_interpreted_as_timezone_aware_beijing_time(self):
        view = read_canonical_news(_legacy_news())

        self.assertIsNotNone(view.publish_time.tzinfo)
        self.assertEqual(view.publish_time.utcoffset(), timedelta(hours=8))
        self.assertEqual(
            view.publish_time.astimezone(timezone.utc),
            datetime(2026, 8, 15, 1, 30, tzinfo=timezone.utc),
        )

    def test_legacy_seconds_do_not_imply_second_precision(self):
        view = read_canonical_news(_legacy_news())
        projected = build_compatibility_projection(_legacy_news())

        self.assertIs(view.publish_time_precision, PublishTimePrecision.UNKNOWN)
        self.assertEqual(projected["publish_time_precision"], "unknown")

    def test_estimated_flag_is_only_added_when_explicitly_available(self):
        without_evidence = build_compatibility_projection(_legacy_news())
        with_legacy_evidence = build_compatibility_projection(
            _legacy_news(time_estimated=False)
        )

        self.assertNotIn("publish_time_is_estimated", without_evidence)
        self.assertIs(with_legacy_evidence["publish_time_is_estimated"], False)

    def test_projection_does_not_fabricate_out_of_scope_fields(self):
        projected = build_compatibility_projection(_legacy_news())

        for field_name in (
            "raw_archive_uri",
            "source_authority",
            "canonical_url",
            "event_id",
            "importance",
            "sentiment",
        ):
            self.assertNotIn(field_name, projected)


if __name__ == "__main__":
    unittest.main()
