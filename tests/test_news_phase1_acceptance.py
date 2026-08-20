"""Phase 1 cross-component acceptance tests with local historical fixtures only."""

from __future__ import annotations

import copy
import gzip
import json

from data_collect.news_model.compat import read_canonical_news
from data_collect.utils import news_archive
from data_collect.utils import opensearch_utils as osu


def test_legacy_archive_replay_to_dual_action_is_non_destructive(
    tmp_path, monkeypatch
):
    """Legacy gzip replay can be dual-projected without rewriting history."""

    archive_root = tmp_path / "archive"
    spool_root = tmp_path / "spool"
    monkeypatch.setattr(news_archive, "_is_posix", lambda: False)
    monkeypatch.setattr(
        news_archive,
        "get_news_config",
        lambda: {
            "archive_base": str(archive_root),
            "spool_dir": str(spool_root),
        },
    )

    legacy_envelope = {
        "_id": "legacy-cls-20260815-001",
        "title": "历史新闻标题",
        "content": "历史新闻原文",
        "pub_time": "2026-08-15 09:30:00",
        "fetch_time": "2026-08-15 09:31:00",
        "source": "cls",
        "channel": "flash",
        "url": "https://example.invalid/legacy/001",
        "stocks": ["600519.SH"],
        "raw_content": {"provider": "legacy-fixture", "sequence": [1, 2]},
    }
    original_envelope = copy.deepcopy(legacy_envelope)
    archive_path = news_archive.archive_path("cls", "20260815")
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(archive_path, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(legacy_envelope, ensure_ascii=False) + "\n")
    original_archive_bytes = archive_path.read_bytes()

    replayed = list(news_archive.replay("cls", ("20260815", "20260815")))
    assert replayed == [original_envelope]
    canonical = read_canonical_news(replayed[0], hit_id=replayed[0]["_id"])
    assert canonical.news_id == legacy_envelope["_id"]
    assert canonical.source_id == "cls"
    assert canonical.stock_codes == ("600519.SH",)
    assert canonical.publish_time.isoformat() == "2026-08-15T09:30:00+08:00"
    assert canonical.collect_time.isoformat() == "2026-08-15T09:31:00+08:00"
    assert not canonical.has_mismatches

    action, = osu._build_actions(
        replayed,
        compatibility_mode=osu.WRITE_MODE_DUAL,
    )
    projected = action["_source"]
    projected_view = read_canonical_news(projected, hit_id=action["_id"])

    assert action["_op_type"] == "create"
    assert action["_index"] == "news-2026"
    assert action["_id"] == projected["news_id"] == legacy_envelope["_id"]
    assert projected["pub_time"] == legacy_envelope["pub_time"]
    assert projected["fetch_time"] == legacy_envelope["fetch_time"]
    assert projected["source"] == projected["source_id"] == "cls"
    assert projected["stocks"] == projected["stock_codes"] == ["600519.SH"]
    assert not projected_view.has_mismatches

    # The compatibility projection is deliberately not an AI/enrichment pass.
    forbidden_phase2_or_backfill_fields = {
        "event_id",
        "importance",
        "importance_level",
        "sentiment",
        "impact",
        "stock_relations",
        "industry_relations",
        "raw_archive_uri",
        "body",
    }
    assert forbidden_phase2_or_backfill_fields.isdisjoint(projected)

    assert legacy_envelope == original_envelope
    assert replayed == [original_envelope]
    assert archive_path.read_bytes() == original_archive_bytes

