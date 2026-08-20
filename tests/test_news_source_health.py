"""Pure unit tests for Step 8 SourceHealth shadow observations."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from data_collect.news_model.contracts import CompletenessStatus, HealthStatus
from data_collect.news_model import source_health as sh


_T0 = datetime(2026, 8, 16, 1, 0, tzinfo=timezone.utc)
_T1 = _T0 + timedelta(hours=1)
_T2 = _T0 + timedelta(hours=2)
_JOB_RUN_ID = sh.make_job_run_id("news_policy", _T0)


def _collect(
    outcome: sh.ObservationOutcome,
    *,
    source_id: str = "govcn_policy",
    started_at: datetime = _T0,
    finished_at: datetime = _T1,
    collected: int | None = None,
    new: int | None = None,
    empty: bool | None = None,
    parse_failures: int | None = None,
    last_publish: datetime | None = None,
    error_code: str | None = None,
) -> sh.SourceObservation:
    return sh.SourceObservation(
        job_run_id=_JOB_RUN_ID,
        source_id=source_id,
        observation_type=sh.ObservationType.COLLECT,
        started_at=started_at,
        finished_at=finished_at,
        attempt_no=1,
        outcome=outcome,
        collected_item_count=collected,
        new_item_count=new,
        empty_success=empty,
        parse_failure_count=parse_failures,
        last_item_publish_time=last_publish,
        error_code=error_code,
        error_summary="short failure" if error_code else None,
    )


def _snapshot(
    observations,
    *,
    start=_T0,
    end=_T2,
    observed=None,
    previous=None,
    source_id="govcn_policy",
):
    return sh.build_source_health(
        source_id=source_id,
        observations=observations,
        window_start=start,
        window_end=end,
        observed_at=observed or end,
        previous_snapshot=previous,
    )


class SourceObservationTests(unittest.TestCase):
    def test_observation_times_must_be_timezone_aware(self):
        with self.assertRaisesRegex(ValueError, "时区"):
            sh.SourceObservation(
                job_run_id=_JOB_RUN_ID,
                source_id="govcn_policy",
                observation_type="collect",
                started_at=datetime(2026, 8, 16, 9, 0),
                finished_at=_T1,
                attempt_no=1,
                outcome="success",
            )

    def test_observation_id_is_stable_for_the_same_facts(self):
        first = _collect(
            sh.ObservationOutcome.SUCCESS,
            collected=2,
            new=1,
            empty=False,
            parse_failures=0,
        )
        second = _collect(
            sh.ObservationOutcome.SUCCESS,
            collected=2,
            new=1,
            empty=False,
            parse_failures=0,
        )
        same_instants_different_offsets = sh.SourceObservation(
            job_run_id=sh.make_job_run_id(
                "news_policy", _T0.astimezone(timezone(timedelta(hours=8)))
            ),
            source_id="govcn_policy",
            observation_type="collect",
            started_at=_T0.astimezone(timezone(timedelta(hours=8))),
            finished_at=_T1.astimezone(timezone(timedelta(hours=8))),
            attempt_no=1,
            outcome="success",
            collected_item_count=2,
            new_item_count=1,
            empty_success=False,
            parse_failure_count=0,
        )
        self.assertEqual(first.observation_id, second.observation_id)
        self.assertEqual(first.observation_id, same_instants_different_offsets.observation_id)
        self.assertTrue(first.observation_id.startswith("obs_"))

    def test_task_observation_allows_no_source_without_inventing_one(self):
        item = sh.SourceObservation(
            job_run_id=_JOB_RUN_ID,
            source_id=None,
            observation_type="task",
            started_at=_T0,
            finished_at=_T1,
            attempt_no=1,
            outcome="failure",
            error_code="task_error",
        )
        self.assertIsNone(item.source_id)

    def test_collect_and_verify_observations_require_a_real_source(self):
        for observation_type in ("collect", "verify"):
            with self.subTest(observation_type=observation_type):
                with self.assertRaisesRegex(ValueError, "source_id"):
                    sh.SourceObservation(
                        job_run_id=_JOB_RUN_ID,
                        source_id=None,
                        observation_type=observation_type,
                        started_at=_T0,
                        finished_at=_T1,
                        attempt_no=1,
                        outcome="success",
                    )

    def test_latest_publish_time_uses_real_document_times_without_mutation(self):
        documents = [
            {"pub_time": "2026-08-16 09:00:00", "nested": {"x": 1}},
            {"publish_time": "2026-08-16T02:00:00Z"},
        ]
        before = deepcopy(documents)
        latest = sh.latest_item_publish_time(documents)
        self.assertEqual(latest, datetime(2026, 8, 16, 2, 0, tzinfo=timezone.utc))
        self.assertEqual(documents, before)

    def test_latest_publish_time_accepts_govcn_gwy_date_precision_envelope(self):
        documents = [{
            "source": "govcn_gwy",
            "pub_time": "2026-08-18 00:00:00",
            "time_estimated": True,
        }]

        latest = sh.latest_item_publish_time(documents)

        self.assertEqual(
            latest,
            datetime(2026, 8, 18, 0, 0, tzinfo=timezone(timedelta(hours=8))),
        )

    def test_error_classification_is_conservative_and_summary_is_bounded(self):
        self.assertEqual(sh.classify_error(TimeoutError("late")), "timeout")
        self.assertEqual(
            sh.classify_error(RuntimeError("boom"), task_level=True),
            "task_error",
        )
        self.assertEqual(sh.classify_error(ValueError("bad")), "unknown_error")
        self.assertLessEqual(len(sh.error_summary(RuntimeError("x" * 1000))), 500)


class SourceHealthAggregationTests(unittest.TestCase):
    def test_source_health_id_is_stable_and_window_sensitive(self):
        first = sh.make_source_health_id("govcn_policy", _T0, _T1)
        second = sh.make_source_health_id("govcn_policy", _T0, _T1)
        equivalent = sh.make_source_health_id(
            "govcn_policy",
            _T0.astimezone(timezone(timedelta(hours=8))),
            _T1.astimezone(timezone(timedelta(hours=8))),
        )
        other = sh.make_source_health_id("govcn_policy", _T0, _T2)
        self.assertEqual(first, second)
        self.assertEqual(first, equivalent)
        self.assertNotEqual(first, other)

    def test_success_updates_last_success_and_real_counts(self):
        observation = _collect(
            sh.ObservationOutcome.SUCCESS,
            collected=3,
            new=2,
            empty=False,
            parse_failures=0,
            last_publish=_T0,
        )
        snapshot = _snapshot([observation])
        self.assertEqual(snapshot.last_success_at, _T1)
        self.assertEqual(snapshot.last_attempt_at, _T0)
        self.assertEqual(snapshot.attempt_count, 1)
        self.assertEqual(snapshot.success_count, 1)
        self.assertEqual(snapshot.collected_item_count, 3)
        self.assertEqual(snapshot.new_item_count, 2)
        self.assertEqual(snapshot.parse_failure_count, 0)
        self.assertEqual(snapshot.data_delay_seconds, 7200)

    def test_failed_collect_does_not_replace_previous_success(self):
        success = _collect(
            sh.ObservationOutcome.SUCCESS,
            finished_at=_T1,
            collected=1,
            new=1,
            empty=False,
            parse_failures=0,
        )
        failure = _collect(
            sh.ObservationOutcome.FAILURE,
            started_at=_T1,
            finished_at=_T2,
            error_code="network_error",
        )
        snapshot = _snapshot([success, failure], end=_T2)
        self.assertEqual(snapshot.last_success_at, _T1)
        self.assertEqual(snapshot.last_attempt_at, _T1)
        self.assertEqual(snapshot.consecutive_failures, 1)
        self.assertEqual(snapshot.last_error_code, "network_error")

    def test_empty_success_is_success_not_failure(self):
        empty = _collect(
            sh.ObservationOutcome.SUCCESS,
            collected=0,
            new=0,
            empty=True,
            parse_failures=0,
        )
        snapshot = _snapshot([empty])
        self.assertEqual(snapshot.success_count, 1)
        self.assertEqual(snapshot.empty_success_count, 1)
        self.assertEqual(snapshot.consecutive_failures, 0)

    def test_unknown_new_item_count_is_explicit_not_proven_zero(self):
        observation = _collect(
            sh.ObservationOutcome.SUCCESS,
            collected=3,
            new=None,
            empty=False,
            parse_failures=0,
        )
        snapshot = _snapshot([observation])
        self.assertEqual(snapshot.new_item_count, 0)
        self.assertEqual(
            snapshot.completeness_metrics["unknown_new_item_attempt_count"], 1
        )

    def test_consecutive_failures_reset_after_success(self):
        first = _collect(
            sh.ObservationOutcome.FAILURE,
            finished_at=_T0 + timedelta(minutes=10),
            error_code="network_error",
        )
        second = _collect(
            sh.ObservationOutcome.TIMEOUT,
            started_at=_T0 + timedelta(minutes=10),
            finished_at=_T0 + timedelta(minutes=20),
            error_code="timeout",
        )
        recovered = _collect(
            sh.ObservationOutcome.SUCCESS,
            started_at=_T0 + timedelta(minutes=20),
            finished_at=_T1,
            collected=1,
            new=0,
            empty=False,
            parse_failures=0,
        )
        self.assertEqual(_snapshot([first, second]).consecutive_failures, 2)
        self.assertEqual(
            _snapshot([first, second, recovered]).consecutive_failures, 0
        )

    def test_previous_success_survives_current_failure_window(self):
        previous = _snapshot([
            _collect(
                sh.ObservationOutcome.SUCCESS,
                started_at=_T0,
                finished_at=_T1,
                collected=1,
                new=1,
                empty=False,
                parse_failures=0,
            )
        ], start=_T0, end=_T1)
        failure = _collect(
            sh.ObservationOutcome.FAILURE,
            started_at=_T1 + timedelta(minutes=10),
            finished_at=_T2,
            error_code="network_error",
        )
        current = _snapshot(
            [failure], start=_T1, end=_T2, previous=previous
        )
        self.assertEqual(current.last_success_at, _T1)
        self.assertEqual(current.consecutive_failures, 1)

    def test_previous_failure_streak_is_extended_without_current_success(self):
        previous_failures = [
            _collect(
                sh.ObservationOutcome.FAILURE,
                started_at=_T0 + timedelta(minutes=index * 10),
                finished_at=_T0 + timedelta(minutes=index * 10 + 5),
                error_code="network_error",
            )
            for index in range(3)
        ]
        previous = _snapshot(previous_failures, start=_T0, end=_T1)
        current_failure = _collect(
            sh.ObservationOutcome.TIMEOUT,
            started_at=_T1 + timedelta(minutes=10),
            finished_at=_T2,
            error_code="timeout",
        )
        current = _snapshot(
            [current_failure], start=_T1, end=_T2, previous=previous
        )
        self.assertEqual(current.consecutive_failures, 4)

    def test_current_success_resets_previous_failure_streak(self):
        previous = _snapshot([
            _collect(
                sh.ObservationOutcome.FAILURE,
                started_at=_T0 + timedelta(minutes=index * 10),
                finished_at=_T0 + timedelta(minutes=index * 10 + 5),
                error_code="network_error",
            )
            for index in range(3)
        ], start=_T0, end=_T1)
        current = _snapshot([
            _collect(
                sh.ObservationOutcome.FAILURE,
                started_at=_T1 + timedelta(minutes=5),
                finished_at=_T1 + timedelta(minutes=10),
                error_code="network_error",
            ),
            _collect(
                sh.ObservationOutcome.SUCCESS,
                started_at=_T1 + timedelta(minutes=15),
                finished_at=_T1 + timedelta(minutes=20),
                collected=1,
                new=1,
                empty=False,
                parse_failures=0,
            ),
        ], start=_T1, end=_T2, previous=previous)
        self.assertEqual(current.consecutive_failures, 0)

    def test_only_failures_after_current_success_remain_consecutive(self):
        previous = _snapshot([
            _collect(
                sh.ObservationOutcome.FAILURE,
                started_at=_T0 + timedelta(minutes=index * 10),
                finished_at=_T0 + timedelta(minutes=index * 10 + 5),
                error_code="network_error",
            )
            for index in range(3)
        ], start=_T0, end=_T1)
        current = _snapshot([
            _collect(
                sh.ObservationOutcome.FAILURE,
                started_at=_T1 + timedelta(minutes=5),
                finished_at=_T1 + timedelta(minutes=10),
                error_code="network_error",
            ),
            _collect(
                sh.ObservationOutcome.SUCCESS,
                started_at=_T1 + timedelta(minutes=15),
                finished_at=_T1 + timedelta(minutes=20),
                collected=1,
                new=1,
                empty=False,
                parse_failures=0,
            ),
            _collect(
                sh.ObservationOutcome.FAILURE,
                started_at=_T1 + timedelta(minutes=25),
                finished_at=_T1 + timedelta(minutes=30),
                error_code="network_error",
            ),
        ], start=_T1, end=_T2, previous=previous)
        self.assertEqual(current.consecutive_failures, 1)

    def test_empty_window_carries_state_but_never_window_counts(self):
        previous = _snapshot([
            _collect(
                sh.ObservationOutcome.SUCCESS,
                started_at=_T0,
                finished_at=_T1,
                collected=5,
                new=2,
                empty=False,
                parse_failures=1,
                last_publish=_T0,
            )
        ], start=_T0, end=_T1)
        current = _snapshot([], start=_T1, end=_T2, previous=previous)
        self.assertEqual(current.last_success_at, previous.last_success_at)
        self.assertEqual(current.last_attempt_at, previous.last_attempt_at)
        self.assertEqual(current.latency_ms, previous.latency_ms)
        self.assertEqual(
            current.last_item_publish_time, previous.last_item_publish_time
        )
        self.assertEqual(current.consecutive_failures, previous.consecutive_failures)
        self.assertEqual(current.attempt_count, 0)
        self.assertEqual(current.success_count, 0)
        self.assertEqual(current.collected_item_count, 0)
        self.assertEqual(current.new_item_count, 0)
        self.assertEqual(current.empty_success_count, 0)
        self.assertEqual(current.parse_failure_count, 0)
        self.assertEqual(current.completeness_status, CompletenessStatus.UNKNOWN)

    def test_previous_snapshot_source_mismatch_is_rejected(self):
        previous = _snapshot(
            [], start=_T0, end=_T1, source_id="stats"
        )
        with self.assertRaisesRegex(ValueError, "source_id"):
            _snapshot([], start=_T1, end=_T2, previous=previous)

    def test_overlapping_or_future_previous_window_is_rejected(self):
        previous = _snapshot(
            [], start=_T0, end=_T1 + timedelta(minutes=1)
        )
        with self.assertRaisesRegex(ValueError, "window_end"):
            _snapshot([], start=_T1, end=_T2, previous=previous)

    def test_carried_publish_time_recomputes_delay_at_current_observation(self):
        previous = _snapshot([
            _collect(
                sh.ObservationOutcome.SUCCESS,
                started_at=_T0,
                finished_at=_T1,
                collected=1,
                new=1,
                empty=False,
                parse_failures=0,
                last_publish=_T0,
            )
        ], start=_T0, end=_T1, observed=_T1)
        self.assertEqual(previous.data_delay_seconds, 3600)
        current = _snapshot(
            [], start=_T1, end=_T2, observed=_T2, previous=previous
        )
        self.assertEqual(current.last_item_publish_time, _T0)
        self.assertEqual(current.data_delay_seconds, 7200)

    def test_verify_completeness_never_updates_collect_last_success(self):
        collect = _collect(
            sh.ObservationOutcome.SUCCESS,
            finished_at=_T0 + timedelta(minutes=30),
            collected=1,
            new=1,
            empty=False,
            parse_failures=0,
        )
        verify = sh.SourceObservation(
            job_run_id=sh.make_job_run_id("news_policy_verify", _T1),
            source_id="govcn_policy",
            observation_type="verify",
            started_at=_T1,
            finished_at=_T2,
            attempt_no=1,
            outcome="success",
            collected_item_count=1,
            completeness_status=CompletenessStatus.WARNING,
            completeness_info={"archive_replay_succeeded": True},
        )
        snapshot = _snapshot([collect, verify])
        self.assertEqual(
            snapshot.last_success_at, _T0 + timedelta(minutes=30)
        )
        self.assertEqual(snapshot.attempt_count, 1)
        self.assertEqual(snapshot.completeness_status, CompletenessStatus.WARNING)
        self.assertEqual(
            snapshot.completeness_metrics["latest_verify"],
            {"archive_replay_succeeded": True},
        )

    def test_health_status_has_no_unfrozen_thresholds(self):
        failures = [
            _collect(
                sh.ObservationOutcome.FAILURE,
                started_at=_T0 + timedelta(minutes=i),
                finished_at=_T0 + timedelta(minutes=i + 1),
                error_code="network_error",
            )
            for i in range(5)
        ]
        snapshot = _snapshot(failures)
        self.assertEqual(snapshot.health_status, HealthStatus.UNKNOWN)
        self.assertEqual(
            snapshot.health_policy_version, sh.SHADOW_HEALTH_POLICY_VERSION
        )
        self.assertEqual(
            snapshot.completeness_metrics["policy_kind"], "shadow_no_scoring"
        )

    def test_current_projection_selects_latest_window_without_mutating_history(self):
        first = _snapshot([], start=_T0, end=_T1)
        second = _snapshot([], start=_T1, end=_T2)
        current = sh.project_current([second, first])
        self.assertEqual(current["govcn_policy"].window_end, _T2)
        self.assertTrue(current["govcn_policy"].is_current)
        self.assertFalse(first.is_current)
        self.assertFalse(second.is_current)

    def test_source_master_and_news_inputs_are_not_modified(self):
        source_master = {"source_id": "govcn_policy", "source_revision": 7}
        news = [{"pub_time": "2026-08-16 09:00:00", "title": "原文"}]
        source_before = deepcopy(source_master)
        news_before = deepcopy(news)
        latest = sh.latest_item_publish_time(news)
        observation = _collect(
            sh.ObservationOutcome.SUCCESS,
            collected=1,
            new=0,
            empty=False,
            parse_failures=0,
            last_publish=latest,
        )
        _snapshot([observation])
        self.assertEqual(source_master, source_before)
        self.assertEqual(news, news_before)


class ShadowSinkTests(unittest.TestCase):
    def test_sink_failure_is_fail_open(self):
        class BrokenSink:
            def emit(self, record):
                raise OSError("disk unavailable")

        observation = _collect(
            sh.ObservationOutcome.SUCCESS,
            collected=1,
            new=1,
            empty=False,
            parse_failures=0,
        )
        with self.assertLogs("data_collect.news_model.source_health", "WARNING"):
            self.assertFalse(sh.emit_shadow(BrokenSink(), observation))

    def test_jsonl_sink_writes_only_when_explicitly_emitted(self):
        observation = _collect(
            sh.ObservationOutcome.SUCCESS,
            collected=1,
            new=1,
            empty=False,
            parse_failures=0,
        )
        with tempfile.TemporaryDirectory(prefix="source-health-") as temp_dir:
            path = Path(temp_dir) / "shadow" / "observations.jsonl"
            sink = sh.JsonlShadowSink(path)
            self.assertFalse(path.exists())
            sink.emit(observation)
            payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(payload["record_type"], "observation")
        self.assertEqual(payload["source_id"], "govcn_policy")

    def test_import_has_no_external_or_file_side_effects(self):
        workspace = Path(__file__).resolve().parents[1]
        script = r'''
import socket
import tempfile
from pathlib import Path
socket.socket = lambda *a, **k: (_ for _ in ()).throw(AssertionError("network used"))
with tempfile.TemporaryDirectory() as directory:
    before = list(Path(directory).iterdir())
    import data_collect.news_model.source_health as module
    after = list(Path(directory).iterdir())
    assert before == after == []
    assert module.SHADOW_HEALTH_POLICY_VERSION == "source_health_shadow_v1"
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
