from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import manage_master_data
from data_collect.master_data.instrument_bootstrap import BootstrapReadSnapshot
from data_collect.master_data.official_snapshot import (
    SNAPSHOT_SCHEMA_VERSION,
    SnapshotCaptureError,
    SnapshotValidationError,
    canonical_content_sha256,
    capture_official_snapshot,
    load_official_snapshot,
    snapshot_envelope_sha256,
    snapshot_payload,
)
from data_collect.master_data.public_instruments import InstrumentSchemaInspection
from tests.test_instrument_bootstrap_plan import _existing, _universe


FETCHED_AT = datetime(2026, 8, 22, 8, 30, tzinfo=timezone(timedelta(hours=8)))
VALIDATION_NOW = FETCHED_AT + timedelta(hours=1)


def _payload():
    return snapshot_payload(_universe(), fetched_at=FETCHED_AT)


def _rehash(payload):
    payload["content_sha256"] = canonical_content_sha256(payload)
    payload["snapshot_sha256"] = snapshot_envelope_sha256(payload)
    return payload


def _write_payload(directory: str | Path, payload, name="snapshot.json") -> Path:
    path = Path(directory) / name
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


class OfficialSnapshotCaptureTests(unittest.TestCase):
    def test_pass_universe_writes_atomic_snapshot_without_raw_evidence(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch(
            "data_collect.master_data.official_snapshot.os.rename",
            wraps=os.rename,
        ) as rename:
            result = capture_official_snapshot(
                _universe(), directory, fetched_at=FETCHED_AT
            )
            self.assertTrue(result.path.exists())
            rename.assert_called_once()
            self.assertFalse(list(Path(directory).glob("*.tmp")))
            payload = json.loads(result.path.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], SNAPSHOT_SCHEMA_VERSION)
            self.assertEqual(payload["content_sha256"], result.content_sha256)
            self.assertEqual(payload["snapshot_sha256"], result.snapshot_sha256)
            self.assertNotIn("raw_evidence", payload["records"][0])

    def test_fail_universe_never_creates_snapshot_or_directory(self):
        with tempfile.TemporaryDirectory() as parent:
            destination = Path(parent) / "not-created"
            failed = replace(_universe(), universe_status="FAIL")
            with self.assertRaises(SnapshotCaptureError):
                capture_official_snapshot(failed, destination, fetched_at=FETCHED_AT)
            self.assertFalse(destination.exists())

    def test_capture_revalidates_instead_of_trusting_pass_label(self):
        with tempfile.TemporaryDirectory() as parent:
            destination = Path(parent) / "not-created"
            forged = replace(_universe(), question_mark_name_count=1)
            with self.assertRaisesRegex(SnapshotCaptureError, "capture gate"):
                capture_official_snapshot(forged, destination, fetched_at=FETCHED_AT)
            self.assertFalse(destination.exists())

    def test_same_content_has_same_hash_despite_different_timestamps(self):
        first = snapshot_payload(_universe(), fetched_at=FETCHED_AT)
        second = snapshot_payload(
            _universe(), fetched_at=FETCHED_AT + timedelta(minutes=30)
        )
        self.assertEqual(first["content_sha256"], second["content_sha256"])
        self.assertNotEqual(first["snapshot_sha256"], second["snapshot_sha256"])
        self.assertNotEqual(first["fetched_at"], second["fetched_at"])

    def test_repeated_capture_never_overwrites_old_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            first = capture_official_snapshot(
                _universe(), directory, fetched_at=FETCHED_AT
            )
            second = capture_official_snapshot(
                _universe(), directory, fetched_at=FETCHED_AT
            )
            self.assertNotEqual(first.path, second.path)
            self.assertEqual(first.content_sha256, second.content_sha256)
            self.assertEqual(len(list(Path(directory).glob("*.json"))), 2)

    def test_provider_failure_does_not_write_snapshot(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            manage_master_data,
            "fetch_official_a_share_universe",
            side_effect=ConnectionError("offline"),
        ):
            with self.assertRaises(ConnectionError):
                manage_master_data.run_capture_official_snapshot(directory)
            self.assertFalse(list(Path(directory).iterdir()))


class OfficialSnapshotLoaderTests(unittest.TestCase):
    def _load(self, payload):
        with tempfile.TemporaryDirectory() as directory:
            path = _write_payload(directory, payload)
            return load_official_snapshot(path, now=VALIDATION_NOW)

    def test_valid_snapshot_recomputes_pass_universe(self):
        loaded = self._load(_payload())
        self.assertEqual(loaded.snapshot_status, "VALID")
        self.assertEqual(loaded.content_sha256, _payload()["content_sha256"])
        self.assertEqual(loaded.snapshot_sha256, _payload()["snapshot_sha256"])
        self.assertEqual(loaded.universe.authoritative_unique_total, 3_000)
        self.assertEqual(loaded.universe.exchange_counts["BSE"], 500)
        self.assertEqual(
            loaded.universe.record_for("000001.SZ").instrument_name, "平安银行"
        )

    def test_modified_content_without_new_hash_fails(self):
        payload = _payload()
        payload["records"][10]["InstrumentName"] = "被修改"
        with self.assertRaisesRegex(SnapshotValidationError, "sha256 mismatch"):
            self._load(payload)

    def test_content_hash_is_verified_even_if_snapshot_hash_is_recomputed(self):
        payload = _payload()
        payload["records"][10]["InstrumentName"] = "被修改"
        payload["snapshot_sha256"] = snapshot_envelope_sha256(payload)
        with self.assertRaisesRegex(SnapshotValidationError, "content_sha256 mismatch"):
            self._load(payload)

    def test_modified_fetched_at_without_new_snapshot_hash_fails(self):
        payload = _payload()
        payload["fetched_at"] = (
            FETCHED_AT + timedelta(minutes=1)
        ).isoformat(timespec="seconds")
        with self.assertRaisesRegex(SnapshotValidationError, "snapshot_sha256 mismatch"):
            self._load(payload)

    def test_modified_created_at_without_new_snapshot_hash_fails(self):
        payload = _payload()
        payload["created_at"] = (
            FETCHED_AT + timedelta(minutes=1)
        ).isoformat(timespec="seconds")
        with self.assertRaisesRegex(SnapshotValidationError, "snapshot_sha256 mismatch"):
            self._load(payload)

    def test_removed_record_fails_even_with_recomputed_hash(self):
        payload = _payload()
        payload["records"].pop()
        with self.assertRaises(SnapshotValidationError):
            self._load(_rehash(payload))

    def test_duplicate_code_fails(self):
        payload = _payload()
        payload["records"].append(deepcopy(payload["records"][-1]))
        with self.assertRaisesRegex(SnapshotValidationError, "duplicate stock_code"):
            self._load(_rehash(payload))

    def test_wrong_exchange_count_fails(self):
        payload = _payload()
        payload["exchange_counts"]["BSE"] -= 1
        with self.assertRaisesRegex(SnapshotValidationError, "exchange_counts"):
            self._load(_rehash(payload))

    def test_question_mark_and_replacement_character_names_fail(self):
        for value in ("????", "坏�名称"):
            with self.subTest(value=value):
                payload = _payload()
                payload["records"][10]["InstrumentName"] = value
                with self.assertRaises(SnapshotValidationError):
                    self._load(_rehash(payload))

    def test_missing_required_sample_fails(self):
        payload = _payload()
        sample = next(
            item for item in payload["records"] if item["stock_code"] == "000001.SZ"
        )
        sample["stock_code"] = "001501.SZ"
        payload["records"] = sorted(
            payload["records"], key=lambda item: item["stock_code"]
        )
        with self.assertRaisesRegex(SnapshotValidationError, "required sample"):
            self._load(_rehash(payload))

    def test_stale_snapshot_fails_without_override(self):
        with self.assertRaisesRegex(SnapshotValidationError, "STALE"):
            self._load_with_now(_payload(), FETCHED_AT + timedelta(hours=24, seconds=1))

    def _load_with_now(self, payload, now):
        with tempfile.TemporaryDirectory() as directory:
            path = _write_payload(directory, payload)
            return load_official_snapshot(path, now=now)

    def test_malformed_json_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.json"
            path.write_text("{bad", encoding="utf-8")
            with self.assertRaisesRegex(SnapshotValidationError, "malformed"):
                load_official_snapshot(path, now=VALIDATION_NOW)

    def test_unknown_schema_version_fails(self):
        payload = _payload()
        payload["schema_version"] = "unknown_v9"
        with self.assertRaisesRegex(SnapshotValidationError, "schema_version"):
            self._load(_rehash(payload))


class OfficialSnapshotBootstrapTests(unittest.TestCase):
    def test_snapshot_bootstrap_does_not_call_exchange_network(self):
        with tempfile.TemporaryDirectory() as directory:
            capture = capture_official_snapshot(
                _universe(), directory, fetched_at=datetime.now().astimezone()
            )
            schema = InstrumentSchemaInspection(
                inspected=True,
                columns=("stock_code", "InstrumentName", "ExchangeID"),
                compatible=True,
            )
            read_snapshot = BootstrapReadSnapshot(
                existing_instruments=(
                    _existing("000001.SZ", "????", "SZ"),
                    _existing("600519.SH", "????", "SH"),
                ),
                relevant_changelog=(),
            )
            with mock.patch.object(
                manage_master_data, "fetch_official_a_share_universe"
            ) as network, mock.patch.object(
                manage_master_data,
                "inspect_instrument_info_schema",
                return_value=schema,
            ) as inspect, mock.patch.object(
                manage_master_data,
                "read_bootstrap_inputs_from_postgres",
                return_value=read_snapshot,
            ) as read:
                plan = manage_master_data.run_bootstrap_dry_run(
                    snapshot_path=capture.path
                )
            network.assert_not_called()
            inspect.assert_called_once_with()
            read.assert_called_once_with()
            self.assertEqual(plan.would_repair_corrupted_name_count, 2)
            self.assertEqual(plan.would_delete_count, 0)

    def test_invalid_snapshot_fails_before_any_database_access(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.json"
            path.write_text("{}", encoding="utf-8")
            with mock.patch.object(
                manage_master_data, "inspect_instrument_info_schema"
            ) as inspect, mock.patch.object(
                manage_master_data, "read_bootstrap_inputs_from_postgres"
            ) as read:
                with self.assertRaises(SnapshotValidationError):
                    manage_master_data.run_bootstrap_dry_run(snapshot_path=path)
            inspect.assert_not_called()
            read.assert_not_called()

    def test_snapshot_cli_is_explicit_and_reports_input_mode(self):
        plan = manage_master_data.build_instrument_bootstrap_plan(
            _universe(), (), (), plan_date=FETCHED_AT.date()
        )
        output = io.StringIO()
        with mock.patch.object(
            manage_master_data, "run_bootstrap_dry_run", return_value=plan
        ) as run, redirect_stdout(output):
            exit_code = manage_master_data.main(
                [
                    "bootstrap-instruments",
                    "--dry-run",
                    "--inspect-postgres",
                    "--snapshot",
                    "D:/runtime/snapshot.json",
                ]
            )
        self.assertEqual(exit_code, 0)
        run.assert_called_once_with(snapshot_path="D:/runtime/snapshot.json")
        self.assertIn("input_mode: validated_snapshot", output.getvalue())

    def test_capture_cli_outputs_path_and_never_connects_database(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            manage_master_data,
            "fetch_official_a_share_universe",
            return_value=_universe(),
        ), mock.patch.object(
            manage_master_data, "inspect_instrument_info_schema"
        ) as inspect, redirect_stdout(io.StringIO()) as output:
            exit_code = manage_master_data.main(
                ["capture-official-snapshot", "--output-dir", directory]
            )
        self.assertEqual(exit_code, 0)
        inspect.assert_not_called()
        self.assertIn("snapshot_status: VALID", output.getvalue())
        self.assertIn("database_connected: false", output.getvalue())

    def test_capture_cli_provider_failure_writes_nothing(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            manage_master_data,
            "fetch_official_a_share_universe",
            side_effect=ConnectionError("provider failed"),
        ), redirect_stderr(io.StringIO()):
            exit_code = manage_master_data.main(
                ["capture-official-snapshot", "--output-dir", directory]
            )
            self.assertEqual(exit_code, 1)
            self.assertFalse(list(Path(directory).iterdir()))

    def test_apply_remains_rejected_before_snapshot_load(self):
        with mock.patch.object(manage_master_data, "load_official_snapshot") as load:
            exit_code = manage_master_data.main(
                [
                    "bootstrap-instruments",
                    "--apply",
                    "--inspect-postgres",
                    "--snapshot",
                    "D:/runtime/snapshot.json",
                ]
            )
        self.assertEqual(exit_code, 2)
        load.assert_not_called()


if __name__ == "__main__":
    unittest.main()
