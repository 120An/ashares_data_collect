"""Explicit public master-data preflight and bootstrap-plan entry point.

Diagnostics and write planning are read-only by default.  The sole write path is
an explicit snapshot-and-plan-hash-gated, locked transaction into legacy
``instrument_info`` with truthful ``instrument_changelog`` records.  Bootstrap
and recurring refresh remain distinct, and neither may overwrite a complete
universe with an incomplete snapshot.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import date
import sys
from pathlib import Path
from typing import Sequence

from data_collect.master_data.instrument_bootstrap import (
    InstrumentBootstrapPlan,
    build_instrument_bootstrap_plan,
    read_bootstrap_inputs_from_postgres,
)
from data_collect.master_data.instrument_bootstrap_apply import (
    ApplyConfigurationError,
    InstrumentBootstrapApplyResult,
    apply_instrument_bootstrap_plan,
)
from data_collect.master_data.official_exchanges import (
    OfficialAShareUniverse,
    fetch_official_a_share_universe,
    name_evidence,
)
from data_collect.master_data.official_snapshot import (
    OfficialSnapshotCapture,
    capture_official_snapshot,
    load_official_snapshot,
)
from data_collect.master_data.public_instruments import (
    EastmoneyDirectAShareProvider,
    InstrumentSchemaInspection,
    PublicInstrumentPreflightReport,
    evaluate_public_instruments,
    inspect_instrument_info_schema,
    sample_text_evidence,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Public A-share master-data preflight (default: read-only dry-run)"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    official = subparsers.add_parser(
        "official-preflight",
        help="authoritative SSE+SZSE+BSE universe preflight (read-only)",
    )
    official.add_argument(
        "--inspect-postgres",
        action="store_true",
        help="also perform read-only public.instrument_info schema inspection",
    )

    preflight = subparsers.add_parser(
        "preflight",
        help="optional Eastmoney provider-specific preflight",
    )
    preflight.add_argument(
        "--inspect-postgres",
        action="store_true",
        help="also perform read-only public.instrument_info schema inspection",
    )

    capture = subparsers.add_parser(
        "capture-official-snapshot",
        help="capture one complete PASS SSE+SZSE+BSE universe as an atomic JSON file",
    )
    capture.add_argument(
        "--output-dir",
        required=True,
        help="local runtime evidence directory; snapshots are never overwritten",
    )

    bootstrap = subparsers.add_parser(
        "bootstrap-instruments",
        help="dry-run or explicitly apply a validated official-universe plan",
    )
    mode = bootstrap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="explicit read-only mode")
    mode.add_argument(
        "--apply",
        action="store_true",
        help="controlled write; requires snapshot and exact reviewed plan SHA-256",
    )
    bootstrap.add_argument(
        "--inspect-postgres",
        action="store_true",
        help="required acknowledgement for read-only PostgreSQL inspection",
    )
    bootstrap.add_argument(
        "--snapshot",
        help="explicit validated official snapshot; disables exchange network access",
    )
    bootstrap.add_argument(
        "--expect-plan-sha256",
        help="exact reviewed dry-run plan SHA-256; required for --apply",
    )
    return parser


def _print_report(report: PublicInstrumentPreflightReport) -> None:
    print(f"provider: {report.provider}")
    print(f"provider_underlying_source: {report.provider_underlying_source}")
    print(f"provider_akshare_reference: {report.provider_akshare_reference}")
    print(f"domestic_network_mode: {report.domestic_network_mode}")
    print(f"inherited_env_proxy: {str(report.inherited_env_proxy).lower()}")
    print(f"total_raw_records: {report.total_raw_records}")
    print(f"valid_stock_records: {report.valid_stock_records}")
    print(f"unique_stock_codes: {report.unique_stock_codes}")
    print(f"SSE count: {report.exchange_counts['SSE']}")
    print(f"SZSE count: {report.exchange_counts['SZSE']}")
    print(f"BSE count: {report.exchange_counts['BSE']}")
    print(f"invalid_code_count: {report.invalid_code_count}")
    print(f"duplicate_code_count: {report.duplicate_code_count}")
    print(f"empty_name_count: {report.empty_name_count}")
    print(f"question_mark_name_count: {report.question_mark_name_count}")
    print(f"replacement_char_name_count: {report.replacement_char_name_count}")
    print(f"unknown_security_type_count: {report.unknown_security_type_count}")
    print(f"classification_uncertain_count: {report.classification_uncertain_count}")

    for exchange_id, label in (("SSE", "SH"), ("SZSE", "SZ"), ("BSE", "BJ")):
        sample = next(
            (row for row in report.records if row.canonical_exchange == exchange_id),
            None,
        )
        value = f"{sample.stock_code} {sample.instrument_name}" if sample else "<missing>"
        print(f"sample {label}: {value}")

    for stock_code in ("000001.SZ", "600519.SH"):
        ascii_name, utf8_hex = sample_text_evidence(report.record_for(stock_code))
        print(f"{stock_code} name_ascii: {ascii_name}")
        print(f"{stock_code} utf8_hex: {utf8_hex}")

    schema = report.schema_inspection
    print(f"instrument_info_schema_inspected: {str(schema.inspected).lower()}")
    print(f"instrument_info_schema_compatible: {str(schema.compatible).lower()}")
    print(f"instrument_info_columns: {','.join(schema.columns) if schema.columns else '<not inspected>'}")
    print(
        "instrument_info_unsupported_required_columns: "
        + (",".join(schema.unsupported_required_columns) or "<none>")
    )
    print(f"secondary_validation_status: {report.secondary_validation_status}")
    print(f"completeness_status: {report.completeness_status}")
    print(f"apply_allowed: {str(report.apply_allowed).lower()}")
    for blocker in report.blockers:
        print(f"blocker: {blocker}")
    print("write_mode: dry-run")
    print("database_dml_executed: false")


def run_preflight(*, inspect_postgres: bool = False) -> PublicInstrumentPreflightReport:
    schema = (
        inspect_instrument_info_schema()
        if inspect_postgres
        else InstrumentSchemaInspection(inspected=False)
    )
    rows = EastmoneyDirectAShareProvider().fetch()
    return evaluate_public_instruments(rows, schema_inspection=schema)


def _print_official_report(report: OfficialAShareUniverse) -> None:
    print(f"provider_mode: {report.provider_mode}")
    print(f"domestic_network_mode: {report.domestic_network_mode}")
    print(f"inherited_env_proxy: {str(report.inherited_env_proxy).lower()}")
    print(f"SSE raw main count: {report.sse.raw_part_counts.get('main', 0)}")
    print(f"SSE raw STAR count: {report.sse.raw_part_counts.get('star', 0)}")
    print(f"SSE excluded CDR count: {report.sse.excluded_cdr_count}")
    print(
        "SSE excluded CDR codes sample: "
        + (",".join(report.sse.excluded_cdr_codes[:10]) or "<none>")
    )
    print(f"SSE ordinary stock count: {report.sse.ordinary_stock_count}")
    print(f"SZSE raw A count: {report.szse.raw_count}")
    print(f"SZSE ordinary stock count: {report.szse.ordinary_stock_count}")
    print(f"BSE totalElements: {report.bse.expected_total}")
    print(f"BSE totalPages: {report.bse.total_pages}")
    print(f"BSE fetched count: {report.bse.fetched_total}")
    print(f"BSE ordinary stock count: {report.bse.ordinary_stock_count}")
    print(f"authoritative_raw_total: {report.authoritative_raw_total}")
    print(f"authoritative_unique_total: {report.authoritative_unique_total}")
    print(f"SSE count: {report.exchange_counts['SSE']}")
    print(f"SZSE count: {report.exchange_counts['SZSE']}")
    print(f"BSE count: {report.exchange_counts['BSE']}")
    print(f"duplicate_code_count: {report.duplicate_code_count}")
    print(f"name_conflict_count: {report.name_conflict_count}")
    print(f"cross_exchange_conflict_count: {report.cross_exchange_conflict_count}")
    print(f"invalid_code_count: {report.invalid_code_count}")
    print(f"empty_name_count: {report.empty_name_count}")
    print(f"question_mark_name_count: {report.question_mark_name_count}")
    print(f"replacement_char_name_count: {report.replacement_char_name_count}")
    print(f"security_type_uncertain_count: {report.security_type_uncertain_count}")

    for exchange_id, label in (("SSE", "SH"), ("SZSE", "SZ"), ("BSE", "BJ")):
        sample = next(
            (row for row in report.records if row.canonical_exchange == exchange_id),
            None,
        )
        value = f"{sample.stock_code} {sample.instrument_name}" if sample else "<missing>"
        print(f"sample {label}: {value}")
    for stock_code in ("600519.SH", "000001.SZ"):
        name_ascii, utf8_hex = name_evidence(report.record_for(stock_code))
        print(f"{stock_code} name_ascii: {name_ascii}")
        print(f"{stock_code} utf8_hex: {utf8_hex}")

    schema = report.schema_inspection
    print(f"instrument_info_schema_inspected: {str(schema.inspected).lower()}")
    print(f"instrument_info_schema_compatible: {str(schema.compatible).lower()}")
    print(f"universe_status: {report.universe_status}")
    print(f"completeness_status: {report.completeness_status}")
    print(f"apply_allowed: {str(report.apply_allowed).lower()}")
    for prerequisite in report.future_apply_prerequisites:
        print(f"future_apply_prerequisite: {prerequisite}")
    for blocker in report.blockers:
        print(f"blocker: {blocker}")
    print("write_mode: dry-run")
    print("database_dml_executed: false")


def run_official_preflight(*, inspect_postgres: bool = False) -> OfficialAShareUniverse:
    schema = (
        inspect_instrument_info_schema()
        if inspect_postgres
        else InstrumentSchemaInspection(inspected=False)
    )
    return fetch_official_a_share_universe(schema_inspection=schema)


def run_capture_official_snapshot(output_dir: str | Path) -> OfficialSnapshotCapture:
    """Fetch all three official providers and save only a complete PASS union."""

    universe = fetch_official_a_share_universe()
    return capture_official_snapshot(universe, output_dir)


def _print_snapshot_capture(capture: OfficialSnapshotCapture) -> None:
    print(f"snapshot_path: {capture.path}")
    print(f"content_sha256: {capture.content_sha256}")
    print(f"snapshot_sha256: {capture.snapshot_sha256}")
    print(f"fetched_at: {capture.fetched_at.isoformat()}")
    print(f"source_total: {capture.source_total}")
    print(f"SSE count: {capture.exchange_counts['SSE']}")
    print(f"SZSE count: {capture.exchange_counts['SZSE']}")
    print(f"BSE count: {capture.exchange_counts['BSE']}")
    print("snapshot_status: VALID")
    print("database_connected: false")
    print("database_dml_executed: false")


def run_bootstrap_dry_run(
    *,
    plan_date: date | None = None,
    snapshot_path: str | Path | None = None,
) -> InstrumentBootstrapPlan:
    """Compare a live or explicit validated official universe with a read-only DB."""

    source_content_sha256: str | None = None
    source_snapshot_sha256: str | None = None
    if snapshot_path is not None:
        validated = load_official_snapshot(snapshot_path)
        source_content_sha256 = validated.content_sha256
        source_snapshot_sha256 = validated.snapshot_sha256
        schema = inspect_instrument_info_schema()
        universe = replace(validated.universe, schema_inspection=schema)
    else:
        schema = inspect_instrument_info_schema()
        universe = fetch_official_a_share_universe(schema_inspection=schema)
    snapshot = read_bootstrap_inputs_from_postgres()
    return build_instrument_bootstrap_plan(
        universe,
        snapshot.existing_instruments,
        snapshot.relevant_changelog,
        plan_date=plan_date or date.today(),
        source_content_sha256=source_content_sha256,
        source_snapshot_sha256=source_snapshot_sha256,
    )


def _sample_codes(values: Sequence[str], limit: int = 10) -> str:
    return ",".join(values[:limit]) or "<none>"


def _print_bootstrap_plan(plan: InstrumentBootstrapPlan, *, input_mode: str) -> None:
    print("provider_mode: official_exchange_union")
    print(f"input_mode: {input_mode}")
    print(f"official_universe_status: {plan.universe_status}")
    print(
        "instrument_info_schema_compatible: "
        f"{str(plan.schema_compatible).lower()}"
    )
    print(f"source_total: {plan.source_total}")
    print(f"content_sha256: {plan.source_content_sha256 or '<live-unbound>'}")
    print(f"snapshot_sha256: {plan.source_snapshot_sha256 or '<live-unbound>'}")
    print(f"database_baseline_sha256: {plan.database_baseline_sha256}")
    print(f"plan_sha256: {plan.plan_sha256}")
    print(f"existing_total: {plan.existing_total}")
    print(f"would_insert: {plan.would_insert_count}")
    print(f"would_update: {plan.would_update_count}")
    print(
        "would_repair_corrupted_names: "
        f"{plan.would_repair_corrupted_name_count}"
    )
    print(f"would_change_exchange: {plan.would_change_exchange_count}")
    print(f"would_unchanged: {plan.would_unchanged_count}")
    print(f"would_insert_changelog: {plan.would_insert_changelog_count}")
    print(f"would_delete: {plan.would_delete_count}")
    print(f"existing_not_in_official: {plan.existing_not_in_official_count}")
    print(f"insert_codes_sample: {_sample_codes(plan.insert_codes)}")
    print(f"update_codes_sample: {_sample_codes(plan.update_codes)}")
    print(f"unchanged_codes_sample: {_sample_codes(plan.unchanged_codes)}")
    print(
        "existing_not_in_official_sample: "
        f"{_sample_codes(plan.existing_not_in_official_codes)}"
    )
    for action in plan.repair_samples:
        print(
            f"repair_sample: {action.stock_code} "
            f"{action.old_instrument_name} -> {action.new_instrument_name}"
        )
    print(f"plan_status: {plan.plan_status}")
    print(f"apply_allowed: {str(plan.apply_allowed).lower()}")
    for prerequisite in plan.future_apply_prerequisites:
        print(f"future_apply_prerequisite: {prerequisite}")
    for blocker in plan.blockers:
        print(f"blocker: {blocker}")
    print("database_dml_executed: false")


def _print_bootstrap_apply_result(result: InstrumentBootstrapApplyResult) -> None:
    print("input_mode: validated_snapshot")
    print(f"content_sha256: {result.content_sha256}")
    print(f"snapshot_sha256: {result.snapshot_sha256}")
    print(f"database_baseline_sha256: {result.database_baseline_sha256}")
    print(f"plan_sha256: {result.plan_sha256}")
    print(f"source_total: {result.source_total}")
    print(f"inserted: {result.inserted_count}")
    print(f"updated: {result.updated_count}")
    print(f"repaired: {result.repaired_count}")
    print(f"changelog_inserted: {result.changelog_inserted_count}")
    print(f"deleted: {result.deleted_count}")
    print(f"post_total: {result.post_total}")
    print(f"missing: {result.missing_count}")
    print(f"mismatches: {result.mismatch_count}")
    print(f"committed: {str(result.committed).lower()}")


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "bootstrap-instruments" and args.apply:
        if not args.snapshot:
            print("ERROR: --snapshot is required for --apply", file=sys.stderr)
            return 2
        if not args.expect_plan_sha256:
            print(
                "ERROR: --expect-plan-sha256 is required for --apply",
                file=sys.stderr,
            )
            return 2
    if (
        args.command == "bootstrap-instruments"
        and not args.apply
        and not args.inspect_postgres
    ):
        print(
            "ERROR: bootstrap dry-run requires --inspect-postgres; "
            "the database boundary is read-only",
            file=sys.stderr,
        )
        return 2

    try:
        if args.command == "capture-official-snapshot":
            capture = run_capture_official_snapshot(args.output_dir)
            _print_snapshot_capture(capture)
            return 0
        if args.command == "bootstrap-instruments":
            if args.apply:
                result = apply_instrument_bootstrap_plan(
                    snapshot_path=args.snapshot,
                    expected_plan_sha256=args.expect_plan_sha256,
                )
                _print_bootstrap_apply_result(result)
                return 0
            plan = run_bootstrap_dry_run(snapshot_path=args.snapshot)
            _print_bootstrap_plan(
                plan,
                input_mode=("validated_snapshot" if args.snapshot else "live_official"),
            )
            return 0 if plan.plan_status == "PASS" else 1
        if args.command == "official-preflight":
            official_report = run_official_preflight(
                inspect_postgres=bool(args.inspect_postgres)
            )
            _print_official_report(official_report)
            return 0 if official_report.universe_status == "PASS" else 1
        report = run_preflight(inspect_postgres=bool(args.inspect_postgres))
    except ApplyConfigurationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"ERROR: public master-data preflight failed: {exc}", file=sys.stderr)
        return 1
    _print_report(report)
    return 0 if report.completeness_status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
