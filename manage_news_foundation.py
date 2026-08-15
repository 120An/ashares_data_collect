"""Read-only local management commands for the news foundation layer."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import sys

from data_collect.news_model.source_catalog import (
    DEFAULT_GOVERNANCE_PATH,
    DEFAULT_SOURCES_PATH,
    SourceCatalogError,
    build_source_catalog_snapshot,
)


def cmd_validate_sources(args: argparse.Namespace) -> int:
    """Validate and summarize the static Source Catalog without side effects."""

    try:
        snapshot = build_source_catalog_snapshot(
            sources_path=args.sources_path,
            governance_path=args.governance_path,
        )
    except SourceCatalogError as exc:
        print("Source Catalog validation failed")
        print(f"configuration conflicts: 1")
        print(f"diagnostic: {exc}")
        print("mode: read-only/dry-run")
        return 1

    unregistered = ", ".join(snapshot.unregistered_production_source_ids) or "none"
    unresolved = ", ".join(snapshot.unresolved_source_ids) or "none"
    print("Source Catalog validation passed")
    print(f"catalog sources: {snapshot.catalog_source_count}")
    print(f"sources.yaml sources: {snapshot.sources_yaml_count}")
    print(
        "unregistered production sources "
        f"({len(snapshot.unregistered_production_source_ids)}): {unregistered}"
    )
    print(f"unrated sources: {len(snapshot.unrated_source_ids)}")
    print(f"unresolved source_ids ({len(snapshot.unresolved_source_ids)}): {unresolved}")
    print(f"configuration conflicts: {len(snapshot.configuration_conflicts)}")
    print("mode: read-only/dry-run")
    return 0


def cmd_inspect_entities(args: argparse.Namespace) -> int:
    """Inspect the Entity shadow through an explicitly enabled read-only DB path."""

    if not args.connect_postgres:
        print("Entity shadow inspection not executed")
        print("diagnostic: PostgreSQL access requires explicit --connect-postgres")
        print("mode: read-only/dry-run; no connection attempted")
        return 0

    # Lazy import keeps module import and validate-sources free of DB dependencies.
    from data_collect.news_model.entity_catalog import (
        inspect_postgres_shadow,
    )

    try:
        inspection = inspect_postgres_shadow(
            observed_at=datetime.now(timezone.utc),
            limit=args.limit,
        )
    except Exception as exc:
        print("Entity shadow inspection unavailable")
        print(f"diagnostic: {exc}")
        print("mode: read-only/dry-run; no tables or data modified")
        return 1

    snapshot = inspection.snapshot
    print("Entity shadow inspection passed")
    print(f"instrument columns: {', '.join(inspection.instrument_columns)}")
    print(f"stock entities: {len(snapshot.stock_entities)}")
    print(f"company entities: {len(snapshot.company_entities)}")
    print(f"entity aliases: {len(snapshot.aliases)}")
    print(f"historical aliases: {len(snapshot.historical_aliases)}")
    print(f"sector crosswalk stocks: {len(inspection.sector_crosswalk.by_stock_code)}")
    print(
        "diagnostics: "
        f"{len(snapshot.diagnostics) + len(inspection.sector_crosswalk.diagnostics)}"
    )
    print("mode: read-only/dry-run; no tables or data modified")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Local, read-only news foundation validation"
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    validate = subcommands.add_parser(
        "validate-sources", help="validate the static V1.1 Source Catalog"
    )
    validate.add_argument("--sources-path", default=str(DEFAULT_SOURCES_PATH))
    validate.add_argument("--governance-path", default=str(DEFAULT_GOVERNANCE_PATH))
    validate.set_defaults(handler=cmd_validate_sources)
    inspect_entities = subcommands.add_parser(
        "inspect-entities",
        help="inspect the stock Entity/Alias shadow through a read-only boundary",
    )
    inspect_entities.add_argument(
        "--connect-postgres",
        action="store_true",
        help="explicitly permit a read-only PostgreSQL connection",
    )
    inspect_entities.add_argument(
        "--limit",
        type=int,
        default=100,
        help="maximum instrument_info rows to inspect (default: 100)",
    )
    inspect_entities.set_defaults(handler=cmd_inspect_entities)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    sys.exit(main())
