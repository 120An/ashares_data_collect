"""Explicit news-foundation management with dry-run defaults.

Read-only validation remains the default.  ``sync-entities --apply`` is the
single explicit PostgreSQL write boundary and is never invoked implicitly.
"""

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


def cmd_preflight_entity_migration(args: argparse.Namespace) -> int:
    """Audit the migration locally and optionally inspect PostgreSQL read-only."""

    from data_collect.news_model.entity_persistence import (
        EntityPersistenceError,
        audit_entity_migration_sql,
        inspect_entity_foundation,
    )

    try:
        audit = audit_entity_migration_sql(args.migration_path)
    except EntityPersistenceError as exc:
        print("Entity foundation migration preflight failed")
        print(f"diagnostic: {exc}")
        print("mode: read-only/dry-run; no SQL executed")
        return 1
    print("Entity foundation migration SQL passed")
    print(f"migration: {audit.path}")
    print(f"statements: {audit.statement_count}")
    print(f"tables: {', '.join(audit.table_names)}")
    print(f"indexes: {audit.index_count}")
    print("contract columns: match")
    print("repeat safety: CREATE IF NOT EXISTS only")
    if not args.connect_postgres:
        print("database foundation: not inspected")
        print("mode: read-only/dry-run; no connection attempted")
        return 0
    try:
        status = inspect_entity_foundation()
    except Exception as exc:
        print("Entity foundation database preflight unavailable")
        print(f"diagnostic: {exc}")
        print("mode: read-only/dry-run; no DDL/DML executed")
        return 1
    print(f"database foundation ready: {str(status.ready).lower()}")
    for diagnostic in status.diagnostics:
        print(f"diagnostic: {diagnostic}")
    print("mode: read-only/dry-run; no DDL/DML executed")
    return 0 if status.ready else 2


def _parse_observed_at(value: str | None) -> datetime | None:
    if value is None:
        return None
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("--observed-at must be timezone-aware ISO 8601")
    return parsed


def cmd_sync_entities(args: argparse.Namespace) -> int:
    """Explicitly plan or apply the Phase 1 stock Entity revision sync."""

    if args.apply and args.limit is not None:
        print("Entity foundation sync failed")
        print("diagnostic: --limit is dry-run only and cannot be used with --apply")
        print("mode: apply")
        return 2

    from data_collect.news_model.entity_persistence import (
        EntityPersistenceError,
        synchronize_entities,
    )

    try:
        observed_at = _parse_observed_at(args.observed_at)
        result = synchronize_entities(
            apply=args.apply,
            observed_at=observed_at,
            limit=args.limit,
        )
    except (EntityPersistenceError, ValueError) as exc:
        print("Entity foundation sync failed")
        print(f"diagnostic: {exc}")
        print(f"mode: {'apply' if args.apply else 'dry-run'}")
        return 1
    except Exception as exc:
        print("Entity foundation sync unavailable")
        print(f"diagnostic: {exc}")
        print(f"mode: {'apply' if args.apply else 'dry-run'}")
        return 1

    plan = result.plan
    print("Entity foundation sync planned" if not result.applied else "Entity foundation sync applied")
    print(f"foundation ready: {str(plan.foundation_status.ready).lower()}")
    for diagnostic in plan.foundation_status.diagnostics:
        print(f"diagnostic: {diagnostic}")
    print(f"source stock entities: {plan.source_stock_count}")
    print(f"sector crosswalk stocks: {plan.sector_crosswalk_stock_count}")
    print(f"inserted entity revisions: {plan.inserted_entity_revisions if result.applied else 0}")
    print(f"inserted alias revisions: {plan.inserted_alias_revisions if result.applied else 0}")
    print(f"would insert entity revisions: {plan.inserted_entity_revisions}")
    print(f"would insert alias revisions: {plan.inserted_alias_revisions}")
    print(f"manual-locked aliases skipped: {len(plan.locked_alias_ids)}")
    print(f"apply allowed: {str(plan.foundation_status.ready).lower()}")
    print(f"mode: {'apply' if result.applied else 'dry-run'}")
    return 0 if plan.foundation_status.ready else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "News foundation validation and explicit management; commands default "
            "to dry-run and writes require --apply"
        )
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
    migration = subcommands.add_parser(
        "preflight-entity-migration",
        help="audit the Entity foundation migration without executing it",
    )
    migration.add_argument(
        "--migration-path",
        default="sql/012_create_news_entity_foundation.sql",
    )
    migration.add_argument(
        "--connect-postgres",
        action="store_true",
        help="also inspect target tables through a read-only transaction",
    )
    migration.set_defaults(handler=cmd_preflight_entity_migration)
    sync_entities = subcommands.add_parser(
        "sync-entities",
        help="plan or explicitly apply the Phase 1 stock Entity revision sync",
    )
    mode = sync_entities.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="plan only (default; never executes DML)",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="explicitly write only the two Entity foundation revision tables",
    )
    sync_entities.add_argument(
        "--limit",
        type=int,
        default=None,
        help="optional instrument limit for diagnostic dry-runs",
    )
    sync_entities.add_argument(
        "--observed-at",
        default=None,
        help="optional timezone-aware ISO 8601 synchronization timestamp",
    )
    sync_entities.set_defaults(handler=cmd_sync_entities)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    sys.exit(main())
