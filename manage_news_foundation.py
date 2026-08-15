"""Read-only local management commands for the news foundation layer."""

from __future__ import annotations

import argparse
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
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    sys.exit(main())
