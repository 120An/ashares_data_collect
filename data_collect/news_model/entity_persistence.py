"""Explicit PostgreSQL persistence boundary for Phase 1 stock entities.

The projection logic remains in :mod:`entity_catalog`.  This module only
plans revision changes and, when explicitly requested, writes the two new
foundation tables.  It never creates tables, touches legacy master-data
tables, connects at import time, or produces Phase 2 entities/relations.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, fields, replace
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
import re
from typing import Any

from data_collect.news_model.contracts import Entity, EntityAlias, EntityType
from data_collect.news_model.entity_catalog import (
    EntityCatalogSnapshot,
    build_entity_catalog,
    build_sector_crosswalk,
    load_shadow_inputs_from_postgres,
)


ENTITY_TABLE = "news_entity_revision"
ENTITY_ALIAS_TABLE = "news_entity_alias_revision"
FOUNDATION_TABLES = (ENTITY_TABLE, ENTITY_ALIAS_TABLE)
DEFAULT_MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "sql"
    / "012_create_news_entity_foundation.sql"
)

ENTITY_COLUMNS = tuple(field.name for field in fields(Entity))
ENTITY_ALIAS_COLUMNS = tuple(field.name for field in fields(EntityAlias))
_ENTITY_DB_COLUMNS = frozenset((*ENTITY_COLUMNS, "is_latest_revision"))
_ALIAS_DB_COLUMNS = frozenset((*ENTITY_ALIAS_COLUMNS, "is_latest_revision"))
_EXPECTED_COLUMNS = {
    ENTITY_TABLE: _ENTITY_DB_COLUMNS,
    ENTITY_ALIAS_TABLE: _ALIAS_DB_COLUMNS,
}
_EXPECTED_PRIMARY_KEYS = {
    ENTITY_TABLE: ("entity_id", "entity_revision"),
    ENTITY_ALIAS_TABLE: ("entity_alias_id", "revision"),
}
_EXPECTED_LATEST_INDEXES = {
    "uq_news_entity_latest_revision": (ENTITY_TABLE, "entity_id"),
    "uq_news_entity_alias_latest_revision": (
        ENTITY_ALIAS_TABLE,
        "entity_alias_id",
    ),
}
_LEGACY_TABLES = frozenset(
    {
        "instrument_info",
        "instrument_changelog",
        "sector_stock",
        "sector_changelog",
    }
)
_FORBIDDEN_SQL = re.compile(
    r"\b(?:DROP|ALTER|INSERT|UPDATE|DELETE|TRUNCATE|MERGE|COPY)\b",
    flags=re.IGNORECASE,
)
_NON_FACT_FIELDS = frozenset(
    {"entity_revision", "revision", "created_at", "updated_at", "manual_lock"}
)
_DB_DECIMAL_FLOAT_FIELDS = frozenset({"confidence"})


class EntityPersistenceError(RuntimeError):
    """Base error for migration validation or controlled synchronization."""


class EntityFoundationNotReadyError(EntityPersistenceError):
    """The two revision tables are missing or do not match the frozen schema."""


@dataclass(frozen=True, slots=True)
class MigrationAudit:
    path: Path
    statement_count: int
    table_names: tuple[str, ...]
    index_count: int
    contract_columns_match: bool
    repeat_safe: bool


@dataclass(frozen=True, slots=True)
class EntityFoundationStatus:
    ready: bool
    table_columns: Mapping[str, tuple[str, ...]]
    primary_key_tables: tuple[str, ...]
    latest_revision_indexes: tuple[str, ...]
    diagnostics: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RevisionPointer:
    record_id: str
    revision: int


@dataclass(frozen=True, slots=True)
class EntitySyncPlan:
    foundation_status: EntityFoundationStatus
    source_stock_count: int
    sector_crosswalk_stock_count: int
    entity_inserts: tuple[Entity, ...]
    alias_inserts: tuple[EntityAlias, ...]
    entity_supersedes: tuple[RevisionPointer, ...]
    alias_supersedes: tuple[RevisionPointer, ...]
    locked_alias_ids: tuple[str, ...]

    @property
    def inserted_entity_revisions(self) -> int:
        return len(self.entity_inserts)

    @property
    def inserted_alias_revisions(self) -> int:
        return len(self.alias_inserts)

    @property
    def has_changes(self) -> bool:
        return bool(self.entity_inserts or self.alias_inserts)


@dataclass(frozen=True, slots=True)
class EntitySyncResult:
    plan: EntitySyncPlan
    applied: bool


def _strip_sql_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _table_body(sql: str, table_name: str) -> str:
    match = re.search(
        rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{re.escape(table_name)}\s*\((.*?)\);",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        raise EntityPersistenceError(f"migration missing table: {table_name}")
    return match.group(1)


def _declared_columns(table_body: str) -> frozenset[str]:
    column_pattern = re.compile(
        r"^\s*([a-z][a-z0-9_]*)\s+"
        r"(?:VARCHAR|INTEGER|TEXT|JSONB|NUMERIC|TIMESTAMPTZ|BOOLEAN)\b",
        flags=re.IGNORECASE | re.MULTILINE,
    )
    return frozenset(match.group(1).lower() for match in column_pattern.finditer(table_body))


def audit_entity_migration_sql(
    path: str | Path = DEFAULT_MIGRATION_PATH,
) -> MigrationAudit:
    """Validate the checked-in migration without connecting to PostgreSQL."""

    migration_path = Path(path)
    if not migration_path.is_file():
        raise EntityPersistenceError(f"entity migration does not exist: {migration_path}")
    raw_sql = migration_path.read_text(encoding="utf-8")
    sql = _strip_sql_comments(raw_sql)
    if _FORBIDDEN_SQL.search(sql):
        raise EntityPersistenceError("entity migration contains forbidden destructive/DML SQL")
    for legacy_table in _LEGACY_TABLES:
        if re.search(rf"\b{re.escape(legacy_table)}\b", sql, flags=re.IGNORECASE):
            raise EntityPersistenceError(
                f"entity migration must not reference legacy table {legacy_table}"
            )

    statements = tuple(statement.strip() for statement in sql.split(";") if statement.strip())
    table_names: list[str] = []
    index_count = 0
    for statement in statements:
        table_match = re.match(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-z][a-z0-9_]*)\b",
            statement,
            flags=re.IGNORECASE,
        )
        if table_match:
            table_name = table_match.group(1).lower()
            if table_name not in FOUNDATION_TABLES:
                raise EntityPersistenceError(
                    f"entity migration creates unexpected table {table_name}"
                )
            table_names.append(table_name)
            continue
        index_match = re.match(
            r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS\s+"
            r"[a-z][a-z0-9_]*\s+ON\s+([a-z][a-z0-9_]*)\b",
            statement,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if index_match:
            target_table = index_match.group(1).lower()
            if target_table not in FOUNDATION_TABLES:
                raise EntityPersistenceError(
                    f"entity migration index targets unexpected table {target_table}"
                )
            index_count += 1
            continue
        raise EntityPersistenceError(
            "entity migration contains a statement outside CREATE TABLE/INDEX IF NOT EXISTS"
        )

    if tuple(sorted(table_names)) != tuple(sorted(FOUNDATION_TABLES)):
        raise EntityPersistenceError("entity migration must create exactly both foundation tables")
    for table_name in FOUNDATION_TABLES:
        actual = _declared_columns(_table_body(sql, table_name))
        expected = _EXPECTED_COLUMNS[table_name]
        if actual != expected:
            raise EntityPersistenceError(
                f"{table_name} columns do not match frozen contract; "
                f"missing={sorted(expected - actual)}, unexpected={sorted(actual - expected)}"
            )

    return MigrationAudit(
        path=migration_path,
        statement_count=len(statements),
        table_names=tuple(sorted(table_names)),
        index_count=index_count,
        contract_columns_match=True,
        repeat_safe=True,
    )


def _inspect_foundation_cursor(cursor: Any) -> EntityFoundationStatus:
    cursor.execute(
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN ('news_entity_revision', 'news_entity_alias_revision')
        ORDER BY table_name, ordinal_position
        """
    )
    columns: dict[str, list[str]] = {}
    for table_name, column_name in cursor.fetchall():
        columns.setdefault(table_name, []).append(column_name)

    cursor.execute(
        """
        SELECT tc.table_name,
               kcu.column_name,
               kcu.ordinal_position
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON kcu.constraint_schema = tc.constraint_schema
         AND kcu.constraint_name = tc.constraint_name
         AND kcu.table_schema = tc.table_schema
         AND kcu.table_name = tc.table_name
        WHERE tc.table_schema = 'public'
          AND tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_name IN ('news_entity_revision', 'news_entity_alias_revision')
        ORDER BY tc.table_name, kcu.ordinal_position
        """
    )
    primary_key_rows: dict[str, list[tuple[int, str]]] = {}
    for table_name, column_name, ordinal_position in cursor.fetchall():
        primary_key_rows.setdefault(table_name, []).append(
            (int(ordinal_position), column_name)
        )
    primary_keys = {
        table_name: tuple(
            column_name
            for _, column_name in sorted(rows, key=lambda row: row[0])
        )
        for table_name, rows in primary_key_rows.items()
    }
    cursor.execute(
        """
        SELECT target.relname AS table_name,
               index_rel.relname AS index_name,
               index_meta.indisunique,
               index_meta.indnkeyatts,
               index_meta.indnatts,
               pg_get_indexdef(index_meta.indexrelid, 1, TRUE) AS first_key,
               pg_get_expr(index_meta.indpred, index_meta.indrelid) AS predicate
        FROM pg_index AS index_meta
        JOIN pg_class AS index_rel
          ON index_rel.oid = index_meta.indexrelid
        JOIN pg_class AS target
          ON target.oid = index_meta.indrelid
        JOIN pg_namespace AS namespace
          ON namespace.oid = target.relnamespace
        WHERE namespace.nspname = 'public'
          AND index_rel.relname IN (
              'uq_news_entity_latest_revision',
              'uq_news_entity_alias_latest_revision'
          )
        ORDER BY index_rel.relname
        """
    )
    latest_indexes = {
        index_name: {
            "table_name": table_name,
            "is_unique": is_unique,
            "key_count": key_count,
            "attribute_count": attribute_count,
            "first_key": first_key,
            "predicate": predicate,
        }
        for (
            table_name,
            index_name,
            is_unique,
            key_count,
            attribute_count,
            first_key,
            predicate,
        ) in cursor.fetchall()
    }

    diagnostics: list[str] = []
    for table_name in FOUNDATION_TABLES:
        actual = frozenset(columns.get(table_name, ()))
        expected = _EXPECTED_COLUMNS[table_name]
        if not actual:
            diagnostics.append(f"missing table public.{table_name}")
        elif actual != expected:
            diagnostics.append(
                f"public.{table_name} column mismatch: "
                f"missing={sorted(expected - actual)}, unexpected={sorted(actual - expected)}"
            )
    valid_primary_key_tables: list[str] = []
    for table_name, expected_columns in _EXPECTED_PRIMARY_KEYS.items():
        actual_columns = primary_keys.get(table_name)
        if actual_columns != expected_columns:
            diagnostics.append(
                f"public.{table_name} primary key mismatch: "
                f"expected={expected_columns}, actual={actual_columns}"
            )
        else:
            valid_primary_key_tables.append(table_name)

    def normalize_identifier(value: Any) -> str:
        return str(value or "").strip().strip('"').lower()

    def normalize_predicate(value: Any) -> str:
        text = re.sub(r"\s+", "", str(value or "").lower()).replace('"', "")
        while text.startswith("(") and text.endswith(")"):
            text = text[1:-1]
        return text

    valid_latest_indexes: list[str] = []
    for index_name, (expected_table, expected_key) in _EXPECTED_LATEST_INDEXES.items():
        definition = latest_indexes.get(index_name)
        if definition is None:
            diagnostics.append(f"missing latest-revision index: {index_name}")
            continue
        index_diagnostics: list[str] = []
        if definition["table_name"] != expected_table:
            index_diagnostics.append(
                f"table expected={expected_table}, actual={definition['table_name']}"
            )
        if definition["is_unique"] is not True:
            index_diagnostics.append("index must be UNIQUE")
        if definition["key_count"] != 1 or definition["attribute_count"] != 1:
            index_diagnostics.append(
                "index must contain exactly one key and no INCLUDE columns"
            )
        if normalize_identifier(definition["first_key"]) != expected_key:
            index_diagnostics.append(
                f"key expected={expected_key}, actual={definition['first_key']}"
            )
        if normalize_predicate(definition["predicate"]) != "is_latest_revision":
            index_diagnostics.append(
                "predicate expected=is_latest_revision, "
                f"actual={definition['predicate']}"
            )
        if index_diagnostics:
            diagnostics.append(f"{index_name} mismatch: {'; '.join(index_diagnostics)}")
        else:
            valid_latest_indexes.append(index_name)

    return EntityFoundationStatus(
        ready=not diagnostics,
        table_columns={key: tuple(value) for key, value in columns.items()},
        primary_key_tables=tuple(sorted(valid_primary_key_tables)),
        latest_revision_indexes=tuple(sorted(valid_latest_indexes)),
        diagnostics=tuple(diagnostics),
    )


def inspect_entity_foundation(
    *, connection_factory: Callable[[], Any] | None = None
) -> EntityFoundationStatus:
    """Inspect target-table readiness in a read-only transaction."""

    if connection_factory is None:
        from data_collect.utils.db import get_connection

        connection_factory = get_connection
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            return _inspect_foundation_cursor(cursor)
    finally:
        try:
            connection.rollback()
        finally:
            connection.close()


def _enum_value(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else value


def _freeze(value: Any) -> Any:
    value = _enum_value(value)
    if isinstance(value, Mapping):
        return tuple(sorted((str(key), _freeze(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _fact_signature(record: Entity | EntityAlias) -> tuple[tuple[str, Any], ...]:
    return tuple(
        (field.name, _freeze(getattr(record, field.name)))
        for field in fields(record)
        if field.name not in _NON_FACT_FIELDS
    )


def _unique_by_id(
    records: Iterable[Entity | EntityAlias], id_field: str
) -> dict[str, Entity | EntityAlias]:
    result: dict[str, Entity | EntityAlias] = {}
    for record in records:
        record_id = getattr(record, id_field)
        if record_id in result:
            raise EntityPersistenceError(f"duplicate current record: {record_id}")
        result[record_id] = record
    return result


def _current_alias_projection(
    entity_id: str,
    aliases: Iterable[EntityAlias],
) -> tuple[str, ...]:
    """Project current alias facts into a deterministic compatibility tuple."""

    return tuple(sorted({
        alias.alias
        for alias in aliases
        if alias.entity_id == entity_id and alias.is_current
    }))


def plan_entity_sync(
    snapshot: EntityCatalogSnapshot,
    current_entities: Iterable[Entity] = (),
    current_aliases: Iterable[EntityAlias] = (),
    *,
    observed_at: datetime | None = None,
    foundation_status: EntityFoundationStatus | None = None,
    sector_crosswalk_stock_count: int = 0,
) -> EntitySyncPlan:
    """Purely plan append-only revisions for one complete stock snapshot."""

    effective_at = observed_at or datetime.now(timezone.utc)
    if effective_at.tzinfo is None or effective_at.utcoffset() is None:
        raise EntityPersistenceError("observed_at must be timezone-aware")
    if snapshot.company_entities:
        raise EntityPersistenceError("Phase 1 sync refuses Company entities")
    if any(entity.entity_type is not EntityType.STOCK for entity in snapshot.entities):
        raise EntityPersistenceError("Phase 1 sync accepts only STOCK entities")

    existing_entities = _unique_by_id(current_entities, "entity_id")
    existing_aliases = _unique_by_id(current_aliases, "entity_alias_id")
    proposed_entities = _unique_by_id(snapshot.entities, "entity_id")
    proposed_aliases = _unique_by_id(snapshot.aliases, "entity_alias_id")

    alias_inserts: list[EntityAlias] = []
    alias_supersedes: list[RevisionPointer] = []
    locked_alias_ids: list[str] = []
    final_aliases = dict(existing_aliases)

    for alias_id in sorted(proposed_aliases):
        proposed = proposed_aliases[alias_id]
        assert isinstance(proposed, EntityAlias)
        current = existing_aliases.get(alias_id)
        if current is None:
            next_alias = replace(
                proposed,
                revision=1,
                manual_lock=False,
                created_at=effective_at,
                updated_at=effective_at,
            )
            alias_inserts.append(next_alias)
            final_aliases[alias_id] = next_alias
            continue
        assert isinstance(current, EntityAlias)
        if _fact_signature(current) == _fact_signature(proposed):
            final_aliases[alias_id] = current
            continue
        if current.manual_lock:
            locked_alias_ids.append(alias_id)
            final_aliases[alias_id] = current
            continue
        alias_supersedes.append(RevisionPointer(alias_id, current.revision))
        next_alias = replace(
            proposed,
            revision=current.revision + 1,
            manual_lock=False,
            created_at=current.created_at,
            updated_at=effective_at,
        )
        alias_inserts.append(next_alias)
        final_aliases[alias_id] = next_alias

    scoped_entity_ids = frozenset(proposed_entities)
    for alias_id in sorted(set(existing_aliases) - set(proposed_aliases)):
        current = existing_aliases[alias_id]
        assert isinstance(current, EntityAlias)
        if current.entity_id not in scoped_entity_ids or not current.is_current:
            continue
        if current.manual_lock:
            locked_alias_ids.append(alias_id)
            final_aliases[alias_id] = current
            continue
        alias_supersedes.append(RevisionPointer(alias_id, current.revision))
        next_alias = replace(
            current,
            revision=current.revision + 1,
            is_current=False,
            updated_at=effective_at,
        )
        alias_inserts.append(next_alias)
        final_aliases[alias_id] = next_alias

    # Entity.aliases is a compatibility projection only.  Alias governance,
    # including manual locks and retirement, is resolved above and is the sole
    # source for this field.
    entity_inserts: list[Entity] = []
    entity_supersedes: list[RevisionPointer] = []
    final_alias_records = tuple(
        alias for alias in final_aliases.values() if isinstance(alias, EntityAlias)
    )
    for entity_id in sorted(proposed_entities):
        proposed = proposed_entities[entity_id]
        assert isinstance(proposed, Entity)
        projected = replace(
            proposed,
            aliases=_current_alias_projection(entity_id, final_alias_records),
        )
        current = existing_entities.get(entity_id)
        if current is None:
            entity_inserts.append(replace(
                projected,
                entity_revision=1,
                created_at=effective_at,
                updated_at=effective_at,
            ))
        elif _fact_signature(current) != _fact_signature(projected):
            assert isinstance(current, Entity)
            entity_supersedes.append(
                RevisionPointer(entity_id, current.entity_revision)
            )
            entity_inserts.append(replace(
                projected,
                entity_revision=current.entity_revision + 1,
                created_at=current.created_at,
                updated_at=effective_at,
            ))

    if foundation_status is None:
        foundation_status = EntityFoundationStatus(
            ready=True,
            table_columns={},
            primary_key_tables=(),
            latest_revision_indexes=(),
            diagnostics=(),
        )
    return EntitySyncPlan(
        foundation_status=foundation_status,
        source_stock_count=len(snapshot.stock_entities),
        sector_crosswalk_stock_count=sector_crosswalk_stock_count,
        entity_inserts=tuple(entity_inserts),
        alias_inserts=tuple(alias_inserts),
        entity_supersedes=tuple(entity_supersedes),
        alias_supersedes=tuple(alias_supersedes),
        locked_alias_ids=tuple(sorted(set(locked_alias_ids))),
    )


def _select_columns(columns: tuple[str, ...]) -> str:
    return ", ".join(f'"{column}"' for column in columns)


def _decode_db_record(
    columns: tuple[str, ...], row: Iterable[Any]
) -> dict[str, Any]:
    """Normalize explicit PostgreSQL NUMERIC contract fields at the read boundary."""

    values = dict(zip(columns, row, strict=True))
    for field_name in _DB_DECIMAL_FLOAT_FIELDS:
        value = values.get(field_name)
        if isinstance(value, Decimal):
            values[field_name] = float(value)
    return values


def _load_current_records(
    cursor: Any, entity_ids: tuple[str, ...]
) -> tuple[tuple[Entity, ...], tuple[EntityAlias, ...]]:
    if not entity_ids:
        return (), ()
    cursor.execute(
        f"SELECT {_select_columns(ENTITY_COLUMNS)} FROM {ENTITY_TABLE} "
        "WHERE is_latest_revision AND entity_id = ANY(%s) ORDER BY entity_id",
        (list(entity_ids),),
    )
    current_entities = tuple(
        Entity(**_decode_db_record(ENTITY_COLUMNS, row)) for row in cursor.fetchall()
    )
    cursor.execute(
        f"SELECT {_select_columns(ENTITY_ALIAS_COLUMNS)} FROM {ENTITY_ALIAS_TABLE} "
        "WHERE is_latest_revision AND entity_id = ANY(%s) "
        "ORDER BY entity_id, entity_alias_id",
        (list(entity_ids),),
    )
    current_aliases = tuple(
        EntityAlias(**_decode_db_record(ENTITY_ALIAS_COLUMNS, row))
        for row in cursor.fetchall()
    )
    return current_entities, current_aliases


def _db_value(field_name: str, value: Any, json_adapter: Callable[[Any], Any]) -> Any:
    value = _enum_value(value)
    if field_name in {"aliases", "external_ids", "provenance_refs"}:
        if isinstance(value, tuple):
            value = list(value)
        return json_adapter(value)
    if isinstance(value, tuple):
        return list(value)
    return value


def _insert_record(
    cursor: Any,
    table_name: str,
    record: Entity | EntityAlias,
    columns: tuple[str, ...],
    json_adapter: Callable[[Any], Any],
) -> None:
    all_columns = (*columns, "is_latest_revision")
    placeholders = ", ".join(["%s"] * len(all_columns))
    values = tuple(
        _db_value(column, getattr(record, column), json_adapter)
        for column in columns
    ) + (True,)
    cursor.execute(
        f"INSERT INTO {table_name} ({_select_columns(all_columns)}) "
        f"VALUES ({placeholders})",
        values,
    )


def _apply_plan_cursor(
    cursor: Any,
    plan: EntitySyncPlan,
    *,
    json_adapter: Callable[[Any], Any],
) -> None:
    for pointer in plan.entity_supersedes:
        cursor.execute(
            f"UPDATE {ENTITY_TABLE} SET is_latest_revision = FALSE "
            "WHERE entity_id = %s AND entity_revision = %s AND is_latest_revision",
            (pointer.record_id, pointer.revision),
        )
        if getattr(cursor, "rowcount", 1) != 1:
            raise EntityPersistenceError(
                f"stale Entity revision during sync: {pointer.record_id}:{pointer.revision}"
            )
    for record in plan.entity_inserts:
        _insert_record(cursor, ENTITY_TABLE, record, ENTITY_COLUMNS, json_adapter)
    for pointer in plan.alias_supersedes:
        cursor.execute(
            f"UPDATE {ENTITY_ALIAS_TABLE} SET is_latest_revision = FALSE "
            "WHERE entity_alias_id = %s AND revision = %s AND is_latest_revision",
            (pointer.record_id, pointer.revision),
        )
        if getattr(cursor, "rowcount", 1) != 1:
            raise EntityPersistenceError(
                f"stale EntityAlias revision during sync: {pointer.record_id}:{pointer.revision}"
            )
    for record in plan.alias_inserts:
        _insert_record(
            cursor,
            ENTITY_ALIAS_TABLE,
            record,
            ENTITY_ALIAS_COLUMNS,
            json_adapter,
        )


def synchronize_entities(
    *,
    apply: bool = False,
    observed_at: datetime | None = None,
    limit: int | None = None,
    connection_factory: Callable[[], Any] | None = None,
    json_adapter: Callable[[Any], Any] | None = None,
) -> EntitySyncResult:
    """Build and optionally apply one explicit, append-only sync transaction."""

    if apply and limit is not None:
        raise EntityPersistenceError("--limit is dry-run only and cannot be used with apply")
    audit_entity_migration_sql()
    if connection_factory is None:
        from data_collect.utils.db import get_connection

        connection_factory = get_connection
    effective_at = observed_at or datetime.now(timezone.utc)
    source_rows = load_shadow_inputs_from_postgres(
        limit=limit,
        connection_factory=connection_factory,
    )
    snapshot = build_entity_catalog(
        source_rows.instrument_rows,
        source_rows.changelog_rows,
        observed_at=effective_at,
    )
    sector_crosswalk = build_sector_crosswalk(source_rows.sector_rows)

    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            if not apply:
                cursor.execute("SET TRANSACTION READ ONLY")
            foundation_status = _inspect_foundation_cursor(cursor)
            if foundation_status.ready:
                current_entities, current_aliases = _load_current_records(
                    cursor,
                    tuple(entity.entity_id for entity in snapshot.stock_entities),
                )
            else:
                current_entities, current_aliases = (), ()
            plan = plan_entity_sync(
                snapshot,
                current_entities,
                current_aliases,
                observed_at=effective_at,
                foundation_status=foundation_status,
                sector_crosswalk_stock_count=len(sector_crosswalk.by_stock_code),
            )
            if apply:
                if not foundation_status.ready:
                    raise EntityFoundationNotReadyError(
                        "; ".join(foundation_status.diagnostics)
                    )
                if json_adapter is None:
                    from psycopg2.extras import Json

                    json_adapter = Json
                _apply_plan_cursor(cursor, plan, json_adapter=json_adapter)
        if apply:
            connection.commit()
        else:
            connection.rollback()
        return EntitySyncResult(plan=plan, applied=apply)
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


__all__ = [
    "DEFAULT_MIGRATION_PATH",
    "ENTITY_ALIAS_TABLE",
    "ENTITY_TABLE",
    "EntityFoundationNotReadyError",
    "EntityFoundationStatus",
    "EntityPersistenceError",
    "EntitySyncPlan",
    "EntitySyncResult",
    "MigrationAudit",
    "RevisionPointer",
    "audit_entity_migration_sql",
    "inspect_entity_foundation",
    "plan_entity_sync",
    "synchronize_entities",
]
