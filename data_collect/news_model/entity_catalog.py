"""Pure shadow projections for Phase 1 stock Entity and EntityAlias.

The source tables remain authoritative inputs.  Importing this module performs
no database, OpenSearch, filesystem, crawler, or network operation.  Database
access is isolated in ``load_shadow_inputs_from_postgres`` and occurs only when
that function is called explicitly.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
import hashlib
import json
import re
from types import MappingProxyType
from typing import Any
import unicodedata

from data_collect.news_model.contracts import (
    ContractValidationError,
    DerivedBy,
    Entity,
    EntityAlias,
    EntityAliasType,
    EntityStatus,
    EntityType,
    Exchange,
    exchange_for_stock_code,
    make_stock_entity_id,
    validate_stock_code,
)


INSTRUMENT_INFO_PROVENANCE = "instrument_info"
INSTRUMENT_CHANGELOG_PROVENANCE = "instrument_changelog"
SECTOR_STOCK_PROVENANCE = "sector_stock"

_SHANGHAI_TIMEZONE = timezone(timedelta(hours=8), "Asia/Shanghai")
_WHITESPACE_RE = re.compile(r"\s+")
_COMPANY_FIELD_CANDIDATES = frozenset(
    {
        "CompanyName",
        "CompanyFullName",
        "EnterpriseName",
        "FullName",
        "IssuerFullName",
        "IssuerName",
        "OrganizationName",
    }
)
_EXCHANGE_ID_MAP = {
    "SH": Exchange.SSE,
    "SSE": Exchange.SSE,
    "XSHG": Exchange.SSE,
    "SZ": Exchange.SZSE,
    "SZSE": Exchange.SZSE,
    "XSHE": Exchange.SZSE,
    "BJ": Exchange.BSE,
    "BSE": Exchange.BSE,
    "XBSE": Exchange.BSE,
}


class EntityCatalogError(ValueError):
    """Base error for invalid shadow input or an unsafe inference."""


class MissingInstrumentFieldError(EntityCatalogError):
    """Raised when a required real instrument_info field is absent."""


class ExchangeMismatchError(EntityCatalogError):
    """Raised when ExchangeID conflicts with the canonical code suffix."""


class AliasMatchStatus(str, Enum):
    """Candidate cardinality only; UNIQUE does not prove complete history."""

    NO_MATCH = "no_match"
    UNIQUE = "unique"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True, slots=True)
class EntityDiagnostic:
    code: str
    message: str
    stock_code: str | None = None
    field_name: str | None = None
    value: Any = None


@dataclass(frozen=True, slots=True)
class StockEntityProjection:
    entity: Entity
    aliases: tuple[EntityAlias, ...]
    diagnostics: tuple[EntityDiagnostic, ...] = ()


@dataclass(frozen=True, slots=True)
class EntityCatalogSnapshot:
    entities: tuple[Entity, ...]
    aliases: tuple[EntityAlias, ...]
    diagnostics: tuple[EntityDiagnostic, ...] = ()

    @property
    def stock_entities(self) -> tuple[Entity, ...]:
        return tuple(entity for entity in self.entities if entity.entity_type is EntityType.STOCK)

    @property
    def company_entities(self) -> tuple[Entity, ...]:
        return tuple(entity for entity in self.entities if entity.entity_type is EntityType.COMPANY)

    @property
    def historical_aliases(self) -> tuple[EntityAlias, ...]:
        return tuple(alias for alias in self.aliases if not alias.is_current)


@dataclass(frozen=True, slots=True)
class AliasMatchResult:
    query: str
    normalized_query: str
    status: AliasMatchStatus
    entity_ids: tuple[str, ...]
    matches: tuple[EntityAlias, ...]

    @property
    def ambiguous(self) -> bool:
        return self.status is AliasMatchStatus.AMBIGUOUS


@dataclass(frozen=True, slots=True)
class SectorClassification:
    prefix: str
    system_hint: str
    kind_hint: str
    level_hint: int | None


@dataclass(frozen=True, slots=True)
class SectorCrosswalk:
    by_stock_code: Mapping[str, tuple[str, ...]]
    classifications: Mapping[str, SectorClassification]
    diagnostics: tuple[EntityDiagnostic, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "by_stock_code",
            MappingProxyType(dict(self.by_stock_code)),
        )
        object.__setattr__(
            self,
            "classifications",
            MappingProxyType(dict(self.classifications)),
        )


@dataclass(frozen=True, slots=True)
class ShadowInputRows:
    instrument_rows: tuple[Mapping[str, Any], ...]
    changelog_rows: tuple[Mapping[str, Any], ...]
    sector_rows: tuple[Mapping[str, Any], ...]
    instrument_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PostgresShadowInspection:
    snapshot: EntityCatalogSnapshot
    sector_crosswalk: SectorCrosswalk
    instrument_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _NameChange:
    stock_code: str
    changed_date: date
    boundary: datetime
    old_name: str
    new_name: str

    @property
    def fact_key(self) -> str:
        return (
            f"instrument_changelog:{self.stock_code}:"
            f"{self.changed_date.isoformat()}:InstrumentName:old_value"
        )


def _require_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EntityCatalogError(f"{field_name} must be a non-empty string")
    return value.strip()


def _coerce_aware_datetime(value: datetime | str, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise EntityCatalogError(f"{field_name} must be ISO 8601 datetime") from exc
    else:
        raise EntityCatalogError(f"{field_name} must be a datetime or ISO 8601 string")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EntityCatalogError(f"{field_name} must be timezone-aware")
    return parsed


def _coerce_observation_date(value: Any, field_name: str = "changed_at") -> tuple[date, datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise EntityCatalogError(
                f"{field_name} datetime must be timezone-aware; actual table semantics are DATE"
            )
        observed_date = value.astimezone(_SHANGHAI_TIMEZONE).date()
    elif isinstance(value, date):
        observed_date = value
    elif isinstance(value, str):
        try:
            observed_date = date.fromisoformat(value.strip())
        except ValueError as exc:
            raise EntityCatalogError(f"{field_name} must be an ISO calendar date") from exc
    else:
        raise EntityCatalogError(f"{field_name} must be a date or ISO calendar date")
    boundary = datetime.combine(observed_date, time.min, tzinfo=_SHANGHAI_TIMEZONE)
    return observed_date, boundary


def normalize_entity_alias(value: str) -> str:
    """Return a minimal deterministic NFKC/casefold matching form."""

    original = _require_text(value, "alias")
    normalized = unicodedata.normalize("NFKC", original)
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip().casefold()
    if not normalized:
        raise EntityCatalogError("alias normalizes to an empty string")
    return normalized


def make_entity_alias_id(
    entity_id: str,
    alias_type: EntityAliasType | str,
    alias: str,
    *,
    fact_key: str,
) -> str:
    """Create a stable ID for one logical alias fact/occurrence.

    ``fact_key`` names the source fact slot or observed historical occurrence.
    It prevents a repeated name in disjoint validity intervals from collapsing
    into a single identity.  Revision is intentionally excluded.
    """

    entity_id = _require_text(entity_id, "entity_id")
    if not entity_id.startswith("ent_"):
        raise EntityCatalogError("entity_id must use the ent_ namespace")
    try:
        alias_type_value = (
            alias_type.value if isinstance(alias_type, EntityAliasType) else EntityAliasType(alias_type).value
        )
    except (TypeError, ValueError) as exc:
        raise EntityCatalogError(f"invalid alias_type: {alias_type!r}") from exc
    original_alias = _require_text(alias, "alias")
    fact_key = _require_text(fact_key, "fact_key")
    payload = json.dumps(
        {
            "alias": original_alias,
            "alias_type": alias_type_value,
            "entity_id": entity_id,
            "fact_key": fact_key,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"ealias_{hashlib.sha256(payload).hexdigest()[:32]}"


def _make_alias(
    *,
    entity_id: str,
    alias: str,
    alias_type: EntityAliasType,
    language: str,
    fact_key: str,
    observed_at: datetime,
    provenance_source_ids: Sequence[str],
    provenance_refs: Sequence[Mapping[str, Any]],
    valid_from: datetime | None = None,
    valid_to: datetime | None = None,
    is_current: bool,
) -> EntityAlias:
    original_alias = _require_text(alias, "alias")
    return EntityAlias(
        entity_alias_id=make_entity_alias_id(
            entity_id,
            alias_type,
            original_alias,
            fact_key=fact_key,
        ),
        entity_id=entity_id,
        alias=original_alias,
        normalized_alias=normalize_entity_alias(original_alias),
        alias_type=alias_type,
        language=language,
        valid_from=valid_from,
        valid_to=valid_to,
        provenance_source_ids=tuple(provenance_source_ids),
        provenance_refs=tuple(provenance_refs),
        confidence=1.0,
        derived_by=DerivedBy.MASTER_DATA,
        revision=1,
        is_current=is_current,
        manual_lock=False,
        created_at=observed_at,
        updated_at=observed_at,
    )


def _cross_check_exchange(
    stock_code: str,
    exchange_id: Any,
    *,
    strict: bool,
) -> tuple[Exchange, tuple[EntityDiagnostic, ...]]:
    expected = exchange_for_stock_code(stock_code)
    if exchange_id is None:
        return expected, ()
    raw = str(exchange_id).strip().upper()
    observed = _EXCHANGE_ID_MAP.get(raw)
    if observed is None:
        diagnostic = EntityDiagnostic(
            code="unknown_exchange_id",
            message=(
                f"ExchangeID {exchange_id!r} is not a recognized exchange code; "
                f"stock suffix resolves to {expected.value}"
            ),
            stock_code=stock_code,
            field_name="ExchangeID",
            value=exchange_id,
        )
        if strict:
            raise EntityCatalogError(diagnostic.message)
        return expected, (diagnostic,)
    if observed is not expected:
        diagnostic = EntityDiagnostic(
            code="exchange_mismatch",
            message=(
                f"ExchangeID {exchange_id!r} resolves to {observed.value}, but "
                f"stock code {stock_code} requires {expected.value}"
            ),
            stock_code=stock_code,
            field_name="ExchangeID",
            value=exchange_id,
        )
        if strict:
            raise ExchangeMismatchError(diagnostic.message)
        return expected, (diagnostic,)
    return expected, ()


def stock_entity_from_instrument_row(
    row: Mapping[str, Any],
    *,
    observed_at: datetime | str,
    strict_exchange: bool = True,
    current_name_valid_from: datetime | None = None,
    current_name_fact_key: str = "instrument_info:InstrumentName:current:baseline",
    current_name_boundary_ref: Mapping[str, Any] | None = None,
) -> StockEntityProjection:
    """Project one real instrument_info row without mutating it."""

    if not isinstance(row, Mapping):
        raise EntityCatalogError("instrument row must be a mapping")
    if "stock_code" not in row:
        raise MissingInstrumentFieldError("instrument_info row missing stock_code")
    if "InstrumentName" not in row:
        raise MissingInstrumentFieldError("instrument_info row missing InstrumentName")
    try:
        stock_code = validate_stock_code(row["stock_code"])
    except ContractValidationError as exc:
        raise EntityCatalogError(f"invalid instrument stock_code: {exc}") from exc
    name = _require_text(row["InstrumentName"], "InstrumentName")
    generated_at = _coerce_aware_datetime(observed_at, "observed_at")
    exchange, exchange_diagnostics = _cross_check_exchange(
        stock_code,
        row.get("ExchangeID"),
        strict=strict_exchange,
    )
    entity_id = make_stock_entity_id(stock_code)
    bare_code = stock_code.split(".", 1)[0]
    if (current_name_valid_from is None) != (current_name_boundary_ref is None):
        raise EntityCatalogError(
            "current InstrumentName valid_from and its changelog boundary provenance "
            "must be supplied together"
        )
    current_name_sources = [INSTRUMENT_INFO_PROVENANCE]
    current_name_refs: list[Mapping[str, Any]] = [
        {"table": "instrument_info", "column": "InstrumentName"}
    ]
    if current_name_boundary_ref is not None:
        current_name_sources.append(INSTRUMENT_CHANGELOG_PROVENANCE)
        current_name_refs.append(dict(current_name_boundary_ref))

    aliases = (
        _make_alias(
            entity_id=entity_id,
            alias=stock_code,
            alias_type=EntityAliasType.TICKER,
            language="und",
            fact_key="instrument_info:stock_code:qualified",
            observed_at=generated_at,
            provenance_source_ids=(INSTRUMENT_INFO_PROVENANCE,),
            provenance_refs=({"table": "instrument_info", "column": "stock_code", "form": "qualified"},),
            is_current=True,
        ),
        _make_alias(
            entity_id=entity_id,
            alias=bare_code,
            alias_type=EntityAliasType.TICKER,
            language="und",
            fact_key="instrument_info:stock_code:bare",
            observed_at=generated_at,
            provenance_source_ids=(INSTRUMENT_INFO_PROVENANCE,),
            provenance_refs=({"table": "instrument_info", "column": "stock_code", "form": "bare"},),
            is_current=True,
        ),
        _make_alias(
            entity_id=entity_id,
            alias=name,
            alias_type=EntityAliasType.STOCK_SHORT_NAME,
            language="zh-CN",
            fact_key=current_name_fact_key,
            observed_at=generated_at,
            provenance_source_ids=current_name_sources,
            provenance_refs=current_name_refs,
            valid_from=current_name_valid_from,
            is_current=True,
        ),
    )

    diagnostics = list(exchange_diagnostics)
    for field_name in sorted(_COMPANY_FIELD_CANDIDATES & set(row)):
        value = row.get(field_name)
        if value is not None and str(value).strip():
            diagnostics.append(
                EntityDiagnostic(
                    code="unverified_company_field",
                    message=(
                        f"{field_name} is present but current repository semantics do not "
                        "prove an issuer Company Entity; no company was created"
                    ),
                    stock_code=stock_code,
                    field_name=field_name,
                    value=value,
                )
            )

    current_alias_values = tuple(dict.fromkeys((stock_code, bare_code, name)))
    entity = Entity(
        entity_id=entity_id,
        entity_revision=1,
        entity_type=EntityType.STOCK,
        canonical_name=name,
        normalized_name=normalize_entity_alias(name),
        short_name=name,
        aliases=current_alias_values,
        stock_code=stock_code,
        exchange=exchange,
        external_ids={},
        country_region_codes=("CN",),
        status=EntityStatus.ACTIVE,
        provenance_source_ids=(INSTRUMENT_INFO_PROVENANCE,),
        confidence=1.0,
        created_at=generated_at,
        updated_at=generated_at,
    )
    return StockEntityProjection(entity=entity, aliases=aliases, diagnostics=tuple(diagnostics))


def _parse_name_changes(
    changelog_rows: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, list[_NameChange]], list[EntityDiagnostic]]:
    grouped: dict[str, list[_NameChange]] = {}
    diagnostics: list[EntityDiagnostic] = []
    seen_keys: dict[tuple[str, date], tuple[str, str]] = {}
    for row in changelog_rows:
        if not isinstance(row, Mapping):
            diagnostics.append(
                EntityDiagnostic("invalid_changelog_row", "changelog row must be a mapping")
            )
            continue
        if row.get("field_name") != "InstrumentName":
            continue
        try:
            stock_code = validate_stock_code(row.get("stock_code"))
            old_name = _require_text(row.get("old_value"), "old_value")
            new_name = _require_text(row.get("new_value"), "new_value")
            changed_date, boundary = _coerce_observation_date(row.get("changed_at"))
        except (ContractValidationError, EntityCatalogError) as exc:
            diagnostics.append(
                EntityDiagnostic(
                    code="insufficient_name_changelog",
                    message=f"InstrumentName changelog cannot produce an alias: {exc}",
                    stock_code=str(row.get("stock_code") or "") or None,
                    field_name="InstrumentName",
                )
            )
            continue
        if old_name == new_name:
            diagnostics.append(
                EntityDiagnostic(
                    code="no_op_name_changelog",
                    message="InstrumentName old_value equals new_value; no historical alias emitted",
                    stock_code=stock_code,
                    field_name="InstrumentName",
                    value=old_name,
                )
            )
            continue
        logical_key = (stock_code, changed_date)
        values = (old_name, new_name)
        if logical_key in seen_keys:
            if seen_keys[logical_key] != values:
                raise EntityCatalogError(
                    f"conflicting InstrumentName changelog facts for {stock_code} on {changed_date}"
                )
            continue
        seen_keys[logical_key] = values
        grouped.setdefault(stock_code, []).append(
            _NameChange(
                stock_code=stock_code,
                changed_date=changed_date,
                boundary=boundary,
                old_name=old_name,
                new_name=new_name,
            )
        )
    for changes in grouped.values():
        changes.sort(key=lambda item: item.changed_date)
    return grouped, diagnostics


def _historical_aliases(
    changes_by_stock: Mapping[str, Sequence[_NameChange]],
    current_names: Mapping[str, str],
    *,
    observed_at: datetime,
) -> tuple[
    list[EntityAlias],
    dict[str, datetime],
    dict[str, str],
    dict[str, Mapping[str, Any]],
    list[EntityDiagnostic],
]:
    aliases: list[EntityAlias] = []
    current_valid_from: dict[str, datetime] = {}
    current_fact_keys: dict[str, str] = {}
    current_boundary_refs: dict[str, Mapping[str, Any]] = {}
    diagnostics: list[EntityDiagnostic] = []
    for stock_code, changes in sorted(changes_by_stock.items()):
        current_name = current_names.get(stock_code)
        if current_name is None:
            diagnostics.append(
                EntityDiagnostic(
                    code="orphan_name_changelog",
                    message="InstrumentName changelog has no current instrument_info row",
                    stock_code=stock_code,
                    field_name="InstrumentName",
                )
            )
            continue
        entity_id = make_stock_entity_id(stock_code)
        previous: _NameChange | None = None
        for change in changes:
            valid_from = None
            if previous is not None:
                if previous.new_name == change.old_name:
                    valid_from = previous.boundary
                else:
                    diagnostics.append(
                        EntityDiagnostic(
                            code="name_changelog_chain_gap",
                            message=(
                                f"previous new_value {previous.new_name!r} does not equal "
                                f"next old_value {change.old_name!r}; valid_from remains unknown"
                            ),
                            stock_code=stock_code,
                            field_name="InstrumentName",
                        )
                    )
            aliases.append(
                _make_alias(
                    entity_id=entity_id,
                    alias=change.old_name,
                    alias_type=EntityAliasType.FORMER_NAME,
                    language="zh-CN",
                    fact_key=change.fact_key,
                    observed_at=observed_at,
                    provenance_source_ids=(INSTRUMENT_CHANGELOG_PROVENANCE,),
                    provenance_refs=(
                        {
                            "table": "instrument_changelog",
                            "field_name": "InstrumentName",
                            "changed_at": change.changed_date.isoformat(),
                            "time_semantics": "system_observation_date",
                            "time_precision": "day",
                            "value_role": "old_value",
                        },
                    ),
                    valid_from=valid_from,
                    valid_to=change.boundary,
                    is_current=False,
                )
            )
            diagnostics.append(
                EntityDiagnostic(
                    code="historical_boundary_is_observation_date",
                    message=(
                        "validity boundary is the system-observed changelog date, "
                        "not an official name-change effective timestamp"
                    ),
                    stock_code=stock_code,
                    field_name="changed_at",
                    value=change.changed_date.isoformat(),
                )
            )
            previous = change
        if changes and changes[-1].new_name == current_name:
            current_valid_from[stock_code] = changes[-1].boundary
            current_fact_keys[stock_code] = (
                f"instrument_changelog:{stock_code}:"
                f"{changes[-1].changed_date.isoformat()}:InstrumentName:new_value:current"
            )
            current_boundary_refs[stock_code] = {
                "table": "instrument_changelog",
                "field_name": "InstrumentName",
                "changed_at": changes[-1].changed_date.isoformat(),
                "time_semantics": "system_observation_date",
                "time_precision": "day",
                "value_role": "new_value",
            }
        elif changes:
            diagnostics.append(
                EntityDiagnostic(
                    code="current_name_not_changelog_tail",
                    message=(
                        f"current InstrumentName {current_name!r} does not equal last "
                        f"changelog new_value {changes[-1].new_name!r}; current valid_from remains unknown"
                    ),
                    stock_code=stock_code,
                    field_name="InstrumentName",
                )
            )
    return (
        aliases,
        current_valid_from,
        current_fact_keys,
        current_boundary_refs,
        diagnostics,
    )


def build_entity_catalog(
    instrument_rows: Iterable[Mapping[str, Any]],
    changelog_rows: Iterable[Mapping[str, Any]] = (),
    *,
    observed_at: datetime | str,
    strict_exchange: bool = True,
) -> EntityCatalogSnapshot:
    """Build a deterministic stock-only shadow Catalog from current rows."""

    generated_at = _coerce_aware_datetime(observed_at, "observed_at")
    rows_by_code: dict[str, Mapping[str, Any]] = {}
    current_names: dict[str, str] = {}
    for row in instrument_rows:
        if not isinstance(row, Mapping):
            raise EntityCatalogError("instrument row must be a mapping")
        if "stock_code" not in row:
            raise MissingInstrumentFieldError("instrument_info row missing stock_code")
        try:
            stock_code = validate_stock_code(row["stock_code"])
        except ContractValidationError as exc:
            raise EntityCatalogError(f"invalid instrument stock_code: {exc}") from exc
        if stock_code in rows_by_code:
            raise EntityCatalogError(f"duplicate instrument_info stock_code: {stock_code}")
        if "InstrumentName" not in row:
            raise MissingInstrumentFieldError(
                f"instrument_info row {stock_code} missing InstrumentName"
            )
        current_name = _require_text(row["InstrumentName"], "InstrumentName")
        rows_by_code[stock_code] = row
        current_names[stock_code] = current_name

    changes_by_stock, diagnostics = _parse_name_changes(changelog_rows)
    (
        historical,
        current_valid_from,
        current_fact_keys,
        current_boundary_refs,
        history_diagnostics,
    ) = _historical_aliases(changes_by_stock, current_names, observed_at=generated_at)
    diagnostics.extend(history_diagnostics)

    entities: list[Entity] = []
    aliases: list[EntityAlias] = []
    for stock_code in sorted(rows_by_code):
        projection = stock_entity_from_instrument_row(
            rows_by_code[stock_code],
            observed_at=generated_at,
            strict_exchange=strict_exchange,
            current_name_valid_from=current_valid_from.get(stock_code),
            current_name_fact_key=current_fact_keys.get(
                stock_code,
                "instrument_info:InstrumentName:current:baseline",
            ),
            current_name_boundary_ref=current_boundary_refs.get(stock_code),
        )
        entities.append(projection.entity)
        aliases.extend(projection.aliases)
        diagnostics.extend(projection.diagnostics)
    aliases.extend(historical)
    aliases.sort(key=lambda item: (item.entity_id, item.entity_alias_id))
    return EntityCatalogSnapshot(
        entities=tuple(entities),
        aliases=tuple(aliases),
        diagnostics=tuple(diagnostics),
    )


def match_entity_alias(
    query: str,
    aliases: Iterable[EntityAlias],
    *,
    at_time: datetime | str | None = None,
) -> AliasMatchResult:
    """Return all matching entity candidates; never choose the first one."""

    normalized_query = normalize_entity_alias(query)
    instant = None if at_time is None else _coerce_aware_datetime(at_time, "at_time")
    matches: list[EntityAlias] = []
    for alias in aliases:
        if alias.normalized_alias != normalized_query:
            continue
        if instant is None:
            if not alias.is_current:
                continue
        else:
            if alias.valid_from is not None and instant < alias.valid_from:
                continue
            # Validity intervals are half-open: [valid_from, valid_to).
            if alias.valid_to is not None and instant >= alias.valid_to:
                continue
        matches.append(alias)
    matches.sort(key=lambda item: (item.entity_id, item.entity_alias_id))
    entity_ids = tuple(sorted({alias.entity_id for alias in matches}))
    if not entity_ids:
        status = AliasMatchStatus.NO_MATCH
    elif len(entity_ids) == 1:
        status = AliasMatchStatus.UNIQUE
    else:
        status = AliasMatchStatus.AMBIGUOUS
    return AliasMatchResult(
        query=query,
        normalized_query=normalized_query,
        status=status,
        entity_ids=entity_ids,
        matches=tuple(matches),
    )


def classify_sector_name(sector_name: str) -> SectorClassification | None:
    name = _require_text(sector_name, "sector_name")
    match = re.match(r"^(GICS|THY)([1-4])", name)
    if match:
        family, level_text = match.groups()
        if family == "GICS":
            return SectorClassification(f"GICS{level_text}", "GICS", "industry", int(level_text))
        if int(level_text) <= 3:
            return SectorClassification(f"THY{level_text}", "THS", "industry", int(level_text))
        return None
    if name.startswith("TDGN"):
        return SectorClassification("TDGN", "TDX", "concept", None)
    if name.startswith("TGN"):
        return SectorClassification("TGN", "THS", "concept", None)
    if name.startswith("TFG"):
        return SectorClassification("TFG", "THS", "style", None)
    return None


def build_sector_crosswalk(
    sector_rows: Iterable[Mapping[str, Any]],
) -> SectorCrosswalk:
    """Build only stock_code -> sector names; no Industry entity/relation."""

    grouped: dict[str, set[str]] = {}
    classifications: dict[str, SectorClassification] = {}
    diagnostics: list[EntityDiagnostic] = []
    seen_sectors: set[str] = set()
    for row in sector_rows:
        if not isinstance(row, Mapping):
            raise EntityCatalogError("sector_stock row must be a mapping")
        try:
            stock_code = validate_stock_code(row.get("stock_code"))
        except ContractValidationError as exc:
            raise EntityCatalogError(f"invalid sector_stock stock_code: {exc}") from exc
        sector_name = _require_text(row.get("sector_name"), "sector_name")
        grouped.setdefault(stock_code, set()).add(sector_name)
        if sector_name not in seen_sectors:
            seen_sectors.add(sector_name)
            classification = classify_sector_name(sector_name)
            if classification is None:
                diagnostics.append(
                    EntityDiagnostic(
                        code="unknown_sector_prefix",
                        message=(
                            "sector_name has no recognized compatibility prefix; "
                            "no Industry Entity or stable industry ID was inferred"
                        ),
                        stock_code=stock_code,
                        field_name="sector_name",
                        value=sector_name,
                    )
                )
            else:
                classifications[sector_name] = classification
    crosswalk = {
        stock_code: tuple(sorted(names))
        for stock_code, names in sorted(grouped.items())
    }
    return SectorCrosswalk(
        by_stock_code=crosswalk,
        classifications=classifications,
        diagnostics=tuple(diagnostics),
    )


def load_shadow_inputs_from_postgres(
    *,
    limit: int | None = None,
    connection_factory: Callable[[], Any] | None = None,
) -> ShadowInputRows:
    """Explicit read-only PostgreSQL boundary; never called during import.

    Columns are discovered from ``information_schema`` before static SELECTs.
    Only the real Step 4 whitelist is read.  No table is created or modified.
    """

    if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 1):
        raise EntityCatalogError("limit must be a positive integer or None")
    if connection_factory is None:
        from data_collect.utils.db import get_connection

        connection_factory = get_connection

    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name IN ('instrument_info', 'instrument_changelog', 'sector_stock')
                ORDER BY table_name, ordinal_position
                """
            )
            columns_by_table: dict[str, list[str]] = {}
            for table_name, column_name in cursor.fetchall():
                columns_by_table.setdefault(table_name, []).append(column_name)

            instrument_columns = columns_by_table.get("instrument_info", [])
            required_instrument = {"stock_code", "InstrumentName"}
            missing_instrument = sorted(required_instrument - set(instrument_columns))
            if missing_instrument:
                raise MissingInstrumentFieldError(
                    f"instrument_info missing required columns: {missing_instrument}"
                )
            for table_name, required in (
                (
                    "instrument_changelog",
                    {"stock_code", "changed_at", "field_name", "old_value", "new_value"},
                ),
                ("sector_stock", {"sector_name", "stock_code", "update_date"}),
            ):
                missing = sorted(required - set(columns_by_table.get(table_name, [])))
                if missing:
                    raise EntityCatalogError(f"{table_name} missing required columns: {missing}")

            selected_columns = ["stock_code", "InstrumentName"]
            if "ExchangeID" in instrument_columns:
                selected_columns.append("ExchangeID")
            selected_columns.extend(
                sorted(_COMPANY_FIELD_CANDIDATES & set(instrument_columns))
            )
            quoted = ", ".join(f'"{column}"' for column in selected_columns)
            instrument_sql = f'SELECT {quoted} FROM "instrument_info" ORDER BY "stock_code"'
            params: tuple[Any, ...] = ()
            if limit is not None:
                instrument_sql += " LIMIT %s"
                params = (limit,)
            cursor.execute(instrument_sql, params)
            instrument_rows = tuple(
                dict(zip(selected_columns, values)) for values in cursor.fetchall()
            )
            selected_codes = [row["stock_code"] for row in instrument_rows]

            if selected_codes:
                cursor.execute(
                    """
                    SELECT stock_code, changed_at, field_name, old_value, new_value
                    FROM instrument_changelog
                    WHERE field_name = 'InstrumentName' AND stock_code = ANY(%s)
                    ORDER BY stock_code, changed_at
                    """,
                    (selected_codes,),
                )
                changelog_rows = tuple(
                    dict(zip(
                        ("stock_code", "changed_at", "field_name", "old_value", "new_value"),
                        values,
                    ))
                    for values in cursor.fetchall()
                )
                cursor.execute(
                    """
                    SELECT sector_name, stock_code, update_date
                    FROM sector_stock
                    WHERE stock_code = ANY(%s)
                    ORDER BY stock_code, sector_name
                    """,
                    (selected_codes,),
                )
                sector_rows = tuple(
                    dict(zip(("sector_name", "stock_code", "update_date"), values))
                    for values in cursor.fetchall()
                )
            else:
                changelog_rows = ()
                sector_rows = ()
        return ShadowInputRows(
            instrument_rows=instrument_rows,
            changelog_rows=changelog_rows,
            sector_rows=sector_rows,
            instrument_columns=tuple(instrument_columns),
        )
    finally:
        try:
            connection.rollback()
        except Exception:
            pass
        finally:
            connection.close()


def inspect_postgres_shadow(
    *,
    observed_at: datetime | str,
    limit: int | None = None,
    connection_factory: Callable[[], Any] | None = None,
) -> PostgresShadowInspection:
    inputs = load_shadow_inputs_from_postgres(
        limit=limit,
        connection_factory=connection_factory,
    )
    return PostgresShadowInspection(
        snapshot=build_entity_catalog(
            inputs.instrument_rows,
            inputs.changelog_rows,
            observed_at=observed_at,
        ),
        sector_crosswalk=build_sector_crosswalk(inputs.sector_rows),
        instrument_columns=inputs.instrument_columns,
    )


__all__ = [
    "AliasMatchResult",
    "AliasMatchStatus",
    "EntityCatalogError",
    "EntityCatalogSnapshot",
    "EntityDiagnostic",
    "ExchangeMismatchError",
    "MissingInstrumentFieldError",
    "PostgresShadowInspection",
    "SectorClassification",
    "SectorCrosswalk",
    "ShadowInputRows",
    "StockEntityProjection",
    "build_entity_catalog",
    "build_sector_crosswalk",
    "classify_sector_name",
    "inspect_postgres_shadow",
    "load_shadow_inputs_from_postgres",
    "make_entity_alias_id",
    "match_entity_alias",
    "normalize_entity_alias",
    "stock_entity_from_instrument_row",
]
