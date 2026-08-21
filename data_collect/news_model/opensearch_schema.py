"""Phase 1 OpenSearch mapping definitions and offline compatibility checks.

This module is deliberately client-free.  It builds dictionaries and compares
mapping snapshots; importing it never reads configuration or connects to
OpenSearch.  Phase 1 keeps the physical ``news-{year}`` indices and only adds
explicit compatibility fields to their existing mappings.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Mapping


NEWS_INDEX_PATTERN = "news-{year}"
SOURCE_INDEX = "news-sources-v1"
SOURCE_HEALTH_INDEX = "news-source-health-current-v1"
ENTITY_INDEX = "news-entities-v1"
ENTITY_ALIAS_INDEX = "news-entity-aliases-v1"

CANONICAL_DATE_FORMAT = "strict_date_optional_time||epoch_millis"


class MappingCompatibilityError(ValueError):
    """Raised when a proposed mapping is not an additive-only change."""


@dataclass(frozen=True, slots=True)
class MappingChange:
    path: str
    definition: Any


@dataclass(frozen=True, slots=True)
class MappingConflict:
    path: str
    existing: Any
    target: Any
    reason: str


@dataclass(frozen=True, slots=True)
class MappingDiff:
    additive_changes: tuple[MappingChange, ...]
    incompatible_changes: tuple[MappingConflict, ...]

    @property
    def is_additive_compatible(self) -> bool:
        return not self.incompatible_changes

    def require_compatible(self) -> "MappingDiff":
        if self.incompatible_changes:
            detail = "; ".join(
                f"{item.path}: {item.reason}"
                for item in self.incompatible_changes
            )
            raise MappingCompatibilityError(
                f"mapping contains incompatible changes: {detail}"
            )
        return self


def _date() -> dict[str, Any]:
    return {"type": "date", "format": CANONICAL_DATE_FORMAT}


def _name_text() -> dict[str, Any]:
    return {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
    }


def _opaque_object() -> dict[str, Any]:
    # Both switches are intentional: values remain in _source, while arbitrary
    # keys can never create mapping fields.
    return {"type": "object", "enabled": False, "dynamic": False}


PHASE1_NEWS_ADDITIVE_PROPERTIES: dict[str, dict[str, Any]] = {
    "news_id": {"type": "keyword"},
    "schema_version": {"type": "keyword"},
    "publish_time": _date(),
    "collect_time": _date(),
    "source_id": {"type": "keyword"},
    "stock_codes": {"type": "keyword"},
    "publish_time_precision": {"type": "keyword"},
    "publish_time_is_estimated": {"type": "boolean"},
    # Phase 1 lifecycle/enrichment fields.  These definitions prepare future
    # news-{year} indices and the offline additive diff only; they do not alter
    # an already deployed index or make its mapping ready for writes.
    "created_at": _date(),
    "updated_at": _date(),
    "embedding_model_version": {"type": "keyword"},
    "raw_archive_uri": {"type": "keyword"},
    "body_truncated": {"type": "boolean"},
}


SOURCE_PROPERTIES: dict[str, dict[str, Any]] = {
    "source_id": {"type": "keyword"},
    "source_revision": {"type": "integer"},
    "schema_version": {"type": "keyword"},
    "source_name": _name_text(),
    "source_category": {"type": "keyword"},
    "acquisition_type": {"type": "keyword"},
    "directness": {"type": "keyword"},
    "default_channel": {"type": "keyword"},
    "country_region_codes": {"type": "keyword"},
    "languages": {"type": "keyword"},
    "source_timezone": {"type": "keyword"},
    "enabled": {"type": "boolean"},
    "authority_status": {"type": "keyword"},
    "source_authority": {"type": "integer"},
    "authority_level": {"type": "keyword"},
    "authority_basis": {"type": "text", "index": False},
    "authority_version": {"type": "keyword"},
    "authority_effective_from": _date(),
    "is_official": {"type": "boolean"},
    "content_license": {"type": "keyword"},
    "paywall_type": {"type": "keyword"},
    "publisher_entity_id": {"type": "keyword"},
    "homepage_url": {"type": "keyword", "ignore_above": 2048},
    "endpoint_url": {"type": "keyword", "ignore_above": 2048},
    "expected_frequency": {"type": "keyword"},
    "collect_interval_seconds": {"type": "long"},
    "created_at": _date(),
    "updated_at": _date(),
}


SOURCE_HEALTH_PROPERTIES: dict[str, dict[str, Any]] = {
    "source_health_id": {"type": "keyword"},
    "source_id": {"type": "keyword"},
    "observed_at": _date(),
    "window_start": _date(),
    "window_end": _date(),
    "health_status": {"type": "keyword"},
    "last_success_at": _date(),
    "last_attempt_at": _date(),
    "consecutive_failures": {"type": "long"},
    "attempt_count": {"type": "long"},
    "success_count": {"type": "long"},
    "collected_item_count": {"type": "long"},
    "new_item_count": {"type": "long"},
    "empty_success_count": {"type": "long"},
    "parse_failure_count": {"type": "long"},
    "latency_ms": {"type": "long"},
    "last_item_publish_time": _date(),
    "data_delay_seconds": {"type": "long"},
    "completeness_status": {"type": "keyword"},
    "completeness_metrics": _opaque_object(),
    "last_error_code": {"type": "keyword"},
    "last_error_summary": {"type": "text", "index": False},
    "health_policy_version": {"type": "keyword"},
    "is_current": {"type": "boolean"},
    "created_at": _date(),
}


ENTITY_PROPERTIES: dict[str, dict[str, Any]] = {
    "entity_id": {"type": "keyword"},
    "entity_revision": {"type": "integer"},
    "schema_version": {"type": "keyword"},
    "entity_type": {"type": "keyword"},
    "canonical_name": _name_text(),
    "normalized_name": {"type": "keyword", "ignore_above": 512},
    "short_name": _name_text(),
    "english_name": _name_text(),
    "aliases": {"type": "keyword", "ignore_above": 512},
    "stock_code": {"type": "keyword"},
    "exchange": {"type": "keyword"},
    "external_ids": _opaque_object(),
    "parent_entity_id": {"type": "keyword"},
    "country_region_codes": {"type": "keyword"},
    "description": {"type": "text"},
    "status": {"type": "keyword"},
    "valid_from": _date(),
    "valid_to": _date(),
    "provenance_source_ids": {"type": "keyword"},
    "confidence": {"type": "scaled_float", "scaling_factor": 10000},
    "entity_model_version": {"type": "keyword"},
    "merged_into_entity_id": {"type": "keyword"},
    "created_at": _date(),
    "updated_at": _date(),
}


ENTITY_ALIAS_PROPERTIES: dict[str, dict[str, Any]] = {
    "entity_alias_id": {"type": "keyword"},
    "entity_id": {"type": "keyword"},
    "alias": _name_text(),
    "normalized_alias": {"type": "keyword", "ignore_above": 512},
    "alias_type": {"type": "keyword"},
    "language": {"type": "keyword"},
    "valid_from": _date(),
    "valid_to": _date(),
    "provenance_source_ids": {"type": "keyword"},
    "provenance_refs": _opaque_object(),
    "confidence": {"type": "scaled_float", "scaling_factor": 10000},
    "derived_by": {"type": "keyword"},
    "entity_model_version": {"type": "keyword"},
    "revision": {"type": "integer"},
    "is_current": {"type": "boolean"},
    "manual_lock": {"type": "boolean"},
    "created_at": _date(),
    "updated_at": _date(),
}


def _standalone_mapping(properties: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "mappings": {
            "dynamic": "strict",
            "properties": deepcopy(dict(properties)),
        }
    }


def build_source_mapping() -> dict[str, Any]:
    return _standalone_mapping(SOURCE_PROPERTIES)


def build_source_health_mapping() -> dict[str, Any]:
    return _standalone_mapping(SOURCE_HEALTH_PROPERTIES)


def build_entity_mapping() -> dict[str, Any]:
    return _standalone_mapping(ENTITY_PROPERTIES)


def build_entity_alias_mapping() -> dict[str, Any]:
    return _standalone_mapping(ENTITY_ALIAS_PROPERTIES)


def build_phase1_standalone_mappings() -> dict[str, dict[str, Any]]:
    """Return fresh offline mapping bodies for the four Phase 1 object indices."""

    return {
        SOURCE_INDEX: build_source_mapping(),
        SOURCE_HEALTH_INDEX: build_source_health_mapping(),
        ENTITY_INDEX: build_entity_mapping(),
        ENTITY_ALIAS_INDEX: build_entity_alias_mapping(),
    }


def _unwrap_index_body(body: Mapping[str, Any]) -> Mapping[str, Any]:
    if "mappings" in body or "properties" in body:
        return body
    candidates = [value for value in body.values() if isinstance(value, Mapping)]
    if len(candidates) == 1 and "mappings" in candidates[0]:
        return candidates[0]
    raise MappingCompatibilityError(
        "expected an index body, mappings body, or a single-index mapping response"
    )


def _mapping_section(body: Mapping[str, Any]) -> Mapping[str, Any]:
    unwrapped = _unwrap_index_body(body)
    mappings = unwrapped.get("mappings", unwrapped)
    if not isinstance(mappings, Mapping):
        raise MappingCompatibilityError("mappings must be an object")
    return mappings


def _settings_section(body: Mapping[str, Any]) -> Mapping[str, Any] | None:
    unwrapped = _unwrap_index_body(body)
    settings = unwrapped.get("settings")
    if settings is None:
        return None
    if not isinstance(settings, Mapping):
        raise MappingCompatibilityError("settings must be an object")
    return settings


def _field_mapping_with_effective_defaults(
    definition: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Normalize only OpenSearch defaults that are semantically explicit.

    OpenSearch treats a ``date`` field without ``format`` as if it declared
    ``strict_date_optional_time||epoch_millis``.  Mapping responses omit that
    default, so comparison must materialize it on both sides.  No other field
    type or mapping parameter is relaxed here.
    """

    if definition.get("type") != "date" or "format" in definition:
        return definition
    normalized = dict(definition)
    normalized["format"] = CANONICAL_DATE_FORMAT
    return normalized


def _compare_dict(
    existing: Mapping[str, Any],
    target: Mapping[str, Any],
    path: str,
    additions: list[MappingChange],
    conflicts: list[MappingConflict],
    *,
    field_definition: bool,
) -> None:
    if field_definition:
        existing = _field_mapping_with_effective_defaults(existing)
        target = _field_mapping_with_effective_defaults(target)

    for key, target_value in target.items():
        child_path = f"{path}.{key}" if path else key
        if key not in existing:
            if field_definition or path.startswith("settings"):
                conflicts.append(MappingConflict(
                    child_path, None, deepcopy(target_value),
                    "existing mapping behavior would gain a new parameter",
                ))
            else:
                additions.append(MappingChange(child_path, deepcopy(target_value)))
            continue

        existing_value = existing[key]
        if key in {"properties", "fields"}:
            if not isinstance(existing_value, Mapping) or not isinstance(target_value, Mapping):
                conflicts.append(MappingConflict(
                    child_path, deepcopy(existing_value), deepcopy(target_value),
                    "field collection changed shape",
                ))
            else:
                _compare_dict(
                    existing_value,
                    target_value,
                    child_path,
                    additions,
                    conflicts,
                    field_definition=False,
                )
            continue

        if isinstance(target_value, Mapping):
            if not isinstance(existing_value, Mapping):
                conflicts.append(MappingConflict(
                    child_path, deepcopy(existing_value), deepcopy(target_value),
                    "object changed shape",
                ))
            else:
                _compare_dict(
                    existing_value,
                    target_value,
                    child_path,
                    additions,
                    conflicts,
                    field_definition=(
                        field_definition
                        or path.endswith(".properties")
                        or path.endswith(".fields")
                    ),
                )
            continue

        if existing_value != target_value:
            reason = "existing mapping parameter would change"
            if key == "type":
                reason = "field type differs"
            elif key in {"analyzer", "search_analyzer", "similarity"}:
                reason = "analysis/BM25 behavior would change"
            elif key in {"dimension", "dims"}:
                reason = "vector dimension would change"
            conflicts.append(MappingConflict(
                child_path, deepcopy(existing_value), deepcopy(target_value), reason
            ))


def diff_index_mappings(
    existing: Mapping[str, Any], target: Mapping[str, Any]
) -> MappingDiff:
    """Compare snapshots, treating omissions from ``target`` as no-op.

    Every target value that already exists must remain identical.  New mapping
    fields are reported as additive changes.  Existing fields omitted from the
    target are preserved, which matches OpenSearch's additive PUT-mapping
    semantics rather than replacement semantics.
    """

    additions: list[MappingChange] = []
    conflicts: list[MappingConflict] = []
    existing_mapping = _mapping_section(existing)
    target_mapping = _mapping_section(target)
    _compare_dict(
        existing_mapping,
        target_mapping,
        "mappings",
        additions,
        conflicts,
        field_definition=False,
    )

    target_settings = _settings_section(target)
    if target_settings is not None:
        existing_settings = _settings_section(existing)
        if existing_settings is None:
            conflicts.append(MappingConflict(
                "settings", None, deepcopy(target_settings),
                "index settings are not an additive mapping change",
            ))
        else:
            # Any declared settings difference is unsafe here: this also locks
            # the current analyzer/similarity and KNN behavior.
            _compare_dict(
                existing_settings,
                target_settings,
                "settings",
                additions,
                conflicts,
                field_definition=False,
            )

    return MappingDiff(tuple(additions), tuple(conflicts))


def require_additive_mapping(
    existing: Mapping[str, Any], target: Mapping[str, Any]
) -> MappingDiff:
    return diff_index_mappings(existing, target).require_compatible()


def build_news_year_target_mapping(existing: Mapping[str, Any]) -> dict[str, Any]:
    """Add the eight Phase 1 fields to a copy of an existing ``news-{year}`` body.

    Existing settings, analyzers, BM25 behavior, legacy fields and ``content_vec``
    are copied byte-for-byte.  A pre-existing incompatible field fails loudly.
    No new physical index name or client operation is produced.
    """

    unwrapped = _unwrap_index_body(existing)
    if "mappings" in unwrapped:
        target = deepcopy(dict(unwrapped))
    else:
        target = {"mappings": deepcopy(dict(unwrapped))}
    mappings = target.setdefault("mappings", {})
    if not isinstance(mappings, dict):
        raise MappingCompatibilityError("mappings must be an object")
    properties = mappings.setdefault("properties", {})
    if not isinstance(properties, dict):
        raise MappingCompatibilityError("mappings.properties must be an object")

    for field_name, definition in PHASE1_NEWS_ADDITIVE_PROPERTIES.items():
        if field_name not in properties:
            properties[field_name] = deepcopy(definition)
            continue
        probe_existing = {"mappings": {"properties": {field_name: properties[field_name]}}}
        probe_target = {"mappings": {"properties": {field_name: definition}}}
        require_additive_mapping(probe_existing, probe_target)

    require_additive_mapping(existing, target)
    return target


def news_year_additive_diff(existing: Mapping[str, Any]) -> MappingDiff:
    """Return the dry-run diff needed to prepare an existing yearly index."""

    return diff_index_mappings(existing, build_news_year_target_mapping(existing))


def build_additive_mapping_patch(existing: Mapping[str, Any]) -> dict[str, Any]:
    """Return an offline PUT-mapping body containing only missing Phase 1 fields."""

    existing_properties = _mapping_section(existing).get("properties", {})
    if not isinstance(existing_properties, Mapping):
        raise MappingCompatibilityError("mappings.properties must be an object")
    target = build_news_year_target_mapping(existing)
    target_properties = _mapping_section(target)["properties"]
    return {
        "properties": {
            name: deepcopy(definition)
            for name, definition in target_properties.items()
            if name not in existing_properties
        }
    }


__all__ = [
    "CANONICAL_DATE_FORMAT",
    "ENTITY_ALIAS_INDEX",
    "ENTITY_INDEX",
    "MappingChange",
    "MappingCompatibilityError",
    "MappingConflict",
    "MappingDiff",
    "NEWS_INDEX_PATTERN",
    "PHASE1_NEWS_ADDITIVE_PROPERTIES",
    "SOURCE_HEALTH_INDEX",
    "SOURCE_INDEX",
    "build_additive_mapping_patch",
    "build_entity_alias_mapping",
    "build_entity_mapping",
    "build_news_year_target_mapping",
    "build_phase1_standalone_mappings",
    "build_source_health_mapping",
    "build_source_mapping",
    "diff_index_mappings",
    "news_year_additive_diff",
    "require_additive_mapping",
]
