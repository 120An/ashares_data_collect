"""Static, side-effect-free Source Catalog for news model V1.1.

The existing ``source_registry.Source`` remains the runtime acquisition
configuration.  This module reads its on-disk facts without importing or
calling ``source_registry.load_all()`` (which resolves deployment config and
writes a last-good copy), combines them with low-frequency governance data,
and returns frozen ``SourceRecord`` contracts.

No function in this module connects to PostgreSQL or OpenSearch, imports a
news job, performs HTTP requests, writes files, or computes SourceHealth.
"""

from __future__ import annotations

import ast
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import json
from pathlib import Path
from types import MappingProxyType
from typing import Any

from data_collect.news_model.contracts import (
    AcquisitionType,
    AuthorityStatus,
    ContractValidationError,
    SourceRecord,
    validate_source_id,
)


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCES_PATH = _PROJECT_ROOT / "sources.yaml"
DEFAULT_GOVERNANCE_PATH = _PROJECT_ROOT / "source_governance.yaml"

_REGISTRY_VERSION = 1
_GOVERNANCE_VERSION = 1
_REGISTRY_TOP_KEYS = frozenset({"version", "defaults", "sources"})
_REGISTRY_DEFAULT_KEYS = frozenset({"enabled", "timeout"})
_REGISTRY_SOURCE_KEYS = frozenset(
    {
        "id",
        "adapter",
        "channel",
        "job",
        "url",
        "route",
        "proxy",
        "headers",
        "timeout",
        "enabled",
        "note",
    }
)
_REGISTRY_ADAPTERS = frozenset({"rss", "rsshub", "listpage", "akshare", "api"})
_REGISTRY_CHANNELS = frozenset(
    {
        "flash",
        "cctv",
        "policy",
        "media",
        "report",
        "announcement",
        "stock",
        "us_policy",
        "us_filing",
        "us_news",
        "intl_news",
    }
)
_REGISTRY_JOBS = frozenset(
    {
        "news_flash",
        "news_cctv",
        "news_policy",
        "news_regulator",
        "news_us",
        "news_announcement",
        "news_stock",
    }
)
_ADAPTER_TO_ACQUISITION_TYPE = {
    "rss": AcquisitionType.RSS,
    "rsshub": AcquisitionType.RSSHUB,
    "listpage": AcquisitionType.WEB,
    "akshare": AcquisitionType.AKSHARE,
    "api": AcquisitionType.API,
}

# These locators are explicit constants in current production code but are
# intentionally not duplicated into sources.yaml.  RSSHub routes remain
# relative because resolving the deployment-specific base would read config.
_CODE_ENDPOINTS = {
    "csrc": "http://www.csrc.gov.cn/csrc/c100028/common_list.shtml",
    "mof": "https://www.mof.gov.cn/zhengwuxinxi/caizhengxinwen/",
    "people": "http://finance.people.com.cn/",
    "cninfo": "http://www.cninfo.com.cn/new/hisAnnouncement/query",
}

_GOVERNANCE_TOP_KEYS = frozenset({"version", "defaults", "sources"})
_GOVERNANCE_FIELDS = frozenset(
    {
        "source_id",
        "source_revision",
        "source_name",
        "source_category",
        "directness",
        "country_region_codes",
        "languages",
        "source_timezone",
        "is_official",
        "authority_status",
        "source_authority",
        "authority_level",
        "authority_basis",
        "authority_version",
        "authority_effective_from",
        "content_license",
        "paywall_type",
        "homepage_url",
        "expected_frequency",
        "collect_interval_seconds",
        "created_at",
        "updated_at",
    }
)
# Only catalog-wide lifecycle/authority-policy values may be inherited.  The
# identity and classification of a source must be stated on that source's own
# entry so a generic default can never silently fabricate governance facts.
_GOVERNANCE_DEFAULT_FIELDS = frozenset(
    {
        "source_revision",
        "authority_status",
        "source_authority",
        "authority_level",
        "authority_basis",
        "authority_version",
        "authority_effective_from",
        "content_license",
        "paywall_type",
        "created_at",
        "updated_at",
    }
)
_REQUIRED_GOVERNANCE_FIELDS = frozenset(
    {
        "source_revision",
        "source_name",
        "source_category",
        "directness",
        "country_region_codes",
        "languages",
        "source_timezone",
        "is_official",
        "authority_status",
        "source_authority",
        "authority_version",
        "authority_effective_from",
        "content_license",
        "paywall_type",
        "created_at",
        "updated_at",
    }
)


class SourceCatalogError(ValueError):
    """Base error for source fact or governance validation failures."""


class SourceCatalogParseError(SourceCatalogError):
    """Raised when a catalog input is outside the supported flat YAML shape."""


class SourceCatalogConflictError(SourceCatalogError):
    """Raised for duplicate IDs, orphan governance, or conflicting facts."""


class MissingSourceGovernanceError(SourceCatalogError):
    """Raised when a production source lacks explicit required governance."""


def _require_exact_source_id(value: Any, field_name: str = "source_id") -> str:
    try:
        validated = validate_source_id(value)
    except ContractValidationError as exc:
        raise SourceCatalogError(
            f"{field_name} is invalid; Source Catalog never renames IDs: {value!r}"
        ) from exc
    if value != validated:
        raise SourceCatalogError(
            f"{field_name} must already be canonical; Source Catalog never renames IDs"
        )
    return validated


def _require_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SourceCatalogError(f"{field_name} must be a non-empty string")
    return value.strip()


def _strip_inline_comment(value: str) -> str:
    quote: str | None = None
    escaped = False
    for index, character in enumerate(value):
        if escaped:
            escaped = False
            continue
        if character == "\\" and quote is not None:
            escaped = True
            continue
        if character in {"'", '"'}:
            if quote is None:
                quote = character
            elif quote == character:
                quote = None
            continue
        if character == "#" and quote is None:
            if index == 0 or value[index - 1].isspace():
                return value[:index].rstrip()
    return value.rstrip()


def _parse_scalar(raw_value: str, *, path: Path, line_number: int) -> Any:
    value = raw_value.strip()
    if not value:
        raise SourceCatalogParseError(
            f"{path}:{line_number}: empty scalar values are not supported"
        )
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"null", "~"}:
        return None
    if value.startswith(("[", "{")):
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise SourceCatalogParseError(
                f"{path}:{line_number}: inline collections must use JSON syntax"
            ) from exc
    if value.startswith('"'):
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise SourceCatalogParseError(
                f"{path}:{line_number}: invalid quoted string"
            ) from exc
    if value.startswith("'"):
        try:
            parsed = ast.literal_eval(value)
        except (SyntaxError, ValueError) as exc:
            raise SourceCatalogParseError(
                f"{path}:{line_number}: invalid quoted string"
            ) from exc
        if not isinstance(parsed, str):
            raise SourceCatalogParseError(
                f"{path}:{line_number}: single-quoted value must be a string"
            )
        return parsed
    try:
        return int(value)
    except ValueError:
        return value


def _split_key_value(content: str, *, path: Path, line_number: int) -> tuple[str, str]:
    if ":" not in content:
        raise SourceCatalogParseError(
            f"{path}:{line_number}: expected key: value"
        )
    key, value = content.split(":", 1)
    key = key.strip()
    if not key:
        raise SourceCatalogParseError(f"{path}:{line_number}: key must not be empty")
    return key, value.strip()


def _put_unique(
    target: dict[str, Any], key: str, value: Any, *, path: Path, line_number: int
) -> None:
    if key in target:
        raise SourceCatalogConflictError(
            f"{path}:{line_number}: duplicate configuration key {key!r}"
        )
    target[key] = value


def _load_flat_yaml(path: str | Path) -> dict[str, Any]:
    """Read the controlled flat YAML subset used by both catalog files.

    The project declares PyYAML, but the Catalog's read-only validator must
    also run in minimal maintenance environments.  The accepted subset is
    intentionally narrow: top-level scalars, one top-level defaults mapping,
    and one list of flat source mappings.  Unsupported nesting fails clearly
    instead of being guessed.
    """

    resolved_path = Path(path)
    try:
        text = resolved_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SourceCatalogParseError(
            f"cannot read catalog input {resolved_path}: {exc}"
        ) from exc

    result: dict[str, Any] = {}
    section: str | None = None
    current_source: dict[str, Any] | None = None

    for line_number, original_line in enumerate(text.splitlines(), start=1):
        if "\t" in original_line[: len(original_line) - len(original_line.lstrip())]:
            raise SourceCatalogParseError(
                f"{resolved_path}:{line_number}: tabs are not allowed for indentation"
            )
        uncommented = _strip_inline_comment(original_line).rstrip()
        if not uncommented.strip():
            continue
        indent = len(uncommented) - len(uncommented.lstrip(" "))
        content = uncommented.strip()

        if indent == 0:
            key, raw_value = _split_key_value(
                content, path=resolved_path, line_number=line_number
            )
            if raw_value:
                _put_unique(
                    result,
                    key,
                    _parse_scalar(raw_value, path=resolved_path, line_number=line_number),
                    path=resolved_path,
                    line_number=line_number,
                )
                section = None
                current_source = None
            else:
                if key == "sources":
                    container: Any = []
                else:
                    container = {}
                _put_unique(
                    result,
                    key,
                    container,
                    path=resolved_path,
                    line_number=line_number,
                )
                section = key
                current_source = None
            continue

        if section == "defaults" and indent == 2:
            key, raw_value = _split_key_value(
                content, path=resolved_path, line_number=line_number
            )
            if not raw_value:
                raise SourceCatalogParseError(
                    f"{resolved_path}:{line_number}: nested defaults are not supported"
                )
            defaults = result["defaults"]
            _put_unique(
                defaults,
                key,
                _parse_scalar(raw_value, path=resolved_path, line_number=line_number),
                path=resolved_path,
                line_number=line_number,
            )
            continue

        if section == "sources" and indent == 2 and content.startswith("-"):
            remainder = content[1:].strip()
            if not remainder:
                raise SourceCatalogParseError(
                    f"{resolved_path}:{line_number}: source list items must start with a field"
                )
            key, raw_value = _split_key_value(
                remainder, path=resolved_path, line_number=line_number
            )
            if not raw_value:
                raise SourceCatalogParseError(
                    f"{resolved_path}:{line_number}: nested source fields are not supported"
                )
            current_source = {}
            _put_unique(
                current_source,
                key,
                _parse_scalar(raw_value, path=resolved_path, line_number=line_number),
                path=resolved_path,
                line_number=line_number,
            )
            result["sources"].append(current_source)
            continue

        if section == "sources" and indent == 4 and current_source is not None:
            key, raw_value = _split_key_value(
                content, path=resolved_path, line_number=line_number
            )
            if not raw_value:
                raise SourceCatalogParseError(
                    f"{resolved_path}:{line_number}: nested source fields are not supported"
                )
            _put_unique(
                current_source,
                key,
                _parse_scalar(raw_value, path=resolved_path, line_number=line_number),
                path=resolved_path,
                line_number=line_number,
            )
            continue

        raise SourceCatalogParseError(
            f"{resolved_path}:{line_number}: unsupported YAML structure or indentation"
        )

    if not isinstance(result, dict):
        raise SourceCatalogParseError(f"{resolved_path}: top level must be a mapping")
    return result


@dataclass(frozen=True, slots=True, kw_only=True)
class SourceAcquisitionFact:
    """Static acquisition facts observed in sources.yaml or production code.

    For sources outside ``sources.yaml``, ``enabled`` means the production
    code permits the source to be scheduled.  It deliberately does not claim
    that a config-selectable source is in the current/default active set.
    """

    source_id: str
    adapter: str
    default_channel: str
    job: str
    endpoint_url: str | None
    endpoint_route: str | None
    enabled: bool
    registered_in_sources_yaml: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "source_id", _require_exact_source_id(self.source_id)
        )
        if self.adapter not in _REGISTRY_ADAPTERS:
            raise SourceCatalogError(
                f"source {self.source_id}: unsupported adapter {self.adapter!r}"
            )
        object.__setattr__(
            self,
            "default_channel",
            _require_text(self.default_channel, "default_channel"),
        )
        object.__setattr__(self, "job", _require_text(self.job, "job"))
        if self.endpoint_url is not None:
            object.__setattr__(
                self,
                "endpoint_url",
                _require_text(self.endpoint_url, "endpoint_url"),
            )
        if self.endpoint_route is not None:
            object.__setattr__(
                self,
                "endpoint_route",
                _require_text(self.endpoint_route, "endpoint_route"),
            )
        if self.endpoint_url is not None and self.endpoint_route is not None:
            raise SourceCatalogError(
                f"source {self.source_id}: endpoint_url and endpoint_route are mutually exclusive"
            )
        if not isinstance(self.enabled, bool):
            raise SourceCatalogError(f"source {self.source_id}: enabled must be boolean")
        if not isinstance(self.registered_in_sources_yaml, bool):
            raise SourceCatalogError(
                f"source {self.source_id}: registered_in_sources_yaml must be boolean"
            )

    @property
    def acquisition_type(self) -> AcquisitionType:
        return _ADAPTER_TO_ACQUISITION_TYPE[self.adapter]

    @property
    def endpoint_locator(self) -> str | None:
        """Return the configured URL or unresolved deployment-relative route."""

        return self.endpoint_url or self.endpoint_route

    def conflict_signature(self) -> tuple[Any, ...]:
        return (
            self.adapter,
            self.default_channel,
            self.job,
            self.endpoint_url,
            self.endpoint_route,
            self.enabled,
        )


_BUILTIN_UNREGISTERED_FACTS = (
    SourceAcquisitionFact(
        source_id="cls",
        adapter="api",
        default_channel="flash",
        job="news_flash",
        endpoint_url="https://www.cls.cn/v1/roll/get_roll_list",
        endpoint_route=None,
        enabled=True,
        registered_in_sources_yaml=False,
    ),
    SourceAcquisitionFact(
        source_id="em",
        adapter="akshare",
        default_channel="flash",
        job="news_flash",
        endpoint_url=None,
        endpoint_route=None,
        enabled=True,
        registered_in_sources_yaml=False,
    ),
    SourceAcquisitionFact(
        source_id="sina",
        adapter="akshare",
        default_channel="flash",
        job="news_flash",
        endpoint_url=None,
        endpoint_route=None,
        enabled=True,
        registered_in_sources_yaml=False,
    ),
    SourceAcquisitionFact(
        source_id="em_cjzc",
        adapter="akshare",
        default_channel="media",
        job="news_policy",
        endpoint_url=None,
        endpoint_route=None,
        enabled=True,
        registered_in_sources_yaml=False,
    ),
)

PRODUCTION_UNREGISTERED_SOURCE_IDS = tuple(
    fact.source_id for fact in _BUILTIN_UNREGISTERED_FACTS
)


def _check_allowed_keys(
    values: Mapping[str, Any], allowed: frozenset[str], context: str
) -> None:
    unknown = sorted(set(values) - allowed)
    if unknown:
        raise SourceCatalogError(f"{context} contains unknown fields: {unknown}")


def load_registry_acquisition_facts(
    path: str | Path = DEFAULT_SOURCES_PATH,
) -> tuple[SourceAcquisitionFact, ...]:
    """Read all sources.yaml entries, including disabled ones, with no side effects."""

    data = _load_flat_yaml(path)
    _check_allowed_keys(data, _REGISTRY_TOP_KEYS, "sources.yaml")
    if data.get("version") != _REGISTRY_VERSION:
        raise SourceCatalogError(
            f"sources.yaml version must be {_REGISTRY_VERSION}; got {data.get('version')!r}"
        )
    defaults = data.get("defaults") or {}
    if not isinstance(defaults, Mapping):
        raise SourceCatalogError("sources.yaml defaults must be a mapping")
    _check_allowed_keys(defaults, _REGISTRY_DEFAULT_KEYS, "sources.yaml defaults")
    default_enabled = defaults.get("enabled", True)
    if not isinstance(default_enabled, bool):
        raise SourceCatalogError("sources.yaml defaults.enabled must be boolean")

    raw_sources = data.get("sources")
    if not isinstance(raw_sources, list):
        raise SourceCatalogError("sources.yaml sources must be a list")

    facts: list[SourceAcquisitionFact] = []
    seen: set[str] = set()
    for index, raw_source in enumerate(raw_sources):
        context = f"sources.yaml sources[{index}]"
        if not isinstance(raw_source, Mapping):
            raise SourceCatalogError(f"{context} must be a mapping")
        _check_allowed_keys(raw_source, _REGISTRY_SOURCE_KEYS, context)
        missing = sorted({"id", "adapter", "channel", "job"} - set(raw_source))
        if missing:
            raise SourceCatalogError(f"{context} missing required fields: {missing}")

        source_id = _require_exact_source_id(raw_source["id"], f"{context}.id")
        if source_id in seen:
            raise SourceCatalogConflictError(
                f"duplicate source_id in sources.yaml: {source_id}"
            )
        seen.add(source_id)

        adapter = _require_text(raw_source["adapter"], f"{context}.adapter")
        channel = _require_text(raw_source["channel"], f"{context}.channel")
        job = _require_text(raw_source["job"], f"{context}.job")
        if adapter not in _REGISTRY_ADAPTERS:
            raise SourceCatalogError(f"{context}: unsupported adapter {adapter!r}")
        if channel not in _REGISTRY_CHANNELS:
            raise SourceCatalogError(f"{context}: unsupported channel {channel!r}")
        if job not in _REGISTRY_JOBS:
            raise SourceCatalogError(f"{context}: unsupported job {job!r}")
        if adapter == "rss" and not raw_source.get("url"):
            raise SourceCatalogError(f"{context}: adapter=rss requires url")
        if adapter == "rsshub" and not raw_source.get("route"):
            raise SourceCatalogError(f"{context}: adapter=rsshub requires route")

        enabled = raw_source.get("enabled", default_enabled)
        if not isinstance(enabled, bool):
            raise SourceCatalogError(f"{context}.enabled must be boolean")
        endpoint_url = raw_source.get("url")
        endpoint_route = raw_source.get("route")
        if endpoint_url is None and endpoint_route is None:
            endpoint_url = _CODE_ENDPOINTS.get(source_id)
        if endpoint_url is not None and not isinstance(endpoint_url, str):
            raise SourceCatalogError(f"{context}: url must be a string")
        if endpoint_route is not None and not isinstance(endpoint_route, str):
            raise SourceCatalogError(f"{context}: route must be a string")

        facts.append(
            SourceAcquisitionFact(
                source_id=source_id,
                adapter=adapter,
                default_channel=channel,
                job=job,
                endpoint_url=endpoint_url,
                endpoint_route=endpoint_route,
                enabled=enabled,
                registered_in_sources_yaml=True,
            )
        )
    return tuple(facts)


def _merge_acquisition_fact(
    facts: dict[str, SourceAcquisitionFact], candidate: SourceAcquisitionFact
) -> None:
    existing = facts.get(candidate.source_id)
    if existing is None:
        facts[candidate.source_id] = candidate
        return
    if existing.conflict_signature() != candidate.conflict_signature():
        raise SourceCatalogConflictError(
            f"conflicting acquisition facts for source_id {candidate.source_id!r}: "
            f"{existing.conflict_signature()!r} != {candidate.conflict_signature()!r}"
        )
    if candidate.registered_in_sources_yaml:
        facts[candidate.source_id] = candidate


def _load_governance(
    path: str | Path,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    data = _load_flat_yaml(path)
    _check_allowed_keys(data, _GOVERNANCE_TOP_KEYS, "source_governance.yaml")
    if data.get("version") != _GOVERNANCE_VERSION:
        raise SourceCatalogError(
            "source_governance.yaml version must be "
            f"{_GOVERNANCE_VERSION}; got {data.get('version')!r}"
        )
    defaults = data.get("defaults") or {}
    if not isinstance(defaults, Mapping):
        raise SourceCatalogError("source_governance.yaml defaults must be a mapping")
    _check_allowed_keys(
        defaults,
        _GOVERNANCE_DEFAULT_FIELDS,
        "source_governance.yaml defaults",
    )

    raw_sources = data.get("sources")
    if not isinstance(raw_sources, list):
        raise SourceCatalogError("source_governance.yaml sources must be a list")
    by_id: dict[str, dict[str, Any]] = {}
    for index, raw_source in enumerate(raw_sources):
        context = f"source_governance.yaml sources[{index}]"
        if not isinstance(raw_source, Mapping):
            raise SourceCatalogError(f"{context} must be a mapping")
        _check_allowed_keys(raw_source, _GOVERNANCE_FIELDS, context)
        if "source_id" not in raw_source:
            raise MissingSourceGovernanceError(f"{context} missing source_id")
        source_id = _require_exact_source_id(
            raw_source["source_id"], f"{context}.source_id"
        )
        if source_id in by_id:
            raise SourceCatalogConflictError(
                f"duplicate source_id in source_governance.yaml: {source_id}"
            )
        merged = dict(defaults)
        merged.update(raw_source)
        missing = sorted(_REQUIRED_GOVERNANCE_FIELDS - set(merged))
        if missing:
            raise MissingSourceGovernanceError(
                f"source {source_id!r} missing required governance fields: {missing}"
            )
        by_id[source_id] = merged
    return dict(defaults), by_id


@dataclass(frozen=True, slots=True)
class SourceCatalogSnapshot:
    """One immutable, fully validated local Source Catalog snapshot."""

    records: Mapping[str, SourceRecord]
    acquisition_facts: Mapping[str, SourceAcquisitionFact]
    registry_source_ids: tuple[str, ...]
    production_source_ids: tuple[str, ...]
    unregistered_production_source_ids: tuple[str, ...]
    unresolved_source_ids: tuple[str, ...]
    configuration_conflicts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "records", MappingProxyType(dict(self.records)))
        object.__setattr__(
            self,
            "acquisition_facts",
            MappingProxyType(dict(self.acquisition_facts)),
        )

    @property
    def sources_yaml_count(self) -> int:
        return len(self.registry_source_ids)

    @property
    def production_source_count(self) -> int:
        return len(self.production_source_ids)

    @property
    def catalog_source_count(self) -> int:
        return len(self.records)

    @property
    def unrated_source_ids(self) -> tuple[str, ...]:
        return tuple(
            source_id
            for source_id, record in self.records.items()
            if record.authority_status is AuthorityStatus.UNRATED
        )


def build_source_catalog_snapshot(
    sources_path: str | Path = DEFAULT_SOURCES_PATH,
    governance_path: str | Path = DEFAULT_GOVERNANCE_PATH,
    *,
    include_builtin_unregistered: bool = True,
    additional_facts: Sequence[SourceAcquisitionFact] = (),
) -> SourceCatalogSnapshot:
    """Build and validate a complete local snapshot.

    ``additional_facts`` exists for explicit extensions and conflict tests; it
    never replaces a conflicting fact silently.  Production callers should use
    the defaults, which include the four code-backed sources outside
    sources.yaml.
    """

    registry_facts = load_registry_acquisition_facts(sources_path)
    facts: dict[str, SourceAcquisitionFact] = {
        fact.source_id: fact for fact in registry_facts
    }
    if include_builtin_unregistered:
        for fact in _BUILTIN_UNREGISTERED_FACTS:
            _merge_acquisition_fact(facts, fact)
    for fact in additional_facts:
        if not isinstance(fact, SourceAcquisitionFact):
            raise SourceCatalogError("additional_facts must contain SourceAcquisitionFact")
        _merge_acquisition_fact(facts, fact)

    _, governance = _load_governance(governance_path)
    production_ids = tuple(facts)
    governance_ids = set(governance)
    production_id_set = set(production_ids)
    missing_governance = sorted(production_id_set - governance_ids)
    if missing_governance:
        raise MissingSourceGovernanceError(
            "production source_ids missing from source_governance.yaml: "
            f"{missing_governance}"
        )
    orphan_governance = sorted(governance_ids - production_id_set)
    if orphan_governance:
        raise SourceCatalogConflictError(
            "source_governance.yaml contains non-production source_ids: "
            f"{orphan_governance}"
        )

    records: dict[str, SourceRecord] = {}
    for source_id, fact in facts.items():
        values = governance[source_id]
        try:
            record = SourceRecord(
                source_id=source_id,
                source_revision=values["source_revision"],
                source_name=values["source_name"],
                source_category=values["source_category"],
                acquisition_type=fact.acquisition_type,
                directness=values["directness"],
                default_channel=fact.default_channel,
                country_region_codes=values["country_region_codes"],
                languages=values["languages"],
                source_timezone=values["source_timezone"],
                enabled=fact.enabled,
                authority_status=values["authority_status"],
                source_authority=values["source_authority"],
                authority_level=values.get("authority_level"),
                authority_basis=values.get("authority_basis"),
                authority_version=values["authority_version"],
                authority_effective_from=values["authority_effective_from"],
                is_official=values["is_official"],
                content_license=values["content_license"],
                paywall_type=values["paywall_type"],
                homepage_url=values.get("homepage_url"),
                endpoint_url=fact.endpoint_url,
                expected_frequency=values.get("expected_frequency"),
                collect_interval_seconds=values.get("collect_interval_seconds"),
                created_at=values["created_at"],
                updated_at=values["updated_at"],
            )
        except (ContractValidationError, TypeError) as exc:
            raise SourceCatalogError(
                f"source {source_id!r} does not satisfy SourceRecord: {exc}"
            ) from exc
        if record.source_id != source_id:
            raise SourceCatalogError(
                f"source_id changed during validation: {source_id!r} -> {record.source_id!r}"
            )
        records[source_id] = record

    unresolved = tuple(source_id for source_id in production_ids if source_id not in records)
    registry_ids = tuple(fact.source_id for fact in registry_facts)
    unregistered_ids = tuple(
        source_id
        for source_id, fact in facts.items()
        if not fact.registered_in_sources_yaml
    )
    return SourceCatalogSnapshot(
        records=records,
        acquisition_facts=facts,
        registry_source_ids=registry_ids,
        production_source_ids=production_ids,
        unregistered_production_source_ids=unregistered_ids,
        unresolved_source_ids=unresolved,
    )


def load_source_catalog(
    sources_path: str | Path = DEFAULT_SOURCES_PATH,
    governance_path: str | Path = DEFAULT_GOVERNANCE_PATH,
) -> dict[str, SourceRecord]:
    """Return a fresh mapping of every production source_id to SourceRecord."""

    return dict(
        build_source_catalog_snapshot(
            sources_path=sources_path, governance_path=governance_path
        ).records
    )


def get_source_record(
    source_id: str,
    sources_path: str | Path = DEFAULT_SOURCES_PATH,
    governance_path: str | Path = DEFAULT_GOVERNANCE_PATH,
) -> SourceRecord:
    """Resolve one exact source_id; never normalize or invent an alias."""

    exact_id = _require_exact_source_id(source_id)
    catalog = load_source_catalog(sources_path, governance_path)
    try:
        return catalog[exact_id]
    except KeyError as exc:
        raise KeyError(f"Source Catalog has no source_id {exact_id!r}") from exc


__all__ = [
    "DEFAULT_GOVERNANCE_PATH",
    "DEFAULT_SOURCES_PATH",
    "MissingSourceGovernanceError",
    "PRODUCTION_UNREGISTERED_SOURCE_IDS",
    "SourceAcquisitionFact",
    "SourceCatalogConflictError",
    "SourceCatalogError",
    "SourceCatalogParseError",
    "SourceCatalogSnapshot",
    "build_source_catalog_snapshot",
    "get_source_record",
    "load_registry_acquisition_facts",
    "load_source_catalog",
]
