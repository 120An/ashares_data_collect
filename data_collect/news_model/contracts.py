"""新闻数据模型 V1.1 第一阶段的纯 Python 数据契约。

本模块只定义值对象、枚举与校验，不连接 PostgreSQL/OpenSearch，不执行采集，
也不包含事件、评分、情绪、影响或关系推理。

时间兼容边界：
- ``pub_time`` / ``fetch_time`` 是旧系统兼容字段，保持字符串，不在这里改变格式；
- ``publish_time`` / ``collect_time`` 是规范时间，必须带时区。带偏移的 ISO 8601
  字符串会在构造时转换为 timezone-aware ``datetime``。

归档兼容边界：
- ``NewsDocumentValidationMode.FINAL`` 要求 ``raw_archive_uri``；
- ``PHASE1_COMPAT`` 允许旧数据在 archive receipt 上线前暂缺该字段，但绝不合成路径。
"""

from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from datetime import datetime
from enum import Enum
import re
from typing import Any, Mapping, Sequence, TypeVar


class ContractValidationError(ValueError):
    """数据不满足冻结契约。"""


class _StringEnum(str, Enum):
    """可直接序列化为模型约定字符串的枚举基类。"""


class NewsDocumentValidationMode(_StringEnum):
    FINAL = "final"
    PHASE1_COMPAT = "phase1_compat"


class AuthorityStatus(_StringEnum):
    RATED = "rated"
    UNRATED = "unrated"
    PROVISIONAL = "provisional"


class AuthorityLevel(_StringEnum):
    PRIMARY_OFFICIAL = "primary_official"
    HIGH = "high"
    STANDARD = "standard"
    LOW = "low"
    UNVERIFIED = "unverified"


class DocumentType(_StringEnum):
    FLASH = "flash"
    NEWS = "news"
    ANNOUNCEMENT = "announcement"
    POLICY = "policy"
    REGULATORY = "regulatory"
    RESEARCH = "research"
    FILING = "filing"
    TRANSCRIPT = "transcript"
    OTHER = "other"


class SummaryType(_StringEnum):
    SOURCE = "source"
    EXTRACTIVE = "extractive"
    AI = "ai"
    MANUAL = "manual"


class PublishTimePrecision(_StringEnum):
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    UNKNOWN = "unknown"


class DocumentStatus(_StringEnum):
    ACTIVE = "active"
    CORRECTED = "corrected"
    RETRACTED = "retracted"
    DELETED = "deleted"
    UNAVAILABLE = "unavailable"


class DuplicateType(_StringEnum):
    NONE = "none"
    EXACT = "exact"
    NEAR = "near"
    REPRINT = "reprint"


class ProcessingStatus(_StringEnum):
    PENDING = "pending"
    DONE = "done"
    FAILED = "failed"


class SourceCategory(_StringEnum):
    REGULATOR = "regulator"
    EXCHANGE = "exchange"
    COMPANY_OFFICIAL = "company_official"
    GOVERNMENT = "government"
    MEDIA = "media"
    RESEARCH_INSTITUTION = "research_institution"
    AGGREGATOR = "aggregator"
    DATA_VENDOR = "data_vendor"
    OTHER = "other"


class AcquisitionType(_StringEnum):
    API = "api"
    RSS = "rss"
    ATOM = "atom"
    RSSHUB = "rsshub"
    WEB = "web"
    PDF = "pdf"
    AKSHARE = "akshare"
    OTHER = "other"


class SourceDirectness(_StringEnum):
    ORIGINAL = "original"
    AGGREGATOR = "aggregator"
    REPRINT = "reprint"
    UNKNOWN = "unknown"


class ContentLicense(_StringEnum):
    FULLTEXT_ALLOWED = "fulltext_allowed"
    FULLTEXT_INTERNAL_ONLY = "fulltext_internal_only"
    SNIPPET_ONLY = "snippet_only"
    LINK_ONLY = "link_only"
    UNKNOWN = "unknown"


class PaywallType(_StringEnum):
    NONE = "none"
    SOFT = "soft"
    HARD = "hard"
    UNKNOWN = "unknown"


class ExpectedFrequency(_StringEnum):
    CONTINUOUS = "continuous"
    DAILY = "daily"
    WEEKLY = "weekly"
    IRREGULAR = "irregular"


class HealthStatus(_StringEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    DOWN = "down"
    DISABLED = "disabled"
    UNKNOWN = "unknown"


class CompletenessStatus(_StringEnum):
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class EntityType(_StringEnum):
    COMPANY = "company"
    STOCK = "stock"
    INDUSTRY = "industry"
    CONCEPT = "concept"
    PRODUCT = "product"
    RAW_MATERIAL = "raw_material"
    PERSON = "person"
    INSTITUTION = "institution"
    COUNTRY = "country"
    REGION = "region"
    COMMODITY = "commodity"
    INDEX = "index"
    CURRENCY = "currency"
    POLICY_DOCUMENT = "policy_document"
    OTHER = "other"


class EntityStatus(_StringEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    DELISTED = "delisted"
    MERGED = "merged"
    UNKNOWN = "unknown"


class Exchange(_StringEnum):
    SSE = "SSE"
    SZSE = "SZSE"
    BSE = "BSE"


class EntityAliasType(_StringEnum):
    OFFICIAL_NAME = "official_name"
    STOCK_SHORT_NAME = "stock_short_name"
    SHORT_NAME = "short_name"
    FORMER_NAME = "former_name"
    HISTORICAL_NAME = "historical_name"
    BRAND = "brand"
    SUBSIDIARY_NAME = "subsidiary_name"
    ENGLISH_NAME = "english_name"
    ABBREVIATION = "abbreviation"
    TICKER = "ticker"
    OTHER = "other"


class DerivedBy(_StringEnum):
    MASTER_DATA = "master_data"
    SOURCE_METADATA = "source_metadata"
    RULE = "rule"
    AI = "ai"
    MANUAL = "manual"


EnumT = TypeVar("EnumT", bound=Enum)

_STABLE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,191}$")
_SOURCE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_STOCK_CODE_RE = re.compile(r"^(\d{6})\.(SH|SZ|BJ)$", re.IGNORECASE)
_EVENT_ID_PREFIXES = ("evt_", "evt-", "event_", "event-")
_EXCHANGE_BY_SUFFIX = {
    "SH": Exchange.SSE,
    "SZ": Exchange.SZSE,
    "BJ": Exchange.BSE,
}


def _coerce_enum(value: EnumT | str, enum_type: type[EnumT], field_name: str) -> EnumT:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        allowed = ", ".join(member.value for member in enum_type)
        raise ContractValidationError(
            f"{field_name} 必须是以下值之一: {allowed}; 实际为 {value!r}"
        ) from exc


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractValidationError(f"{field_name} 必须是非空字符串")
    return value.strip()


def _optional_text(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name)


def _validate_stable_id(value: str, field_name: str) -> str:
    value = _require_text(value, field_name)
    if not _STABLE_ID_RE.fullmatch(value):
        raise ContractValidationError(
            f"{field_name} 不是合法稳定 ID（只允许字母、数字、点、下划线、冒号和连字符）"
        )
    return value


def validate_news_id(value: str) -> str:
    """校验现有确定性新闻 ID，并阻止 Event 命名空间混入。

    当前 ``make_id`` 的 native-id 分支会保留来源 GUID；RSS/Atom GUID 可能是 URL
    或 URN，因而这里不能强制套用 Entity ID 的窄字符集。
    """
    value = _require_text(value, "news_id")
    if len(value) > 1024:
        raise ContractValidationError("news_id 长度不得超过 1024 个字符")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ContractValidationError("news_id 不得包含控制字符")
    if value.lower().startswith(_EVENT_ID_PREFIXES):
        raise ContractValidationError("news_id 不得使用 event_id 命名空间")
    return value


def validate_source_id(value: str) -> str:
    value = _require_text(value, "source_id")
    if not _SOURCE_ID_RE.fullmatch(value):
        raise ContractValidationError(
            "source_id 必须以小写字母或数字开头，且只允许小写字母、数字、下划线和连字符"
        )
    return value


def validate_stock_code(value: str) -> str:
    """返回规范大写 A 股代码；支持上交所、深交所和北交所。"""
    value = _require_text(value, "stock_code").upper()
    if not _STOCK_CODE_RE.fullmatch(value):
        raise ContractValidationError(
            "stock_code 必须是六位数字并带 .SH、.SZ 或 .BJ 后缀"
        )
    return value


def exchange_for_stock_code(stock_code: str) -> Exchange:
    stock_code = validate_stock_code(stock_code)
    return _EXCHANGE_BY_SUFFIX[stock_code.rsplit(".", 1)[1]]


def make_stock_entity_id(stock_code: str) -> str:
    """由规范证券代码生成稳定、可重复的证券 Entity ID。"""
    code = validate_stock_code(stock_code)
    return f"ent_stock_{code.replace('.', '_').lower()}"


def _validate_prefixed_id(value: str, field_name: str, prefix: str) -> str:
    value = _validate_stable_id(value, field_name)
    if not value.startswith(prefix):
        raise ContractValidationError(f"{field_name} 必须使用 {prefix} 命名空间")
    return value


def _coerce_aware_datetime(
    value: datetime | str | None,
    field_name: str,
    *,
    required: bool = True,
) -> datetime | None:
    if value is None:
        if required:
            raise ContractValidationError(f"{field_name} 必填")
        return None
    parsed = value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            raise ContractValidationError(f"{field_name} 必须是非空 ISO 8601 时间")
        if raw.endswith(("Z", "z")):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ContractValidationError(
                f"{field_name} 必须是合法 ISO 8601 时间: {value!r}"
            ) from exc
    if not isinstance(parsed, datetime):
        raise ContractValidationError(f"{field_name} 必须是 datetime 或 ISO 8601 字符串")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ContractValidationError(f"{field_name} 必须带时区")
    return parsed


def _coerce_string_tuple(
    value: Sequence[str] | None,
    field_name: str,
    *,
    required: bool = False,
) -> tuple[str, ...]:
    if value is None:
        result: tuple[str, ...] = ()
    else:
        if isinstance(value, (str, bytes)):
            raise ContractValidationError(f"{field_name} 必须是字符串序列，不能是单个字符串")
        result = tuple(_require_text(item, f"{field_name}[]") for item in value)
    if required and not result:
        raise ContractValidationError(f"{field_name} 至少包含一项")
    if len(set(result)) != len(result):
        raise ContractValidationError(f"{field_name} 不允许重复值")
    return result


def _coerce_optional_string_tuple(
    value: Sequence[str] | None,
    field_name: str,
) -> tuple[str, ...] | None:
    if value is None:
        return None
    return _coerce_string_tuple(value, field_name)


def _coerce_mapping(value: Mapping[str, Any] | None, field_name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ContractValidationError(f"{field_name} 必须是 mapping")
    return dict(value)


def _require_non_negative_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ContractValidationError(f"{field_name} 必须是非负整数")
    return value


def _optional_non_negative_int(value: int | None, field_name: str) -> int | None:
    if value is None:
        return None
    return _require_non_negative_int(value, field_name)


def _validate_confidence(value: float, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractValidationError(f"{field_name} 必须是 0～1 的数值")
    number = float(value)
    if not 0.0 <= number <= 1.0:
        raise ContractValidationError(f"{field_name} 必须在 0～1 之间")
    return number


def _validate_authority(
    status: AuthorityStatus,
    score: int | None,
    *,
    require_governance_details: bool = False,
    level: AuthorityLevel | None = None,
    basis: str | None = None,
) -> None:
    if score is not None:
        if isinstance(score, bool) or not isinstance(score, int):
            raise ContractValidationError("source_authority 必须是 0～100 的整数或 None")
        if not 0 <= score <= 100:
            raise ContractValidationError("source_authority 必须在 0～100 之间")

    if status is AuthorityStatus.UNRATED and score is not None:
        raise ContractValidationError("authority_status=unrated 时 source_authority 必须为 None")
    if status is AuthorityStatus.RATED and score is None:
        raise ContractValidationError("authority_status=rated 时 source_authority 必填")

    if require_governance_details:
        if status is AuthorityStatus.RATED and level is None:
            raise ContractValidationError("authority_status=rated 时 authority_level 必填")
        if status in (AuthorityStatus.RATED, AuthorityStatus.PROVISIONAL):
            _require_text(basis or "", "authority_basis")


@dataclass(frozen=True, slots=True, kw_only=True)
class NewsDocument:
    """第一阶段可落地的 NewsDocument 契约；不包含 Event/关系/AI 对象。"""

    news_id: str
    source_id: str
    source_authority_status: AuthorityStatus | str
    source_authority_version: str
    document_type: DocumentType | str
    channel: str
    language: str
    publish_time: datetime | str
    publish_time_precision: PublishTimePrecision | str
    publish_time_is_estimated: bool
    collect_time: datetime | str
    document_status: DocumentStatus | str
    created_at: datetime | str
    updated_at: datetime | str

    schema_version: str = "news_document_v1"
    source_native_id: str | None = None
    source_authority: int | None = None
    country_region_codes: Sequence[str] = field(default_factory=tuple)
    authors: Sequence[str] = field(default_factory=tuple)

    title: str | None = None
    content: str | None = None
    summary: str | None = None
    summary_type: SummaryType | str | None = None
    body: str | None = None
    raw_title: str | None = None
    raw_content: str | None = None
    url: str | None = None
    canonical_url: str | None = None
    raw_archive_uri: str | None = None
    content_license: ContentLicense | str | None = None

    publish_time_raw: str | None = None
    last_seen_time: datetime | str | None = None
    source_update_time: datetime | str | None = None

    title_hash: str | None = None
    content_hash: str | None = None
    canonical_url_hash: str | None = None
    duplicate_type: DuplicateType | str = DuplicateType.NONE
    duplicate_of_news_id: str | None = None

    stock_codes: Sequence[str] | None = None
    vec_status: ProcessingStatus | str | None = None
    content_vec: Sequence[float] | None = None
    embedding_model_version: str | None = None
    ann_type: str | None = None
    pdf_status: ProcessingStatus | str | None = None
    body_status: ProcessingStatus | str | None = None
    body_truncated: bool | None = None

    # 旧字段只为迁移期兼容保留；其格式和语义不在本契约中重写。
    pub_time: str | None = None
    fetch_time: str | None = None
    source: str | None = None
    stocks: Sequence[str] | None = None

    validation_mode: InitVar[NewsDocumentValidationMode | str] = NewsDocumentValidationMode.FINAL

    def __post_init__(self, validation_mode: NewsDocumentValidationMode | str) -> None:
        mode = _coerce_enum(
            validation_mode, NewsDocumentValidationMode, "validation_mode"
        )
        object.__setattr__(self, "news_id", validate_news_id(self.news_id))
        object.__setattr__(self, "source_id", validate_source_id(self.source_id))
        object.__setattr__(self, "schema_version", _validate_stable_id(self.schema_version, "schema_version"))
        object.__setattr__(self, "source_authority_version", _validate_stable_id(
            self.source_authority_version, "source_authority_version"
        ))

        authority_status = _coerce_enum(
            self.source_authority_status, AuthorityStatus, "source_authority_status"
        )
        _validate_authority(authority_status, self.source_authority)
        object.__setattr__(self, "source_authority_status", authority_status)
        object.__setattr__(self, "document_type", _coerce_enum(
            self.document_type, DocumentType, "document_type"
        ))
        object.__setattr__(self, "publish_time_precision", _coerce_enum(
            self.publish_time_precision, PublishTimePrecision, "publish_time_precision"
        ))
        object.__setattr__(self, "document_status", _coerce_enum(
            self.document_status, DocumentStatus, "document_status"
        ))

        object.__setattr__(self, "channel", _require_text(self.channel, "channel"))
        object.__setattr__(self, "language", _require_text(self.language, "language"))
        object.__setattr__(self, "publish_time", _coerce_aware_datetime(
            self.publish_time, "publish_time"
        ))
        object.__setattr__(self, "collect_time", _coerce_aware_datetime(
            self.collect_time, "collect_time"
        ))
        created_at = _coerce_aware_datetime(self.created_at, "created_at")
        updated_at = _coerce_aware_datetime(self.updated_at, "updated_at")
        if updated_at < created_at:
            raise ContractValidationError("updated_at 不得早于 created_at")
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "last_seen_time", _coerce_aware_datetime(
            self.last_seen_time, "last_seen_time", required=False
        ))
        object.__setattr__(self, "source_update_time", _coerce_aware_datetime(
            self.source_update_time, "source_update_time", required=False
        ))

        if not isinstance(self.publish_time_is_estimated, bool):
            raise ContractValidationError("publish_time_is_estimated 必须是 boolean")
        if not any(
            isinstance(value, str) and bool(value.strip())
            for value in (self.title, self.content, self.body)
        ):
            raise ContractValidationError("title、content、body 至少一项非空")

        if mode is NewsDocumentValidationMode.FINAL:
            _require_text(self.raw_archive_uri or "", "raw_archive_uri")
        elif self.raw_archive_uri is not None:
            _require_text(self.raw_archive_uri, "raw_archive_uri")

        object.__setattr__(self, "country_region_codes", _coerce_string_tuple(
            self.country_region_codes, "country_region_codes"
        ))
        object.__setattr__(self, "authors", _coerce_string_tuple(self.authors, "authors"))

        if self.summary_type is not None:
            object.__setattr__(self, "summary_type", _coerce_enum(
                self.summary_type, SummaryType, "summary_type"
            ))
        if self.summary is not None and self.summary.strip() and self.summary_type is None:
            raise ContractValidationError("有 summary 时 summary_type 必填")
        if self.content_license is not None:
            object.__setattr__(self, "content_license", _coerce_enum(
                self.content_license, ContentLicense, "content_license"
            ))

        duplicate_type = _coerce_enum(self.duplicate_type, DuplicateType, "duplicate_type")
        object.__setattr__(self, "duplicate_type", duplicate_type)
        if duplicate_type is DuplicateType.NONE and self.duplicate_of_news_id is not None:
            raise ContractValidationError(
                "duplicate_type=none 时 duplicate_of_news_id 必须为空"
            )
        if duplicate_type is not DuplicateType.NONE:
            object.__setattr__(self, "duplicate_of_news_id", validate_news_id(
                self.duplicate_of_news_id or ""
            ))

        stock_codes = _coerce_optional_string_tuple(self.stock_codes, "stock_codes")
        stocks = _coerce_optional_string_tuple(self.stocks, "stocks")
        if stock_codes is not None:
            stock_codes = tuple(validate_stock_code(code) for code in stock_codes)
        if stocks is not None:
            stocks = tuple(validate_stock_code(code) for code in stocks)
        if stock_codes is not None and stocks is not None and stock_codes != stocks:
            raise ContractValidationError(
                "Phase 1 中 stock_codes 与兼容字段 stocks 必须完全一致"
            )
        object.__setattr__(self, "stock_codes", stock_codes)
        object.__setattr__(self, "stocks", stocks)

        if self.source is not None:
            legacy_source = validate_source_id(self.source)
            if legacy_source != self.source_id:
                raise ContractValidationError("source 与 source_id 不一致")
            object.__setattr__(self, "source", legacy_source)
        for old_field_name in ("pub_time", "fetch_time"):
            old_value = getattr(self, old_field_name)
            if old_value is not None and not isinstance(old_value, str):
                raise ContractValidationError(
                    f"{old_field_name} 是旧兼容字段，必须保持字符串类型"
                )

        for status_field in ("vec_status", "pdf_status", "body_status"):
            status = getattr(self, status_field)
            if status is not None:
                object.__setattr__(self, status_field, _coerce_enum(
                    status, ProcessingStatus, status_field
                ))

        if self.content_vec is not None:
            if isinstance(self.content_vec, (str, bytes)):
                raise ContractValidationError("content_vec 必须是数值序列")
            try:
                vector = tuple(float(value) for value in self.content_vec)
            except (TypeError, ValueError) as exc:
                raise ContractValidationError("content_vec 必须是数值序列") from exc
            if not vector:
                raise ContractValidationError("content_vec 不能为空序列")
            object.__setattr__(self, "content_vec", vector)
            object.__setattr__(self, "embedding_model_version", _validate_stable_id(
                self.embedding_model_version or "", "embedding_model_version"
            ))


@dataclass(frozen=True, slots=True, kw_only=True)
class SourceRecord:
    """V1.1 静态 Source 主数据；不包含任何运行健康字段。"""

    source_id: str
    source_revision: int
    source_name: str
    source_category: SourceCategory | str
    acquisition_type: AcquisitionType | str
    directness: SourceDirectness | str
    default_channel: str
    country_region_codes: Sequence[str]
    languages: Sequence[str]
    source_timezone: str
    enabled: bool
    authority_status: AuthorityStatus | str
    authority_version: str
    authority_effective_from: datetime | str
    is_official: bool
    content_license: ContentLicense | str
    paywall_type: PaywallType | str
    created_at: datetime | str
    updated_at: datetime | str

    schema_version: str = "source_v1"
    publisher_entity_id: str | None = None
    homepage_url: str | None = None
    endpoint_url: str | None = None
    source_authority: int | None = None
    authority_level: AuthorityLevel | str | None = None
    authority_basis: str | None = None
    expected_frequency: ExpectedFrequency | str | None = None
    collect_interval_seconds: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_id", validate_source_id(self.source_id))
        object.__setattr__(self, "schema_version", _validate_stable_id(
            self.schema_version, "schema_version"
        ))
        if isinstance(self.source_revision, bool) or not isinstance(self.source_revision, int) or self.source_revision < 1:
            raise ContractValidationError("source_revision 必须是从 1 开始的整数")
        object.__setattr__(self, "source_name", _require_text(self.source_name, "source_name"))
        object.__setattr__(self, "source_category", _coerce_enum(
            self.source_category, SourceCategory, "source_category"
        ))
        object.__setattr__(self, "acquisition_type", _coerce_enum(
            self.acquisition_type, AcquisitionType, "acquisition_type"
        ))
        object.__setattr__(self, "directness", _coerce_enum(
            self.directness, SourceDirectness, "directness"
        ))
        object.__setattr__(self, "default_channel", _require_text(
            self.default_channel, "default_channel"
        ))
        object.__setattr__(self, "country_region_codes", _coerce_string_tuple(
            self.country_region_codes, "country_region_codes", required=True
        ))
        object.__setattr__(self, "languages", _coerce_string_tuple(
            self.languages, "languages", required=True
        ))
        object.__setattr__(self, "source_timezone", _require_text(
            self.source_timezone, "source_timezone"
        ))
        if not isinstance(self.enabled, bool) or not isinstance(self.is_official, bool):
            raise ContractValidationError("enabled 和 is_official 必须是 boolean")

        authority_status = _coerce_enum(
            self.authority_status, AuthorityStatus, "authority_status"
        )
        authority_level = None
        if self.authority_level is not None:
            authority_level = _coerce_enum(
                self.authority_level, AuthorityLevel, "authority_level"
            )
        _validate_authority(
            authority_status,
            self.source_authority,
            require_governance_details=True,
            level=authority_level,
            basis=self.authority_basis,
        )
        object.__setattr__(self, "authority_status", authority_status)
        object.__setattr__(self, "authority_level", authority_level)
        object.__setattr__(self, "authority_version", _validate_stable_id(
            self.authority_version, "authority_version"
        ))
        object.__setattr__(self, "authority_effective_from", _coerce_aware_datetime(
            self.authority_effective_from, "authority_effective_from"
        ))
        object.__setattr__(self, "content_license", _coerce_enum(
            self.content_license, ContentLicense, "content_license"
        ))
        object.__setattr__(self, "paywall_type", _coerce_enum(
            self.paywall_type, PaywallType, "paywall_type"
        ))
        if self.expected_frequency is not None:
            object.__setattr__(self, "expected_frequency", _coerce_enum(
                self.expected_frequency, ExpectedFrequency, "expected_frequency"
            ))
        if self.collect_interval_seconds is not None:
            _require_non_negative_int(
                self.collect_interval_seconds, "collect_interval_seconds"
            )
            if self.collect_interval_seconds == 0:
                raise ContractValidationError("collect_interval_seconds 必须大于 0")

        if self.publisher_entity_id is not None:
            object.__setattr__(self, "publisher_entity_id", _validate_prefixed_id(
                self.publisher_entity_id, "publisher_entity_id", "ent_"
            ))
        created_at = _coerce_aware_datetime(self.created_at, "created_at")
        updated_at = _coerce_aware_datetime(self.updated_at, "updated_at")
        if updated_at < created_at:
            raise ContractValidationError("updated_at 不得早于 created_at")
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)


@dataclass(frozen=True, slots=True, kw_only=True)
class SourceHealth:
    """按来源产生的高频运行观测，与 SourceRecord revision 分离。"""

    source_health_id: str
    source_id: str
    observed_at: datetime | str
    window_start: datetime | str
    window_end: datetime | str
    health_status: HealthStatus | str
    consecutive_failures: int
    attempt_count: int
    success_count: int
    collected_item_count: int
    new_item_count: int
    empty_success_count: int
    parse_failure_count: int
    completeness_status: CompletenessStatus | str
    health_policy_version: str
    is_current: bool
    created_at: datetime | str

    last_success_at: datetime | str | None = None
    last_attempt_at: datetime | str | None = None
    latency_ms: int | None = None
    last_item_publish_time: datetime | str | None = None
    data_delay_seconds: int | None = None
    completeness_metrics: Mapping[str, Any] = field(default_factory=dict)
    last_error_code: str | None = None
    last_error_summary: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_health_id", _validate_prefixed_id(
            self.source_health_id, "source_health_id", "shealth_"
        ))
        object.__setattr__(self, "source_id", validate_source_id(self.source_id))
        observed_at = _coerce_aware_datetime(self.observed_at, "observed_at")
        window_start = _coerce_aware_datetime(self.window_start, "window_start")
        window_end = _coerce_aware_datetime(self.window_end, "window_end")
        if window_start > window_end:
            raise ContractValidationError("window_start 不得晚于 window_end")
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "window_start", window_start)
        object.__setattr__(self, "window_end", window_end)
        object.__setattr__(self, "health_status", _coerce_enum(
            self.health_status, HealthStatus, "health_status"
        ))
        object.__setattr__(self, "completeness_status", _coerce_enum(
            self.completeness_status, CompletenessStatus, "completeness_status"
        ))
        object.__setattr__(self, "health_policy_version", _validate_stable_id(
            self.health_policy_version, "health_policy_version"
        ))
        if not isinstance(self.is_current, bool):
            raise ContractValidationError("is_current 必须是 boolean")

        for field_name in (
            "consecutive_failures",
            "attempt_count",
            "success_count",
            "collected_item_count",
            "new_item_count",
            "empty_success_count",
            "parse_failure_count",
        ):
            _require_non_negative_int(getattr(self, field_name), field_name)
        if self.success_count > self.attempt_count:
            raise ContractValidationError("success_count 不得大于 attempt_count")
        if self.empty_success_count > self.success_count:
            raise ContractValidationError("empty_success_count 不得大于 success_count")
        if self.new_item_count > self.collected_item_count:
            raise ContractValidationError("new_item_count 不得大于 collected_item_count")

        object.__setattr__(self, "latency_ms", _optional_non_negative_int(
            self.latency_ms, "latency_ms"
        ))
        object.__setattr__(self, "data_delay_seconds", _optional_non_negative_int(
            self.data_delay_seconds, "data_delay_seconds"
        ))
        for field_name in (
            "last_success_at",
            "last_attempt_at",
            "last_item_publish_time",
        ):
            object.__setattr__(self, field_name, _coerce_aware_datetime(
                getattr(self, field_name), field_name, required=False
            ))
        object.__setattr__(self, "completeness_metrics", _coerce_mapping(
            self.completeness_metrics, "completeness_metrics"
        ))
        object.__setattr__(self, "last_error_code", _optional_text(
            self.last_error_code, "last_error_code"
        ))
        object.__setattr__(self, "last_error_summary", _optional_text(
            self.last_error_summary, "last_error_summary"
        ))
        object.__setattr__(self, "created_at", _coerce_aware_datetime(
            self.created_at, "created_at"
        ))


@dataclass(frozen=True, slots=True, kw_only=True)
class Entity:
    """V1.1 Entity；第一阶段主要实例化 A 股证券实体。"""

    entity_id: str
    entity_revision: int
    entity_type: EntityType | str
    canonical_name: str
    normalized_name: str
    status: EntityStatus | str
    provenance_source_ids: Sequence[str]
    confidence: float
    created_at: datetime | str
    updated_at: datetime | str

    schema_version: str = "entity_v1"
    short_name: str | None = None
    english_name: str | None = None
    aliases: Sequence[str] = field(default_factory=tuple)
    stock_code: str | None = None
    exchange: Exchange | str | None = None
    external_ids: Mapping[str, Any] = field(default_factory=dict)
    parent_entity_id: str | None = None
    country_region_codes: Sequence[str] = field(default_factory=tuple)
    description: str | None = None
    valid_from: datetime | str | None = None
    valid_to: datetime | str | None = None
    entity_model_version: str | None = None
    merged_into_entity_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "entity_id", _validate_prefixed_id(
            self.entity_id, "entity_id", "ent_"
        ))
        object.__setattr__(self, "schema_version", _validate_stable_id(
            self.schema_version, "schema_version"
        ))
        if isinstance(self.entity_revision, bool) or not isinstance(self.entity_revision, int) or self.entity_revision < 1:
            raise ContractValidationError("entity_revision 必须是从 1 开始的整数")
        entity_type = _coerce_enum(self.entity_type, EntityType, "entity_type")
        object.__setattr__(self, "entity_type", entity_type)
        object.__setattr__(self, "canonical_name", _require_text(
            self.canonical_name, "canonical_name"
        ))
        object.__setattr__(self, "normalized_name", _require_text(
            self.normalized_name, "normalized_name"
        ))
        object.__setattr__(self, "status", _coerce_enum(
            self.status, EntityStatus, "status"
        ))
        object.__setattr__(self, "provenance_source_ids", tuple(
            validate_source_id(source_id)
            for source_id in _coerce_string_tuple(
                self.provenance_source_ids, "provenance_source_ids", required=True
            )
        ))
        object.__setattr__(self, "confidence", _validate_confidence(
            self.confidence, "confidence"
        ))
        object.__setattr__(self, "aliases", _coerce_string_tuple(self.aliases, "aliases"))
        object.__setattr__(self, "country_region_codes", _coerce_string_tuple(
            self.country_region_codes, "country_region_codes"
        ))
        object.__setattr__(self, "external_ids", _coerce_mapping(
            self.external_ids, "external_ids"
        ))

        exchange = None
        if self.exchange is not None:
            exchange = _coerce_enum(self.exchange, Exchange, "exchange")
        stock_code = None
        if self.stock_code is not None:
            stock_code = validate_stock_code(self.stock_code)
        if entity_type is EntityType.STOCK:
            if stock_code is None or exchange is None:
                raise ContractValidationError(
                    "entity_type=stock 时 stock_code 和 exchange 必填"
                )
            expected_exchange = exchange_for_stock_code(stock_code)
            if exchange is not expected_exchange:
                raise ContractValidationError(
                    f"stock_code {stock_code} 与 exchange {exchange.value} 不一致"
                )
            expected_id = make_stock_entity_id(stock_code)
            if self.entity_id != expected_id:
                raise ContractValidationError(
                    f"证券 entity_id 必须稳定派生为 {expected_id}"
                )
        object.__setattr__(self, "stock_code", stock_code)
        object.__setattr__(self, "exchange", exchange)

        for field_name in ("parent_entity_id", "merged_into_entity_id"):
            value = getattr(self, field_name)
            if value is not None:
                value = _validate_prefixed_id(value, field_name, "ent_")
                if value == self.entity_id:
                    raise ContractValidationError(f"{field_name} 不得指向自身")
                object.__setattr__(self, field_name, value)
        valid_from = _coerce_aware_datetime(self.valid_from, "valid_from", required=False)
        valid_to = _coerce_aware_datetime(self.valid_to, "valid_to", required=False)
        if valid_from is not None and valid_to is not None and valid_from > valid_to:
            raise ContractValidationError("valid_from 不得晚于 valid_to")
        object.__setattr__(self, "valid_from", valid_from)
        object.__setattr__(self, "valid_to", valid_to)

        created_at = _coerce_aware_datetime(self.created_at, "created_at")
        updated_at = _coerce_aware_datetime(self.updated_at, "updated_at")
        if updated_at < created_at:
            raise ContractValidationError("updated_at 不得早于 created_at")
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)


@dataclass(frozen=True, slots=True, kw_only=True)
class EntityAlias:
    """实体名称匹配的正式时序记录。"""

    entity_alias_id: str
    entity_id: str
    alias: str
    normalized_alias: str
    alias_type: EntityAliasType | str
    language: str
    provenance_source_ids: Sequence[str]
    confidence: float
    derived_by: DerivedBy | str
    revision: int
    is_current: bool
    manual_lock: bool
    created_at: datetime | str
    updated_at: datetime | str

    valid_from: datetime | str | None = None
    valid_to: datetime | str | None = None
    provenance_refs: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    entity_model_version: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "entity_alias_id", _validate_prefixed_id(
            self.entity_alias_id, "entity_alias_id", "ealias_"
        ))
        object.__setattr__(self, "entity_id", _validate_prefixed_id(
            self.entity_id, "entity_id", "ent_"
        ))
        object.__setattr__(self, "alias", _require_text(self.alias, "alias"))
        object.__setattr__(self, "normalized_alias", _require_text(
            self.normalized_alias, "normalized_alias"
        ))
        object.__setattr__(self, "alias_type", _coerce_enum(
            self.alias_type, EntityAliasType, "alias_type"
        ))
        object.__setattr__(self, "language", _require_text(self.language, "language"))
        object.__setattr__(self, "provenance_source_ids", tuple(
            validate_source_id(source_id)
            for source_id in _coerce_string_tuple(
                self.provenance_source_ids, "provenance_source_ids", required=True
            )
        ))
        object.__setattr__(self, "confidence", _validate_confidence(
            self.confidence, "confidence"
        ))
        derived_by = _coerce_enum(self.derived_by, DerivedBy, "derived_by")
        object.__setattr__(self, "derived_by", derived_by)
        if isinstance(self.revision, bool) or not isinstance(self.revision, int) or self.revision < 1:
            raise ContractValidationError("revision 必须是从 1 开始的整数")
        if not isinstance(self.is_current, bool) or not isinstance(self.manual_lock, bool):
            raise ContractValidationError("is_current 和 manual_lock 必须是 boolean")
        if derived_by in (DerivedBy.RULE, DerivedBy.AI):
            object.__setattr__(self, "entity_model_version", _validate_stable_id(
                self.entity_model_version or "", "entity_model_version"
            ))

        valid_from = _coerce_aware_datetime(self.valid_from, "valid_from", required=False)
        valid_to = _coerce_aware_datetime(self.valid_to, "valid_to", required=False)
        if valid_from is not None and valid_to is not None and valid_from > valid_to:
            raise ContractValidationError("valid_from 不得晚于 valid_to")
        object.__setattr__(self, "valid_from", valid_from)
        object.__setattr__(self, "valid_to", valid_to)

        if isinstance(self.provenance_refs, (str, bytes)):
            raise ContractValidationError("provenance_refs 必须是 mapping 序列")
        refs: list[Mapping[str, Any]] = []
        for ref in self.provenance_refs:
            refs.append(_coerce_mapping(ref, "provenance_refs[]"))
        object.__setattr__(self, "provenance_refs", tuple(refs))

        created_at = _coerce_aware_datetime(self.created_at, "created_at")
        updated_at = _coerce_aware_datetime(self.updated_at, "updated_at")
        if updated_at < created_at:
            raise ContractValidationError("updated_at 不得早于 created_at")
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)


__all__ = [
    "AcquisitionType",
    "AuthorityLevel",
    "AuthorityStatus",
    "CompletenessStatus",
    "ContentLicense",
    "ContractValidationError",
    "DerivedBy",
    "DocumentStatus",
    "DocumentType",
    "DuplicateType",
    "Entity",
    "EntityAlias",
    "EntityAliasType",
    "EntityStatus",
    "EntityType",
    "Exchange",
    "ExpectedFrequency",
    "HealthStatus",
    "NewsDocument",
    "NewsDocumentValidationMode",
    "PaywallType",
    "ProcessingStatus",
    "PublishTimePrecision",
    "SourceCategory",
    "SourceDirectness",
    "SourceHealth",
    "SourceRecord",
    "SummaryType",
    "exchange_for_stock_code",
    "make_stock_entity_id",
    "validate_news_id",
    "validate_source_id",
    "validate_stock_code",
]
