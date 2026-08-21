"""Read-only public A-share universe preflight.

The provider follows the endpoint and market filter used by AkShare
``stock_zh_a_spot_em`` 1.18.92, but owns a request-local Session so domestic
traffic never inherits ambient/system proxies.  This module never writes master
data.  A single provider is sufficient for snapshot diagnostics, not for an
authoritative apply: a separately audited validation source remains mandatory.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
import time
from typing import Any

from data_collect.news_model.contracts import (
    ContractValidationError,
    exchange_for_stock_code,
    validate_stock_code,
)


PROVIDER_ID = "eastmoney_a_share_spot_direct_v1"
PROVIDER_UNDERLYING_SOURCE = "Eastmoney push2 A-share market grid"
PROVIDER_AKSHARE_REFERENCE = "akshare.stock_zh_a_spot_em (audited with AkShare 1.18.92)"
DOMESTIC_NETWORK_MODE = "DIRECT"
INHERITED_ENV_PROXY = False

EASTMONEY_A_SHARE_URL = "https://82.push2.eastmoney.com/api/qt/clist/get"
EASTMONEY_A_SHARE_FILTER = (
    "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048"
)

MINIMUM_BOOTSTRAP_COLUMNS = ("stock_code", "InstrumentName", "ExchangeID")
REQUIRED_SAMPLE_CODES = ("000001.SZ", "600519.SH")


class PublicInstrumentError(RuntimeError):
    """Base error for public instrument preflight failures."""


class ProviderResponseError(PublicInstrumentError):
    """The domestic provider returned an unusable or incomplete response."""


@dataclass(frozen=True)
class PublicInstrument:
    """One normalized current-snapshot stock fact.

    ``classification_uncertain`` is intentionally true for this first provider:
    its A-share market filter is useful evidence but is not an exchange-issued
    classification assertion.
    """

    stock_code: str
    instrument_name: str
    # Preserve the legacy a_share_instrument/QMT values intended for
    # instrument_info.  ``canonical_exchange`` serves the frozen Entity model.
    exchange_id: str
    canonical_exchange: str
    source_security_type: str = "a_share"
    classification_basis: str = "eastmoney_a_share_market_filter"
    classification_uncertain: bool = True
    raw_market_id: int | None = None
    listing_status: str = "present_in_current_provider_universe"


@dataclass(frozen=True)
class InstrumentSchemaInspection:
    inspected: bool
    columns: tuple[str, ...] = ()
    column_types: Mapping[str, str] = field(default_factory=dict)
    missing_minimum_columns: tuple[str, ...] = ()
    unsupported_required_columns: tuple[str, ...] = ()
    compatible: bool = False
    diagnostics: tuple[str, ...] = ()


@dataclass(frozen=True)
class CompletenessPolicy:
    minimum_total_records: int = 3_000
    minimum_valid_ratio: float = 0.98
    require_samples: tuple[str, ...] = REQUIRED_SAMPLE_CODES
    require_secondary_validation_for_apply: bool = True

    def __post_init__(self) -> None:
        if self.minimum_total_records <= 0:
            raise ValueError("minimum_total_records must be positive")
        if not 0 < self.minimum_valid_ratio <= 1:
            raise ValueError("minimum_valid_ratio must be in (0, 1]")


@dataclass(frozen=True)
class PublicInstrumentPreflightReport:
    provider: str
    provider_underlying_source: str
    provider_akshare_reference: str
    domestic_network_mode: str
    inherited_env_proxy: bool
    total_raw_records: int
    valid_stock_records: int
    unique_stock_codes: int
    exchange_counts: Mapping[str, int]
    invalid_code_count: int
    duplicate_code_count: int
    empty_name_count: int
    question_mark_name_count: int
    replacement_char_name_count: int
    unknown_security_type_count: int
    classification_uncertain_count: int
    records: tuple[PublicInstrument, ...]
    completeness_status: str
    apply_allowed: bool
    secondary_validation_status: str
    blockers: tuple[str, ...] = ()
    schema_inspection: InstrumentSchemaInspection = field(
        default_factory=lambda: InstrumentSchemaInspection(inspected=False)
    )

    def record_for(self, stock_code: str) -> PublicInstrument | None:
        canonical = validate_stock_code(stock_code)
        return next((item for item in self.records if item.stock_code == canonical), None)


def infer_stock_code(raw_code: Any) -> tuple[str, str, int]:
    """Map a six-digit A-share code to code/legacy ExchangeID/source market id.

    Accepted prefixes intentionally exclude known B-share, fund/ETF, index and
    bond/convertible-bond code families.  Final syntax validation always uses the
    frozen News V1.1 stock-code validator.
    """

    code = str(raw_code).strip()
    if len(code) != 6 or not code.isascii() or not code.isdigit():
        raise ValueError(f"not a six-digit stock code: {raw_code!r}")

    if code.startswith(("60", "68")):
        suffix, exchange_id, market_id = "SH", "SH", 1
    elif code.startswith(("00", "30")):
        suffix, exchange_id, market_id = "SZ", "SZ", 0
    elif code.startswith(("43", "83", "87", "92")):
        suffix, exchange_id, market_id = "BJ", "BJ", 0
    else:
        raise ValueError(f"code family is not an accepted A-share stock: {code}")

    stock_code = validate_stock_code(f"{code}.{suffix}")
    return stock_code, exchange_id, market_id


def _is_question_mark_name(value: str) -> bool:
    compact = "".join(character for character in value if not character.isspace())
    return bool(compact) and all(character in {"?", "？"} for character in compact)


def _coerce_market_id(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_public_instrument(raw: Mapping[str, Any]) -> PublicInstrument:
    """Normalize one provider record, rejecting uncertain code/type facts."""

    if not isinstance(raw, Mapping):
        raise ValueError("provider record must be a mapping")
    security_type = str(raw.get("security_type", "")).strip().lower()
    if security_type != "a_share":
        raise ValueError(f"security type is not proven A-share: {security_type or '<missing>'}")

    stock_code, exchange_id, expected_market_id = infer_stock_code(raw.get("code", ""))
    market_id = _coerce_market_id(raw.get("market_id"))
    if market_id is None:
        raise ValueError(f"missing/invalid source market id for {stock_code}")
    if market_id != expected_market_id:
        raise ValueError(
            f"source market id conflicts with code family for {stock_code}: "
            f"expected={expected_market_id}, actual={market_id}"
        )

    name = str(raw.get("name", "")).strip()
    if not name:
        raise ValueError(f"empty InstrumentName for {stock_code}")
    if _is_question_mark_name(name):
        raise ValueError(f"question-mark-only InstrumentName for {stock_code}")
    if "\ufffd" in name:
        raise ValueError(f"replacement character in InstrumentName for {stock_code}")

    return PublicInstrument(
        stock_code=stock_code,
        instrument_name=name,
        exchange_id=exchange_id,
        canonical_exchange=exchange_for_stock_code(stock_code).value,
        raw_market_id=market_id,
    )


def evaluate_public_instruments(
    raw_records: Sequence[Mapping[str, Any]] | Iterable[Mapping[str, Any]],
    *,
    policy: CompletenessPolicy | None = None,
    schema_inspection: InstrumentSchemaInspection | None = None,
    secondary_validation_passed: bool = False,
) -> PublicInstrumentPreflightReport:
    """Evaluate a provider snapshot without modifying its rows or any database."""

    selected_policy = policy or CompletenessPolicy()
    schema = schema_inspection or InstrumentSchemaInspection(inspected=False)
    raw_snapshot = tuple(raw_records)

    valid: list[PublicInstrument] = []
    valid_row_count = 0
    seen: set[str] = set()
    duplicate_count = 0
    invalid_code_count = 0
    empty_name_count = 0
    question_mark_count = 0
    replacement_count = 0
    unknown_type_count = 0

    for raw in raw_snapshot:
        security_type = str(raw.get("security_type", "")).strip().lower() if isinstance(raw, Mapping) else ""
        if security_type != "a_share":
            unknown_type_count += 1
            continue

        raw_name = str(raw.get("name", "")).strip()
        if not raw_name:
            empty_name_count += 1
            continue
        if _is_question_mark_name(raw_name):
            question_mark_count += 1
            continue
        if "\ufffd" in raw_name:
            replacement_count += 1
            continue

        try:
            normalized = normalize_public_instrument(raw)
        except (ValueError, ContractValidationError):
            invalid_code_count += 1
            continue

        valid_row_count += 1
        if normalized.stock_code in seen:
            duplicate_count += 1
            continue
        seen.add(normalized.stock_code)
        valid.append(normalized)

    records = tuple(sorted(valid, key=lambda item: item.stock_code))
    exchange_counts = {exchange: 0 for exchange in ("SSE", "SZSE", "BSE")}
    for item in records:
        exchange_counts[item.canonical_exchange] += 1

    blockers: list[str] = []
    total = len(raw_snapshot)
    if total < selected_policy.minimum_total_records:
        blockers.append(
            f"total_raw_records below conservative minimum: "
            f"{total} < {selected_policy.minimum_total_records}"
        )
    valid_ratio = len(records) / total if total else 0.0
    if valid_ratio < selected_policy.minimum_valid_ratio:
        blockers.append(
            f"valid/unique ratio too low: {valid_ratio:.6f} < "
            f"{selected_policy.minimum_valid_ratio:.6f}"
        )
    for exchange_id in ("SSE", "SZSE", "BSE"):
        if exchange_counts[exchange_id] <= 0:
            blockers.append(f"missing required exchange: {exchange_id}")
    for label, count in (
        ("invalid_code_count", invalid_code_count),
        ("duplicate_code_count", duplicate_count),
        ("empty_name_count", empty_name_count),
        ("question_mark_name_count", question_mark_count),
        ("replacement_char_name_count", replacement_count),
        ("unknown_security_type_count", unknown_type_count),
    ):
        if count:
            blockers.append(f"{label} must be zero, actual={count}")
    missing_samples = [code for code in selected_policy.require_samples if code not in seen]
    if missing_samples:
        blockers.append(f"required current sample codes missing: {', '.join(missing_samples)}")

    completeness_status = "PASS" if not blockers else "FAIL"
    apply_blockers: list[str] = []
    if completeness_status != "PASS":
        apply_blockers.append("snapshot completeness gate failed")
    if not schema.inspected:
        apply_blockers.append("instrument_info schema has not been inspected")
    elif not schema.compatible:
        apply_blockers.append("instrument_info schema is not compatible")
    if selected_policy.require_secondary_validation_for_apply and not secondary_validation_passed:
        apply_blockers.append("independent security-universe validation is pending")

    return PublicInstrumentPreflightReport(
        provider=PROVIDER_ID,
        provider_underlying_source=PROVIDER_UNDERLYING_SOURCE,
        provider_akshare_reference=PROVIDER_AKSHARE_REFERENCE,
        domestic_network_mode=DOMESTIC_NETWORK_MODE,
        inherited_env_proxy=INHERITED_ENV_PROXY,
        total_raw_records=total,
        valid_stock_records=valid_row_count,
        unique_stock_codes=len(records),
        exchange_counts=exchange_counts,
        invalid_code_count=invalid_code_count,
        duplicate_code_count=duplicate_count,
        empty_name_count=empty_name_count,
        question_mark_name_count=question_mark_count,
        replacement_char_name_count=replacement_count,
        unknown_security_type_count=unknown_type_count,
        classification_uncertain_count=sum(
            1 for item in records if item.classification_uncertain
        ),
        records=records,
        completeness_status=completeness_status,
        apply_allowed=completeness_status == "PASS" and not apply_blockers,
        secondary_validation_status=("PASS" if secondary_validation_passed else "PENDING"),
        blockers=tuple(blockers + apply_blockers),
        schema_inspection=schema,
    )


class EastmoneyDirectAShareProvider:
    """Fetch the audited Eastmoney A-share grid with a request-local DIRECT session."""

    def __init__(
        self,
        *,
        session_factory: Callable[[], Any] | None = None,
        page_size: int = 500,
        timeout_seconds: float = 15.0,
        max_pages: int = 100,
        request_attempts: int = 3,
        retry_interval_seconds: float = 1.0,
        min_interval_seconds: float = 0.25,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if page_size <= 0 or timeout_seconds <= 0 or max_pages <= 0 or request_attempts <= 0:
            raise ValueError(
                "page_size, timeout_seconds, max_pages and request_attempts must be positive"
            )
        if min_interval_seconds < 0 or retry_interval_seconds < 0:
            raise ValueError("request intervals cannot be negative")
        self._session_factory = session_factory
        self.page_size = page_size
        self.timeout_seconds = timeout_seconds
        self.max_pages = max_pages
        self.request_attempts = request_attempts
        self.retry_interval_seconds = retry_interval_seconds
        self.min_interval_seconds = min_interval_seconds
        self._sleeper = sleeper

    @staticmethod
    def _default_session_factory() -> Any:
        import requests

        return requests.Session()

    def fetch(self) -> tuple[Mapping[str, Any], ...]:
        session = (self._session_factory or self._default_session_factory)()
        # Request-local policy only: never mutate requests globals or the process
        # environment.  Explicit provider proxies are intentionally unsupported
        # because domestic master-data is a DIRECT-only core dependency.
        session.trust_env = False
        session.proxies.clear()
        records: list[Mapping[str, Any]] = []
        expected_total: int | None = None
        try:
            for page in range(1, self.max_pages + 1):
                params = {
                    "pn": page,
                    "pz": self.page_size,
                    "po": 1,
                    "np": 1,
                    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                    "fltt": 2,
                    "invt": 2,
                    "fid": "f12",
                    "fs": EASTMONEY_A_SHARE_FILTER,
                    "fields": "f12,f13,f14",
                }
                response = None
                last_request_error: Exception | None = None
                for attempt in range(1, self.request_attempts + 1):
                    try:
                        response = session.get(
                            EASTMONEY_A_SHARE_URL,
                            params=params,
                            timeout=self.timeout_seconds,
                        )
                        response.raise_for_status()
                        last_request_error = None
                        break
                    except Exception as exc:
                        last_request_error = exc
                        if attempt < self.request_attempts and self.retry_interval_seconds:
                            self._sleeper(self.retry_interval_seconds)
                if response is None or last_request_error is not None:
                    raise ProviderResponseError(
                        f"provider request failed after {self.request_attempts} attempts "
                        f"on page {page}: {type(last_request_error).__name__}: "
                        f"{last_request_error}"
                    ) from last_request_error
                try:
                    payload = response.json()
                except Exception as exc:
                    raise ProviderResponseError(
                        f"provider returned malformed JSON on page {page}"
                    ) from exc
                if not isinstance(payload, Mapping):
                    raise ProviderResponseError("provider payload must be an object")
                data = payload.get("data")
                if not isinstance(data, Mapping):
                    raise ProviderResponseError("provider payload missing data object")
                try:
                    expected_total = int(data.get("total"))
                except (TypeError, ValueError) as exc:
                    raise ProviderResponseError("provider data.total is not an integer") from exc
                raw_diff = data.get("diff")
                if isinstance(raw_diff, Mapping):
                    page_rows = list(raw_diff.values())
                elif isinstance(raw_diff, list):
                    page_rows = raw_diff
                else:
                    raise ProviderResponseError("provider data.diff must be a list or object")
                if any(not isinstance(row, Mapping) for row in page_rows):
                    raise ProviderResponseError("provider data.diff contains a non-object row")
                if not page_rows and len(records) < expected_total:
                    raise ProviderResponseError(
                        f"provider pagination ended early: received={len(records)}, "
                        f"expected={expected_total}"
                    )
                for row in page_rows:
                    records.append(
                        {
                            "code": row.get("f12"),
                            "market_id": row.get("f13"),
                            "name": row.get("f14"),
                            "security_type": "a_share",
                        }
                    )
                if len(records) >= expected_total:
                    break
                if self.min_interval_seconds:
                    self._sleeper(self.min_interval_seconds)
            else:
                raise ProviderResponseError(
                    f"provider pagination exceeded max_pages={self.max_pages}"
                )
        finally:
            session.close()

        if expected_total is None or len(records) != expected_total:
            raise ProviderResponseError(
                f"provider total mismatch: received={len(records)}, expected={expected_total}"
            )
        return tuple(records)


def inspect_instrument_info_schema(
    connection_factory: Callable[[], Any] | None = None,
) -> InstrumentSchemaInspection:
    """Read-only inspection of the legacy dynamic ``instrument_info`` table."""

    if connection_factory is None:
        from data_collect.utils.db import get_connection

        connection_factory = get_connection

    connection = connection_factory()
    diagnostics: list[str] = []
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute(
                """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'instrument_info'
                ORDER BY ordinal_position
                """
            )
            rows = cursor.fetchall()
        columns = tuple(str(row[0]) for row in rows)
        column_types = {str(row[0]): str(row[1]) for row in rows}
        missing = tuple(column for column in MINIMUM_BOOTSTRAP_COLUMNS if column not in columns)
        unsupported_required = tuple(
            str(row[0])
            for row in rows
            if str(row[2]).upper() == "NO"
            and row[3] is None
            and str(row[0]) not in MINIMUM_BOOTSTRAP_COLUMNS
        )
        if not columns:
            diagnostics.append("public.instrument_info does not exist or has no columns")
        if missing:
            diagnostics.append(
                f"instrument_info missing minimum bootstrap columns: {', '.join(missing)}"
            )
        if unsupported_required:
            diagnostics.append(
                "instrument_info has required columns unavailable from the public provider: "
                + ", ".join(unsupported_required)
            )
        text_types = {"text", "character varying", "character"}
        incompatible_types = tuple(
            column
            for column in MINIMUM_BOOTSTRAP_COLUMNS
            if column in column_types and column_types[column].lower() not in text_types
        )
        if incompatible_types:
            diagnostics.append(
                "instrument_info minimum columns have incompatible types: "
                + ", ".join(incompatible_types)
            )
        return InstrumentSchemaInspection(
            inspected=True,
            columns=columns,
            column_types=column_types,
            missing_minimum_columns=missing,
            unsupported_required_columns=unsupported_required,
            compatible=(
                bool(columns)
                and not missing
                and not unsupported_required
                and not incompatible_types
            ),
            diagnostics=tuple(diagnostics),
        )
    finally:
        try:
            connection.rollback()
        finally:
            connection.close()


def sample_text_evidence(record: PublicInstrument | None) -> tuple[str, str]:
    if record is None:
        return "<missing>", "<missing>"
    return ascii(record.instrument_name), record.instrument_name.encode("utf-8").hex()


__all__ = [
    "CompletenessPolicy",
    "DOMESTIC_NETWORK_MODE",
    "EASTMONEY_A_SHARE_FILTER",
    "EASTMONEY_A_SHARE_URL",
    "EastmoneyDirectAShareProvider",
    "INHERITED_ENV_PROXY",
    "InstrumentSchemaInspection",
    "MINIMUM_BOOTSTRAP_COLUMNS",
    "PROVIDER_AKSHARE_REFERENCE",
    "PROVIDER_ID",
    "PROVIDER_UNDERLYING_SOURCE",
    "ProviderResponseError",
    "PublicInstrument",
    "PublicInstrumentError",
    "PublicInstrumentPreflightReport",
    "evaluate_public_instruments",
    "infer_stock_code",
    "inspect_instrument_info_schema",
    "normalize_public_instrument",
    "sample_text_evidence",
]
