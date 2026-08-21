"""Official-exchange A-share universe preflight (read-only).

SSE, SZSE and BSE are the authoritative identity sources in this module.
Every adapter owns a request-local DIRECT session and preserves source evidence.
Importing this module performs no network, database or filesystem operation.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from io import BytesIO
import json
import time
from typing import Any

from data_collect.master_data.public_instruments import InstrumentSchemaInspection
from data_collect.news_model.contracts import (
    ContractValidationError,
    validate_stock_code,
)


PROVIDER_MODE = "official_exchange_union"
DOMESTIC_NETWORK_MODE = "DIRECT"
INHERITED_ENV_PROXY = False
DATABASE_APPLY_BLOCKER = "database_apply_not_implemented_in_this_phase"

SSE_SOURCE_ID = "sse_official_stock_list"
SZSE_SOURCE_ID = "szse_official_a_share_list"
BSE_SOURCE_ID = "bse_official_listed_company"

SSE_URL = "https://query.sse.com.cn/sseQuery/commonQuery.do"
SZSE_URL = "https://www.szse.cn/api/report/ShowReport"
BSE_URL = "https://www.bse.cn/nqxxController/nqxxCnzq.do"

_SSE_HEADERS = {
    "Host": "query.sse.com.cn",
    "Pragma": "no-cache",
    "Referer": "https://www.sse.com.cn/assortment/stock/list/share/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/81.0.4044.138 Safari/537.36"
    ),
}
_BSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/110.0.0.0 Safari/537.36"
    )
}


class OfficialExchangeError(RuntimeError):
    """Base error for an unusable official exchange response."""


class OfficialProviderResponseError(OfficialExchangeError):
    """An official endpoint response failed structural validation."""


class OfficialUniverseValidationError(OfficialExchangeError):
    """Provider orchestration could not produce a complete universe."""


@dataclass(frozen=True)
class OfficialInstrumentCandidate:
    raw_code: str
    instrument_name: str
    exchange_id: str
    canonical_exchange: str
    source_id: str
    source_record_type: str
    listing_presence: str
    source_security_type: str
    classification_basis: str
    security_type_uncertain: bool
    raw_evidence: Mapping[str, Any]


@dataclass(frozen=True)
class OfficialInstrumentRecord:
    stock_code: str
    instrument_name: str
    exchange_id: str
    canonical_exchange: str
    source_id: str
    source_record_type: str
    listing_presence: str
    source_security_type: str
    classification_basis: str
    provenance: Mapping[str, Any]


@dataclass(frozen=True)
class OfficialProviderResult:
    provider_id: str
    candidates: tuple[OfficialInstrumentCandidate, ...]
    raw_count: int
    ordinary_stock_count: int
    raw_part_counts: Mapping[str, int] = field(default_factory=dict)
    excluded_cdr_codes: tuple[str, ...] = ()
    excluded_cdr_records: tuple[Mapping[str, Any], ...] = ()
    expected_total: int | None = None
    fetched_total: int | None = None
    total_pages: int | None = None

    @property
    def excluded_cdr_count(self) -> int:
        return len(self.excluded_cdr_codes)


@dataclass(frozen=True)
class OfficialUniversePolicy:
    minimum_total_records: int = 3_000
    required_samples: Mapping[str, str] = field(
        default_factory=lambda: {
            "600519.SH": "贵州茅台",
            "000001.SZ": "平安银行",
        }
    )

    def __post_init__(self) -> None:
        if self.minimum_total_records <= 0:
            raise ValueError("minimum_total_records must be positive")


@dataclass(frozen=True)
class OfficialAShareUniverse:
    provider_mode: str
    domestic_network_mode: str
    inherited_env_proxy: bool
    sse: OfficialProviderResult
    szse: OfficialProviderResult
    bse: OfficialProviderResult
    records: tuple[OfficialInstrumentRecord, ...]
    authoritative_raw_total: int
    authoritative_unique_total: int
    exchange_counts: Mapping[str, int]
    duplicate_code_count: int
    name_conflict_count: int
    cross_exchange_conflict_count: int
    invalid_code_count: int
    empty_name_count: int
    question_mark_name_count: int
    replacement_char_name_count: int
    security_type_uncertain_count: int
    universe_status: str
    completeness_status: str
    apply_allowed: bool
    future_apply_prerequisites: tuple[str, ...]
    blockers: tuple[str, ...]
    schema_inspection: InstrumentSchemaInspection

    def record_for(self, stock_code: str) -> OfficialInstrumentRecord | None:
        canonical = validate_stock_code(stock_code)
        return next((item for item in self.records if item.stock_code == canonical), None)


def _new_direct_session(session_factory: Callable[[], Any] | None) -> Any:
    if session_factory is None:
        import requests

        session = requests.Session()
    else:
        session = session_factory()
    session.trust_env = False
    session.proxies.clear()
    return session


def _is_transient_request_error(exc: BaseException) -> bool:
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return True
    try:
        import requests

        return isinstance(exc, (requests.ConnectionError, requests.Timeout))
    except ImportError:
        return False


def _request_with_transient_retry(
    session: Any,
    method_name: str,
    url: str,
    *,
    attempts: int,
    sleeper: Callable[[float], None],
    **kwargs: Any,
) -> Any:
    """Retry only connection/timeout and HTTP 5xx failures with 1s/2s backoff."""

    for attempt_no in range(1, attempts + 1):
        response = None
        try:
            response = getattr(session, method_name)(url, **kwargs)
            response.raise_for_status()
            return response
        except Exception as exc:
            status_code = getattr(response, "status_code", None)
            transient = _is_transient_request_error(exc) or (
                isinstance(status_code, int) and 500 <= status_code <= 599
            )
            if not transient or attempt_no >= attempts:
                raise
            sleeper(float(attempt_no))
    raise AssertionError("unreachable retry state")


def _require_six_digit_code(value: Any, field_name: str) -> str:
    code = str(value).strip()
    if len(code) != 6 or not code.isascii() or not code.isdigit():
        raise ValueError(f"{field_name} must be exactly six ASCII digits: {value!r}")
    return code


def _is_question_mark_name(value: str) -> bool:
    compact = "".join(character for character in value if not character.isspace())
    return bool(compact) and all(character in {"?", "？"} for character in compact)


def _response_json(response: Any, provider_id: str) -> Mapping[str, Any]:
    try:
        payload = response.json()
    except Exception as exc:
        raise OfficialProviderResponseError(
            f"{provider_id} returned malformed JSON"
        ) from exc
    if not isinstance(payload, Mapping):
        raise OfficialProviderResponseError(f"{provider_id} payload must be an object")
    return payload


class SSEOfficialInstrumentProvider:
    """SSE main-board A shares plus STAR, excluding official 689 CDR codes."""

    def __init__(
        self,
        *,
        session_factory: Callable[[], Any] | None = None,
        timeout_seconds: float = 20.0,
        request_attempts: int = 3,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if request_attempts <= 0:
            raise ValueError("request_attempts must be positive")
        self._session_factory = session_factory
        self.timeout_seconds = timeout_seconds
        self.request_attempts = request_attempts
        self._sleeper = sleeper

    @staticmethod
    def _params(stock_type: str) -> dict[str, str]:
        return {
            "STOCK_TYPE": stock_type,
            "REG_PROVINCE": "",
            "CSRC_CODE": "",
            "STOCK_CODE": "",
            "sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
            "COMPANY_STATUS": "2,4,5,7,8",
            "type": "inParams",
            "isPagination": "true",
            "pageHelp.cacheSize": "1",
            "pageHelp.beginPage": "1",
            "pageHelp.pageSize": "10000",
            "pageHelp.pageNo": "1",
            "pageHelp.endPage": "1",
        }

    def fetch(self) -> OfficialProviderResult:
        session = _new_direct_session(self._session_factory)
        candidates: list[OfficialInstrumentCandidate] = []
        excluded_records: list[Mapping[str, Any]] = []
        part_counts: dict[str, int] = {}
        try:
            for stock_type, record_type, part_name in (
                ("1", "sse_main_a_share", "main"),
                ("8", "sse_star_market", "star"),
            ):
                try:
                    response = _request_with_transient_retry(
                        session,
                        "get",
                        SSE_URL,
                        params=self._params(stock_type),
                        headers=dict(_SSE_HEADERS),
                        timeout=self.timeout_seconds,
                        attempts=self.request_attempts,
                        sleeper=self._sleeper,
                    )
                except Exception as exc:
                    raise OfficialProviderResponseError(
                        f"{SSE_SOURCE_ID} {part_name} request failed"
                    ) from exc
                payload = _response_json(response, f"{SSE_SOURCE_ID}:{part_name}")
                rows = payload.get("result")
                if not isinstance(rows, list) or any(
                    not isinstance(row, Mapping) for row in rows
                ):
                    raise OfficialProviderResponseError(
                        f"{SSE_SOURCE_ID} {part_name} result must be a list of objects"
                    )
                part_counts[part_name] = len(rows)
                for source_row in rows:
                    raw = dict(source_row)
                    code = str(raw.get("A_STOCK_CODE", "")).strip()
                    if code.startswith("689"):
                        excluded_records.append(raw)
                        continue
                    candidates.append(
                        OfficialInstrumentCandidate(
                            raw_code=code,
                            instrument_name=str(raw.get("SEC_NAME_CN", "")).strip(),
                            exchange_id="SH",
                            canonical_exchange="SSE",
                            source_id=SSE_SOURCE_ID,
                            source_record_type=record_type,
                            listing_presence="present_in_current_sse_official_list",
                            source_security_type="ordinary_a_share",
                            classification_basis=(
                                f"SSE official STOCK_TYPE={stock_type}; "
                                "official 689 CDR code family excluded"
                            ),
                            security_type_uncertain=False,
                            raw_evidence=raw,
                        )
                    )
        finally:
            session.close()

        excluded_codes = tuple(
            sorted(str(row.get("A_STOCK_CODE", "")).strip() for row in excluded_records)
        )
        raw_count = sum(part_counts.values())
        return OfficialProviderResult(
            provider_id=SSE_SOURCE_ID,
            candidates=tuple(candidates),
            raw_count=raw_count,
            ordinary_stock_count=len(candidates),
            raw_part_counts=dict(part_counts),
            excluded_cdr_codes=excluded_codes,
            excluded_cdr_records=tuple(excluded_records),
        )


def _default_szse_xlsx_reader(content: bytes) -> Sequence[Mapping[str, Any]]:
    try:
        import pandas as pd

        frame = pd.read_excel(BytesIO(content))
    except Exception as exc:
        raise OfficialProviderResponseError(
            f"{SZSE_SOURCE_ID} returned malformed XLSX"
        ) from exc
    return tuple(frame.to_dict(orient="records"))


def _normalize_excel_code(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if value != value:
            return ""
        if value.is_integer():
            return str(int(value)).zfill(6)
    raw = str(value).strip()
    if not raw or raw.lower() == "nan":
        return ""
    if "." in raw:
        integer, fraction = raw.split(".", 1)
        if fraction and set(fraction) <= {"0"}:
            raw = integer
    return raw.zfill(6)


class SZSEOfficialInstrumentProvider:
    """SZSE official tab1 A-share list; tab2 B shares and tab3 CDR are unused."""

    def __init__(
        self,
        *,
        session_factory: Callable[[], Any] | None = None,
        xlsx_reader: Callable[[bytes], Sequence[Mapping[str, Any]]] | None = None,
        timeout_seconds: float = 20.0,
        request_attempts: int = 3,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if request_attempts <= 0:
            raise ValueError("request_attempts must be positive")
        self._session_factory = session_factory
        self._xlsx_reader = xlsx_reader or _default_szse_xlsx_reader
        self.timeout_seconds = timeout_seconds
        self.request_attempts = request_attempts
        self._sleeper = sleeper

    def fetch(self) -> OfficialProviderResult:
        session = _new_direct_session(self._session_factory)
        try:
            try:
                response = _request_with_transient_retry(
                    session,
                    "get",
                    SZSE_URL,
                    params={
                        "SHOWTYPE": "xlsx",
                        "CATALOGID": "1110",
                        "TABKEY": "tab1",
                        "random": "0.6935816432433362",
                    },
                    timeout=self.timeout_seconds,
                    attempts=self.request_attempts,
                    sleeper=self._sleeper,
                )
            except Exception as exc:
                raise OfficialProviderResponseError(
                    f"{SZSE_SOURCE_ID} request failed"
                ) from exc
            try:
                rows = tuple(self._xlsx_reader(bytes(response.content)))
            except OfficialProviderResponseError:
                raise
            except Exception as exc:
                raise OfficialProviderResponseError(
                    f"{SZSE_SOURCE_ID} returned malformed XLSX"
                ) from exc
        finally:
            session.close()

        if any(not isinstance(row, Mapping) for row in rows):
            raise OfficialProviderResponseError(
                f"{SZSE_SOURCE_ID} XLSX rows must be objects"
            )
        required = {"A股代码", "A股简称"}
        if rows and not required.issubset(rows[0]):
            raise OfficialProviderResponseError(
                f"{SZSE_SOURCE_ID} XLSX missing required A-share columns"
            )

        candidates = tuple(
            OfficialInstrumentCandidate(
                raw_code=_normalize_excel_code(row.get("A股代码")),
                instrument_name=(
                    "" if row.get("A股简称") is None else str(row.get("A股简称")).strip()
                ),
                exchange_id="SZ",
                canonical_exchange="SZSE",
                source_id=SZSE_SOURCE_ID,
                source_record_type=(
                    "szse_official_tab1_a_share:"
                    + str(row.get("板块", "unknown")).strip()
                ),
                listing_presence="present_in_current_szse_official_a_share_list",
                source_security_type="ordinary_a_share",
                classification_basis=(
                    "SZSE official CATALOGID=1110 TABKEY=tab1 A-share list; "
                    "tab2 B shares and tab3 CDR not requested"
                ),
                security_type_uncertain=False,
                raw_evidence=dict(row),
            )
            for row in rows
        )
        return OfficialProviderResult(
            provider_id=SZSE_SOURCE_ID,
            candidates=candidates,
            raw_count=len(rows),
            ordinary_stock_count=len(candidates),
            raw_part_counts={"a_share_tab1": len(rows)},
        )


def _parse_bse_page(response: Any, requested_page: int) -> Mapping[str, Any]:
    try:
        text = response.text
    except Exception as exc:
        raise OfficialProviderResponseError(
            f"{BSE_SOURCE_ID} page {requested_page} response text unavailable"
        ) from exc
    start = text.find("[")
    end = text.rfind("]")
    if start < 0 or end < start:
        raise OfficialProviderResponseError(
            f"{BSE_SOURCE_ID} page {requested_page} has no JSON array"
        )
    try:
        payload = json.loads(text[start : end + 1])
    except Exception as exc:
        raise OfficialProviderResponseError(
            f"{BSE_SOURCE_ID} page {requested_page} returned malformed JSON"
        ) from exc
    if (
        not isinstance(payload, list)
        or not payload
        or not isinstance(payload[0], Mapping)
    ):
        raise OfficialProviderResponseError(
            f"{BSE_SOURCE_ID} page {requested_page} payload is not a page object"
        )
    return payload[0]


class BSEOfficialInstrumentProvider:
    """Fully paginate the official BSE listed-company endpoint, fail closed."""

    def __init__(
        self,
        *,
        session_factory: Callable[[], Any] | None = None,
        timeout_seconds: float = 20.0,
        request_attempts: int = 3,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if request_attempts <= 0:
            raise ValueError("request_attempts must be positive")
        self._session_factory = session_factory
        self.timeout_seconds = timeout_seconds
        self.request_attempts = request_attempts
        self._sleeper = sleeper

    @staticmethod
    def _body(page: int) -> dict[str, str]:
        return {
            "page": str(page),
            "typejb": "T",
            "xxfcbj[]": "2",
            "xxzqdm": "",
            "sortfield": "xxzqdm",
            "sorttype": "asc",
        }

    def _request_page(self, session: Any, page: int) -> Mapping[str, Any]:
        try:
            response = _request_with_transient_retry(
                session,
                "post",
                BSE_URL,
                data=self._body(page),
                headers=dict(_BSE_HEADERS),
                timeout=self.timeout_seconds,
                attempts=self.request_attempts,
                sleeper=self._sleeper,
            )
        except Exception as exc:
            raise OfficialProviderResponseError(
                f"{BSE_SOURCE_ID} page {page} request failed"
            ) from exc
        return _parse_bse_page(response, page)

    @staticmethod
    def _metadata(page_object: Mapping[str, Any], page: int) -> tuple[int, int]:
        try:
            total_pages = int(page_object["totalPages"])
            total_elements = int(page_object["totalElements"])
        except (KeyError, TypeError, ValueError) as exc:
            raise OfficialProviderResponseError(
                f"{BSE_SOURCE_ID} page {page} missing valid pagination metadata"
            ) from exc
        if total_pages <= 0 or total_elements < 0:
            raise OfficialProviderResponseError(
                f"{BSE_SOURCE_ID} page {page} has invalid pagination metadata"
            )
        return total_pages, total_elements

    @staticmethod
    def _content(page_object: Mapping[str, Any], page: int) -> list[Mapping[str, Any]]:
        content = page_object.get("content")
        if not isinstance(content, list) or any(
            not isinstance(row, Mapping) for row in content
        ):
            raise OfficialProviderResponseError(
                f"{BSE_SOURCE_ID} page {page} content must be a list of objects"
            )
        return content

    def fetch(self) -> OfficialProviderResult:
        session = _new_direct_session(self._session_factory)
        rows: list[Mapping[str, Any]] = []
        try:
            first = self._request_page(session, 0)
            total_pages, expected_total = self._metadata(first, 0)
            rows.extend(self._content(first, 0))
            for page_number in range(1, total_pages):
                page = self._request_page(session, page_number)
                page_total_pages, page_total_elements = self._metadata(page, page_number)
                if (page_total_pages, page_total_elements) != (
                    total_pages,
                    expected_total,
                ):
                    raise OfficialProviderResponseError(
                        f"{BSE_SOURCE_ID} pagination metadata changed on page {page_number}"
                    )
                rows.extend(self._content(page, page_number))
        finally:
            session.close()

        if len(rows) != expected_total:
            raise OfficialProviderResponseError(
                f"{BSE_SOURCE_ID} total mismatch: "
                f"expected={expected_total}, fetched={len(rows)}"
            )
        raw_codes = [str(row.get("xxzqdm", "")).strip() for row in rows]
        seen_codes: set[str] = set()
        duplicate_codes: set[str] = set()
        for code in raw_codes:
            if code and code in seen_codes:
                duplicate_codes.add(code)
            seen_codes.add(code)
        if duplicate_codes:
            raise OfficialProviderResponseError(
                f"{BSE_SOURCE_ID} duplicate codes across pages: "
                + ", ".join(sorted(duplicate_codes)[:10])
            )

        candidates: list[OfficialInstrumentCandidate] = []
        for source_row in rows:
            raw = dict(source_row)
            code = str(raw.get("xxzqdm", "")).strip()
            security_uncertain = False
            try:
                validated = validate_stock_code(f"{_require_six_digit_code(code, 'xxzqdm')}.BJ")
                security_uncertain = not validated.endswith(".BJ")
            except (ValueError, ContractValidationError):
                security_uncertain = True
            candidates.append(
                OfficialInstrumentCandidate(
                    raw_code=code,
                    instrument_name=str(raw.get("xxzqjc", "")).strip(),
                    exchange_id="BJ",
                    canonical_exchange="BSE",
                    source_id=BSE_SOURCE_ID,
                    source_record_type="bse_official_listed_company",
                    listing_presence="present_in_current_bse_official_list",
                    source_security_type="ordinary_a_share",
                    classification_basis=(
                        "BSE official listed-company endpoint; typejb=T; "
                        "xxfcbj=2; valid frozen BJ stock code"
                    ),
                    security_type_uncertain=security_uncertain,
                    raw_evidence=raw,
                )
            )
        return OfficialProviderResult(
            provider_id=BSE_SOURCE_ID,
            candidates=tuple(candidates),
            raw_count=len(rows),
            ordinary_stock_count=len(candidates),
            raw_part_counts={"listed_company": len(rows)},
            expected_total=expected_total,
            fetched_total=len(rows),
            total_pages=total_pages,
        )


def _normalize_candidate(
    candidate: OfficialInstrumentCandidate,
) -> OfficialInstrumentRecord:
    code = _require_six_digit_code(candidate.raw_code, "official stock code")
    suffix_by_exchange = {"SH": "SH", "SZ": "SZ", "BJ": "BJ"}
    expected_canonical = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}
    suffix = suffix_by_exchange.get(candidate.exchange_id)
    if suffix is None:
        raise ValueError(f"unknown legacy ExchangeID: {candidate.exchange_id!r}")
    if candidate.canonical_exchange != expected_canonical[candidate.exchange_id]:
        raise ValueError(
            f"canonical exchange mismatch for {candidate.raw_code}: "
            f"{candidate.canonical_exchange!r}"
        )
    stock_code = validate_stock_code(f"{code}.{suffix}")
    name = str(candidate.instrument_name).strip()
    if not name:
        raise ValueError(f"empty InstrumentName for {stock_code}")
    if _is_question_mark_name(name):
        raise ValueError(f"question-mark-only InstrumentName for {stock_code}")
    if "\ufffd" in name:
        raise ValueError(f"replacement character in InstrumentName for {stock_code}")
    if candidate.security_type_uncertain:
        raise ValueError(f"security type remains uncertain for {stock_code}")
    return OfficialInstrumentRecord(
        stock_code=stock_code,
        instrument_name=name,
        exchange_id=candidate.exchange_id,
        canonical_exchange=candidate.canonical_exchange,
        source_id=candidate.source_id,
        source_record_type=candidate.source_record_type,
        listing_presence=candidate.listing_presence,
        source_security_type=candidate.source_security_type,
        classification_basis=candidate.classification_basis,
        provenance=dict(candidate.raw_evidence),
    )


def build_official_a_share_universe(
    sse: OfficialProviderResult,
    szse: OfficialProviderResult,
    bse: OfficialProviderResult,
    *,
    policy: OfficialUniversePolicy | None = None,
    schema_inspection: InstrumentSchemaInspection | None = None,
) -> OfficialAShareUniverse:
    """Build the authoritative union and fail its Gate without writing data."""

    expected_provider_ids = (SSE_SOURCE_ID, SZSE_SOURCE_ID, BSE_SOURCE_ID)
    actual_provider_ids = (sse.provider_id, szse.provider_id, bse.provider_id)
    if actual_provider_ids != expected_provider_ids:
        raise OfficialUniverseValidationError(
            f"official providers incomplete or out of order: {actual_provider_ids!r}"
        )

    selected_policy = policy or OfficialUniversePolicy()
    schema = schema_inspection or InstrumentSchemaInspection(inspected=False)
    candidates = (*sse.candidates, *szse.candidates, *bse.candidates)
    records: list[OfficialInstrumentRecord] = []
    by_code: dict[str, OfficialInstrumentRecord] = {}
    exchange_by_bare_code: dict[str, str] = {}
    duplicate_count = 0
    name_conflict_count = 0
    cross_exchange_conflict_count = 0
    invalid_code_count = 0
    empty_name_count = 0
    question_mark_count = 0
    replacement_count = 0
    uncertain_count = 0

    for candidate in candidates:
        raw_name = str(candidate.instrument_name).strip()
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
            code = _require_six_digit_code(candidate.raw_code, "official stock code")
            suffix = {"SH": "SH", "SZ": "SZ", "BJ": "BJ"}[candidate.exchange_id]
            validate_stock_code(f"{code}.{suffix}")
        except (KeyError, ValueError, ContractValidationError):
            invalid_code_count += 1
            if candidate.security_type_uncertain:
                uncertain_count += 1
            continue
        if candidate.security_type_uncertain:
            uncertain_count += 1
            continue
        try:
            record = _normalize_candidate(candidate)
        except (ValueError, ContractValidationError):
            invalid_code_count += 1
            continue

        existing = by_code.get(record.stock_code)
        if existing is not None:
            duplicate_count += 1
            if existing.instrument_name != record.instrument_name:
                name_conflict_count += 1
            continue
        bare_code = record.stock_code.split(".", 1)[0]
        prior_exchange = exchange_by_bare_code.get(bare_code)
        if prior_exchange is not None and prior_exchange != record.exchange_id:
            cross_exchange_conflict_count += 1
        else:
            exchange_by_bare_code[bare_code] = record.exchange_id
        by_code[record.stock_code] = record
        records.append(record)

    normalized_records = tuple(sorted(records, key=lambda item: item.stock_code))
    exchange_counts = {"SSE": 0, "SZSE": 0, "BSE": 0}
    for record in normalized_records:
        exchange_counts[record.canonical_exchange] += 1

    raw_total = sse.raw_count + szse.raw_count + bse.raw_count
    blockers: list[str] = []
    if len(normalized_records) < selected_policy.minimum_total_records:
        blockers.append(
            f"authoritative_unique_total below conservative minimum: "
            f"{len(normalized_records)} < {selected_policy.minimum_total_records}"
        )
    if set(sse.raw_part_counts) != {"main", "star"}:
        blockers.append("SSE provider must contain complete main and STAR partitions")
    elif sum(sse.raw_part_counts.values()) != sse.raw_count:
        blockers.append("SSE raw partition counts do not match raw_count")
    if sse.ordinary_stock_count != len(sse.candidates):
        blockers.append("SSE ordinary_stock_count does not match candidates")
    if sse.ordinary_stock_count + sse.excluded_cdr_count != sse.raw_count:
        blockers.append("SSE ordinary plus excluded CDR count does not match raw_count")
    if szse.raw_part_counts.get("a_share_tab1") != szse.raw_count:
        blockers.append("SZSE tab1 count does not match raw_count")
    if szse.ordinary_stock_count != len(szse.candidates):
        blockers.append("SZSE ordinary_stock_count does not match candidates")
    if bse.raw_part_counts.get("listed_company") != bse.raw_count:
        blockers.append("BSE listed-company count does not match raw_count")
    if bse.ordinary_stock_count != len(bse.candidates):
        blockers.append("BSE ordinary_stock_count does not match candidates")
    if bse.fetched_total != bse.raw_count:
        blockers.append("BSE fetched_total does not match raw_count")
    for exchange in ("SSE", "SZSE", "BSE"):
        if exchange_counts[exchange] <= 0:
            blockers.append(f"missing required official exchange: {exchange}")
    for label, count in (
        ("duplicate_code_count", duplicate_count),
        ("name_conflict_count", name_conflict_count),
        ("cross_exchange_conflict_count", cross_exchange_conflict_count),
        ("invalid_code_count", invalid_code_count),
        ("empty_name_count", empty_name_count),
        ("question_mark_name_count", question_mark_count),
        ("replacement_char_name_count", replacement_count),
        ("security_type_uncertain_count", uncertain_count),
    ):
        if count:
            blockers.append(f"{label} must be zero, actual={count}")
    if bse.expected_total is None or bse.fetched_total != bse.expected_total:
        blockers.append(
            f"BSE fetched total mismatch: expected={bse.expected_total}, "
            f"fetched={bse.fetched_total}"
        )
    if bse.total_pages is None or bse.total_pages <= 0:
        blockers.append("BSE total_pages must be positive")
    for code, expected_name in selected_policy.required_samples.items():
        record = by_code.get(validate_stock_code(code))
        if record is None:
            blockers.append(f"required official sample missing: {code}")
        elif record.instrument_name != expected_name:
            blockers.append(
                f"required official sample name mismatch: {code}; "
                f"expected={expected_name!r}, actual={record.instrument_name!r}"
            )

    status = "PASS" if not blockers else "FAIL"
    future_prerequisites = [DATABASE_APPLY_BLOCKER]
    if not schema.inspected:
        future_prerequisites.append("instrument_info_schema_not_inspected")
    elif not schema.compatible:
        future_prerequisites.append("instrument_info_schema_not_compatible")
    return OfficialAShareUniverse(
        provider_mode=PROVIDER_MODE,
        domestic_network_mode=DOMESTIC_NETWORK_MODE,
        inherited_env_proxy=INHERITED_ENV_PROXY,
        sse=sse,
        szse=szse,
        bse=bse,
        records=normalized_records,
        authoritative_raw_total=raw_total,
        authoritative_unique_total=len(normalized_records),
        exchange_counts=exchange_counts,
        duplicate_code_count=duplicate_count,
        name_conflict_count=name_conflict_count,
        cross_exchange_conflict_count=cross_exchange_conflict_count,
        invalid_code_count=invalid_code_count,
        empty_name_count=empty_name_count,
        question_mark_name_count=question_mark_count,
        replacement_char_name_count=replacement_count,
        security_type_uncertain_count=uncertain_count,
        universe_status=status,
        completeness_status=status,
        apply_allowed=False,
        future_apply_prerequisites=tuple(future_prerequisites),
        blockers=tuple(blockers),
        schema_inspection=schema,
    )


def fetch_official_a_share_universe(
    *,
    sse_provider: Any | None = None,
    szse_provider: Any | None = None,
    bse_provider: Any | None = None,
    policy: OfficialUniversePolicy | None = None,
    schema_inspection: InstrumentSchemaInspection | None = None,
) -> OfficialAShareUniverse:
    """Explicit network boundary; any provider failure aborts the whole union."""

    sse = (sse_provider or SSEOfficialInstrumentProvider()).fetch()
    szse = (szse_provider or SZSEOfficialInstrumentProvider()).fetch()
    bse = (bse_provider or BSEOfficialInstrumentProvider()).fetch()
    return build_official_a_share_universe(
        sse,
        szse,
        bse,
        policy=policy,
        schema_inspection=schema_inspection,
    )


def name_evidence(record: OfficialInstrumentRecord | None) -> tuple[str, str]:
    if record is None:
        return "<missing>", "<missing>"
    return ascii(record.instrument_name), record.instrument_name.encode("utf-8").hex()


__all__ = [
    "BSEOfficialInstrumentProvider",
    "BSE_SOURCE_ID",
    "BSE_URL",
    "DATABASE_APPLY_BLOCKER",
    "DOMESTIC_NETWORK_MODE",
    "INHERITED_ENV_PROXY",
    "OfficialAShareUniverse",
    "OfficialExchangeError",
    "OfficialInstrumentCandidate",
    "OfficialInstrumentRecord",
    "OfficialProviderResponseError",
    "OfficialProviderResult",
    "OfficialUniversePolicy",
    "OfficialUniverseValidationError",
    "PROVIDER_MODE",
    "SSEOfficialInstrumentProvider",
    "SSE_SOURCE_ID",
    "SSE_URL",
    "SZSEOfficialInstrumentProvider",
    "SZSE_SOURCE_ID",
    "SZSE_URL",
    "build_official_a_share_universe",
    "fetch_official_a_share_universe",
    "name_evidence",
]
