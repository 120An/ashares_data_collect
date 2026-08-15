"""NEWS_DATA_MODEL_V1_1 第一阶段纯数据契约测试。

使用标准库 unittest，pytest 也可直接发现这些用例。本文件不连接 PostgreSQL、
OpenSearch，不加载采集器，也不发起网络请求。
"""

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import unittest

from data_collect.news_model.contracts import (
    AcquisitionType,
    AuthorityLevel,
    AuthorityStatus,
    CompletenessStatus,
    ContentLicense,
    ContractValidationError,
    DerivedBy,
    DocumentStatus,
    DocumentType,
    Entity,
    EntityAlias,
    EntityAliasType,
    EntityStatus,
    EntityType,
    Exchange,
    HealthStatus,
    NewsDocument,
    NewsDocumentValidationMode,
    PaywallType,
    PublishTimePrecision,
    SourceCategory,
    SourceDirectness,
    SourceHealth,
    SourceRecord,
    make_stock_entity_id,
    validate_news_id,
    validate_stock_code,
)


_T0 = "2026-08-15T09:30:00+08:00"
_T1 = "2026-08-15T09:31:00+08:00"


def _source(**overrides) -> SourceRecord:
    values = {
        "source_id": "csrc",
        "source_revision": 1,
        "source_name": "中国证券监督管理委员会",
        "source_category": SourceCategory.REGULATOR,
        "acquisition_type": AcquisitionType.WEB,
        "directness": SourceDirectness.ORIGINAL,
        "default_channel": "policy",
        "country_region_codes": ("CN",),
        "languages": ("zh-CN",),
        "source_timezone": "Asia/Shanghai",
        "enabled": True,
        "authority_status": AuthorityStatus.UNRATED,
        "source_authority": None,
        "authority_version": "source_authority_v1",
        "authority_effective_from": _T0,
        "is_official": True,
        "content_license": ContentLicense.LINK_ONLY,
        "paywall_type": PaywallType.NONE,
        "created_at": _T0,
        "updated_at": _T0,
    }
    values.update(overrides)
    return SourceRecord(**values)


def _news(**overrides) -> NewsDocument:
    values = {
        "news_id": "cninfo-1219987654",
        "source_id": "cninfo",
        "source_authority_status": AuthorityStatus.UNRATED,
        "source_authority": None,
        "source_authority_version": "source_authority_v1",
        "document_type": DocumentType.ANNOUNCEMENT,
        "channel": "announcement",
        "language": "zh-CN",
        "publish_time": _T0,
        "publish_time_precision": PublishTimePrecision.SECOND,
        "publish_time_is_estimated": False,
        "collect_time": _T1,
        "document_status": DocumentStatus.ACTIVE,
        "created_at": _T1,
        "updated_at": _T1,
        "title": "上市公司公告",
        "raw_archive_uri": "nas://news/2026/08/15/cninfo.jsonl.gz#news_id=cninfo-1219987654",
        "stock_codes": ("600519.SH",),
    }
    values.update(overrides)
    return NewsDocument(**values)


def _stock_entity(stock_code: str, exchange: Exchange) -> Entity:
    return Entity(
        entity_id=make_stock_entity_id(stock_code),
        entity_revision=1,
        entity_type=EntityType.STOCK,
        canonical_name="示例证券",
        normalized_name="示例证券",
        short_name="示例证券",
        stock_code=stock_code,
        exchange=exchange,
        status=EntityStatus.ACTIVE,
        provenance_source_ids=("instrument_info",),
        confidence=1.0,
        created_at=_T0,
        updated_at=_T0,
    )


class NewsModelContractTests(unittest.TestCase):
    # ---------- Source authority 三态 ----------

    def test_authority_three_states_accept_valid_combinations(self):
        cases = (
            (AuthorityStatus.UNRATED, None, None, None),
            (AuthorityStatus.RATED, 0, AuthorityLevel.PRIMARY_OFFICIAL, "官方监管网站"),
            (AuthorityStatus.RATED, 100, AuthorityLevel.PRIMARY_OFFICIAL, "官方监管网站"),
            (AuthorityStatus.PROVISIONAL, None, None, "等待正式评审"),
            (AuthorityStatus.PROVISIONAL, 70, AuthorityLevel.HIGH, "临时评审结果"),
        )
        for status, score, level, basis in cases:
            with self.subTest(status=status, score=score):
                source = _source(
                    authority_status=status,
                    source_authority=score,
                    authority_level=level,
                    authority_basis=basis,
                )
                self.assertIs(source.authority_status, status)
                self.assertEqual(source.source_authority, score)

    def test_unrated_with_numeric_authority_fails(self):
        with self.assertRaisesRegex(ContractValidationError, "unrated"):
            _source(authority_status=AuthorityStatus.UNRATED, source_authority=0)

    def test_rated_without_authority_fails(self):
        with self.assertRaisesRegex(ContractValidationError, "rated"):
            _source(
                authority_status=AuthorityStatus.RATED,
                source_authority=None,
                authority_level=AuthorityLevel.HIGH,
                authority_basis="已评审",
            )

    def test_authority_out_of_range_fails(self):
        for score in (-1, 101):
            with self.subTest(score=score):
                with self.assertRaisesRegex(ContractValidationError, "0～100"):
                    _source(
                        authority_status=AuthorityStatus.RATED,
                        source_authority=score,
                        authority_level=AuthorityLevel.HIGH,
                        authority_basis="已评审",
                    )

    def test_news_document_authority_snapshot_obeys_same_null_rule(self):
        with self.assertRaisesRegex(ContractValidationError, "unrated"):
            _news(source_authority_status="unrated", source_authority=0)
        with self.assertRaisesRegex(ContractValidationError, "rated"):
            _news(source_authority_status="rated", source_authority=None)

    # ---------- NewsDocument 时间与 validation mode ----------

    def test_timezone_aware_publish_time_iso_is_valid_and_normalized(self):
        news = _news(
            publish_time="2026-08-15T01:30:00Z",
            collect_time="2026-08-15T09:31:00+08:00",
        )
        self.assertIsInstance(news.publish_time, datetime)
        self.assertEqual(news.publish_time.utcoffset(), timedelta(0))
        self.assertEqual(news.collect_time.utcoffset(), timedelta(hours=8))

    def test_timezone_aware_datetime_object_is_valid(self):
        published = datetime(
            2026, 8, 15, 9, 30, tzinfo=timezone(timedelta(hours=8))
        )
        news = _news(publish_time=published)
        self.assertIs(news.publish_time, published)

    def test_naive_canonical_time_fails(self):
        with self.assertRaisesRegex(
            ContractValidationError, "publish_time 必须带时区"
        ):
            _news(publish_time="2026-08-15 09:30:00")

    def test_legacy_time_fields_remain_strings_and_are_not_redefined(self):
        news = _news(
            pub_time="2026-08-15 09:30:00",
            fetch_time="2026-08-15 09:31:00",
        )
        self.assertEqual(news.pub_time, "2026-08-15 09:30:00")
        self.assertEqual(news.fetch_time, "2026-08-15 09:31:00")
        self.assertIsInstance(news.pub_time, str)
        self.assertIsInstance(news.publish_time, datetime)

    def test_phase1_compatibility_document_can_omit_raw_archive_uri(self):
        news = _news(
            raw_archive_uri=None,
            validation_mode=NewsDocumentValidationMode.PHASE1_COMPAT,
        )
        self.assertIsNone(news.raw_archive_uri)

    def test_final_news_document_requires_raw_archive_uri(self):
        with self.assertRaisesRegex(ContractValidationError, "raw_archive_uri"):
            _news(
                raw_archive_uri=None,
                validation_mode=NewsDocumentValidationMode.FINAL,
            )

    def test_phase1_compatibility_does_not_fabricate_archive_uri(self):
        news = _news(raw_archive_uri=None, validation_mode="phase1_compat")
        self.assertIsNone(news.raw_archive_uri)

    # ---------- A 股证券身份 ----------

    def test_stock_entity_supports_sh_sz_bj(self):
        cases = (
            ("600519.SH", Exchange.SSE),
            ("000001.SZ", Exchange.SZSE),
            ("920001.BJ", Exchange.BSE),
        )
        for stock_code, exchange in cases:
            with self.subTest(stock_code=stock_code):
                entity = _stock_entity(stock_code, exchange)
                self.assertEqual(entity.stock_code, stock_code)
                self.assertIs(entity.exchange, exchange)

    def test_stock_code_normalizes_suffix_case(self):
        self.assertEqual(validate_stock_code("600519.sh"), "600519.SH")

    def test_stock_code_and_exchange_must_agree(self):
        with self.assertRaisesRegex(ContractValidationError, "不一致"):
            _stock_entity("600519.SH", Exchange.SZSE)

    # ---------- EntityAlias 时序 ----------

    def test_entity_alias_valid_from_after_valid_to_fails(self):
        with self.assertRaisesRegex(ContractValidationError, "valid_from"):
            EntityAlias(
                entity_alias_id="ealias_600519_former_001",
                entity_id="ent_stock_600519_sh",
                alias="贵州茅台股份",
                normalized_alias="贵州茅台股份",
                alias_type=EntityAliasType.FORMER_NAME,
                language="zh-CN",
                valid_from="2021-01-01T00:00:00+08:00",
                valid_to="2020-12-31T23:59:59+08:00",
                provenance_source_ids=("instrument_info",),
                confidence=0.9,
                derived_by=DerivedBy.MASTER_DATA,
                revision=1,
                is_current=False,
                manual_lock=False,
                created_at=_T0,
                updated_at=_T0,
            )

    def test_entity_alias_allows_unknown_validity_without_fabrication(self):
        alias = EntityAlias(
            entity_alias_id="ealias_600519_current_001",
            entity_id="ent_stock_600519_sh",
            alias="贵州茅台",
            normalized_alias="贵州茅台",
            alias_type=EntityAliasType.STOCK_SHORT_NAME,
            language="zh-CN",
            provenance_source_ids=("instrument_info",),
            confidence=1.0,
            derived_by=DerivedBy.MASTER_DATA,
            revision=1,
            is_current=True,
            manual_lock=False,
            created_at=_T0,
            updated_at=_T0,
        )
        self.assertIsNone(alias.valid_from)
        self.assertIsNone(alias.valid_to)

    # ---------- SourceHealth 与稳定 ID 基本不变式 ----------

    def test_source_health_contract_is_independent_and_valid(self):
        health = SourceHealth(
            source_health_id="shealth_csrc_20260815T093100Z",
            source_id="csrc",
            observed_at=_T1,
            window_start=_T0,
            window_end=_T1,
            health_status=HealthStatus.HEALTHY,
            last_success_at=_T1,
            last_attempt_at=_T0,
            consecutive_failures=0,
            latency_ms=842,
            last_item_publish_time=_T0,
            data_delay_seconds=60,
            attempt_count=1,
            success_count=1,
            collected_item_count=10,
            new_item_count=8,
            empty_success_count=0,
            parse_failure_count=0,
            completeness_status=CompletenessStatus.OK,
            health_policy_version="source_health_policy_v1",
            is_current=True,
            created_at=_T1,
        )
        self.assertEqual(health.source_id, "csrc")
        self.assertEqual(health.latency_ms, 842)

    def test_news_id_cannot_use_event_namespace(self):
        with self.assertRaisesRegex(ContractValidationError, "event_id"):
            validate_news_id("evt_01KABC")
        with self.assertRaisesRegex(ContractValidationError, "event_id"):
            _news(news_id="event_01KABC")

    def test_news_id_accepts_existing_rss_atom_guid_shape(self):
        guid_id = (
            "govcn_policy-https://www.gov.cn/zhengce/content/2026/example?id=1"
        )
        self.assertEqual(validate_news_id(guid_id), guid_id)

    def test_stock_entity_id_is_deterministic_and_canonical(self):
        self.assertEqual(
            make_stock_entity_id("600519.SH"), "ent_stock_600519_sh"
        )
        self.assertEqual(
            make_stock_entity_id("600519.sh"), "ent_stock_600519_sh"
        )

    def test_identity_fields_are_immutable_after_validation(self):
        news = _news()
        with self.assertRaises(FrozenInstanceError):
            news.news_id = "another-id"


if __name__ == "__main__":
    unittest.main()
