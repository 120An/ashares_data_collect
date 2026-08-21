# News Engine Phase 1 Final Acceptance Report

## 1. Executive Summary

**Final decision: `PASS_PHASE1`**

Phase 1 的新闻数据基础层、公开 A 股主数据基线、Entity/EntityAlias foundation、OpenSearch 兼容 mapping、受控 enrichment、归档回放和 SourceHealth Windows spawn 传播均已完成离线回归与真实环境验收。

验收日期为 2026-08-22（Asia/Shanghai）。所有需要项目配置的外部验收及全部 pytest 命令均在以下环境变量存在时运行：

```powershell
$env:DATA_COLLECT_CONFIG='D:\AIStockSystem\runtime\ashares_config.local.yaml'
```

本轮只执行只读 PostgreSQL/OpenSearch/NAS 检查和本地测试；没有执行 bootstrap apply、Entity apply、DDL、DML、OpenSearch mapping PUT、新闻重抓、全量 embedding、DingTalk 通知或 Git commit。

## 2. Phase 1 Boundary

Phase 1 最终范围包括：

- NewsDocument compatibility contracts and dual-read/dual-write boundary
- Source Catalog、SourceHealth shadow observation
- Stock Entity、EntityAlias 及 PostgreSQL revision foundation
- 公开三交易所 A 股 master-data bootstrap
- 现有 `news-{year}` additive schema readiness
- ArchiveReceipt 和受控 enrichment metadata
- 现有搜索、pending snapshot、create-only、archive/replay 兼容性

明确未进入：Event、EventDocumentMembership、Importance/S-A-B-C、Sentiment、Impact、StockRelation、IndustryRelation、产业链图谱、海外事件映射或 UI。没有删除 legacy fields、切换 search alias、回填历史正文或扩建 sector master data。

## 3. Step 1–8 Result

| Step | Scope | Status | Final evidence |
|---|---|---|---|
| 1 | Contracts | PASS | 时区时间、稳定 ID、Source/SourceHealth/Entity/EntityAlias 条件校验通过 |
| 2 | Compatibility | PASS | legacy/canonical 双读、diagnostics、非破坏投影通过 |
| 3 | Source Catalog | PASS | 51 production sources 全覆盖；47 YAML + 4 code-defined；0 unresolved/conflict |
| 4 | Entity shadow | PASS | Stock Entity/Alias 稳定 ID、歧义、历史边界、sector crosswalk 通过 |
| 5 | OpenSearch schema | PASS | 三个真实 yearly index 均 Compatible=true、Additive=0、Incompatible=0 |
| 6 | News dual-write | PASS | 默认 legacy；shadow/dual 显式开启；dual mismatch fail closed |
| 7 | Archive/enrichment | PASS | ArchiveReceipt、whitelist、mapping readiness gate、embedding version 原子写入通过 |
| 8 | SourceHealth | PASS | fail-open、retry correlation、Windows spawn JSONL sink 传播已真实验证 |

## 4. Official Master Data Architecture

Master universe 使用 SSE、SZSE、BSE 三个交易所官方上市名单的 authoritative union；国内 provider 使用 request-local DIRECT 网络边界，不继承 ambient HTTP/HTTPS proxy。Eastmoney spot provider 保留为 optional cross-check，不是 bootstrap 的唯一或权威来源。

```text
SSE official + SZSE official + BSE official
                  |
                  v
       authoritative A-share union
                  |
        completeness + Unicode gates
                  |
       dual-hash validated snapshot
                  |
     deterministic non-delete write plan
                  |
       explicit transactional apply
```

Snapshot 使用两个独立哈希：

- `content_sha256`：稳定 universe 内容身份
- `snapshot_sha256`：绑定 created/fetched 时间、metadata 和具体 capture envelope

| Field | Value |
|---|---|
| Snapshot status | VALID |
| Source total | 5,548 |
| SSE | 2,313 |
| SZSE | 2,897 |
| BSE | 338 |
| All anomaly counts | 0 |
| `content_sha256` | `0ad02db07ac7ed2073a0a6d107632ddf336e7268024a946236d2e42de601dad6` |
| `snapshot_sha256` | `d34b0658264271cc26c163787af128d32e3ef068c2879f4058c3d5fff2f9533e` |

## 5. Instrument Bootstrap Evidence

真实 controlled apply 结果：

| Signal | Result |
|---|---:|
| Source total | 5,548 |
| Inserted | 5,546 |
| Repaired corrupted names | 2 |
| Deleted | 0 |
| Post total | 5,548 |
| Missing | 0 |
| Mismatches | 0 |
| Transaction | committed |

最终只读 dry-run：

| Signal | Result |
|---|---:|
| Existing total | 5,548 |
| Would insert | 0 |
| Would update | 0 |
| Would repair | 0 |
| Would change exchange | 0 |
| Would remain unchanged | 5,548 |
| Would changelog | 0 |
| Existing outside official universe | 0 |
| Would delete | 0 |
| Plan status | PASS |

Final dry-run hashes：

- `database_baseline_sha256=76a68dccc23e81fc70337262604c8de76761425b167f57b9511d1afd64516f05`
- `plan_sha256=fbe3ffcd8d62e67f0388c0f13cbcff67721e173174e4e564dff8ebde209cec8c`

## 6. PostgreSQL and Entity Evidence

全部查询使用 `SET TRANSACTION READ ONLY`，完成后 rollback/close。

### Instrument master

| Check | Result |
|---|---:|
| `instrument_info COUNT(*)` | 5,548 |
| Distinct `stock_code` | 5,548 |
| Empty names | 0 |
| Question-mark-only names | 0 |
| U+FFFD names | 0 |

| Stock | Name | UTF-8 hex |
|---|---|---|
| `000001.SZ` | 平安银行 | `e5b9b3e5ae89e993b6e8a18c` |
| `600519.SH` | 贵州茅台 | `e8b4b5e5b79ee88c85e58fb0` |

### Entity foundation

| Check | Result |
|---|---:|
| Foundation ready | true |
| Total/latest Entity revisions | 5,548 / 5,548 |
| Latest distinct Entity IDs | 5,548 |
| Duplicate latest Entity IDs | 0 |
| Latest Entity types | `stock: 5,548` only |
| Total/latest Alias revisions | 16,645 / 16,645 |
| Latest distinct Alias IDs | 16,645 |
| Duplicate latest Alias IDs | 0 |
| Current aliases | 16,644 |
| Historical aliases | 1 |
| Alias projection mismatches | 0 |

Alias type distribution is `ticker=11,096`, `stock_short_name=5,548`, `former_name=1`. This exactly explains the live total of 16,645. An earlier operator transcript recorded 16,465; the database count, identical creation timestamp, deterministic three-current-alias rule, one evidenced historical alias, and zero-change dry-run establish 16,645 as the authoritative value. The earlier figure is treated as a digit transposition, not a data divergence.

Stable ID samples：

- `600519.SH -> ent_stock_600519_sh`
- `000001.SZ -> ent_stock_000001_sz`

Final Entity dry-run：

```text
foundation ready: true
source stock entities: 5548
would insert entity revisions: 0
would insert alias revisions: 0
manual-locked aliases skipped: 0
apply allowed: true
mode: dry-run
```

## 7. OpenSearch and Embedding Evidence

| Index | Documents | Compatible | Additive | Incompatible | Vector dimension |
|---|---:|---:|---:|---:|---:|
| `news-2024` | 5 | true | 0 | 0 | 1,024 |
| `news-2025` | 241 | true | 0 | 0 | 1,024 |
| `news-2026` | 264 | true | 0 | 0 | 1,024 |
| **Total** | **510** |  |  |  |  |

All required Phase 1 fields have their explicit target types: keyword (`news_id`, `schema_version`, `source_id`, `stock_codes`, `publish_time_precision`, `embedding_model_version`, `raw_archive_uri`), date (`publish_time`, `collect_time`, `created_at`, `updated_at`), and boolean (`publish_time_is_estimated`, `body_truncated`). No mapping PUT was executed。

| Embedding signal | Result |
|---|---:|
| `pending` | 0 |
| `done` | 510 |
| `content_vec` exists | 510 |
| `embedding_model_version` exists | 510 |
| `updated_at` exists | 510 |
| Model | `bge-m3` on 510 documents |
| Dimension | 1,024 |

The 510 live documents remain legacy-only because the configured compatibility mode is unset and therefore defaults to `legacy`. Canonical field presence is zero; all 510 documents are readable by the compatibility reader and produce zero mismatch diagnostics. Shadow/dual behavior remains an explicit rollout choice.

Live BM25 query `国务院` returned `total=437`. The response contract remains exactly `{total, hits}`; each hit retains `title/source/channel/pub_time/url/score/highlight`. No embedding model was loaded for BM25.

## 8. NAS and Archive Evidence

Configured archive root `\\localhost\AIStockNewsNAS\news` is accessible and is a directory. Existing gray archives were read only：

| Source | Gzip files | Replay records | Unique IDs | Bytes unchanged after read |
|---|---:|---:|---:|---|
| `govcn_policy` | 45 | 60 | 60 | true |
| `govcn_gwy` | 41 | 50 | 50 | true |
| `em_cjzc` | 400 | 400 | 400 | true |

Every gzip file retained the same SHA-256 before and after replay. Current spool status is 0 pending files / 0 bytes, with no `.nas_alert_ts` marker. Previously completed live spool fallback/flush/replay evidence is retained; this final acceptance did not manufacture a NAS failure or invoke `flush_spool()`.

ArchiveReceipt construction, NAS/spool receipt provenance, replay, gzip append, and failure behavior are covered by the passing Phase 1 core tests.

## 9. SourceHealth and Windows Spawn Evidence

Evidence file: `D:\AIStockSystem\runtime\pipeline_gray_collect_20260821_210408.jsonl`.

It contains exactly five observations, all sharing：

- `job_run_id=jrun_3d56b04f5f7aa916ffd4b6e634e1231c2643b4c9fd317294b91ed5a52c418e08`
- `attempt_no=1`
- `retry_scheduled=false`
- `incomplete=false`

| Observation | Outcome | Collected | Latency |
|---|---|---:|---:|
| Task | STARTED | n/a | n/a |
| `govcn_policy` COLLECT | SUCCESS | 60 | 428 ms |
| `govcn_gwy` COLLECT | SUCCESS | 50 | 766 ms |
| `em_cjzc` COLLECT | SUCCESS | 400 | 6,602 ms |
| Task | SUCCESS | n/a | 9,909 ms |

`new_item_count` is intentionally null on source observations rather than inventing per-source OpenSearch outcomes. Existing task/source evidence, archive state, and index state are kept separate：

| Signal | Value | Evidence boundary |
|---|---:|---|
| Per-source collected | 60 / 50 / 400 | SourceHealth JSONL |
| Archive records | 510 unique | NAS gzip replay |
| OpenSearch documents | 510 | OpenSearch count |
| Duplicate | 0 in original gray summary | Gray operator evidence |
| Pending / done | 0 / 510 | OpenSearch count |
| Retry count | 0 | attempt/retry fields |
| Pipeline duration | 9.909 s | task observation |

The same-run task/source correlation proves the Windows `ProcessPoolExecutor` spawn sink propagation fix is active. No DingTalk message was sent during final acceptance.

## 10. Final Gate Matrix

| # | Gate | Status | Evidence |
|---:|---|---|---|
| 1 | Every production source resolves in Source Catalog | PASS | 51/51, 0 unresolved/conflicts |
| 2 | Canonical/legacy pairs cannot diverge on new dual writes | PASS | Dual-write tests fail closed; live legacy documents have 0 diagnostics |
| 3 | Default compatibility mode remains legacy | PASS | Real config unset; code default and regression tests |
| 4 | Shadow/dual require explicit enablement | PASS | Write-mode tests |
| 5 | Create-only and 409 duplicate semantics unchanged | PASS | OpenSearch utility regression |
| 6 | `news-{year}` routing unchanged | PASS | Cross-year routing tests and live indices |
| 7 | Existing mappings are additive-only | PASS | Three live diffs: 0 additive / 0 incompatible |
| 8 | `content_vec` remains 1,024 dimensions | PASS | Live mappings and 510 vectors |
| 9 | Default enrichment mode remains legacy | PASS | Real config unset and gate tests |
| 10 | Only Phase 1 enrichment writes version/updated time | PASS | Atomic enrichment tests and live metadata |
| 11 | `raw_archive_uri` requires a real ArchiveReceipt | PASS | Receipt validation tests |
| 12 | Archive replay and old envelopes remain unchanged | PASS | Replay SHA evidence and tests |
| 13 | Entity/Alias IDs are deterministic | PASS | Live samples and idempotent dry-run |
| 14 | No Company/Industry/Relation facts are fabricated | PASS | Latest DB entities are stock-only |
| 15 | SourceHealth sink failure remains fail-open | PASS | SourceHealth tests |
| 16 | Verify does not pollute collect last-success | PASS | SourceHealth tests |
| 17 | Task health does not fabricate per-source failure | PASS | Pipeline tests |
| 18 | Windows task/source observations share job_run_id | PASS | Five-row live JSONL |
| 19 | Retry retains job_run_id and increments attempt | PASS | Pipeline retry tests |
| 20 | Non-news_policy jobs receive no internal health kwargs | PASS | Pipeline tests |
| 21 | BM25/filter/hybrid/RRF/collapse/pagination contract unchanged | PASS | Core tests and live BM25 |
| 22 | Pending uses bounded snapshot and explicit index/id writeback | PASS | Embed/body tests |
| 23 | No `update_by_query` write path | PASS | Production audit and regressions |
| 24 | New pending cannot be marked done by an older snapshot | PASS | Embed snapshot tests |
| 25 | No Phase 2 objects/features introduced | PASS | Boundary audit |
| 26 | Official master universe is complete and Unicode-clean | PASS | 5,548 union, three exchanges, 0 anomalies |
| 27 | Instrument bootstrap is non-delete and idempotent | PASS | Post-write verification + zero-change dry-run |
| 28 | Entity foundation is ready and idempotent | PASS | DB structure/counts + zero-change dry-run |

## 11. Regression Results

All commands used the real `DATA_COLLECT_CONFIG` value shown in the Executive Summary.

| Suite | Passed | Failed | Deselected | Subtests | Result |
|---|---:|---:|---:|---:|---|
| Phase 1 core | 591 | 0 | 3 | 38 | PASS |
| `test_pipeline.py` | 21 | 0 | 0 | 0 | PASS, including real Windows subprocess tests |
| Safe full suite excluding `test_tick.py` | 885 | 3 | 5 | 38 | Three explained non-Phase1 failures |

Five integration tests were intentionally deselected by `pytest.ini`：

- `tests/test_cninfo.py::test_fetch_announcements_live`
- `tests/test_embedding.py::test_integration_real_model_roundtrip`
- `tests/test_fulltext.py::test_fetch_article_live`
- `tests/test_news_search.py::test_integration_bm25_smoke`
- `tests/test_opensearch_utils.py::test_integration_ensure_create_idempotent`

## 12. Known Non-Phase1 Failures and Exclusions

These failures are fully classified and do not affect a Phase 1 gate：

| Test | Classification | Exact cause |
|---|---|---|
| `tests/test_export.py::test_export_disabled_by_default` | Environment/config, legacy unrelated | Runtime validation config intentionally has no `export` section; legacy `get_export_config()` indexes the missing key and raises `KeyError('export')` |
| `tests/test_notify.py::test_ensure_ding_prefix_adds_prefix` | Known legacy unrelated | Runtime validation config has no `dingtalk` section, so the legacy prefix function raises `KeyError('dingtalk')` before its historical assertion |
| `tests/test_notify.py::test_ensure_ding_prefix_keeps_existing` | Known legacy unrelated | Same missing legacy `dingtalk` configuration boundary |

`tests/test_tick.py` was not included in the safe full suite. Explicit collection produces `ModuleNotFoundError: No module named 'pyarrow'` from `data_collect.jobs.a_share_tick`; no tests are collected. PyArrow/QMT/tick storage are outside the News Engine Phase 1 scope and no dependency was installed.

## 13. Deferred Sector Master Data

`sector_stock` currently contains 2 rows for 2 distinct stocks. Phase 1 deliberately treats it only as a read-only optional crosswalk and does not create Industry Entity or IndustryRelation. Expanding GICS/同花顺行业/概念/主题/风格 coverage is deferred and is not a Phase 1 blocker.

## 14. Final Decision

**`PASS_PHASE1`**

All News Engine Phase 1 hard gates pass. PostgreSQL master data and Entity foundations are deployed and idempotent; OpenSearch mappings and vectors are ready; archive and SourceHealth live evidence are coherent; search and pipeline contracts regress cleanly. The three safe-full failures and the PyArrow collection blocker are explicitly classified outside Phase 1.

## 15. Phase 2 Entry Preconditions

Phase 2 must not start implicitly. Before any Event, scoring, sentiment, impact or relation work：

1. Human-review this final report and the complete Git working tree, including every untracked Phase 1 file.
2. Create an explicit reviewed Git checkpoint; this report does not authorize a commit.
3. Freeze the deployed Phase 1 schema/migration hashes and keep legacy read/write rollback paths.
4. Decide sector master-data expansion separately; do not smuggle it into event or relation work.
5. Define new Phase 2 acceptance gates before enabling any Event/Importance/Sentiment/Impact/Relation write path.
