# Phase 1 Acceptance

## Environment

| Item | Result |
|---|---|
| Date | 2026-08-20 (Asia/Shanghai) |
| Python | 3.13.2, CPython, 64-bit |
| OS | Windows 10 10.0.19045 |
| External production access | No PostgreSQL, OpenSearch, NAS, crawler, production collection, migration, index mutation, database mutation, or DingTalk send was performed. All archive writes used temporary directories. |
| Network note | No successful external response was used. A preliminary custom-runner pass failed to retain an `integration` marker and entered `test_fetch_article_live`; the restricted sandbox returned no usable data. That result is excluded. The authoritative marker-aware run skipped the test. |
| Dependencies not installed | `pytest`, PyYAML (`yaml`), `opensearchpy`, `psycopg2`, `opencc`, `feedparser`, `sentence_transformers`, and `xtquant` |
| Local configuration limitation | `config.yaml` is absent. Tests requiring configuration were run only with explicit in-memory substitutes. |
| Test execution method | Pure `unittest` suites ran directly. Pytest-style Phase 1 tests ran through a local fixture/monkeypatch runner with external clients blocked or replaced in memory. This is evidence for the tested logic, but is not represented as a successful full pytest session. |

The repository-wide command `python -B -m pytest -q` could not start because the current interpreter has no `pytest`. No dependency was installed.

## Gate Matrix

| Gate | Status | Evidence | Actual result |
|---|---|---|---|
| Contracts | PASS | `test_news_model_contracts.py`: deterministic IDs, timezone-aware times, source/stock validation, enum/schema stability, Source/SourceHealth separation | 22 passed |
| Compat old-read | PASS | `test_news_model_compat.py`; historical archive acceptance fixture | 19 passed; legacy-only fields canonicalized without input mutation; identity conflicts remain hard errors |
| Dual-write dry-run | PASS | `test_opensearch_utils.py`; `test_news_phase1_acceptance.py` | Dual action retained legacy fields, added canonical fields, kept `_op_type=create`, and rejected semantic pair conflicts |
| Legacy default | PASS | `test_legacy_write_mode_is_the_unchanged_default`; enrichment legacy-mode tests | Default compatibility and enrichment modes remain `legacy`; routing/create-only/409 behavior unchanged |
| Schema additivity | PASS | `test_news_opensearch_schema.py`; future-template tests in `test_opensearch_utils.py` | 22 schema tests passed; additive-only diff, analyzer/BM25/vector protection, explicit Step 7 fields |
| Archive replay | PASS | `test_news_archive.py`; cross-component acceptance test | 32 archive tests plus 1 acceptance test passed; gzip bytes/envelope/identity unchanged |
| Archive receipt | PASS | NAS-success, spool-fallback, and double-failure receipt tests using temporary paths | Verified receipts exist only after an actual local append; no theoretical URI fabricated |
| Enrichment | PASS | `test_opensearch_utils.py`, `test_news_embed.py`, `test_bodyfill.py`, fulltext/PDF job tests | Legacy metadata behavior preserved; Phase 1 mode enforces model/version atomicity, timestamp boundary, verified receipt, and explicit `_index/_id` |
| Source Catalog | PASS | `test_news_source_catalog.py`; `manage_news_foundation.py validate-sources` | 20 passed; 51 catalog sources, 47 YAML sources, 4 code-defined sources, 0 unresolved/conflicts, 51 unrated |
| Entity determinism | PASS | `test_news_entity_catalog.py` | 31 passed; stable stock/alias IDs, order-independent facts, ambiguity retained, no fabricated Company/IndustryRelation |
| SourceHealth | PASS | `test_news_source_health.py`, `test_news_policy.py`, in-process pipeline tests | 27 + 16 passed; cross-window carry rules, fail-open sink, shared job run, retry attempts, archive/index count separation; health remains `UNKNOWN` |
| Search regression | PASS | `test_news_search.py` with deterministic OpenSearch responses | 28 passed, 1 integration skipped; BM25, stock/date filters, hybrid/RRF, collapse and return shape unchanged |
| Pending/embed | PASS | `test_news_embed.py` | 19 passed; bounded snapshot, explicit physical identity, atomic vector state, newly inserted pending isolation, backlog behavior |
| Pipeline | PENDING_ENVIRONMENT | 13 in-process/mock tests passed; real Windows child processes require imports/config unavailable in this environment | 4 real subprocess tests not run; therefore hard-kill behavior is not marked PASS for this environment |

Core Gate counts: **13 PASS, 0 FAIL, 1 PENDING_ENVIRONMENT**.

### External validation matrix

| External condition | Status | Reason |
|---|---|---|
| Existing OpenSearch mappings are ready for Phase 1 fields | PENDING_EXTERNAL | No real `_mapping` read or additive PUT was performed |
| Real NAS receipt/spool/flush behavior | PENDING_EXTERNAL | Only temporary-directory behavior was verified |
| PostgreSQL Entity/EntityAlias shadow inspection | PENDING_EXTERNAL | No PostgreSQL connection or SQL execution was performed |
| Live source availability and source delay | PENDING_EXTERNAL | No network source was collected |
| Production gray comparison | PENDING_EXTERNAL | No live gray task was executed |

External pending counts: **5 PENDING_EXTERNAL**.

## Regression Summary

Counts below are by `test_*` definition; pytest parametrization expansion is not estimated because pytest could not collect the suite.

| Scope | Passed | Failed | Skipped | Environment-blocked | Not run |
|---|---:|---:|---:|---:|---:|
| Phase 1 core group | 299 | 0 | 2 integration | 4 subprocess | 0 |
| Additional archive/verify/retry/timeout/notify/pending/fulltext/PDF group | 176 | 0 | 1 integration | 0 | 0 |
| Safe executed total | **475** | **0** | **3** | **4** | 0 |
| Remaining repository definitions | 0 | 0 | 0 | 0 | **220** |
| Repository definition inventory | **475** | **0** | **3** | **4** | **220** |

The repository contains 702 test definitions across 60 test files. The remaining 220 definitions were not run because the complete pytest session could not start and several non-Phase-1 suites require unavailable optional/runtime dependencies. They are not reported as PASS.

Integration tests skipped:

- `tests/test_opensearch_utils.py::test_integration_ensure_create_idempotent`
- `tests/test_news_search.py::test_integration_bm25_smoke`
- `tests/test_fulltext.py::test_fetch_article_live`

Environment-blocked real subprocess tests:

- `tests/test_pipeline.py::test_execute_in_subprocess_success`
- `tests/test_pipeline.py::test_execute_in_subprocess_timeout_raises`
- `tests/test_pipeline.py::test_execute_in_subprocess_no_timeout`
- `tests/test_pipeline.py::test_execute_in_subprocess_propagates_exception`

## Acceptance Details

### Legacy read and dual-write

The synthetic legacy document containing only `_id`, `pub_time`, `fetch_time`, `source`, and `stocks` produced the expected canonical identity, timezone-aware Shanghai times, `source_id`, and `stock_codes`. The input remained unchanged. A dual-write dry-run retained every legacy field, added the frozen canonical projection, preserved `news-{year}` routing and `create` semantics, and contained no mismatches.

### Historical archive replay

A local legacy JSONL gzip fixture was replayed through `news_archive.replay`, read through `compat`, and passed to the dual action builder. The archive bytes and envelope remained byte/value identical; `_id/news_id` remained stable. No `body`, `raw_archive_uri`, Event, Importance, Sentiment, Impact, StockRelation, or IndustryRelation field was generated.

### Entity determinism

Repeated construction and order-change fixtures retained the frozen entity identity (`600519.SH` maps deterministically to `ent_stock_600519_sh`) and stable alias occurrence IDs. Current name/ticker/bare-code aliases were retained, historical aliases required changelog evidence, ambiguous aliases were not resolved by first match, and Company/IndustryRelation objects were not fabricated.

### SourceHealth

Empty success remains success; verify observations do not update collect `last_success_at`; sink failure remains fail-open; retry and notify counts remain unchanged in mock/in-process execution; task failure does not invent per-source failures; timeouts remain task-level incomplete/unknown; previous snapshots carry only latest-state fields; window counters do not carry. Pipeline and `news_policy` share one job run ID, retry attempts increment, direct calls remain compatible, and archive-new counts are not represented as OpenSearch `new_item_count`.

### Search and pending

Deterministic mock responses produced unchanged BM25, stock/date filtering, hybrid/RRF, collapse, pagination/request, and result-shape behavior. No importance or event ranking was introduced. Pending selection and explicit `(_index, _id)` writeback behavior remained unchanged, and no real embedding model was loaded.

## Local Deterministic Before/After Comparison

| Signal | Legacy fixture | Phase 1 dry-run | Result |
|---|---|---|---|
| Identity | `_id=legacy-cls-20260815-001` | same `_id` and `news_id` | Equal |
| Archive bytes | captured before replay | unchanged after replay/projection | Equal |
| Legacy fields | present | present and unchanged | Equal |
| Canonical fields | absent | additive compatibility projection | Expected additive change |
| Index route | `pub_time` year 2026 | `news-2026` | Equal |
| Create-only | expected | `_op_type=create` | Equal |
| Search regression | legacy deterministic response | same response contract | Equal in mocked tests |
| Pending behavior | bounded snapshot fixture | same explicit snapshot/write protocol | Equal in mocked tests |

Per-source indexed-new counts, duplicates, live search top-N, real pending counts, and task durations cannot be measured honestly without deployment and remain pending.

## Live Gray Pending Checklist

1. Select one low-risk production time window.
2. Record pre-gray per-source collected, archive-new, index-new, and duplicate counts without conflating archive-new with index-new.
3. Confirm existing `news-{year}` mappings using read-only diff; have an administrator apply only approved additive mappings before enabling writes.
4. Enable the corresponding shadow/dual/enrichment gate explicitly; defaults remain legacy.
5. Repeat an equivalent time window and compare search top-N results.
6. Compare pending counts and ensure newly inserted pending documents are not marked done by an earlier snapshot.
7. Compare pipeline task duration and timeout behavior.
8. Compare retry attempts, job run correlation, and DingTalk notification count/text.
9. On any anomaly, immediately return the relevant gate to legacy.
10. Do not delete legacy fields and do not switch the search alias.

## Phase 1 Boundary

- No Event or EventDocumentMembership production implementation.
- No Importance or S/A/B/C ranking.
- No Sentiment or Impact assessment.
- No StockRelation or IndustryRelation production implementation.
- No historical news body bulk backfill.
- No search alias cutover.
- No production mapping/index/table creation or migration in this acceptance run.

The existing `ensure_index()` capability for future year-index creation remains unchanged; it was not called against a real cluster and is not evidence of an alias cutover.

## Final Decision

**PASS_OFFLINE_PENDING_LIVE_GRAY**

Offline Phase 1 logic and deterministic regressions pass within the stated dependency limitations. Production mapping readiness, NAS/PG behavior, live source health, real Windows subprocess execution in a configured environment, and gray before/after metrics remain pending. This is not `PASS_PHASE1` because no real gray run was performed.
