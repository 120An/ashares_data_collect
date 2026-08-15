# 统一新闻数据模型 V1.1

文档状态：设计稿 V1.1  
适用项目：`ashares_data_collect`  
设计范围：新闻、公告、政策、监管、研报等内容进入统一新闻系统后的数据契约  
本阶段边界：只定义对象、字段、约束、版本与 OpenSearch 索引投影；不定义具体评分算法、聚类算法或实施代码  
设计依据：`NEWS_ENGINE_AUDIT.md`、`NEWS_DATA_MODEL_V1.md` 与当前项目已有采集、归档、OpenSearch、搜索、股票和行业基础字段

> V1.1 修订边界：仅调整未评级来源的权威度表达、拆分来源运行健康状态、增加正式实体别名对象。V1 已确定的新闻、事件、股票/行业关系、重要度、情绪与影响结构保持不变。

## 1. 设计目标与核心结论

这套模型用同一批底层数据同时服务两个读取视图：

1. **全市场重要新闻雷达**：主要读取 `Event`，一件事只展示一次，再按分类、状态、重要度、行业和股票影响排序。
2. **候选股新闻深挖**：从股票实体出发，通过 `StockRelation`、`IndustryRelation` 和 `EntityRelation` 找到直接新闻、公司公告、行业/政策、上下游、海外事件及研报观点，并返回“为什么相关”。

核心数据流：

```text
Source（静态治理）── SourceHealth（高频运行状态）
  └─ NewsDocument（原始证据，news_id）
       ├─ EntityMention ── Entity ── EntityRelation（产业链/海外映射）
       └─ EventDocumentMembership ── Event（事件聚合，event_id）
                                      ├─ StockRelation（逐股票相关性与理由）
                                      ├─ IndustryRelation（逐行业/概念相关性与理由）
                                      ├─ Importance（全局重要度）
                                      └─ SentimentAssessment / ImpactAssessment
```

必须坚持以下分离：

- `news_id` 表示一篇来源文档，`event_id` 表示现实世界中的一件事，二者绝不复用。
- 一个 `Event` 可以包含多条 `NewsDocument`；一条文档的聚类归属也要保留版本和历史。
- 文档或事件与股票、行业都是多对多关系，不能只存一个股票或一个行业。
- `importance_score` 是事件对市场的全局重要程度；`stock_relevance_score` 是该事件与某只股票的关联强度；`impact_strength` 是对指定对象的影响强弱；OpenSearch `_score` 只是对当前查询的检索相关性。四者不可混用。
- `sentiment` 描述文本表达或信息倾向；`impact` 描述事件对指定股票、行业或实体的实际方向和周期。二者不可互相替代。
- 原始证据尽量不可变；AI/规则生成结果允许按模型版本重新计算，并保留旧版本、人工覆盖与证据链。
- 来源权威度必须先记录 `authority_status`。未完成评级时允许 `source_authority=null`，不得用默认分或占位数字伪装成已评级。
- `Source` 只承载低频静态治理信息；采集健康、延迟和连续失败等高频状态写入独立 `SourceHealth`，不触发 `Source.source_revision`。

## 2. 通用约定

### 2.1 字段类型

本文的数据类型是逻辑类型；括号中给出建议的 OpenSearch 映射：

- `string`：普通字符串；标识符、枚举和代码通常映射为 `keyword`。
- `text`：需要全文检索的长文本，映射为 `text`。
- `timestamp`：带时区的 ISO 8601 时间，映射为 `date`。
- `date`：自然日，格式 `YYYY-MM-DD`。
- `decimal`：定点或浮点数；分数建议映射为 `scaled_float`。
- `integer`、`boolean`：整数和布尔值。
- `array<T>`：可多值；对象数组如需按同一对象内部字段联合过滤，映射为 `nested`。
- `object` / `json`：结构化对象；字段形态不稳定的审计快照可关闭动态索引。

### 2.2 必填含义

- **是**：对象一旦进入正式库就必须存在。
- **条件**：满足字段含义所述条件时必须存在。
- **否**：允许为空，但不得用伪值掩盖未知信息。
- 表中的“可重算”指是否允许以后用规则、模型或主数据重新生成；它不表示可以无审计地覆盖历史值。

### 2.3 生成来源分类

| 标记 | 含义 |
|---|---|
| 原始 | 来源接口、RSS、网页、PDF 或原始归档直接提供 |
| 采集规则 | 抓取时确定性生成，如清洗、规范化、哈希、时间解析、ID |
| 主数据/配置 | 来源注册表、证券主数据、行业主数据或人工治理配置 |
| 规则/AI | 规则、统计模型、Embedding、LLM 或组合流程生成 |
| 人工 | 人工审核、纠错或锁定 |
| 投影 | 从规范化对象冗余到搜索文档的当前快照 |

### 2.4 ID、时间与版本约束

- 所有业务 ID 都是稳定 `keyword`，不可因标题、聚类模型或展示内容变化而改变。
- `news_id` 优先兼容当前确定性 `_id`；OpenSearch `_id = news_id`，同时在 `_source` 中显式保存 `news_id`。
- `event_id` 使用独立的全局稳定 ID，例如 `evt_01K...`；事件改标题、状态或成员时不换 ID，拆分、合并按第 6 节处理。
- 所有 `publish_time`、`collect_time`、`computed_at` 使用带时区时间；进入索引时统一为 UTC，保留来源原时区和原始时间字符串。
- 模型版本必须是可复现实验或发布版本，不得只写 `latest`。最低需要记录：`importance_model_version`、`sentiment_model_version`、`entity_model_version`、`cluster_model_version`。
- 派生结果采用“追加新 revision + 切换 `is_current`”方式重算；不得把原始值或旧模型结果物理抹掉。

## 3. 核心对象

## 3.1 NewsDocument

`NewsDocument` 是从一个来源取得的一篇原始新闻、快讯、公告、政策、监管材料或研报。它是证据层，而不是去重后的展示事件。

### 3.1.1 身份、来源与类型

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `news_id` | string(keyword) | 是 | `cninfo_1219987654` | 文档稳定主键，与 `event_id` 分离；兼容当前确定性 `_id` | 原生 ID/URL/来源时间标题 | 采集规则 | 否 |
| `schema_version` | string(keyword) | 是 | `news_document_v1` | 文档数据契约版本 | 系统配置 | 采集规则 | 否 |
| `source_id` | string(keyword) | 是 | `cninfo` | 指向 `Source.source_id`；由当前 `source` 规范化而来 | `sources.yaml`/采集器 | 主数据/配置 | 可治理修正 |
| `source_native_id` | string(keyword) | 否 | `1219987654` | 来源系统自己的文章/公告 ID | 来源载荷 | 原始 | 否 |
| `source_authority_status` | enum(keyword) | 是 | `unrated` | 采集时来源权威评级状态快照：`rated/unrated/provisional`；即使无分数也必须明确 | `Source.authority_status` | 投影 | 可按新版本另算，不覆盖快照 |
| `source_authority` | integer 0..100 | 条件 | `null` | 权威数值快照；`rated` 时必填，`unrated` 时必须为空，`provisional` 时可为空或填临时分 | `Source` 当前有效版本 | 投影 | 可按新版本另算，不覆盖快照 |
| `source_authority_version` | string(keyword) | 是 | `source_authority_2026q3` | 上述状态/分值所采用的治理版本 | `Source` | 投影 | 是 |
| `document_type` | enum(keyword) | 是 | `announcement` | `flash/news/announcement/policy/regulatory/research/filing/transcript/other` | 来源通道 + 分类 | 原始/规则 | 是 |
| `channel` | string(keyword) | 是 | `announcement` | 兼容当前粗粒度采集通道，如 `flash/policy/report/stock` | 当前采集器 | 原始/采集规则 | 否；可另建分类 |
| `language` | string(keyword) | 是 | `zh-CN` | 文档主要语言 | 来源配置/检测 | 主数据/规则 | 是 |
| `country_region_codes` | array<string> | 否 | `["CN"]` | 文档来源或主要报道地区，不等同于影响地区 | 来源配置/实体识别 | 主数据/规则 | 是 |
| `authors` | array<string> | 否 | `["张三"]` | 作者、记者或报告作者原文名 | 来源载荷/正文 | 原始/抽取 | 是 |

### 3.1.2 正文、地址与原始证据

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `title` | text + keyword | 条件 | `某公司拟扩建产能` | 清洗后的标题；与 `content/body` 至少一项非空 | 来源载荷 | 原始/采集规则 | 可从原始归档重建 |
| `content` | text | 条件 | `公司公告称……` | Feed/API 提供的正文或摘要，兼容当前字段 | 来源载荷 | 原始/采集规则 | 可从原始归档重建 |
| `summary` | text | 否 | `公司计划投资……` | 来源摘要或机器摘要；需用 `summary_type` 区分 | 来源/AI | 原始或规则/AI | AI 摘要可重算 |
| `summary_type` | enum(keyword) | 条件 | `ai` | `source/extractive/ai/manual`；有 `summary` 时必填 | 生成流程 | 采集规则 | 是 |
| `body` | text | 否 | `公告 PDF 全文……` | 网页、PDF 回填的完整正文，兼容现有字段 | 原网页/PDF | 原始/抽取 | 可从归档重提取 |
| `raw_title` | text(store only) | 否 | `原标题…` | 来源未清洗标题；当前归档字段，通常不进入检索索引 | 来源载荷 | 原始 | 否 |
| `raw_content` | text(store only) | 否 | `<p>原始内容</p>` | 未清洗正文/摘要；通常仅存 NAS 原始归档 | 来源载荷 | 原始 | 否 |
| `url` | string(keyword) | 否 | `https://example.com/a/1` | 来源返回的访问地址，兼容当前字段 | 来源载荷 | 原始 | 否 |
| `canonical_url` | string(keyword) | 否 | `https://example.com/a/1` | 去跟踪参数、处理跳转后的规范地址 | URL 规范化 | 采集规则 | 是 |
| `raw_archive_uri` | string(keyword) | 是 | `nas://news/2026/08/15/cninfo.jsonl.gz#...` | 可追溯到原始载荷的归档位置 | 当前归档系统 | 采集规则 | 否 |
| `content_license` | enum(keyword) | 否 | `fulltext_internal_only` | `fulltext_allowed/fulltext_internal_only/snippet_only/link_only/unknown` | `Source` 配置/协议 | 主数据/配置 | 可治理修正 |

### 3.1.3 时间与生命周期

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `publish_time` | timestamp | 是 | `2026-08-15T01:23:45+08:00` | 来源声明的发布时间；兼容当前 `pub_time` 的规范名称 | 来源载荷/页面 | 原始/时间解析 | 可纠错，保留历史 |
| `publish_time_raw` | string(keyword) | 否 | `08-15 01:23` | 来源原始时间文本 | 来源载荷 | 原始 | 否 |
| `publish_time_precision` | enum(keyword) | 是 | `minute` | `second/minute/hour/day/unknown` | 时间解析 | 采集规则 | 是 |
| `publish_time_is_estimated` | boolean | 是 | `false` | 发布时间是否为推断值；兼容当前 `time_estimated` | 时间解析 | 采集规则 | 是 |
| `collect_time` | timestamp | 是 | `2026-08-15T01:25:02+08:00` | 系统首次取得该文档的时间；兼容当前 `fetch_time` | 采集器时钟 | 采集规则 | 否 |
| `last_seen_time` | timestamp | 否 | `2026-08-15T02:00:00+08:00` | 轮询源时最后一次仍观察到该文档的时间 | 采集器 | 采集规则 | 是 |
| `source_update_time` | timestamp | 否 | `2026-08-15T01:40:00+08:00` | 来源声明的内容更新时间 | 来源载荷/页面 | 原始 | 可纠错 |
| `document_status` | enum(keyword) | 是 | `active` | `active/corrected/retracted/deleted/unavailable` | 来源变化/人工 | 原始/规则/人工 | 是，须留审计记录 |
| `created_at` | timestamp | 是 | `2026-08-15T01:25:03Z` | 规范文档首次落库时间 | 系统 | 采集规则 | 否 |
| `updated_at` | timestamp | 是 | `2026-08-15T01:40:05Z` | 文档元数据或投影最后更新时间 | 系统 | 采集规则 | 是 |

### 3.1.4 精确去重、事件投影与分类

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `title_hash` | string(keyword) | 否 | `sha256:...` | 规范化标题哈希 | 标题规范化 | 采集规则 | 是 |
| `content_hash` | string(keyword) | 否 | `sha256:...` | 规范化正文哈希 | 正文规范化 | 采集规则 | 是 |
| `canonical_url_hash` | string(keyword) | 否 | `sha256:...` | 规范 URL 哈希 | URL 规范化 | 采集规则 | 是 |
| `duplicate_type` | enum(keyword) | 是 | `none` | `none/exact/near/reprint`；文档仍保留，事件层负责聚合展示 | 去重流程 | 规则/AI | 是 |
| `duplicate_of_news_id` | string(keyword) | 条件 | `news_xxx` | 当前判断的代表文档；非 `none` 时使用 | 去重流程 | 规则/AI | 是 |
| `event_id` | string(keyword) | 否 | `evt_01K...` | 当前主事件投影，便于检索；规范归属以成员表为准 | 事件成员关系 | 投影 | 是 |
| `event_membership_confidence` | decimal 0..1 | 否 | `0.94` | 当前文档归入主事件的置信度 | 聚类流程 | 规则/AI | 是 |
| `is_event_first_report` | boolean | 否 | `true` | 在当前事件与指定首发口径下是否为系统识别的首发 | 首发识别 | 规则/AI | 是 |
| `first_report_scope` | enum(keyword) | 否 | `observed_sources` | `observed_sources/covered_sources/verified_original/unknown` | 首发识别 | 规则/AI | 是 |
| `first_report_confidence` | decimal 0..1 | 否 | `0.88` | 首发判断置信度 | 首发识别 | 规则/AI | 是 |
| `primary_category` | string(keyword) | 否 | `corporate.capex` | 当前主分类代码 | 分类流程 | 规则/AI | 是 |
| `category_codes` | array<string> | 否 | `["corporate.capex","industry.capacity"]` | 多标签分类代码 | 分类流程 | 规则/AI | 是 |
| `topic_tags` | array<string> | 否 | `["锂电池","扩产"]` | 可展示和筛选的主题标签 | 分类流程 | 规则/AI | 是 |
| `classification_confidence` | decimal 0..1 | 否 | `0.91` | 当前分类置信度 | 分类流程 | 规则/AI | 是 |
| `classification_model_version` | string(keyword) | 条件 | `news_cls_v1.2.0` | 有机器分类时必填 | 分类流程 | 规则/AI | 是 |

### 3.1.5 实体、检索与处理状态投影

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `entity_ids` | array<string> | 否 | `["ent_company_600519"]` | 当前实体识别结果的扁平过滤投影 | `EntityMention` | 投影 | 是 |
| `stock_codes` | array<string> | 否 | `["600519.SH"]` | 当前相关股票代码粗过滤投影；兼容现有 `stocks` | `StockRelation`/现有打标 | 投影 | 是 |
| `industry_ids` | array<string> | 否 | `["ind_sw_801120"]` | 当前相关行业/概念过滤投影 | `IndustryRelation` | 投影 | 是 |
| `entity_model_version` | string(keyword) | 否 | `entity_link_v2.0.1` | 生成当前实体投影的版本 | 实体流程 | 规则/AI | 是 |
| `cluster_model_version` | string(keyword) | 否 | `cluster_v1.3.0` | 生成当前事件投影的版本 | 聚类流程 | 规则/AI | 是 |
| `vec_status` | enum(keyword) | 否 | `done` | 兼容当前向量状态，如 `pending/done/failed` | 向量流程 | 采集规则 | 是 |
| `content_vec` | vector | 否 | `[0.012,…]` | 文档语义向量；维度由模型和索引版本固定 | 标题/正文 | 规则/AI | 是 |
| `embedding_model_version` | string(keyword) | 条件 | `bge_m3_1024_v1` | 有 `content_vec` 时必填 | 向量流程 | 规则/AI | 是 |
| `ann_type` | string(keyword) | 否 | `业绩预告` | 公告类型，兼容当前预留字段 | 公告元数据/分类 | 原始/规则/AI | 是 |
| `pdf_status` | enum(keyword) | 否 | `done` | 兼容当前公告 PDF 处理状态 | PDF 流程 | 采集规则 | 是 |
| `body_status` | enum(keyword) | 否 | `done` | 兼容当前网页全文处理状态 | 全文流程 | 采集规则 | 是 |
| `body_truncated` | boolean | 否 | `false` | 正文是否因长度或许可被截断 | 全文流程 | 采集规则 | 是 |

> 约束：正式文档中 `title`、`content`、`body` 至少一项非空。`raw_title/raw_content` 留在现有 NAS 原始归档即可，不要求复制进 OpenSearch。

## 3.2 Event

`Event` 是对“同一现实事件”的聚合。雷达以它为主要展示和排序单位；文档是事件的证据列表。事件允许持续演进、澄清、否认、合并或拆分。

### 3.2.1 事件身份与内容

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `event_id` | string(keyword) | 是 | `evt_01K2ABC...` | 事件稳定主键，与任何 `news_id` 独立 | 事件服务 | 采集规则 | 否；拆并另留关系 |
| `schema_version` | string(keyword) | 是 | `event_v1` | 事件对象契约版本 | 系统配置 | 采集规则 | 否 |
| `event_revision` | integer | 是 | `7` | 同一事件每次实质更新递增 | 事件服务 | 采集规则 | 否 |
| `canonical_title` | text + keyword | 是 | `某公司宣布扩建锂电池产能` | 跨来源统一展示标题 | 代表文档/摘要流程 | 规则/AI/人工 | 是 |
| `canonical_summary` | text | 否 | `公司计划……` | 当前事件级摘要，不复制每篇报道 | 成员文档 | 规则/AI/人工 | 是 |
| `event_type` | string(keyword) | 是 | `corporate.capex` | 事件类型代码，粒度高于采集 `channel` | 分类流程 | 规则/AI/人工 | 是 |
| `primary_category` | string(keyword) | 是 | `corporate` | 雷达主栏目分类 | 分类流程 | 规则/AI/人工 | 是 |
| `category_codes` | array<string> | 否 | `["corporate","industry"]` | 事件多分类 | 分类流程 | 规则/AI/人工 | 是 |
| `topic_tags` | array<string> | 否 | `["扩产","动力电池"]` | 事件主题标签 | 分类流程 | 规则/AI/人工 | 是 |
| `classification_confidence` | decimal 0..1 | 否 | `0.93` | 当前分类置信度 | 分类流程 | 规则/AI | 是 |
| `classification_model_version` | string(keyword) | 条件 | `event_cls_v1.1.0` | 机器分类存在时必填 | 分类流程 | 规则/AI | 是 |

### 3.2.2 状态、时间与首发

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `event_status` | enum(keyword) | 是 | `confirmed` | 必须支持 `developing/confirmed/clarified/denied/expired` | 成员证据/人工 | 规则/AI/人工 | 是，保留状态历史 |
| `status_reason` | text | 否 | `交易所公告确认` | 当前状态的可读解释 | 证据/审核 | 规则/AI/人工 | 是 |
| `status_evidence_news_ids` | array<string> | 否 | `["cninfo_..."]` | 支持当前状态的新闻证据 | 成员关系 | 规则/人工 | 是 |
| `status_changed_at` | timestamp | 是 | `2026-08-15T03:10:00Z` | 当前状态生效时间 | 事件服务 | 采集规则 | 是 |
| `event_start_time` | timestamp | 否 | `2026-08-15T00:30:00Z` | 现实事件开始/发生时间，不等于报道时间 | 文档/结构化抽取 | 原始/规则/AI | 是 |
| `event_end_time` | timestamp | 否 | `2026-08-15T04:00:00Z` | 已知的现实事件结束时间 | 文档/结构化抽取 | 原始/规则/AI | 是 |
| `first_publish_time` | timestamp | 是 | `2026-08-15T00:35:00Z` | 当前成员中最早有效发布时间 | 成员文档 | 投影 | 是 |
| `last_publish_time` | timestamp | 是 | `2026-08-15T06:20:00Z` | 当前成员中最晚有效发布时间 | 成员文档 | 投影 | 是 |
| `first_collect_time` | timestamp | 是 | `2026-08-15T00:36:10Z` | 系统首次看到该事件任何文档的时间 | 成员文档 | 投影 | 是 |
| `last_collect_time` | timestamp | 是 | `2026-08-15T06:21:00Z` | 系统最近取得该事件新证据的时间 | 成员文档 | 投影 | 是 |
| `first_news_id` | string(keyword) | 是 | `cls_12345` | 按当前首发口径识别的首篇文档 | 成员文档 | 规则/AI | 是 |
| `first_source_id` | string(keyword) | 是 | `cls` | 首篇文档来源 | `first_news_id` | 投影 | 是 |
| `first_report_scope` | enum(keyword) | 是 | `observed_sources` | `observed_sources/covered_sources/verified_original/unknown` | 首发识别 | 规则/AI | 是 |
| `first_report_confidence` | decimal 0..1 | 是 | `0.87` | 首发判断置信度 | 首发识别 | 规则/AI | 是 |
| `expires_at` | timestamp | 否 | `2026-08-22T00:00:00Z` | 预期不再演进、可转 `expired` 的观察时间，不代表删除 | 状态流程 | 规则/人工 | 是 |

“首发”只能表述为系统在某一覆盖范围内识别到的首发。`observed_sources` 表示当前实际采集到的来源；`covered_sources` 表示系统应覆盖的注册来源；`verified_original` 表示有来源链证据确认原创。不得无证据宣称全网绝对首发。

### 3.2.3 成员、实体与聚类治理

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `primary_news_id` | string(keyword) | 是 | `cninfo_...` | 当前最适合作为主要证据/展示入口的成员，不必等于首发 | 成员评选 | 规则/AI/人工 | 是 |
| `news_count` | integer | 是 | `8` | 当前有效成员文档数 | 成员表 | 投影 | 是 |
| `source_count` | integer | 是 | `5` | 当前不同来源数 | 成员表 | 投影 | 是 |
| `source_ids` | array<string> | 是 | `["cninfo","cls"]` | 当前事件来源集合 | 成员表 | 投影 | 是 |
| `source_authority_statuses` | array<enum> | 是 | `["rated","unrated"]` | 当前成员来源评级状态集合，避免缺分被误解为低分 | 文档/Source | 投影 | 是 |
| `max_source_authority` | integer 0..100 | 条件 | `95` | 当前有数值评级成员中的最高权威分快照；全部 `unrated` 时为空 | 文档/Source | 投影 | 是 |
| `entity_ids` | array<string> | 否 | `["ent_company_600519"]` | 当前事件实体过滤投影 | 实体关系 | 投影 | 是 |
| `stock_codes` | array<string> | 否 | `["600519.SH","000858.SZ"]` | 当前事件相关股票粗过滤投影 | `StockRelation` | 投影 | 是 |
| `industry_ids` | array<string> | 否 | `["ind_sw_food_beverage"]` | 当前事件相关行业/概念粗过滤投影 | `IndustryRelation` | 投影 | 是 |
| `cluster_confidence` | decimal 0..1 | 是 | `0.92` | 当前事件成员整体聚类置信度 | 聚类流程 | 规则/AI | 是 |
| `cluster_model_version` | string(keyword) | 是 | `cluster_v1.3.0` | 当前事件聚类模型/规则版本 | 聚类流程 | 规则/AI | 是 |
| `cluster_computed_at` | timestamp | 是 | `2026-08-15T06:22:00Z` | 当前聚类结果计算时间 | 聚类流程 | 采集规则 | 是 |
| `entity_model_version` | string(keyword) | 否 | `entity_link_v2.0.1` | 当前实体投影版本 | 实体流程 | 规则/AI | 是 |
| `summary_model_version` | string(keyword) | 否 | `event_summary_v1.0.0` | 当前事件摘要模型版本 | 摘要流程 | 规则/AI | 是 |
| `event_vec` | vector | 否 | `[0.021,…]` | 事件语义向量 | 标题/摘要/成员 | 规则/AI | 是 |
| `embedding_model_version` | string(keyword) | 条件 | `bge_m3_1024_v1` | 有 `event_vec` 时必填 | 向量流程 | 规则/AI | 是 |
| `merged_into_event_id` | string(keyword) | 否 | `evt_01K2XYZ...` | 本事件已并入的目标事件；保留旧 ID 作重定向 | 事件治理 | 规则/人工 | 可修正 |
| `split_from_event_id` | string(keyword) | 否 | `evt_01K2OLD...` | 本事件由哪个旧事件拆出 | 事件治理 | 规则/人工 | 可修正 |
| `related_event_ids` | array<string> | 否 | `["evt_..."]` | 非同一事件但有时序/因果/主题关联的事件 | 事件关系 | 规则/AI/人工 | 是 |
| `created_at` | timestamp | 是 | `2026-08-15T00:36:20Z` | 事件首次建立时间 | 事件服务 | 采集规则 | 否 |
| `updated_at` | timestamp | 是 | `2026-08-15T06:22:00Z` | 当前 revision 更新时间 | 事件服务 | 采集规则 | 是 |

## 3.3 Entity

`Entity` 是统一实体主数据。股票是实体的一种，但公司实体与证券实体必须允许分开，以支持同一公司多证券、历史代码、母子公司和海外公司。

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `entity_id` | string(keyword) | 是 | `ent_company_600519` | 稳定实体主键 | 实体主数据服务 | 主数据/配置 | 否；仅允许合并重定向 |
| `schema_version` | string(keyword) | 是 | `entity_v1` | 实体契约版本 | 系统配置 | 采集规则 | 否 |
| `entity_revision` | integer | 是 | `3` | 主数据修订号 | 实体服务 | 采集规则 | 否 |
| `entity_type` | enum(keyword) | 是 | `company` | `company/stock/industry/concept/product/raw_material/person/institution/country/region/commodity/index/currency/policy_document/other` | 主数据/实体识别 | 主数据/规则/人工 | 可治理修正 |
| `canonical_name` | string(keyword + text) | 是 | `贵州茅台酒股份有限公司` | 规范展示名称 | 证券/行业/机构主数据 | 主数据/人工 | 可治理修正 |
| `normalized_name` | string(keyword) | 是 | `贵州茅台酒股份有限公司` | 用于匹配的标准化名称 | 名称规范化 | 采集规则 | 是 |
| `short_name` | string(keyword) | 否 | `贵州茅台` | 常用简称 | 主数据 | 原始/主数据 | 可治理修正 |
| `english_name` | string(keyword + text) | 否 | `Kweichow Moutai Co., Ltd.` | 英文名称 | 主数据/官网 | 原始/主数据 | 可治理修正 |
| `aliases` | array<string> | 否 | `["茅台","贵州茅台股份"]` | 兼容搜索的当前别名扁平投影；正式名称事实、类型、时效和来源以 `EntityAlias` 为准 | `EntityAlias` 当前有效记录 | 投影 | 是 |
| `stock_code` | string(keyword) | 条件 | `600519.SH` | `entity_type=stock` 时的规范证券代码 | 当前 `instrument_info` | 主数据 | 随证券历史治理 |
| `exchange` | string(keyword) | 条件 | `SSE` | 证券交易所代码 | 证券主数据 | 主数据 | 可治理修正 |
| `external_ids` | object | 否 | `{"secCode":"600519","lei":null}` | 外部系统 ID 集；默认不对任意键动态建索引 | 各主数据源 | 原始/主数据 | 可治理修正 |
| `parent_entity_id` | string(keyword) | 否 | `ent_company_group_x` | 简单直接上级；复杂关系使用 `EntityRelation` | 主数据 | 主数据/人工 | 是 |
| `country_region_codes` | array<string> | 否 | `["CN-52"]` | 注册地、主要经营地等规范地区代码 | 主数据/官网 | 主数据/规则 | 是 |
| `description` | text | 否 | `主要从事白酒生产……` | 实体简介 | 主数据/官网/AI 摘要 | 原始/规则/AI | 是 |
| `status` | enum(keyword) | 是 | `active` | `active/inactive/delisted/merged/unknown` | 主数据 | 主数据/人工 | 可治理修正 |
| `valid_from` | timestamp | 否 | `2001-08-27T00:00:00+08:00` | 名称/代码/关系有效起点 | 主数据 | 主数据 | 可治理修正 |
| `valid_to` | timestamp | 否 | `null` | 有效终点 | 主数据 | 主数据 | 可治理修正 |
| `provenance_source_ids` | array<string> | 是 | `["instrument_info","manual"]` | 实体主数据依据 | 主数据服务 | 主数据/人工 | 可追加 |
| `confidence` | decimal 0..1 | 是 | `0.99` | 实体规范化可信度 | 主数据治理 | 规则/人工 | 是 |
| `entity_model_version` | string(keyword) | 否 | `entity_master_v1.0.0` | 机器生成/合并实体时所用版本 | 实体流程 | 规则/AI | 是 |
| `merged_into_entity_id` | string(keyword) | 否 | `ent_company_new` | 重复实体合并后的稳定重定向 | 实体治理 | 人工/规则 | 可修正 |
| `created_at` | timestamp | 是 | `2026-08-15T00:00:00Z` | 首次建档时间 | 实体服务 | 采集规则 | 否 |
| `updated_at` | timestamp | 是 | `2026-08-15T00:00:00Z` | 最近修订时间 | 实体服务 | 采集规则 | 是 |

## 3.4 StockRelation

`StockRelation` 解释一篇文档或一个事件为什么与某只 A 股相关。它是候选股深挖的核心对象，也是雷达输出受影响股票的依据。

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `stock_relation_id` | string(keyword) | 是 | `sr_evt01_600519` | 关系稳定 ID，建议由目标、股票、revision 体系管理 | 关系服务 | 采集规则 | 否 |
| `target_scope` | enum(keyword) | 是 | `event` | `news/event`，说明关系挂在哪一层 | 关系流程 | 采集规则 | 否 |
| `news_id` | string(keyword) | 条件 | `cninfo_...` | `target_scope=news` 时必填，且与 `event_id` 二选一 | 文档 | 投影 | 否 |
| `event_id` | string(keyword) | 条件 | `evt_01K...` | `target_scope=event` 时必填，且与 `news_id` 二选一 | 事件 | 投影 | 否 |
| `stock_entity_id` | string(keyword) | 是 | `ent_stock_600519_sh` | 指向证券实体 | `Entity` | 主数据/投影 | 可治理修正 |
| `company_entity_id` | string(keyword) | 否 | `ent_company_600519` | 对应发行人公司实体 | `EntityRelation`/证券主数据 | 主数据/投影 | 是 |
| `stock_code` | string(keyword) | 是 | `600519.SH` | 便于 OpenSearch 精确过滤；兼容现有 `stocks` 值 | 证券主数据 | 投影 | 是 |
| `relation_type` | enum(keyword) | 是 | `issuer` | 见下方关系枚举 | 原文/实体与产业链关系 | 原始/规则/AI | 是 |
| `is_direct` | boolean | 是 | `true` | 原文是否直接涉及公司/股票，而非行业或关系传播 | 关系路径 | 规则/AI | 是 |
| `stock_relevance_score` | decimal 0..1 | 是 | `0.96` | 对这只股票的关联强度；不是新闻重要度或涨跌方向 | 关系模型 | 规则/AI | 是 |
| `relevance_confidence` | decimal 0..1 | 是 | `0.91` | 相关性判断可信度 | 关系模型 | 规则/AI | 是 |
| `relation_reason` | text | 是 | `该公司是公告发布主体` | 面向用户的“为什么相关”说明 | 原文/关系路径 | 规则/AI/人工 | 是 |
| `relation_path` | array<object> | 否 | `公司→产品→原材料` | 从事件实体到股票的可审计关系路径 | `EntityRelation` | 规则/AI/主数据 | 是 |
| `relation_depth` | integer | 是 | `0` | 0=直接，1=一跳，2=两跳；限制无界扩散 | 关系路径 | 采集规则 | 是 |
| `affected_role` | enum(keyword) | 否 | `beneficiary` | `subject/issuer/beneficiary/adversely_affected/peer/customer/supplier/competitor/observer/unknown` | 影响分析 | 规则/AI | 是 |
| `evidence_news_ids` | array<string> | 是 | `["cninfo_..."]` | 支撑关系的文档 ID | 文档/事件成员 | 规则/人工 | 是 |
| `evidence_spans` | array<object> | 否 | `[{"news_id":"...","text":"公司公告…"}]` | 关系证据文本和位置；受版权策略约束 | 原文抽取 | 原始/规则/AI | 是 |
| `derived_by` | enum(keyword) | 是 | `rule` | `source_metadata/rule/ai/manual` | 关系流程 | 采集规则 | 否 |
| `entity_model_version` | string(keyword) | 条件 | `entity_link_v2.0.1` | 涉及机器实体识别时必填 | 实体流程 | 规则/AI | 是 |
| `relation_model_version` | string(keyword) | 条件 | `stock_relation_v1.0.0` | 规则/模型生成关系时必填 | 关系流程 | 规则/AI | 是 |
| `computed_at` | timestamp | 是 | `2026-08-15T06:30:00Z` | 本 revision 计算时间 | 关系流程 | 采集规则 | 是 |
| `revision` | integer | 是 | `2` | 同一目标—股票关系修订号 | 关系服务 | 采集规则 | 否 |
| `is_current` | boolean | 是 | `true` | 是否为当前有效 revision | 关系服务 | 采集规则 | 是 |
| `manual_override` | boolean | 是 | `false` | 是否存在人工覆盖 | 审核流程 | 人工 | 是 |
| `manual_lock` | boolean | 是 | `false` | 重算时是否禁止自动替换当前结果 | 审核流程 | 人工 | 是 |

建议的 `relation_type` 最小集合：`subject/issuer/announcement/research_coverage/mentioned/industry_exposure/concept_exposure/product_exposure/raw_material_exposure/upstream/supplier/downstream/customer/competitor/policy_beneficiary/policy_affected/overseas_mapping`。枚举可扩展，但必须版本化。

## 3.5 IndustryRelation

`IndustryRelation` 表示文档/事件与行业、概念或主题板块的关系。行业体系必须带命名空间，避免申万、证监会、GICS、同花顺和自定义概念同名冲突。

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `industry_relation_id` | string(keyword) | 是 | `ir_evt01_sw801120` | 行业关系稳定 ID | 关系服务 | 采集规则 | 否 |
| `target_scope` | enum(keyword) | 是 | `event` | `news/event` | 关系流程 | 采集规则 | 否 |
| `news_id` | string(keyword) | 条件 | `news_...` | `target_scope=news` 时必填，与 `event_id` 二选一 | 文档 | 投影 | 否 |
| `event_id` | string(keyword) | 条件 | `evt_...` | `target_scope=event` 时必填，与 `news_id` 二选一 | 事件 | 投影 | 否 |
| `industry_entity_id` | string(keyword) | 是 | `ent_industry_sw_801120` | 指向行业或概念实体 | `Entity` | 主数据/投影 | 可治理修正 |
| `industry_system` | string(keyword) | 是 | `SW2021` | 行业分类命名空间，如 `CSRC/GICS/SW2021/THS/custom` | 当前 `sector_stock`/主数据 | 主数据 | 可治理修正 |
| `industry_code` | string(keyword) | 是 | `801120` | 该体系内的行业/概念代码 | 行业主数据 | 主数据 | 可治理修正 |
| `industry_name` | string(keyword + text) | 是 | `食品饮料` | 当前展示名称快照 | 行业主数据 | 投影 | 是 |
| `industry_kind` | enum(keyword) | 是 | `industry` | `industry/concept/theme/style` | 行业主数据 | 主数据 | 可治理修正 |
| `relation_type` | enum(keyword) | 是 | `direct_topic` | `direct_topic/company_membership/policy_scope/supply/demand/raw_material/price/technology/geopolitical/other` | 原文/实体关系 | 原始/规则/AI | 是 |
| `industry_relevance_score` | decimal 0..1 | 是 | `0.89` | 对该行业/概念的关联强度；不表示重要度或方向 | 关系模型 | 规则/AI | 是 |
| `relevance_confidence` | decimal 0..1 | 是 | `0.86` | 行业关联判断可信度 | 关系模型 | 规则/AI | 是 |
| `relation_reason` | text | 是 | `政策直接适用于白酒行业` | 面向用户的行业关联原因 | 原文/关系路径 | 规则/AI/人工 | 是 |
| `relation_path` | array<object> | 否 | `事件→原材料→行业` | 可审计的实体/行业传导路径 | 实体与行业主数据 | 规则/AI | 是 |
| `relation_depth` | integer | 是 | `0` | 0=直接，其他为传播跳数 | 关系路径 | 采集规则 | 是 |
| `evidence_news_ids` | array<string> | 是 | `["news_..."]` | 支撑关系的文档 | 文档/事件成员 | 规则/人工 | 是 |
| `evidence_spans` | array<object> | 否 | `[{"text":"适用于…"}]` | 支撑片段及位置 | 原文抽取 | 原始/规则/AI | 是 |
| `derived_by` | enum(keyword) | 是 | `ai` | `source_metadata/rule/ai/manual` | 关系流程 | 采集规则 | 否 |
| `relation_model_version` | string(keyword) | 条件 | `industry_relation_v1.0.0` | 规则/模型生成关系时必填 | 关系流程 | 规则/AI | 是 |
| `computed_at` | timestamp | 是 | `2026-08-15T06:30:00Z` | 本 revision 计算时间 | 关系流程 | 采集规则 | 是 |
| `revision` | integer | 是 | `1` | 关系修订号 | 关系服务 | 采集规则 | 否 |
| `is_current` | boolean | 是 | `true` | 是否当前有效 | 关系服务 | 采集规则 | 是 |
| `manual_override` | boolean | 是 | `false` | 是否有人工作业覆盖 | 审核流程 | 人工 | 是 |
| `manual_lock` | boolean | 是 | `false` | 是否锁定不被自动重算替换 | 审核流程 | 人工 | 是 |

## 3.6 Source

`Source` 是新闻来源注册与治理对象。当前 `sources.yaml` 的源 ID 可直接作为 `source_id` 起点；快讯等注册表外来源也应最终纳入统一来源对象。

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `source_id` | string(keyword) | 是 | `csrc` | 稳定来源 ID，兼容当前 source ID | `sources.yaml`/采集器 | 主数据/配置 | 否 |
| `schema_version` | string(keyword) | 是 | `source_v1` | 来源契约版本 | 系统配置 | 采集规则 | 否 |
| `source_revision` | integer | 是 | `4` | 来源配置修订号 | 来源治理 | 采集规则 | 否 |
| `source_name` | string(keyword + text) | 是 | `中国证券监督管理委员会` | 展示名称 | 注册表/官网 | 主数据/配置 | 可治理修正 |
| `publisher_entity_id` | string(keyword) | 否 | `ent_inst_csrc` | 对应机构实体 | 实体主数据 | 主数据/投影 | 是 |
| `source_category` | enum(keyword) | 是 | `regulator` | `regulator/exchange/company_official/government/media/research_institution/aggregator/data_vendor/other` | 来源治理 | 主数据/配置 | 可治理修正 |
| `acquisition_type` | enum(keyword) | 是 | `web` | `api/rss/atom/rsshub/web/pdf/akshare/other` | 当前 adapter/采集器 | 主数据/配置 | 可治理修正 |
| `directness` | enum(keyword) | 是 | `original` | `original/aggregator/reprint/unknown` | 来源治理 | 主数据/配置 | 可治理修正 |
| `homepage_url` | string(keyword) | 否 | `https://www.csrc.gov.cn/` | 来源主页 | 注册表 | 主数据/配置 | 可治理修正 |
| `endpoint_url` | string(keyword) | 否 | `https://.../list.html` | API/RSS/列表入口；敏感凭据不得存入 | 注册表 | 主数据/配置 | 可治理修正 |
| `default_channel` | string(keyword) | 是 | `policy` | 默认采集通道 | 当前 `sources.yaml` | 主数据/配置 | 可治理修正 |
| `country_region_codes` | array<string> | 是 | `["CN"]` | 来源所在地/服务范围 | 来源治理 | 主数据/配置 | 可治理修正 |
| `languages` | array<string> | 是 | `["zh-CN"]` | 来源语言 | 来源治理 | 主数据/配置 | 可治理修正 |
| `source_timezone` | string(keyword) | 是 | `Asia/Shanghai` | 解释来源时间的 IANA 时区 | 来源治理 | 主数据/配置 | 可治理修正 |
| `enabled` | boolean | 是 | `true` | 是否允许调度采集，兼容当前注册表开关 | `sources.yaml` | 主数据/配置 | 是 |
| `authority_status` | enum(keyword) | 是 | `unrated` | 权威评级状态：`rated/unrated/provisional`；新来源默认可为 `unrated` | 来源治理 | 主数据/人工 | 可按版本重评 |
| `source_authority` | integer 0..100 | 条件 | `null` | 来源权威分；`rated` 时必填，`unrated` 时必须为空，`provisional` 时可为空或使用明确的临时分 | 来源治理 | 主数据/人工 | 可按版本重评 |
| `authority_level` | enum(keyword) | 条件 | `primary_official` | `primary_official/high/standard/low/unverified`；`rated` 时必填，其他状态可为空或为 `unverified` | 来源治理 | 主数据/人工 | 可按版本重评 |
| `authority_basis` | text | 条件 | `国家监管机构官方网站` | `rated/provisional` 时记录评级依据；`unrated` 可记录待评原因 | 来源治理 | 人工/配置 | 可治理修正 |
| `authority_version` | string(keyword) | 是 | `source_authority_2026q3` | 状态、分值和等级采用的治理规则/评审版本 | 来源治理 | 主数据/配置 | 是 |
| `authority_effective_from` | timestamp | 是 | `2026-07-01T00:00:00Z` | 当前权威版本生效时间 | 来源治理 | 主数据/配置 | 否 |
| `is_official` | boolean | 是 | `true` | 是否为监管、交易所、部委或公司官方渠道 | 来源治理 | 主数据/人工 | 可治理修正 |
| `expected_frequency` | string(keyword) | 否 | `continuous` | `continuous/daily/weekly/irregular` | 来源运营配置 | 主数据/配置 | 可治理修正 |
| `collect_interval_seconds` | integer | 否 | `900` | 预期采集间隔，不直接等同当前调度实现 | 调度配置 | 主数据/配置 | 是 |
| `content_license` | enum(keyword) | 是 | `link_only` | 全文、摘要、链接等允许范围 | 协议/网站政策 | 主数据/人工 | 可治理修正 |
| `paywall_type` | enum(keyword) | 是 | `none` | `none/soft/hard/unknown` | 来源治理 | 主数据/人工 | 可治理修正 |
| `created_at` | timestamp | 是 | `2026-07-01T00:00:00Z` | 来源首次注册时间 | 来源治理 | 采集规则 | 否 |
| `updated_at` | timestamp | 是 | `2026-08-15T00:00:00Z` | 最近修订时间 | 来源治理 | 采集规则 | 是 |

`authority_status` 必须先于权威分解释：`unrated` 表示尚未完成评估，不等于低权威；`provisional` 表示结论仍可调整；`rated` 才表示正式评级。`source_authority` 是来源层属性，不是某条新闻的重要度。官方来源可以发布低重要度日常通知，普通媒体也可能率先报道高重要度事件；因此 Importance 只能把可用的权威状态/分值作为可审计输入快照，而不能直接等同。

`Source` 只保存来源名称、类型、是否官方、权威度、许可、地址、时区和预期频率等低频治理信息。运行健康字段全部属于 `SourceHealth`；健康波动不得创建新的 `Source.source_revision`。

## 3.7 Importance

`Importance` 是独立、版本化的全局重要度评估。雷达原则上以事件级评估为准；文档级评估只用于事件尚未形成时的临时排序。

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `importance_id` | string(keyword) | 是 | `imp_evt01_r3` | 评估记录 ID | 评估服务 | 采集规则 | 否 |
| `target_scope` | enum(keyword) | 是 | `event` | `news/event` | 评估流程 | 采集规则 | 否 |
| `news_id` | string(keyword) | 条件 | `news_...` | 文档级临时评估时必填，与 `event_id` 二选一 | 文档 | 投影 | 否 |
| `event_id` | string(keyword) | 条件 | `evt_...` | 事件级评估时必填，与 `news_id` 二选一 | 事件 | 投影 | 否 |
| `importance_score` | decimal 0..100 | 是 | `87.5` | 全局市场重要度分；不表示与某股票的相关性 | 文档/事件/来源/范围特征 | 规则/AI/人工 | 是 |
| `importance_grade` | enum(keyword) | 是 | `A` | `S/A/B/C`，按版本化分级政策由分数或人工映射 | 重要度流程 | 规则/AI/人工 | 是 |
| `importance_status` | enum(keyword) | 是 | `final` | `provisional/final/reviewed` | 事件成熟度/审核 | 规则/人工 | 是 |
| `importance_confidence` | decimal 0..1 | 是 | `0.84` | 重要度判断可信度 | 重要度流程 | 规则/AI | 是 |
| `market_scope` | enum(keyword) | 是 | `multi_industry` | `single_company/industry/multi_industry/national/global/cross_market` | 事件实体/关系 | 规则/AI/人工 | 是 |
| `source_authority_statuses_snapshot` | array<enum> | 是 | `["rated","unrated"]` | 计算时事件成员所含 `rated/unrated/provisional` 状态快照；缺分不等于低分 | 文档/Source | 投影 | 新 revision 可更新 |
| `source_authority_snapshot` | integer 0..100 | 条件 | `98` | 计算时采用的可用权威分输入快照；全部来源均 `unrated` 且无临时分时为空 | 文档/Source | 投影 | 新 revision 可更新 |
| `source_authority_version` | string(keyword) | 是 | `source_authority_2026q3` | 权威状态和分值输入版本 | Source | 投影 | 新 revision 可更新 |
| `reason_codes` | array<string> | 是 | `["official_confirmation","broad_market_scope"]` | 可审计的重要原因代码，不在本阶段规定权重 | 重要度流程 | 规则/AI/人工 | 是 |
| `explanation` | text | 是 | `监管正式发布且影响多个行业` | 面向审核和用户的解释 | 评估输入 | 规则/AI/人工 | 是 |
| `score_components` | object | 否 | `{"scope":80,"authority":98}` | 评分维度及原始特征快照；不在 V1 固定公式 | 重要度流程 | 规则/AI | 是 |
| `affected_stock_count` | integer | 否 | `126` | 计算时已知受影响股票数快照 | `StockRelation` | 投影 | 是 |
| `affected_industry_count` | integer | 否 | `4` | 计算时已知受影响行业数快照 | `IndustryRelation` | 投影 | 是 |
| `importance_model_version` | string(keyword) | 是 | `importance_v1.0.0` | 重要度模型/规则版本，强制记录 | 重要度流程 | 规则/AI | 是 |
| `grade_policy_version` | string(keyword) | 是 | `grade_policy_v1` | S/A/B/C 映射政策版本 | 重要度配置 | 主数据/配置 | 是 |
| `computed_by` | enum(keyword) | 是 | `hybrid` | `rule/ai/hybrid/manual` | 重要度流程 | 采集规则 | 否 |
| `computed_at` | timestamp | 是 | `2026-08-15T06:35:00Z` | 本 revision 计算时间 | 重要度流程 | 采集规则 | 是 |
| `revision` | integer | 是 | `3` | 评估修订号 | 评估服务 | 采集规则 | 否 |
| `is_current` | boolean | 是 | `true` | 是否为当前有效评估 | 评估服务 | 采集规则 | 是 |
| `manual_override` | boolean | 是 | `false` | 是否采用人工覆盖 | 审核流程 | 人工 | 是 |
| `override_reason` | text | 条件 | `监管事件升级为 S` | `manual_override=true` 时必填 | 审核人员 | 人工 | 可人工修正 |
| `reviewer_id` | string(keyword) | 条件 | `user_001` | 人工评审时必填 | 审核系统 | 人工 | 否 |

等级语义只在 V1 定义业务含义，不定义阈值或算法：

- `S`：可能造成系统性、全国性或跨市场重大影响，需要最高优先级处置。
- `A`：对市场、重要行业或关键公司有显著影响，需要重点关注。
- `B`：有明确影响但范围或强度有限，适合常规跟踪。
- `C`：例行、背景或低影响信息，主要用于检索和留档。

具体分数阈值、权重和升级/降级规则由以后版本化的 `grade_policy_version` 定义。

## 3.8 Sentiment / Impact

V1 将这一领域拆成两个对象：`SentimentAssessment` 记录信息表达倾向，`ImpactAssessment` 记录对指定对象的影响方向、强度和周期。

### 3.8.1 SentimentAssessment

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `sentiment_id` | string(keyword) | 是 | `sent_evt01_r2` | 情绪评估记录 ID | 评估服务 | 采集规则 | 否 |
| `target_scope` | enum(keyword) | 是 | `event` | `news/event/stock_relation/industry_relation` | 评估流程 | 采集规则 | 否 |
| `target_id` | string(keyword) | 是 | `evt_01K...` | 对应目标对象 ID | 目标对象 | 投影 | 否 |
| `aspect_entity_id` | string(keyword) | 否 | `ent_company_600519` | 情绪针对的实体；空表示整体文本 | 实体识别 | 规则/AI | 是 |
| `sentiment_label` | enum(keyword) | 是 | `negative` | `positive/negative/neutral/mixed/uncertain` | 文本/事件表述 | 规则/AI/人工 | 是 |
| `sentiment_score` | decimal -1..1 | 是 | `-0.72` | 倾向连续值；不直接等同股价影响 | 情绪流程 | 规则/AI/人工 | 是 |
| `sentiment_confidence` | decimal 0..1 | 是 | `0.88` | 情绪判断置信度 | 情绪流程 | 规则/AI | 是 |
| `aspect_tags` | array<string> | 否 | `["业绩","监管"]` | 倾向所针对的方面 | 文本/实体 | 规则/AI | 是 |
| `evidence_news_ids` | array<string> | 是 | `["news_..."]` | 评估依据文档 | 文档/事件成员 | 规则/人工 | 是 |
| `evidence_spans` | array<object> | 否 | `[{"text":"净利润下降…"}]` | 证据片段和位置 | 原文抽取 | 原始/规则/AI | 是 |
| `explanation` | text | 是 | `表述显示盈利明显恶化` | 判断解释 | 评估流程 | 规则/AI/人工 | 是 |
| `sentiment_model_version` | string(keyword) | 是 | `sentiment_zh_fin_v1.1.0` | 情绪模型/规则版本，强制记录 | 情绪流程 | 规则/AI | 是 |
| `computed_by` | enum(keyword) | 是 | `ai` | `rule/ai/hybrid/manual` | 情绪流程 | 采集规则 | 否 |
| `computed_at` | timestamp | 是 | `2026-08-15T06:36:00Z` | 本 revision 计算时间 | 情绪流程 | 采集规则 | 是 |
| `revision` | integer | 是 | `2` | 评估修订号 | 评估服务 | 采集规则 | 否 |
| `is_current` | boolean | 是 | `true` | 是否当前有效 | 评估服务 | 采集规则 | 是 |
| `manual_override` | boolean | 是 | `false` | 是否有人工作业覆盖 | 审核流程 | 人工 | 是 |
| `manual_lock` | boolean | 是 | `false` | 是否阻止自动结果成为当前版本 | 审核流程 | 人工 | 是 |

### 3.8.2 ImpactAssessment

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `impact_id` | string(keyword) | 是 | `impact_sr01_short_r2` | 影响评估记录 ID | 评估服务 | 采集规则 | 否 |
| `target_scope` | enum(keyword) | 是 | `stock_relation` | `event/stock_relation/industry_relation/entity` | 评估流程 | 采集规则 | 否 |
| `target_id` | string(keyword) | 是 | `sr_evt01_600519` | 被评估关系或对象 ID | 目标对象 | 投影 | 否 |
| `affected_entity_id` | string(keyword) | 是 | `ent_stock_600519_sh` | 影响落到的股票、行业、公司等实体 | 实体/关系 | 主数据/投影 | 是 |
| `impact_direction` | enum(keyword) | 是 | `positive` | `positive/negative/neutral/mixed/uncertain` | 事件与关系路径 | 规则/AI/人工 | 是 |
| `impact_strength` | decimal 0..100 | 是 | `72` | 对该目标的影响强度；不是全局重要度或相关性 | 影响流程 | 规则/AI/人工 | 是 |
| `impact_confidence` | decimal 0..1 | 是 | `0.79` | 影响判断可信度 | 影响流程 | 规则/AI | 是 |
| `impact_horizon` | enum(keyword) | 是 | `medium_term` | `intraday/short_term/medium_term/long_term/structural/unknown` | 事件语义/规则 | 规则/AI/人工 | 是 |
| `horizon_start_time` | timestamp | 否 | `2026-08-15T00:00:00Z` | 预计影响起点 | 事件/评估 | 原始/规则/AI | 是 |
| `horizon_end_time` | timestamp | 否 | `2027-06-30T00:00:00Z` | 预计影响终点；未知时为空 | 事件/评估 | 原始/规则/AI | 是 |
| `impact_channels` | array<string> | 是 | `["capacity","revenue"]` | 传导渠道，如 `revenue/cost/demand/supply/capacity/price/valuation/liquidity/regulatory/reputation` | 事件与关系路径 | 规则/AI/人工 | 是 |
| `certainty` | enum(keyword) | 是 | `confirmed` | `confirmed/expected/speculative/unknown` | 事件状态/证据 | 规则/AI/人工 | 是 |
| `relation_path` | array<object> | 否 | `政策→原材料成本→公司利润` | 影响传导路径 | 实体关系 | 规则/AI | 是 |
| `evidence_news_ids` | array<string> | 是 | `["news_..."]` | 影响判断证据 | 文档/事件成员 | 规则/人工 | 是 |
| `explanation` | text | 是 | `原料降价预计降低公司成本` | 面向用户的方向、周期和路径说明 | 评估流程 | 规则/AI/人工 | 是 |
| `impact_model_version` | string(keyword) | 是 | `impact_v1.0.0` | 影响模型/规则版本 | 影响流程 | 规则/AI | 是 |
| `computed_by` | enum(keyword) | 是 | `hybrid` | `rule/ai/hybrid/manual` | 影响流程 | 采集规则 | 否 |
| `computed_at` | timestamp | 是 | `2026-08-15T06:36:00Z` | 本 revision 计算时间 | 影响流程 | 采集规则 | 是 |
| `revision` | integer | 是 | `2` | 评估修订号 | 评估服务 | 采集规则 | 否 |
| `is_current` | boolean | 是 | `true` | 是否当前有效 | 评估服务 | 采集规则 | 是 |
| `manual_override` | boolean | 是 | `false` | 是否采用人工覆盖 | 审核流程 | 人工 | 是 |
| `manual_lock` | boolean | 是 | `false` | 是否锁定当前人工结论 | 审核流程 | 人工 | 是 |

同一事件允许同时存在：对股票 A 的短期负面影响、对股票 B 的中长期正面影响、对行业的混合影响。不能在 `Event` 顶层只存一个笼统 `positive/negative` 就结束分析。

## 4. 必要关联与辅助对象

八个核心对象不足以完整表达多对多、产业链和版本历史；以下对象是 V1 必需的规范化关系层。

## 4.1 EventDocumentMembership

它明确表达一条新闻如何参与一个事件，也保留重新聚类历史。正常情况下一个文档只有一个“当前主事件”，但模型歧义、事件拆分和历史 revision 允许出现多条记录。

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `membership_id` | string(keyword) | 是 | `edm_evt01_news01_r2` | 成员记录 ID | 聚类服务 | 采集规则 | 否 |
| `event_id` | string(keyword) | 是 | `evt_01K...` | 所属事件 | 聚类结果 | 规则/AI/人工 | 是 |
| `news_id` | string(keyword) | 是 | `news_...` | 成员文档 | 文档 | 投影 | 否 |
| `member_role` | enum(keyword) | 是 | `confirmation` | `initial/confirmation/update/clarification/denial/analysis/reprint/background` | 文档语义 | 规则/AI/人工 | 是 |
| `membership_confidence` | decimal 0..1 | 是 | `0.95` | 属于该事件的置信度 | 聚类流程 | 规则/AI | 是 |
| `is_primary` | boolean | 是 | `false` | 是否当前主要证据文档 | 事件流程 | 规则/AI/人工 | 是 |
| `is_first_report` | boolean | 是 | `false` | 是否按当前口径为首发 | 首发识别 | 规则/AI | 是 |
| `assigned_by` | enum(keyword) | 是 | `ai` | `rule/ai/hybrid/manual` | 聚类流程 | 采集规则 | 否 |
| `cluster_model_version` | string(keyword) | 是 | `cluster_v1.3.0` | 聚类版本，强制记录 | 聚类流程 | 规则/AI | 是 |
| `valid_from` | timestamp | 是 | `2026-08-15T06:20:00Z` | 此归属生效时间 | 聚类服务 | 采集规则 | 否 |
| `valid_to` | timestamp | 否 | `null` | 此归属失效时间 | 聚类服务 | 采集规则 | 是 |
| `revision` | integer | 是 | `2` | 文档归属修订号 | 聚类服务 | 采集规则 | 否 |
| `is_current` | boolean | 是 | `true` | 是否当前归属 | 聚类服务 | 采集规则 | 是 |
| `manual_lock` | boolean | 是 | `false` | 是否禁止自动重新归类 | 审核流程 | 人工 | 是 |

## 4.2 EntityMention

`EntityMention` 保留“原文在哪里提到实体”，避免只留下不可解释的 `stocks[]`。

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `mention_id` | string(keyword) | 是 | `men_news01_0003` | 实体提及记录 ID | 实体流程 | 采集规则 | 否 |
| `target_scope` | enum(keyword) | 是 | `news` | `news/event` | 实体流程 | 采集规则 | 否 |
| `target_id` | string(keyword) | 是 | `news_...` | 文档或事件 ID | 目标对象 | 投影 | 否 |
| `entity_id` | string(keyword) | 是 | `ent_company_600519` | 链接后的实体 | 实体识别/主数据 | 规则/AI/人工 | 是 |
| `surface_form` | string(keyword + text) | 是 | `茅台` | 原文出现形式 | 原文 | 原始/抽取 | 是 |
| `source_field` | enum(keyword) | 是 | `body` | `title/content/body/metadata/summary` | 原文位置 | 采集规则 | 是 |
| `char_start` | integer | 否 | `126` | 在规范文本中的起始偏移 | 实体识别 | 规则/AI | 是 |
| `char_end` | integer | 否 | `128` | 结束偏移 | 实体识别 | 规则/AI | 是 |
| `mention_role` | string(keyword) | 否 | `issuer` | 语义角色，如主体、监管方、供应商、分析师 | 实体识别 | 规则/AI | 是 |
| `link_confidence` | decimal 0..1 | 是 | `0.97` | 名称消歧和实体链接置信度 | 实体流程 | 规则/AI | 是 |
| `extraction_method` | enum(keyword) | 是 | `dictionary_ner` | `metadata/dictionary/ner/llm/manual` | 实体流程 | 采集规则 | 否 |
| `entity_model_version` | string(keyword) | 是 | `entity_link_v2.0.1` | 实体模型/词典版本，强制记录 | 实体流程 | 规则/AI | 是 |
| `computed_at` | timestamp | 是 | `2026-08-15T06:25:00Z` | 计算时间 | 实体流程 | 采集规则 | 是 |
| `revision` | integer | 是 | `2` | 识别修订号 | 实体服务 | 采集规则 | 否 |
| `is_current` | boolean | 是 | `true` | 是否当前有效 | 实体服务 | 采集规则 | 是 |

## 4.3 EntityRelation

`EntityRelation` 构成候选股深挖所需的公司—产品—原材料—供应商—客户—竞争对手—地区—海外公司的关系图。

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `entity_relation_id` | string(keyword) | 是 | `er_company_product_001` | 实体关系记录 ID | 关系服务 | 采集规则 | 否 |
| `from_entity_id` | string(keyword) | 是 | `ent_company_600519` | 有向关系起点 | 实体主数据 | 主数据/投影 | 可治理修正 |
| `to_entity_id` | string(keyword) | 是 | `ent_product_baijiu` | 有向关系终点 | 实体主数据 | 主数据/投影 | 可治理修正 |
| `relation_type` | enum(keyword) | 是 | `produces` | `owns/subsidiary_of/issues/produces/consumes/supplies_to/customer_of/competes_with/member_of/located_in/exposed_to/regulated_by/other` | 主数据/文档 | 原始/规则/AI/人工 | 是 |
| `directionality` | enum(keyword) | 是 | `directed` | `directed/symmetric` | 关系定义 | 主数据/配置 | 否 |
| `valid_from` | timestamp | 否 | `2025-01-01T00:00:00Z` | 关系有效起点 | 证据/主数据 | 原始/规则/人工 | 可治理修正 |
| `valid_to` | timestamp | 否 | `null` | 关系有效终点 | 证据/主数据 | 原始/规则/人工 | 可治理修正 |
| `confidence` | decimal 0..1 | 是 | `0.93` | 关系可信度 | 关系流程 | 规则/AI/人工 | 是 |
| `evidence_news_ids` | array<string> | 否 | `["news_..."]` | 新闻证据 | 文档 | 规则/人工 | 可追加 |
| `provenance_source_ids` | array<string> | 是 | `["company_annual_report"]` | 关系数据来源 | 来源/主数据 | 原始/主数据 | 可追加 |
| `relation_model_version` | string(keyword) | 否 | `supply_chain_v1.0.0` | 机器抽取关系的版本 | 关系流程 | 规则/AI | 是 |
| `computed_at` | timestamp | 是 | `2026-08-15T06:25:00Z` | 建立/计算时间 | 关系流程 | 采集规则 | 是 |
| `revision` | integer | 是 | `1` | 关系修订号 | 关系服务 | 采集规则 | 否 |
| `is_current` | boolean | 是 | `true` | 是否当前有效 | 关系服务 | 采集规则 | 是 |
| `manual_lock` | boolean | 是 | `false` | 是否人工锁定 | 审核流程 | 人工 | 是 |

## 4.4 ClassificationAssignment

自动分类需要独立版本记录；`NewsDocument` 和 `Event` 顶层分类字段只是当前搜索投影。

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `classification_id` | string(keyword) | 是 | `cls_evt01_r2` | 分类记录 ID | 分类服务 | 采集规则 | 否 |
| `target_scope` | enum(keyword) | 是 | `event` | `news/event` | 分类流程 | 采集规则 | 否 |
| `target_id` | string(keyword) | 是 | `evt_...` | 目标文档或事件 | 目标对象 | 投影 | 否 |
| `taxonomy_id` | string(keyword) | 是 | `news_taxonomy_v1` | 分类体系及版本 | 分类配置 | 主数据/配置 | 是 |
| `primary_category` | string(keyword) | 是 | `policy.monetary` | 主分类 | 分类流程 | 规则/AI/人工 | 是 |
| `category_codes` | array<string> | 是 | `["policy.monetary","macro.liquidity"]` | 多标签分类 | 分类流程 | 规则/AI/人工 | 是 |
| `topic_tags` | array<string> | 否 | `["降准"]` | 开放主题标签 | 分类流程 | 规则/AI/人工 | 是 |
| `confidence` | decimal 0..1 | 是 | `0.94` | 分类置信度 | 分类流程 | 规则/AI | 是 |
| `classification_model_version` | string(keyword) | 是 | `news_cls_v1.2.0` | 分类规则/模型版本 | 分类流程 | 规则/AI | 是 |
| `computed_at` | timestamp | 是 | `2026-08-15T06:26:00Z` | 计算时间 | 分类流程 | 采集规则 | 是 |
| `revision` | integer | 是 | `2` | 分类修订号 | 分类服务 | 采集规则 | 否 |
| `is_current` | boolean | 是 | `true` | 是否当前有效 | 分类服务 | 采集规则 | 是 |
| `manual_override` | boolean | 是 | `false` | 是否人工覆盖 | 审核流程 | 人工 | 是 |

建议 V1 一级分类至少容纳：`market`、`macro`、`policy`、`regulatory`、`exchange`、`corporate`、`announcement`、`industry`、`commodity`、`overseas`、`research`。二级类型另由 `taxonomy_id` 版本化，避免把分类表硬编码进字段契约。

## 4.5 EntityAlias

`EntityAlias` 是实体名称匹配的正式时序对象。`Entity.aliases` 仅保留当前有效别名的兼容搜索投影，不再承担别名类型、历史有效期或证据职责。

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `entity_alias_id` | string(keyword) | 是 | `ealias_600519_former_001` | 别名记录稳定 ID | 实体主数据服务 | 采集规则 | 否 |
| `entity_id` | string(keyword) | 是 | `ent_company_600519` | 别名所指向的规范实体 | `Entity` | 主数据/投影 | 可治理修正 |
| `alias` | string(keyword + text) | 是 | `贵州茅台股份` | 未丢失字符信息的别名展示值 | 证券主数据、官网、公告或文档 | 原始/主数据/抽取 | 可从证据重建 |
| `normalized_alias` | string(keyword) | 是 | `贵州茅台股份` | 用于精确召回和消歧的规范化形式 | 别名规范化 | 采集规则 | 是 |
| `alias_type` | enum(keyword) | 是 | `short_name` | `official_name/stock_short_name/short_name/former_name/historical_name/brand/subsidiary_name/english_name/abbreviation/ticker/other` | 主数据/语义抽取 | 主数据/规则/AI/人工 | 可治理修正 |
| `language` | string(keyword) | 是 | `zh-CN` | 别名语言 | 来源/检测 | 原始/规则 | 是 |
| `valid_from` | timestamp | 否 | `2010-01-01T00:00:00+08:00` | 名称开始有效时间；未知时为空，不得伪造 | 主数据/公告 | 原始/主数据/规则 | 可治理修正 |
| `valid_to` | timestamp | 否 | `2020-06-30T23:59:59+08:00` | 名称结束有效时间；仍有效或未知时为空 | 主数据/公告 | 原始/主数据/规则 | 可治理修正 |
| `provenance_source_ids` | array<string> | 是 | `["instrument_info","cninfo"]` | 证明别名的来源 ID | 来源/主数据 | 原始/主数据 | 可追加 |
| `provenance_refs` | array<object> | 否 | `[{"news_id":"news_...","url":"https://..."}]` | 原始记录、公告或页面等可审计引用 | 来源/文档 | 原始/主数据 | 可追加 |
| `confidence` | decimal 0..1 | 是 | `0.98` | 别名指向该实体的可信度 | 主数据治理/实体模型 | 规则/AI/人工 | 是 |
| `derived_by` | enum(keyword) | 是 | `master_data` | `master_data/source_metadata/rule/ai/manual` | 别名流程 | 采集规则 | 否 |
| `entity_model_version` | string(keyword) | 条件 | `entity_alias_v1.0.0` | 规则/AI 生成或消歧时必填 | 实体流程 | 规则/AI | 是 |
| `revision` | integer | 是 | `2` | 同一别名事实的修订号 | 实体服务 | 采集规则 | 否 |
| `is_current` | boolean | 是 | `false` | 是否为当前名称匹配候选；历史别名仍保留并可按新闻时间匹配 | 有效期/实体治理 | 规则/人工 | 是 |
| `manual_lock` | boolean | 是 | `false` | 是否禁止模型自动替换当前治理结论 | 审核流程 | 人工 | 是 |
| `created_at` | timestamp | 是 | `2026-08-15T00:00:00Z` | 别名首次建档时间 | 实体服务 | 采集规则 | 否 |
| `updated_at` | timestamp | 是 | `2026-08-15T06:00:00Z` | 最近修订时间 | 实体服务 | 采集规则 | 是 |

历史新闻实体匹配应以 `NewsDocument.publish_time` 与 `valid_from/valid_to` 相交为优先证据，再结合上下文消歧。子公司名称原则上指向子公司自己的 `entity_id`，再通过 `EntityRelation` 连接母公司；只有明确的搜索兼容需求才把它投影为母公司的扩展召回词。

## 4.6 SourceHealth

`SourceHealth` 是按来源产生的高频运行观测，与 `Source` 主数据 revision 完全分离。它既可以保留时间窗口历史，也可以投影一份每源当前快照。

| 字段名 | 数据类型 | 必填 | 示例 | 字段含义 | 字段来源 | 原始/生成 | 可重算 |
|---|---|---:|---|---|---|---|---:|
| `source_health_id` | string(keyword) | 是 | `shealth_csrc_20260815T063000Z` | 一次健康观测的稳定 ID | 监控服务 | 采集规则 | 否 |
| `source_id` | string(keyword) | 是 | `csrc` | 指向静态 `Source` | 来源注册表/任务 | 投影 | 否 |
| `observed_at` | timestamp | 是 | `2026-08-15T06:30:00Z` | 本次健康快照生成时间 | 监控时钟 | 采集规则 | 否 |
| `window_start` | timestamp | 是 | `2026-08-15T06:15:00Z` | 指标统计窗口起点 | 监控任务 | 采集规则 | 可按原始运行日志重算 |
| `window_end` | timestamp | 是 | `2026-08-15T06:30:00Z` | 指标统计窗口终点 | 监控任务 | 采集规则 | 可按原始运行日志重算 |
| `health_status` | enum(keyword) | 是 | `degraded` | `healthy/degraded/down/disabled/unknown` | 运行指标 + 健康策略 | 采集规则 | 是 |
| `last_success_at` | timestamp | 否 | `2026-08-15T06:20:00Z` | 最近一次成功采集结束时间 | 任务运行日志 | 原始/采集规则 | 是 |
| `last_attempt_at` | timestamp | 否 | `2026-08-15T06:29:30Z` | 最近一次发起采集尝试时间 | 任务运行日志 | 原始/采集规则 | 是 |
| `consecutive_failures` | integer | 是 | `3` | 截至观测时连续失败次数 | 任务运行日志 | 采集规则 | 是 |
| `latency_ms` | integer | 否 | `842` | 最近一次尝试的端到端延迟；聚合延迟另放完整性指标 | 任务运行日志 | 原始/采集规则 | 是 |
| `last_item_publish_time` | timestamp | 否 | `2026-08-15T06:18:00Z` | 最近取得内容中的最新发布时间 | 采集结果 | 原始/采集规则 | 是 |
| `data_delay_seconds` | integer | 否 | `720` | 观测时刻相对 `last_item_publish_time` 的数据延迟；未知时为空 | 时间字段 | 采集规则 | 是 |
| `attempt_count` | integer | 是 | `4` | 统计窗口内尝试次数 | 任务运行日志 | 采集规则 | 是 |
| `success_count` | integer | 是 | `3` | 统计窗口内成功次数 | 任务运行日志 | 采集规则 | 是 |
| `collected_item_count` | integer | 是 | `128` | 统计窗口内采集到的总条数 | 采集报告 | 采集规则 | 是 |
| `new_item_count` | integer | 是 | `17` | 统计窗口内新入库条数，不含精确重复 | 入库报告 | 采集规则 | 是 |
| `empty_success_count` | integer | 是 | `0` | 请求成功但结果为空的次数 | 采集报告 | 采集规则 | 是 |
| `parse_failure_count` | integer | 是 | `2` | 统计窗口内解析失败数量 | 采集报告 | 采集规则 | 是 |
| `completeness_status` | enum(keyword) | 是 | `warning` | `ok/warning/critical/unknown`，用于源级完整性监控 | 完整性策略 | 采集规则 | 是 |
| `completeness_metrics` | object | 否 | `{"success_rate":0.75,"latency_p95_ms":1200}` | 可扩展指标快照；常用指标应提升为显式 mapping，任意键关闭动态索引 | 运行日志/采集报告 | 采集规则 | 是 |
| `last_error_code` | string(keyword) | 否 | `HTTP_503` | 最近一次失败的规范错误码 | 任务运行日志 | 原始/采集规则 | 是 |
| `last_error_summary` | text | 否 | `上游服务暂时不可用` | 脱敏后的最近错误摘要 | 任务运行日志 | 原始/采集规则 | 是 |
| `health_policy_version` | string(keyword) | 是 | `source_health_policy_v1` | 状态和完整性判定策略版本；本阶段不定义算法 | 监控配置 | 主数据/配置 | 是 |
| `is_current` | boolean | 是 | `true` | 是否为该来源当前快照；历史观测仍保留 | 监控服务 | 采集规则 | 是 |
| `created_at` | timestamp | 是 | `2026-08-15T06:30:01Z` | 观测记录写入时间 | 监控服务 | 采集规则 | 否 |

高频观测更新只产生新的 `SourceHealth` 记录或覆盖当前健康投影，不改变 `Source.source_revision`。`health_status=disabled` 可以投影 `Source.enabled=false` 的调度结果，但运行层不得反向、无审计地修改静态来源配置。

## 5. 业务读取方式

### 5.1 全市场重要新闻雷达

雷达应以 `Event` 为结果单元，而不是直接把所有 `NewsDocument` 排列出来：

1. 用 `event_status`、时间、分类、`Importance.importance_grade/score` 筛选和排序事件。
2. 展示 `canonical_title`、`canonical_summary`、首发来源、主要证据、来源数、事件状态。
3. 用事件级 `StockRelation`、`IndustryRelation` 和 `ImpactAssessment` 展示受影响股票/行业、方向和周期。
4. 展开事件时再返回成员文档，区分首发、确认、更新、澄清和否认。
5. 暂未聚类的新文档可用 `Importance(target_scope=news, status=provisional)` 临时进入候选队列，但形成事件后以事件级评估为准。

### 5.2 候选股新闻深挖

输入规范 A 股代码后：

1. 通过 `Entity` 与 `EntityAlias` 把代码解析到股票实体、发行人公司、当前别名、曾用名和历史名称，并按新闻发布时间做时序匹配。
2. 先查直接 `StockRelation`：`subject/issuer/announcement/research_coverage/mentioned`。
3. 再沿行业/概念和 `EntityRelation` 扩展：产品、原材料、供应商、客户、竞争对手、政策作用范围和海外实体。
4. 用 `relation_type`、`relation_depth`、`stock_relevance_score` 和 `relation_reason` 控制扩展与解释。
5. 按候选股维度读取 `ImpactAssessment`，而不是使用事件全局 sentiment 代替个股影响。
6. 最终仍按 `event_id` 聚合，避免同一事件因多家转载重复展示；需要证据时返回全部或精选 `NewsDocument`。

候选股结果建议按以下业务栏目分组，但底层仍复用同一模型：

| 栏目 | 主要关系/文档条件 |
|---|---|
| 公司新闻 | 直接公司实体提及，`relation_type=subject/mentioned` |
| 公司公告 | `document_type=announcement/filing` 且 `relation_type=issuer/announcement` |
| 行业新闻 | 经 `IndustryRelation` 或 `industry_exposure/concept_exposure` 关联 |
| 政策新闻 | `event_type` 属政策/监管，且关系为 `policy_beneficiary/policy_affected` |
| 上下游产业链 | `relation_path` 含产品、原材料、供应商或客户，`relation_depth > 0` |
| 海外相关事件 | 事件地区非中国或含海外实体，经 `overseas_mapping`/产业链关系连接 A 股 |
| 券商研报/机构观点 | `document_type=research` 且 `research_coverage` 指向股票 |

## 6. 事件生命周期、首发与重新计算

### 6.1 事件状态

| 状态 | 业务含义 | 典型变化 |
|---|---|---|
| `developing` | 事件仍在发展，事实不完整或只有初始线索 | 可转 `confirmed/clarified/denied/expired` |
| `confirmed` | 已有足够可靠证据或官方确认 | 可因新事实转 `clarified`，最终 `expired` |
| `clarified` | 原报道存在重要补充、修正或边界澄清 | 可回到 `developing/confirmed` 或转 `denied/expired` |
| `denied` | 核心说法被可信来源否认；事件和原始报道都不能删除 | 新证据出现时可回到 `developing/confirmed` |
| `expired` | 观察窗口结束或不再具有当前性；仍可历史检索 | 新证据可重新激活为 `developing` |

事件状态变化必须记录新 `event_revision`、`status_reason`、`status_evidence_news_ids` 和 `status_changed_at`。否认或过期不删除原始文档，也不篡改当时的评估。

### 6.2 事件合并与拆分

- **合并**：保留被合并事件及其 ID，写 `merged_into_event_id`；在线查询重定向到目标事件，历史关系仍可审计。
- **拆分**：新建一个或多个 `event_id`，写 `split_from_event_id`，成员关系产生新 revision；不得复用原事件 ID 表示两个事实。
- **相关但不同**：写 `related_event_ids` 或独立事件关系，不应为了减少条数强行合并。

### 6.3 首发识别

首发识别至少同时考虑并保存：

- 来源自己的 `publish_time` 与其精度/是否估算；
- 系统实际 `collect_time`；
- 来源是否原创、转载或聚合；
- 来源权威与内容证据；
- 当前首发识别的覆盖范围和置信度。

首发结论可随迟到数据、时间修正和来源链证据重算。旧的 `EventDocumentMembership` revision 继续保留，因此可以回答“系统当时认为谁首发”和“现在认为谁首发”。

### 6.4 模型版本与重算协议

| 任务 | 强制版本字段 | 重算后的处理 |
|---|---|---|
| 实体识别/链接 | `entity_model_version` | 新增 Mention/Relation revision，刷新文档/事件实体投影 |
| 事件聚类 | `cluster_model_version` | 新增 Membership revision，必要时拆并 Event，刷新 `event_id` 投影 |
| 重要度 | `importance_model_version` | 新增 Importance revision，旧评估 `is_current=false` |
| 情绪 | `sentiment_model_version` | 新增 Sentiment revision，不覆盖旧结果 |
| 影响 | `impact_model_version` | 新增 Impact revision，不覆盖旧结果 |
| 分类 | `classification_model_version` | 新增 Classification revision，刷新当前分类投影 |
| 向量 | `embedding_model_version` | 新向量字段或新索引代际，不允许维度不兼容覆盖 |

重算的共同要求：

- 保存 `computed_at`、模型/规则版本、输入证据 ID、revision 和 `is_current`。
- 人工覆盖与机器原始输出分别保存；`manual_lock=true` 时，新模型结果可以写入候选 revision，但不能自动成为当前结果。
- 输入原文变化、模型变化、主数据变化和人工修正应能区分触发原因；后续实现可增加统一 `recompute_reason` 与 `job_run_id`。
- 模型结果回滚应通过切换当前 revision 完成，不删除中间版本。

## 7. OpenSearch 索引设计

### 7.1 总体原则：规范对象与搜索投影分离

OpenSearch 不适合在查询时跨多个索引做复杂 join。V1 建议把规范化对象和历史 revision 作为权威记录，同时在文档与事件搜索索引中冗余“当前有效快照”。关系历史仍放独立索引或后续关系型存储。

不建议使用 OpenSearch parent-child 关系承载核心模型；它会增加路由和更新复杂度。使用稳定 ID + 反规范化当前投影更符合现有按年索引与 create-only 底座。

### 7.2 建议索引/别名

| 逻辑索引 | 建议物理索引 | 主要用途 | 写入语义 |
|---|---|---|---|
| 文档 | `news-documents-v1-{year}`，别名 `news-documents` | 原文检索、证据展开、临时未聚类流 | `news_id` create-only；派生投影受控更新 |
| 兼容文档 | 现有 `news-{year}`，别名 `news` | 平滑迁移现有搜索与回填 | V1 过渡期保留，不立即推倒 |
| 事件 | `news-events-v1-{year}`，别名 `news-events` | 雷达、候选股去重结果、事件状态 | `event_id` 可 revision 更新，使用并发控制 |
| 事件成员 | `news-event-memberships-v1-{year}` | 聚类历史和证据角色 | 追加 revision |
| 实体 | `news-entities-v1` | 规范实体检索和当前别名投影 | 版本化更新 |
| 实体别名 | `news-entity-aliases-v1` | 别名类型、时序匹配和溯源 | 追加 revision，维护当前投影 |
| 实体提及 | `news-entity-mentions-v1-{year}` | 证据定位、实体模型历史 | 追加 revision |
| 实体关系 | `news-entity-relations-v1` | 产业链、公司与海外映射 | 追加 revision/有效期 |
| 股票关系 | `news-stock-relations-v1-{year}` | 个股深挖和影响解释 | 追加 revision |
| 行业关系 | `news-industry-relations-v1-{year}` | 行业/概念筛选与深挖 | 追加 revision |
| 评估 | `news-assessments-v1-{year}` 或按类型拆分 | importance/sentiment/impact 历史 | 追加 revision |
| 来源 | `news-sources-v1` | 静态来源治理、权威、许可与地址 | 仅治理变化时版本化更新 |
| 来源健康当前态 | `news-source-health-current-v1` | 每个来源的最新运行状态 | `_id=source_id` 高频受控覆盖 |
| 来源健康历史 | `news-source-health-history-v1-*` | 运行趋势、完整性和 SLA 分析 | 按 `source_health_id` 追加写入 |

`{year}` 的路由基准：文档按 `publish_time`，缺失异常回退规则应显式记录；事件按 `first_publish_time`。跨年持续事件仍留在初始年份，通过别名检索。

### 7.3 文档索引当前投影

在现有 mapping 上增量兼容：

- `pub_time` 在 V1 逻辑层改称 `publish_time`，迁移期双读/别名兼容；`fetch_time` 对应 `collect_time`。
- `source` 对应 `source_id`；`stocks` 对应 `stock_codes`。迁移期保留旧字段，避免现有查询立即失效。
- `title/content/summary/body` 继续使用 `text`，`title.raw` 继续保留精确匹配。
- `news_id/event_id/source_id/document_type/channel/status/model_version` 类字段用 `keyword`。
- `source_authority_status` 用 `keyword`，`source_authority` 保持可空数值；查询必须区分“未评级”与“已评级低分”，不能用 `0` 填补缺失值。
- `publish_time/collect_time/computed_at` 用 `date`。
- 分数用 `scaled_float`；不要依赖动态 mapping 推断。
- `entity_ids/stock_codes/industry_ids/category_codes/topic_tags` 使用多值 `keyword` 便于过滤。
- 文档级 `stock_relations` 如果内嵌，必须映射为 `nested`；不能用普通 object 后再把同一股票的 `relation_type` 与另一股票的分数错误组合。
- `score_components`、原始载荷和不稳定 `external_ids` 建议 `enabled:false` 或明确白名单字段，防止 mapping explosion。

### 7.4 事件索引读取投影

`news-events` 是雷达和候选股去重读取的主索引。每个事件文档建议内嵌当前快照：

- 当前 `Importance`：`importance_score`、`importance_grade`、`importance_confidence`、`importance_model_version`；
- 当前事件整体 sentiment，仅作展示，不代替目标 impact；
- `stock_relations`：`nested`，至少含 `stock_code/relation_type/stock_relevance_score/impact_direction/impact_horizon/relation_reason`；
- `industry_relations`：`nested`，至少含 `industry_system/industry_code/relation_type/industry_relevance_score/impact_direction`；
- `source_ids/news_count/source_count/first_source_id/primary_news_id/source_authority_statuses`；
- `event_status/category_codes/topic_tags/entity_ids`；
- `canonical_title/canonical_summary/event_vec`。

候选股查询先对同一个 nested `stock_relations` 对象限定 `stock_code`，再在该 nested 对象内读取相关度、方向和理由，避免跨股票字段串配。雷达排序只读取 `is_current` 投影，历史审计再查规范评估索引。

### 7.5 检索分数命名

| 分数 | 业务含义 | 所属对象 | 是否持久化 |
|---|---|---|---|
| OpenSearch `_score` / RRF score | 当前查询的文本/向量相关性 | 查询结果 | 通常不持久化 |
| `importance_score` | 对全市场的全局重要程度 | Importance | 是，版本化 |
| `stock_relevance_score` | 与某一只股票的关联强度 | StockRelation | 是，逐股票版本化 |
| `industry_relevance_score` | 与某一行业/概念的关联强度 | IndustryRelation | 是，逐行业版本化 |
| `sentiment_score` | 文本或事件表达倾向 | SentimentAssessment | 是，版本化 |
| `impact_strength` | 对指定目标的影响强弱 | ImpactAssessment | 是，逐目标和周期版本化 |

### 7.6 向量与索引迁移

- 现有 `content_vec` 1024 维可以继续复用，但必须补 `embedding_model_version`。
- 事件可另建 `event_vec`，不能默认等于某一篇新闻向量。
- 向量维度或模型不兼容时创建新字段或新索引代际，通过别名切换；不可在同一 mapping 中静默更换维度。
- 原始归档继续作为最终证据和重放来源，OpenSearch 只承担检索与当前读模型，不替代 NAS 原始归档。

### 7.7 SourceHealth 与 EntityAlias 投影

- `news-sources-v1` 只保存静态 Source；健康状态不得内嵌回 Source 文档并触发其 revision。
- `news-source-health-current-v1` 每个来源只保留一个当前文档，适合运行看板和调度判断；`news-source-health-history-v1-*` 追加时间窗口观测，适合趋势、延迟、连续空窗和 SLA 分析。
- SourceHealth 中状态/错误码用 `keyword`，各时间点用 `date`，计数和毫秒/秒值用 `long`，比例和分位值用 `scaled_float`。`completeness_metrics` 对任意键关闭动态索引，稳定高频指标再升级为显式字段。
- `news-entity-aliases-v1` 以 `normalized_alias`、`alias_type`、`entity_id`、`valid_from/valid_to`、`is_current` 建显式 mapping；候选股解析先检索别名索引，再按目标新闻时间和置信度消歧。
- `news-entities-v1.aliases` 只冗余当前有效、允许搜索的字符串数组。历史名称、来源证据和 revision 不嵌入该数组，统一回查 EntityAlias。

## 8. 一致性与校验规则

1. `news_id != event_id`，且两个 ID 属于不同命名空间。
2. `EventDocumentMembership` 负责 Event—NewsDocument 多对多；事件至少有一条当前成员。
3. `StockRelation` 和 `IndustryRelation` 的 `news_id/event_id` 必须严格二选一，与 `target_scope` 一致。
4. `Importance` 的 `news_id/event_id` 必须严格二选一；正式雷达事件必须有一条当前事件级 Importance。
5. 一个目标在同一模型族/用途下只能有一个 `is_current=true` 的 revision；历史记录不限。
6. `source_authority_status`、`publish_time`、`collect_time` 在正式 `NewsDocument` 中必须存在；`source_authority_status=unrated` 时 `source_authority` 必须为空，禁止填 `0` 或默认分；`rated` 时数值必填。
7. `publish_time_is_estimated=true` 时不得假装秒级精确；必须同时保存 `publish_time_precision`。
8. `is_event_first_report=true` 时必须有 `first_report_scope`、`first_report_confidence`，并对应当前成员关系。
9. `Importance` 不得写入 `stock_relevance_score`；StockRelation 不得用 OpenSearch `_score` 充当相关度。
10. `ImpactAssessment.target_scope=stock_relation` 时必须只表达该关系所指股票的影响，不得复制成所有股票相同方向。
11. 原始文档被更正、撤回或来源删除时更新 `document_status`，不硬删除证据；事件状态按新证据另行演进。
12. 所有自动生成关系和评估都必须能追溯到模型版本、计算时间与证据；无证据的强结论应标为低置信或 `uncertain`。
13. 顶层数组如 `stock_codes`、`industry_ids` 只是当前检索投影，规范事实以关系对象为准。
14. 人工覆盖不得物理覆盖机器结果，必须保留 `manual_override/manual_lock/reviewer/reason` 审计信息。
15. API、索引和离线文件中的枚举统一使用英文稳定代码，中文只用于展示名称和解释。
16. SourceHealth 的任何运行观测不得增加 `Source.source_revision`；只有名称、类型、权威、许可、地址等静态治理信息变化才修订 Source。
17. `Entity.aliases` 只允许由当前有效 `EntityAlias` 生成；别名类型、有效期、来源和历史 revision 不能只存在于扁平数组中。
18. `EntityAlias.valid_from > valid_to` 非法；`is_current=false` 的历史别名仍可在其有效时间范围内用于历史新闻匹配。

## 9. 与当前项目的兼容迁移原则

本模型不要求立即替换现有 `news-{year}` 或重写采集器。建议按以下边界演进：

- 现有采集信封先原样进入 `NewsDocument`；字段规范化可在 enrichment 层完成。
- 当前 create-only 原文契约继续保留；正文、向量和状态仍可走现有受控补齐通道。
- 新的实体、聚类、关系、重要度和影响结果优先写独立版本化索引，再把当前快照投影回文档/事件索引。
- 现有 `stocks[]` 作为初始候选提示和兼容过滤字段，不升级为权威关系；权威解释转为 `StockRelation`。
- 当前 `sector_stock` 作为行业主数据输入，不直接等同某条新闻的 `IndustryRelation`。
- `Entity.aliases` 继续作为搜索兼容投影；新增或修订别名时以 `EntityAlias` 为规范记录，支持曾用名和历史有效期。
- `Source` 保持低频主数据；运行成功、失败、延迟与完整性写 `SourceHealth`，不扰动来源主数据 revision。
- 当前 BM25/kNN/hybrid/RRF 能继续作为检索底座，但搜索排序需显式组合业务分数；本阶段不定义组合算法。

## 10. 研报与公告的扩展位置

为了不让 `NewsDocument` 被各类稀疏字段撑大，V1 只在核心文档保留 `document_type` 和 `ann_type`。后续可用一对一扩展对象：

- `AnnouncementDetail`：公告类型、报告期、金额、交易对手、关键日期、结构化指标。
- `ResearchReportDetail`：机构实体、分析师实体、报告类型、覆盖股票、评级/评级变化、目标价、盈利预测、核心观点、原文权限。

这些扩展对象的所有结论仍必须指向 `news_id`，机构/分析师/股票使用 `entity_id`，机器抽取字段记录抽取模型版本和证据。具体字段口径、授权边界和数值标准化留待专项设计，不阻塞统一新闻模型 V1。

## 11. 字段变更清单

### A. 可以直接复用当前项目的字段

以下字段含义基本稳定，可以直接进入 `NewsDocument`，仅补充显式约束或版本信息：

| 当前字段 | V1 位置 | 说明 |
|---|---|---|
| OpenSearch `_id` | `NewsDocument.news_id` | 继续复用当前确定性 ID；同时写入 `_source.news_id` |
| `title` | `NewsDocument.title` | 保留 `text + title.raw keyword` |
| `content` | `NewsDocument.content` | 保留 Feed/API 文本语义 |
| `summary` | `NewsDocument.summary` | 可复用；新增 `summary_type` 区分来源/机器 |
| `body` | `NewsDocument.body` | 继续承载网页或 PDF 全文 |
| `channel` | `NewsDocument.channel` | 继续作为采集粗分类，不代替新事件分类 |
| `url` | `NewsDocument.url` | 原地址保留不变 |
| `vec_status` | `NewsDocument.vec_status` | 继续使用现有向量状态机 |
| `content_vec` | `NewsDocument.content_vec` | 现有 1024 维向量可复用，须补模型版本 |
| `ann_type` | `NewsDocument.ann_type` | 保留并真正填充；详细公告结构后续拆表 |
| `pdf_status` | `NewsDocument.pdf_status` | 复用现有 PDF 处理状态 |
| `body_status` | `NewsDocument.body_status` | 复用现有全文处理状态 |
| `body_truncated` | `NewsDocument.body_truncated` | 复用当前动态字段并纳入正式 mapping |
| `raw_title` | 原始归档/`NewsDocument.raw_title` | 继续主要存原始归档 |
| `raw_content` | 原始归档/`NewsDocument.raw_content` | 继续主要存原始归档 |
| `time_estimated` | `NewsDocument.publish_time_is_estimated` | 语义可直接复用，名称建议规范化 |

### B. 需要新增的字段

按能力分组，V1.1 必须新增：

- 文档标识与治理：`news_id` 显式源字段、`schema_version`、`source_native_id`、`source_authority_status`、可空 `source_authority`、`source_authority_version`、`document_type`、`language`、`authors`、`canonical_url`、`raw_archive_uri`、`content_license`、`document_status`。
- 时间质量：`publish_time_raw`、`publish_time_precision`、`last_seen_time`、`source_update_time`、`created_at`、`updated_at`。
- 去重与聚类：哈希字段、`duplicate_type`、`duplicate_of_news_id`、文档当前 `event_id` 投影、`event_membership_confidence`、首发字段、`cluster_model_version`。
- 分类与实体：`primary_category`、`category_codes`、`topic_tags`、分类版本/置信度、`entity_ids`、`industry_ids`、`entity_model_version`；正式新增 `EntityAlias` 保存别名类型、时效、来源和置信度。
- 事件全部字段：独立 `event_id`、事件 revision、标题/摘要、五态生命周期、首发、成员统计、聚类置信度、事件向量、合并/拆分/关联字段。
- 关系全部字段：`EventDocumentMembership`、`EntityMention`、`EntityRelation`、`StockRelation`、`IndustryRelation`。
- 评估全部字段：`Importance`、`SentimentAssessment`、`ImpactAssessment`，包括各自分数、置信度、证据、周期、版本与人工覆盖。
- 来源治理：`Source` 的来源类别、采集类型、原创/聚合属性、`authority_status`、可空权威分与版本、地区、语言、时区、许可和预期频率。
- 来源运行：新增 `SourceHealth` 的健康状态、最近尝试/成功、连续失败、延迟、最新内容时间、数据延迟和完整性监控字段。
- 模型与审计：`importance_model_version`、`sentiment_model_version`、`entity_model_version`、`cluster_model_version`，以及其他分类、关系、影响、摘要、向量版本。

### C. 当前字段需要改名/扩展的字段

| 当前/V1字段 | V1.1 建议 | 变更类型 | 原因 |
|---|---|---|---|
| `pub_time` | `publish_time` | 改名，迁移期双读 | 与业务语义一致，并配套原始值、精度、估算标记 |
| `fetch_time` | `collect_time` | 改名，迁移期双读 | 明确是首次采集时间，不是任意 HTTP 请求时间 |
| `source` | `source_id` | 改名/外键化 | 指向统一 Source，并补权威快照 |
| `stocks` | `stock_codes` + `StockRelation` | 扩展 | 数组只作过滤投影；关系对象记录相关度、类型、路径、证据和版本 |
| `channel` | 保留 `channel` + 新增 `document_type/category_codes/event_type` | 扩展 | 当前通道是采集来源分组，不能承担业务分类全部语义 |
| `summary` | `summary` + `summary_type` + 摘要模型版本 | 扩展 | 区分来源摘要、抽取摘要和 AI 摘要 |
| `content_vec` | 保留 + `embedding_model_version` | 扩展 | 支持重算、回滚和维度迁移 |
| `title.raw` | 继续保留，另增加规范化哈希 | 扩展 | 精确标题 collapse 不能替代近重复和事件聚类 |
| OpenSearch `_score` | 保持查询临时值 | 语义澄清 | 不得改名冒充 `importance_score` 或股票相关度 |
| V1 `Source.source_authority` | `authority_status` + 可空 `source_authority` | 扩展/放宽必填 | 未评级不等于低分，禁止伪造占位数字 |
| V1 `Source.health_status/last_success_at` | 独立 `SourceHealth` | 移动/扩展 | 高频运行状态不再制造 Source 主数据 revision |
| V1 `Entity.aliases` | 保留兼容投影 + 正式 `EntityAlias` | 扩展 | 支持别名类型、来源、置信度和历史有效期 |

### D. 暂时无法确定、以后再研究的字段

以下内容需要数据样本、业务评测或授权研究后再定，不应在 V1 凭空固化：

1. `importance_score` 到 S/A/B/C 的具体阈值、各特征权重和状态升级/降级算法。
2. 事件聚类的时间窗、相似度阈值、多语言跨境聚类规则、事件自动过期规则。
3. `source_authority` 的精确评分办法、`unrated/provisional/rated` 的治理审批与转换流程、不同栏目是否需要不同权威分，以及媒体原创识别的外部依据。
4. `stock_relevance_score`、`industry_relevance_score`、`impact_strength` 的标定集、阈值和多跳衰减方式。
5. `intraday/short_term/medium_term/long_term/structural` 的精确时间边界。
6. 申万、证监会、GICS、同花顺、自定义概念之间的主行业体系优先级及跨体系映射口径。
7. 产业链关系的数据授权、更新频率、关系有效期和海外公司到 A 股的映射来源。
8. 研报结构化字段的授权边界、评级标准化、目标价币种/复权、盈利预测口径。
9. 公告按类型的结构化子模型及金额、报告期、交易对手等字段的统一单位和精度。
10. 事件 canonical title/summary 的模型选择、事实一致性评测和版权边界。
11. OpenSearch 历史 revision 与规范化权威库最终采用“全量独立索引”还是“关系数据库 + 搜索投影”的部署选择。
12. 事件和影响向量的模型、维度、多语言策略以及索引保留周期。
13. 人工审核角色、审批流、S 级事件告警权限和 SLA。
14. 数据保存期限、撤回/更正处理、付费内容、境外内容和个人信息的合规规则。

---

V1.1 的落点是：保留当前可靠的采集、原始归档、确定性文档 ID 和 OpenSearch 检索底座，在其上增加独立的事件层、实体关系层和版本化评估层，并用可空评级、独立来源健康和时序别名消除 V1 的三处治理歧义。这样既不会破坏已有数据，也能让同一份新闻证据同时支撑去重后的全市场雷达与可解释的候选股深挖。
