-- etf_minute（既有 chenyee 分区表，采纳为 ETF 1min）原**无任何唯一约束**，
-- 补历史/run 重试/重叠 backfill 会写入重复行（save_to_postgres 用 ON CONFLICT DO NOTHING，
-- 无唯一约束则无冲突可依据，不去重）。
-- 2026-07-06 一次性去重 + 建整表唯一索引，使写入幂等（与 etf_daily_kline 等其他 ETF 表一致）。
-- 背景：2024_05 分区曾整月双灌，446,332 行精确重复（值完全相同，无损）；其余 139 个数据分区本就干净。
-- 属主为 ai_read（已过户），可自行执行。

-- 1) 去重 2024_05：每个 (代码,日期,时间) 保留最小 ctid 的一行
DELETE FROM etf_1min_1day_2024_05
WHERE ctid IN (
  SELECT ctid FROM (
    SELECT ctid,
           row_number() OVER (PARTITION BY "代码", "日期", "时间" ORDER BY ctid) AS rn
    FROM etf_1min_1day_2024_05
  ) t WHERE t.rn > 1
);

-- 2) 整表唯一索引（分区唯一索引必须含分区键「日期」）
--    建成后 save_to_postgres 的 ON CONFLICT DO NOTHING 生效，补历史不再重复。
--    注：390M 行建索引约数分钟且期间锁写，建议盘后执行。
CREATE UNIQUE INDEX IF NOT EXISTS etf_minute_uk
  ON etf_minute ("代码", "日期", "时间");
