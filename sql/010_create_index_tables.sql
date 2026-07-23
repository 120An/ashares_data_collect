-- 指数行情采集建表（见 docs/superpowers/specs/2026-07-21-index-collection-design.md）
-- 属主 ai_read 自建；代码带点（000300.SH）；不复权；字段纯 QMT OHLCV+amount。

-- 指数日线（不分区，全历史 backfill；量级同 daily_kline ~240 万行）
CREATE TABLE IF NOT EXISTS index_daily (
    index_code  VARCHAR(20) NOT NULL,
    trade_date  DATE        NOT NULL,
    open   DOUBLE PRECISION,
    high   DOUBLE PRECISION,
    low    DOUBLE PRECISION,
    close  DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    amount DOUBLE PRECISION,
    PRIMARY KEY (index_code, trade_date)
);

-- 指数分钟线（月分区，前向积累；历史 1m 不可回补，见 spec §2.3）
CREATE TABLE IF NOT EXISTS index_minute (
    index_code  VARCHAR(20) NOT NULL,
    trade_date  DATE        NOT NULL,   -- 分区键
    bar_time    TIME        NOT NULL,   -- 北京时间 HH:MM:SS
    open   DOUBLE PRECISION,
    high   DOUBLE PRECISION,
    low    DOUBLE PRECISION,
    close  DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    amount DOUBLE PRECISION,
    PRIMARY KEY (index_code, trade_date, bar_time)
) PARTITION BY RANGE (trade_date);

-- 首月分区（后续月份由 index_minute.ensure_month_partition 运行时自动创建）
CREATE TABLE IF NOT EXISTS index_minute_2026_07
    PARTITION OF index_minute
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
