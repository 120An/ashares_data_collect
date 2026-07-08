-- ETF 净值（IOPV/折价 来自 akshare spot；官方单位/累计净值 来自 akshare fund_info）
CREATE TABLE IF NOT EXISTS etf_nav (
  code          VARCHAR(6)   NOT NULL,
  trade_date    DATE         NOT NULL,
  close         double precision,     -- 最新价(spot)
  iopv          double precision,     -- IOPV实时估值(spot)
  discount_rate double precision,     -- 基金折价率(spot，正=折价/负=溢价)
  unit_nav      numeric(18,6),        -- 单位净值(fund_info)
  accum_nav     numeric(18,6),        -- 累计净值(fund_info)
  daily_growth  numeric(12,6),        -- 日增长率(fund_info)
  nav_date      date,                 -- 官方净值日期(fund_info)
  updated_at    timestamp DEFAULT NOW(),
  PRIMARY KEY (code, trade_date)
);
-- etf_info / etf_info_changelog 由 data_collect/jobs/etf_info.py 自动建表。
