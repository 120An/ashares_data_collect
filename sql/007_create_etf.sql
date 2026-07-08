-- ETF 采集子系统建表（Plan 1）。代码裸6位；K线不复权。

-- ETF 日线（不复权）
CREATE TABLE IF NOT EXISTS etf_daily_kline (
  code       VARCHAR(6)       NOT NULL,
  trade_date DATE             NOT NULL,
  open  double precision,
  high  double precision,
  low   double precision,
  close double precision,
  volume double precision,
  amount double precision,
  PRIMARY KEY (code, trade_date)
);

-- ETF 复权因子（与股票 divid_factors 对称）
CREATE TABLE IF NOT EXISTS etf_divid_factors (
  code        VARCHAR(6)   NOT NULL,
  date        DATE         NOT NULL,
  interest    NUMERIC(12,4),
  stock_bonus NUMERIC(12,6),
  stock_gift  NUMERIC(12,6),
  allot_num   NUMERIC(12,6),
  allot_price NUMERIC(12,4),
  dr          NUMERIC(18,10),
  created_at  TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (code, date)
);
