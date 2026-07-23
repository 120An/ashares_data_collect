-- 扩展数据层建表（打板/期权/筹码，见 docs/superpowers/specs/2026-07-22-ext-layers-design.md）
-- 属主 ai_read 自建；代码裸6位；金额单位元；全部不分区（最大 fund_flow ~145万行/年）。

-- B1: 东财涨停四池 union（pool_type: zt涨停/zb炸板/dt跌停/yzt昨涨停；各池特有字段可空）
CREATE TABLE IF NOT EXISTS limit_pool (
    trade_date  DATE NOT NULL,
    pool_type   VARCHAR(4) NOT NULL,
    stock_code  VARCHAR(6) NOT NULL,
    name        TEXT,
    price       DOUBLE PRECISION,   -- 已÷1000
    pct         DOUBLE PRECISION,
    amount      DOUBLE PRECISION,   -- 元
    float_cap   DOUBLE PRECISION,
    turnover    DOUBLE PRECISION,
    limit_days  INTEGER,            -- zt: 连板数
    first_seal  TIME,               -- zt/zb
    last_seal   TIME,               -- zt/dt
    seal_fund   DOUBLE PRECISION,   -- zt/dt 封板资金(元)
    break_times INTEGER,            -- zt/zb 炸板次数; dt=开板次数
    amplitude   DOUBLE PRECISION,   -- zb/yzt
    speed       DOUBLE PRECISION,   -- zb/yzt
    industry    TEXT,
    zt_stat     TEXT,               -- "N天M板"
    pe          DOUBLE PRECISION,   -- dt
    board_amount DOUBLE PRECISION,  -- dt 板上成交额
    dt_days     INTEGER,            -- dt 连续跌停
    limit_price DOUBLE PRECISION,   -- zb 涨停价
    y_first_seal TIME,              -- yzt 昨封板时间
    y_limit_days INTEGER,           -- yzt 昨连板
    PRIMARY KEY (trade_date, pool_type, stock_code)
);

-- B2: 同花顺涨停揭秘（原因题材/板型/封板率；历史可回补~2025-12起）
CREATE TABLE IF NOT EXISTS limit_up_reason (
    trade_date  DATE NOT NULL,
    stock_code  VARCHAR(6) NOT NULL,
    name        TEXT,
    price       DOUBLE PRECISION,
    pct         DOUBLE PRECISION,
    reason      TEXT,               -- 涨停原因题材
    board_type  TEXT,               -- 换手板/一字板/T字板
    seal_rate   DOUBLE PRECISION,   -- 封板成功率 0~1
    break_times INTEGER,
    seal_amount DOUBLE PRECISION,   -- 封单额(元)
    high_days   TEXT,               -- "N天M板"
    first_time  TIME,               -- 首次涨停(unix秒转北京时间)
    is_again    SMALLINT,           -- 是否回封
    PRIMARY KEY (trade_date, stock_code)
);

-- C: ETF期权日快照（T型报价+希腊字母合并；实时源无历史→前向积累）
CREATE TABLE IF NOT EXISTS etf_option_daily (
    trade_date  DATE NOT NULL,
    option_code VARCHAR(12) NOT NULL,  -- 如 10009269
    underlying  VARCHAR(6) NOT NULL,   -- 510050/510300/588000/510500
    call_put    CHAR(1) NOT NULL,      -- C/P
    expiry_month VARCHAR(4),           -- YYMM
    name        TEXT,
    strike      DOUBLE PRECISION,
    last        DOUBLE PRECISION,
    prev_close  DOUBLE PRECISION,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    volume      DOUBLE PRECISION,
    amount      DOUBLE PRECISION,
    bid         DOUBLE PRECISION,
    bid_vol     DOUBLE PRECISION,
    ask         DOUBLE PRECISION,
    ask_vol     DOUBLE PRECISION,
    open_interest DOUBLE PRECISION,
    pct         DOUBLE PRECISION,
    limit_up    DOUBLE PRECISION,
    limit_down  DOUBLE PRECISION,
    delta       DOUBLE PRECISION,
    gamma       DOUBLE PRECISION,
    theta       DOUBLE PRECISION,
    vega        DOUBLE PRECISION,
    iv          DOUBLE PRECISION,     -- 小数(0.17=17%)
    theory      DOUBLE PRECISION,     -- 理论价值
    PRIMARY KEY (trade_date, option_code)
);

-- D1: 融资融券明细（T+1早发布→run(T)采T-1；全历史可backfill）
CREATE TABLE IF NOT EXISTS margin_daily (
    trade_date  DATE NOT NULL,
    stock_code  VARCHAR(6) NOT NULL,
    name        TEXT,
    market      TEXT,
    rzye        DOUBLE PRECISION,   -- 融资余额(元)
    rzmre       DOUBLE PRECISION,   -- 融资买入额
    rzche       DOUBLE PRECISION,   -- 融资偿还额
    rzyezb      DOUBLE PRECISION,   -- 融资余额占比%
    rqye        DOUBLE PRECISION,   -- 融券余额(元)
    rqyl        DOUBLE PRECISION,   -- 融券余量
    rqmcl       DOUBLE PRECISION,   -- 融券卖出量
    rqchl       DOUBLE PRECISION,   -- 融券偿还量
    rzrqye      DOUBLE PRECISION,   -- 两融余额合计
    rzrqyecz    DOUBLE PRECISION,   -- 两融余额差值
    PRIMARY KEY (trade_date, stock_code)
);

-- D2: 大宗交易（同股同日多笔无自然主键→全字段唯一索引幂等）
CREATE TABLE IF NOT EXISTS block_trade (
    trade_date  DATE NOT NULL,
    stock_code  VARCHAR(6) NOT NULL,
    name        TEXT,
    deal_price  DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    premium_pct DOUBLE PRECISION,
    deal_volume DOUBLE PRECISION,
    deal_amount DOUBLE PRECISION,
    buyer_name  TEXT,
    seller_name TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS block_trade_uk ON block_trade
    (trade_date, stock_code, deal_price, deal_volume, buyer_name, seller_name);

-- D3: 个股资金流日级（当日clist快照前向 + 120日窗口内per-stock回补）
CREATE TABLE IF NOT EXISTS fund_flow_daily (
    trade_date  DATE NOT NULL,
    stock_code  VARCHAR(6) NOT NULL,
    name        TEXT,
    close       DOUBLE PRECISION,
    pct         DOUBLE PRECISION,
    main_net    DOUBLE PRECISION,   -- 主力净流入(元)
    super_net   DOUBLE PRECISION,
    large_net   DOUBLE PRECISION,
    mid_net     DOUBLE PRECISION,
    small_net   DOUBLE PRECISION,
    main_net_pct DOUBLE PRECISION,  -- 主力净占比%
    PRIMARY KEY (trade_date, stock_code)
);
