PRAGMA foreign_keys = ON;

BEGIN;

-- =========================
-- ingest metadata
-- =========================

CREATE TABLE IF NOT EXISTS ingest_run (
    run_id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,          -- running / success / failed
    rows_read INTEGER NOT NULL DEFAULT 0,
    rows_written INTEGER NOT NULL DEFAULT 0,
    message TEXT
);

CREATE TABLE IF NOT EXISTS ingest_error (
    error_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    source_name TEXT,
    object_name TEXT,
    error_type TEXT,
    error_message TEXT,
    row_payload TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES ingest_run(run_id)
);

-- =========================
-- raw tables
-- =========================

-- 原始_收益率曲线
-- 当前 GAS 是宽表：date + curve + Y_{term}
CREATE TABLE IF NOT EXISTS raw_bond_curve (
    date TEXT NOT NULL,
    curve TEXT NOT NULL,

    y_0 REAL,
    y_0_08 REAL,
    y_0_17 REAL,
    y_0_25 REAL,
    y_0_5 REAL,
    y_0_75 REAL,
    y_1 REAL,
    y_2 REAL,
    y_3 REAL,
    y_4 REAL,
    y_5 REAL,
    y_6 REAL,
    y_7 REAL,
    y_8 REAL,
    y_9 REAL,
    y_10 REAL,
    y_15 REAL,
    y_20 REAL,
    y_30 REAL,
    y_40 REAL,
    y_50 REAL,

    source_sheet TEXT,
    source_row_num INTEGER,
    migrated_at TEXT,

    PRIMARY KEY (date, curve)
);

CREATE INDEX IF NOT EXISTS idx_raw_bond_curve_date
ON raw_bond_curve(date);

-- 原始_债券指数特征
CREATE TABLE IF NOT EXISTS raw_bond_index (
    trade_date TEXT NOT NULL,
    index_name TEXT,
    index_code TEXT NOT NULL,
    provider TEXT,
    type_lv1 TEXT,
    type_lv2 TEXT,
    type_lv3 TEXT,
    source_url TEXT,
    data_date TEXT,
    dm REAL,
    y REAL,
    cons_number INTEGER,
    d REAL,
    v REAL,
    fetch_status TEXT,
    raw_json TEXT,
    fetched_at TEXT,
    error TEXT,

    source_sheet TEXT,
    source_row_num INTEGER,
    migrated_at TEXT,

    PRIMARY KEY (trade_date, index_code)
);

CREATE INDEX IF NOT EXISTS idx_raw_bond_index_trade_date
ON raw_bond_index(trade_date);

CREATE INDEX IF NOT EXISTS idx_raw_bond_index_type
ON raw_bond_index(type_lv1, type_lv2, type_lv3);

-- 原始_政策利率
CREATE TABLE IF NOT EXISTS raw_policy_rate (
    date TEXT NOT NULL,
    type TEXT NOT NULL,
    term TEXT NOT NULL,
    rate REAL,
    amount REAL,
    source TEXT,
    fetched_at TEXT,
    note TEXT,

    source_sheet TEXT,
    source_row_num INTEGER,
    migrated_at TEXT,

    PRIMARY KEY (date, type, term)
);

CREATE INDEX IF NOT EXISTS idx_raw_policy_rate_date
ON raw_policy_rate(date);

CREATE INDEX IF NOT EXISTS idx_raw_policy_rate_type_term
ON raw_policy_rate(type, term);

-- 原始_资金面
-- 当前 GAS 是宽表：按日期一行
CREATE TABLE IF NOT EXISTS raw_money_market (
    date TEXT NOT NULL,
    show_date_cn TEXT,
    source_url TEXT,

    dr001_weighted_rate REAL,
    dr001_latest_rate REAL,
    dr001_avg_prd REAL,

    dr007_weighted_rate REAL,
    dr007_latest_rate REAL,
    dr007_avg_prd REAL,

    dr014_weighted_rate REAL,
    dr014_latest_rate REAL,
    dr014_avg_prd REAL,

    dr021_weighted_rate REAL,
    dr021_latest_rate REAL,
    dr021_avg_prd REAL,

    dr1m_weighted_rate REAL,
    dr1m_latest_rate REAL,
    dr1m_avg_prd REAL,

    fetched_at TEXT,

    source_sheet TEXT,
    source_row_num INTEGER,
    migrated_at TEXT,

    PRIMARY KEY (date)
);

CREATE INDEX IF NOT EXISTS idx_raw_money_market_date
ON raw_money_market(date);

-- 原始_国债期货
CREATE TABLE IF NOT EXISTS raw_futures (
    date TEXT NOT NULL,
    t0_last REAL,
    tf0_last REAL,
    source TEXT,
    fetched_at TEXT,

    source_sheet TEXT,
    source_row_num INTEGER,
    migrated_at TEXT,

    PRIMARY KEY (date)
);

CREATE INDEX IF NOT EXISTS idx_raw_futures_date
ON raw_futures(date);

-- 原始_海外宏观
CREATE TABLE IF NOT EXISTS raw_macro (
    date TEXT NOT NULL,
    fed_upper REAL,
    fed_lower REAL,
    sofr REAL,
    ust_2y REAL,
    ust_10y REAL,
    us_real_10y REAL,
    usd_broad REAL,
    usd_cny REAL,
    gold REAL,
    wti REAL,
    brent REAL,
    copper REAL,
    vix REAL,
    spx REAL,
    nasdaq_100 REAL,
    source TEXT,
    fetched_at TEXT,

    source_sheet TEXT,
    source_row_num INTEGER,
    migrated_at TEXT,

    PRIMARY KEY (date)
);

CREATE INDEX IF NOT EXISTS idx_raw_macro_date
ON raw_macro(date);

-- 原始_民生与资产价格
CREATE TABLE IF NOT EXISTS raw_life_asset (
    date TEXT NOT NULL,
    mortgage_rate_est REAL,
    house_price_tier1 REAL,
    house_price_tier2 REAL,
    house_price_nbs_70city REAL,
    gold_cny REAL,
    money_fund_7d REAL,
    deposit_1y REAL,
    source TEXT,
    fetched_at TEXT,

    source_sheet TEXT,
    source_row_num INTEGER,
    migrated_at TEXT,

    PRIMARY KEY (date)
);

CREATE INDEX IF NOT EXISTS idx_raw_life_asset_date
ON raw_life_asset(date);

-- 原始_指数ETF
CREATE TABLE IF NOT EXISTS raw_jisilu_etf (
    snapshot_date TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    fund_id TEXT NOT NULL,
    fund_nm TEXT,
    index_nm TEXT,
    issuer_nm TEXT,
    price REAL,
    increase_rt REAL,
    volume_wan REAL,
    amount_yi REAL,
    unit_total_yi REAL,
    discount_rt REAL,
    fund_nav REAL,
    nav_dt TEXT,
    estimate_value REAL,
    creation_unit REAL,
    pe REAL,
    pb REAL,
    last_time TEXT,
    last_est_time TEXT,
    is_qdii TEXT,
    is_t0 TEXT,
    apply_fee REAL,
    redeem_fee REAL,
    records_total REAL,
    source_url TEXT,

    source_sheet TEXT,
    source_row_num INTEGER,
    migrated_at TEXT,

    PRIMARY KEY (snapshot_date, fund_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_jisilu_etf_snapshot_date
ON raw_jisilu_etf(snapshot_date);

CREATE INDEX IF NOT EXISTS idx_raw_jisilu_etf_index_nm
ON raw_jisilu_etf(index_nm);

-- =========================
-- staging tables
-- 先全量导入 staging，再 merge 到正式表
-- =========================

CREATE TABLE IF NOT EXISTS stg_raw_bond_curve AS
SELECT * FROM raw_bond_curve WHERE 1=0;

CREATE TABLE IF NOT EXISTS stg_raw_bond_index AS
SELECT * FROM raw_bond_index WHERE 1=0;

CREATE TABLE IF NOT EXISTS stg_raw_policy_rate AS
SELECT * FROM raw_policy_rate WHERE 1=0;

CREATE TABLE IF NOT EXISTS stg_raw_money_market AS
SELECT * FROM raw_money_market WHERE 1=0;

CREATE TABLE IF NOT EXISTS stg_raw_futures AS
SELECT * FROM raw_futures WHERE 1=0;

CREATE TABLE IF NOT EXISTS stg_raw_macro AS
SELECT * FROM raw_macro WHERE 1=0;

CREATE TABLE IF NOT EXISTS stg_raw_life_asset AS
SELECT * FROM raw_life_asset WHERE 1=0;

CREATE TABLE IF NOT EXISTS stg_raw_jisilu_etf AS
SELECT * FROM raw_jisilu_etf WHERE 1=0;

-- =========================
-- derived tables
-- 先预留，后面 Python 接管 metrics / signals 时直接写
-- =========================

CREATE TABLE IF NOT EXISTS metric_values (
    metric_code TEXT NOT NULL,
    metric_name TEXT,
    date TEXT NOT NULL,
    value REAL,
    unit TEXT,
    value_display TEXT,
    source_table TEXT,
    updated_at TEXT,
    PRIMARY KEY (metric_code, date)
);

CREATE INDEX IF NOT EXISTS idx_metric_values_date
ON metric_values(date);

CREATE TABLE IF NOT EXISTS signal_values (
    signal_code TEXT NOT NULL,
    signal_group TEXT,
    date TEXT NOT NULL,
    score REAL,
    label TEXT,
    note TEXT,
    source_metrics TEXT,
    updated_at TEXT,
    PRIMARY KEY (signal_code, date)
);

CREATE INDEX IF NOT EXISTS idx_signal_values_date
ON signal_values(date);

COMMIT;