PRAGMA foreign_keys = ON;

BEGIN;

-- =========================================================
-- 0001_init_import_first.sql
-- 目标：
-- 1) 先把当前 GAS / Google Sheet 的“历史原始数据”稳妥导入 SQLite
-- 2) 先处理 raw / config / mapping / run_log
-- 3) 暂不细化 signal / result 宽表，后续再单独做 0002 / 0003
--
-- 设计原则：
-- A. 正式表 raw_* / cfg_* / map_*：字段尽量规范、便于后续 Python 使用
-- B. staging 表 stg_sheet_*：为“从现有 Sheet 导入”服务，允许更宽松/更脏
-- C. 先导入 staging，再 merge 到正式表
-- =========================================================

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
-- ops / run logs
-- 与现有 Sheet “运行日志”保持一致
-- =========================

CREATE TABLE IF NOT EXISTS run_log (
    timestamp TEXT NOT NULL,
    job_name TEXT NOT NULL,
    status TEXT NOT NULL,
    message TEXT,
    detail TEXT,
    source_sheet TEXT,
    source_row_num INTEGER,
    migrated_at TEXT,
    PRIMARY KEY (timestamp, job_name)
);

CREATE INDEX IF NOT EXISTS idx_run_log_job_ts
ON run_log(job_name, timestamp);

-- =========================
-- config / mapping tables
-- =========================

-- 对应：配置_债券指数清单
CREATE TABLE IF NOT EXISTS cfg_bond_index_list (
    index_name TEXT NOT NULL,
    index_code TEXT,
    note TEXT,
    type_lv1 TEXT,
    type_lv2 TEXT,
    type_lv3 TEXT,
    provider TEXT,
    data_date TEXT,
    dm REAL,
    y REAL,
    cons_number INTEGER,
    d REAL,
    v REAL,

    source_sheet TEXT,
    source_row_num INTEGER,
    migrated_at TEXT,

    PRIMARY KEY (index_name)
);

CREATE INDEX IF NOT EXISTS idx_cfg_bond_index_list_code
ON cfg_bond_index_list(index_code);

-- 对应：映射_Jisilu债券指数候选
CREATE TABLE IF NOT EXISTS map_jisilu_bond_index_candidate (
    index_name TEXT NOT NULL,
    match_key TEXT,
    provider_guess TEXT,
    type_lv1 TEXT,
    type_lv2 TEXT,
    type_lv3 TEXT,
    sample_fund_code TEXT,
    sample_fund_name TEXT,
    source_data_date TEXT,
    source_sheet TEXT,
    note TEXT,
    updated_at TEXT,

    PRIMARY KEY (index_name)
);

-- =========================
-- canonical raw tables
-- =========================

-- 原始_收益率曲线
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

CREATE INDEX IF NOT EXISTS idx_raw_bond_curve_curve_date
ON raw_bond_curve(curve, date);

-- 原始_债券指数特征
-- 注意：
-- 1) index_code 在现有历史表中可能为空
-- 2) 因此正式主键不能依赖 index_code
CREATE TABLE IF NOT EXISTS raw_bond_index (
    trade_date TEXT NOT NULL,
    index_name TEXT NOT NULL,
    index_code TEXT,
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

    PRIMARY KEY (trade_date, index_name)
);

CREATE INDEX IF NOT EXISTS idx_raw_bond_index_trade_date
ON raw_bond_index(trade_date);

CREATE INDEX IF NOT EXISTS idx_raw_bond_index_code_date
ON raw_bond_index(index_code, trade_date);

CREATE INDEX IF NOT EXISTS idx_raw_bond_index_type
ON raw_bond_index(type_lv1, type_lv2, type_lv3);

-- 如果 index_code 存在，则希望同一天唯一
CREATE UNIQUE INDEX IF NOT EXISTS uidx_raw_bond_index_trade_date_code_not_null
ON raw_bond_index(trade_date, index_code)
WHERE index_code IS NOT NULL;

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

CREATE INDEX IF NOT EXISTS idx_raw_policy_rate_type_term_date
ON raw_policy_rate(type, term, date);

-- 原始_资金面
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
CREATE TABLE IF NOT EXISTS raw_overseas_macro (
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

CREATE INDEX IF NOT EXISTS idx_raw_overseas_macro_date
ON raw_overseas_macro(date);

-- 原始_民生与资产价格
-- 正式表只保留 repo 当前 schema 的 10 列；工作簿里重复尾列在 staging 接住
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

CREATE INDEX IF NOT EXISTS idx_raw_jisilu_etf_fund_date
ON raw_jisilu_etf(fund_id, snapshot_date);

CREATE INDEX IF NOT EXISTS idx_raw_jisilu_etf_index_nm
ON raw_jisilu_etf(index_nm);

-- =========================
-- permissive staging tables for sheet import
-- 目标：最大化接住现有 Sheet 的脏历史 / 额外列 / 空列
-- 这里统一宽松为 TEXT
-- =========================

-- 运行日志
CREATE TABLE IF NOT EXISTS stg_sheet_run_log (
    source_row_num INTEGER,
    timestamp TEXT,
    job_name TEXT,
    status TEXT,
    message TEXT,
    detail TEXT
);

-- 原始_收益率曲线
CREATE TABLE IF NOT EXISTS stg_sheet_raw_bond_curve (
    source_row_num INTEGER,
    date TEXT,
    curve TEXT,
    y_0 TEXT,
    y_0_08 TEXT,
    y_0_17 TEXT,
    y_0_25 TEXT,
    y_0_5 TEXT,
    y_0_75 TEXT,
    y_1 TEXT,
    y_2 TEXT,
    y_3 TEXT,
    y_4 TEXT,
    y_5 TEXT,
    y_6 TEXT,
    y_7 TEXT,
    y_8 TEXT,
    y_9 TEXT,
    y_10 TEXT,
    y_15 TEXT,
    y_20 TEXT,
    y_30 TEXT,
    y_40 TEXT,
    y_50 TEXT
);

-- 原始_海外宏观
CREATE TABLE IF NOT EXISTS stg_sheet_raw_overseas_macro (
    source_row_num INTEGER,
    date TEXT,
    fed_upper TEXT,
    fed_lower TEXT,
    sofr TEXT,
    ust_2y TEXT,
    ust_10y TEXT,
    us_real_10y TEXT,
    usd_broad TEXT,
    usd_cny TEXT,
    gold TEXT,
    wti TEXT,
    brent TEXT,
    copper TEXT,
    vix TEXT,
    spx TEXT,
    nasdaq_100 TEXT,
    source TEXT,
    fetched_at TEXT
);

-- 原始_政策利率
-- 现有工作簿尾部有两个空白列，这里显式保留 extra_1 / extra_2，避免导入时行宽不一致
CREATE TABLE IF NOT EXISTS stg_sheet_raw_policy_rate (
    source_row_num INTEGER,
    date TEXT,
    type TEXT,
    term TEXT,
    rate TEXT,
    amount TEXT,
    source TEXT,
    fetched_at TEXT,
    note TEXT,
    extra_1 TEXT,
    extra_2 TEXT
);

-- 原始_资金面
CREATE TABLE IF NOT EXISTS stg_sheet_raw_money_market (
    source_row_num INTEGER,
    date TEXT,
    show_date_cn TEXT,
    source_url TEXT,
    dr001_weighted_rate TEXT,
    dr001_latest_rate TEXT,
    dr001_avg_prd TEXT,
    dr007_weighted_rate TEXT,
    dr007_latest_rate TEXT,
    dr007_avg_prd TEXT,
    dr014_weighted_rate TEXT,
    dr014_latest_rate TEXT,
    dr014_avg_prd TEXT,
    dr021_weighted_rate TEXT,
    dr021_latest_rate TEXT,
    dr021_avg_prd TEXT,
    dr1m_weighted_rate TEXT,
    dr1m_latest_rate TEXT,
    dr1m_avg_prd TEXT,
    fetched_at TEXT
);

-- 原始_货币（legacy）
-- 当前 repo 主流程用的是“原始_资金面”，但历史工作簿里还保留了“原始_货币”。
-- 先完整接住，后续再 merge / 去重到 raw_money_market。
CREATE TABLE IF NOT EXISTS stg_sheet_raw_money_legacy (
    source_row_num INTEGER,
    date TEXT,
    show_date_cn TEXT,
    source_url TEXT,
    dr001_weighted_rate TEXT,
    dr001_latest_rate TEXT,
    dr001_avg_prd TEXT,
    dr007_weighted_rate TEXT,
    dr007_latest_rate TEXT,
    dr007_avg_prd TEXT,
    dr014_weighted_rate TEXT,
    dr014_latest_rate TEXT,
    dr014_avg_prd TEXT,
    dr021_weighted_rate TEXT,
    dr021_latest_rate TEXT,
    dr021_avg_prd TEXT,
    dr1m_weighted_rate TEXT,
    dr1m_latest_rate TEXT,
    dr1m_avg_prd TEXT,
    fetched_at TEXT
);

-- 原始_国债期货
CREATE TABLE IF NOT EXISTS stg_sheet_raw_futures (
    source_row_num INTEGER,
    date TEXT,
    t0_last TEXT,
    tf0_last TEXT,
    source TEXT,
    fetched_at TEXT
);

-- 原始_民生与资产价格
-- 现有工作簿尾部重复了一组 deposit_1y / source / fetched_at
-- 正式表只保留 canonical 10 列；重复部分在 staging 接住
CREATE TABLE IF NOT EXISTS stg_sheet_raw_life_asset (
    source_row_num INTEGER,
    date TEXT,
    mortgage_rate_est TEXT,
    house_price_tier1 TEXT,
    house_price_tier2 TEXT,
    house_price_nbs_70city TEXT,
    gold_cny TEXT,
    money_fund_7d TEXT,
    deposit_1y TEXT,
    source TEXT,
    fetched_at TEXT,
    deposit_1y_dup TEXT,
    source_dup TEXT,
    fetched_at_dup TEXT
);

-- 原始_指数ETF
CREATE TABLE IF NOT EXISTS stg_sheet_raw_jisilu_etf (
    source_row_num INTEGER,
    snapshot_date TEXT,
    fetched_at TEXT,
    fund_id TEXT,
    fund_nm TEXT,
    index_nm TEXT,
    issuer_nm TEXT,
    price TEXT,
    increase_rt TEXT,
    volume_wan TEXT,
    amount_yi TEXT,
    unit_total_yi TEXT,
    discount_rt TEXT,
    fund_nav TEXT,
    nav_dt TEXT,
    estimate_value TEXT,
    creation_unit TEXT,
    pe TEXT,
    pb TEXT,
    last_time TEXT,
    last_est_time TEXT,
    is_qdii TEXT,
    is_t0 TEXT,
    apply_fee TEXT,
    redeem_fee TEXT,
    records_total TEXT,
    source_url TEXT
);

-- 原始_债券指数特征
CREATE TABLE IF NOT EXISTS stg_sheet_raw_bond_index (
    source_row_num INTEGER,
    trade_date TEXT,
    index_name TEXT,
    index_code TEXT,
    provider TEXT,
    type_lv1 TEXT,
    type_lv2 TEXT,
    type_lv3 TEXT,
    source_url TEXT,
    data_date TEXT,
    dm TEXT,
    y TEXT,
    cons_number TEXT,
    d TEXT,
    v TEXT,
    fetch_status TEXT,
    raw_json TEXT,
    fetched_at TEXT,
    error TEXT
);

-- 配置_债券指数清单
-- 现有工作簿第一行为空，实际表头在第二行；导入脚本应从首个非空 header 行开始
CREATE TABLE IF NOT EXISTS stg_sheet_cfg_bond_index_list (
    source_row_num INTEGER,
    index_name TEXT,
    index_code TEXT,
    note TEXT,
    type_lv1 TEXT,
    type_lv2 TEXT,
    type_lv3 TEXT,
    provider TEXT,
    data_date TEXT,
    dm TEXT,
    y TEXT,
    cons_number TEXT,
    d TEXT,
    v TEXT
);

-- 映射_Jisilu债券指数候选
CREATE TABLE IF NOT EXISTS stg_sheet_map_jisilu_bond_index_candidate (
    source_row_num INTEGER,
    index_name TEXT,
    match_key TEXT,
    provider_guess TEXT,
    type_lv1 TEXT,
    type_lv2 TEXT,
    type_lv3 TEXT,
    sample_fund_code TEXT,
    sample_fund_name TEXT,
    source_data_date TEXT,
    source_sheet TEXT,
    note TEXT,
    updated_at TEXT
);

-- =========================
-- deferred tables
-- metrics / signals 先不在 0001 里细化
-- 后续建议单独建：
-- 0002_metrics.sql
-- 0003_signals.sql
-- =========================

COMMIT;
