PRAGMA foreign_keys = ON;
BEGIN;

CREATE TABLE IF NOT EXISTS ingest_run (
    run_id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
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

CREATE TABLE IF NOT EXISTS metrics (
    date TEXT NOT NULL,
    metric_key TEXT NOT NULL,
    metric_name TEXT,
    metric_value REAL,
    metric_unit TEXT,
    metric_group TEXT,
    score REAL,
    percentile REAL,
    note TEXT,
    source TEXT,
    fetched_at TEXT,
    source_sheet TEXT,
    source_row_num INTEGER,
    migrated_at TEXT,
    PRIMARY KEY (date, metric_key)
);

CREATE TABLE IF NOT EXISTS signal_main (
    date TEXT NOT NULL,
    signal_key TEXT NOT NULL,
    title TEXT,
    value TEXT,
    score REAL,
    note TEXT,
    source TEXT,
    fetched_at TEXT,
    source_sheet TEXT,
    source_row_num INTEGER,
    migrated_at TEXT,
    PRIMARY KEY (date, signal_key)
);

CREATE TABLE IF NOT EXISTS signal_detail (
    date TEXT NOT NULL,
    signal_key TEXT NOT NULL,
    component_key TEXT NOT NULL,
    component_value TEXT,
    note TEXT,
    fetched_at TEXT,
    migrated_at TEXT,
    PRIMARY KEY (date, signal_key, component_key)
);

CREATE TABLE IF NOT EXISTS signal_review (
    date TEXT NOT NULL,
    signal_key TEXT NOT NULL,
    review_text TEXT,
    fetched_at TEXT,
    migrated_at TEXT,
    PRIMARY KEY (date, signal_key)
);

COMMIT;
