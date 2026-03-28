PRAGMA foreign_keys = ON;
BEGIN;

CREATE TABLE IF NOT EXISTS stg_sheet_run_log (
    timestamp TEXT,
    job_name TEXT,
    status TEXT,
    message TEXT,
    detail TEXT,
    source_row_num INTEGER
);

CREATE TABLE IF NOT EXISTS stg_sheet_raw_bond_curve (
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
    y_50 TEXT,
    source_row_num INTEGER
);

CREATE TABLE IF NOT EXISTS stg_sheet_raw_fred (
    date TEXT,
    fed_upper TEXT,
    fed_lower TEXT,
    sofr TEXT,
    ust_2y TEXT,
    ust_10y TEXT,
    us_real_10y TEXT,
    usd_broad TEXT,
    usd_cny TEXT,
    vix TEXT,
    spx TEXT,
    nasdaq_100 TEXT,
    source TEXT,
    fetched_at TEXT,
    source_row_num INTEGER
);

CREATE TABLE IF NOT EXISTS stg_sheet_raw_alpha_vantage (
    date TEXT,
    gold TEXT,
    wti TEXT,
    brent TEXT,
    copper TEXT,
    source TEXT,
    fetched_at TEXT,
    source_row_num INTEGER
);

CREATE TABLE IF NOT EXISTS stg_sheet_raw_policy_rate (
    date TEXT,
    type TEXT,
    term TEXT,
    rate TEXT,
    amount TEXT,
    source TEXT,
    fetched_at TEXT,
    note TEXT,
    extra_1 TEXT,
    extra_2 TEXT,
    source_row_num INTEGER
);

CREATE TABLE IF NOT EXISTS stg_sheet_raw_money_market (
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
    fetched_at TEXT,
    source_row_num INTEGER
);

CREATE TABLE IF NOT EXISTS stg_sheet_raw_money_legacy (
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
    fetched_at TEXT,
    source_row_num INTEGER
);

CREATE TABLE IF NOT EXISTS stg_sheet_raw_futures (
    date TEXT,
    t0_last TEXT,
    tf0_last TEXT,
    source TEXT,
    fetched_at TEXT,
    source_row_num INTEGER
);

CREATE TABLE IF NOT EXISTS stg_sheet_raw_life_asset (
    date TEXT,
    mortgage_rate_est TEXT,
    house_price_tier1 TEXT,
    house_price_tier2 TEXT,
    house_price_nbs_70city TEXT,
    gold_cny TEXT,
    money_fund_7d TEXT,
    deposit_1y TEXT,
    deposit_1y_dup TEXT,
    source TEXT,
    source_dup TEXT,
    fetched_at TEXT,
    fetched_at_dup TEXT,
    source_row_num INTEGER
);

CREATE TABLE IF NOT EXISTS stg_sheet_raw_jisilu_etf (
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
    source_url TEXT,
    source_row_num INTEGER
);

CREATE TABLE IF NOT EXISTS stg_sheet_raw_bond_index (
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
    error TEXT,
    source_row_num INTEGER
);

CREATE TABLE IF NOT EXISTS stg_sheet_cfg_bond_index_list (
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
    v TEXT,
    source_row_num INTEGER
);

CREATE TABLE IF NOT EXISTS stg_sheet_map_jisilu_bond_index_candidate (
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
    updated_at TEXT,
    source_row_num INTEGER
);

COMMIT;
