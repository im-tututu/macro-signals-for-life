BEGIN;

DROP VIEW IF EXISTS vw_qdii_latest;
DROP VIEW IF EXISTS vw_qdii_enriched;
DROP VIEW IF EXISTS vw_invest_bucket_latest;
DROP VIEW IF EXISTS vw_invest_exposure_latest;
DROP VIEW IF EXISTS vw_invest_instrument_latest;
DROP VIEW IF EXISTS vw_invest_pivot_daily;

ALTER TABLE raw_jisilu_etf RENAME TO raw_jisilu_etf_old;
ALTER TABLE raw_jisilu_gold RENAME TO raw_jisilu_gold_old;
ALTER TABLE raw_jisilu_money RENAME TO raw_jisilu_money_old;
ALTER TABLE raw_jisilu_qdii RENAME TO raw_jisilu_qdii_old;
ALTER TABLE raw_jisilu_treasury RENAME TO raw_jisilu_treasury_old;

CREATE TABLE raw_jisilu_etf (
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
    m_fee REAL,
    t_fee REAL,
    mt_fee REAL,
    PRIMARY KEY (snapshot_date, fetched_at, fund_id)
);

CREATE TABLE raw_jisilu_gold (
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
    m_fee REAL,
    t_fee REAL,
    mt_fee REAL,
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
    raw_json TEXT,
    migrated_at TEXT,
    PRIMARY KEY (snapshot_date, fetched_at, fund_id)
);

CREATE TABLE raw_jisilu_money (
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
    m_fee REAL,
    t_fee REAL,
    mt_fee REAL,
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
    raw_json TEXT,
    migrated_at TEXT,
    PRIMARY KEY (snapshot_date, fetched_at, fund_id)
);

CREATE TABLE raw_jisilu_qdii (
    snapshot_date TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    market TEXT NOT NULL,
    market_code TEXT NOT NULL,
    fund_id TEXT NOT NULL,
    fund_nm TEXT,
    fund_nm_display TEXT,
    qtype TEXT,
    index_nm TEXT,
    index_id TEXT,
    issuer_nm TEXT,
    price REAL,
    pre_close REAL,
    increase_rt REAL,
    volume_wan REAL,
    stock_volume_wan REAL,
    amount_yi REAL,
    amount_incr REAL,
    amount_increase_rt REAL,
    unit_total_yi REAL,
    discount_rt REAL,
    fund_nav REAL,
    iopv REAL,
    ref_price REAL,
    ref_increase_rt REAL,
    est_val_increase_rt REAL,
    m_fee REAL,
    t_fee REAL,
    mt_fee REAL,
    nav_discount_rt REAL,
    iopv_discount_rt REAL,
    turnover_rt REAL,
    price_dt TEXT,
    nav_dt TEXT,
    iopv_dt TEXT,
    estimate_value REAL,
    last_est_dt TEXT,
    last_time TEXT,
    last_est_time TEXT,
    apply_fee TEXT,
    apply_status TEXT,
    apply_fee_tips TEXT,
    redeem_fee TEXT,
    redeem_status TEXT,
    redeem_fee_tips TEXT,
    money_cd TEXT,
    asset_ratio TEXT,
    lof_type TEXT,
    t0 TEXT,
    eval_show TEXT,
    notes TEXT,
    has_iopv TEXT,
    has_us_ref TEXT,
    records_total REAL,
    source_url TEXT,
    source_sheet TEXT,
    source_row_num INTEGER,
    raw_json TEXT,
    migrated_at TEXT,
    PRIMARY KEY (snapshot_date, fetched_at, market, fund_id)
);

CREATE TABLE raw_jisilu_treasury (
    snapshot_date TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    bond_id TEXT NOT NULL,
    bond_nm TEXT,
    price REAL,
    full_price REAL,
    increase_rt REAL,
    volume_wan REAL,
    ask_1 REAL,
    bid_1 REAL,
    days_to_coupon REAL,
    years_left REAL,
    duration REAL,
    ytm REAL,
    coupon_rt REAL,
    repo_ratio REAL,
    repo_usage_rt REAL,
    maturity_dt TEXT,
    size_yi REAL,
    source_url TEXT,
    source_sheet TEXT,
    source_row_num INTEGER,
    raw_json TEXT,
    migrated_at TEXT,
    PRIMARY KEY (snapshot_date, fetched_at, bond_id)
);

INSERT INTO raw_jisilu_etf
SELECT * FROM raw_jisilu_etf_old;
INSERT INTO raw_jisilu_gold
SELECT * FROM raw_jisilu_gold_old;
INSERT INTO raw_jisilu_money
SELECT * FROM raw_jisilu_money_old;
INSERT INTO raw_jisilu_qdii
SELECT * FROM raw_jisilu_qdii_old;
INSERT INTO raw_jisilu_treasury
SELECT * FROM raw_jisilu_treasury_old;

DROP TABLE raw_jisilu_etf_old;
DROP TABLE raw_jisilu_gold_old;
DROP TABLE raw_jisilu_money_old;
DROP TABLE raw_jisilu_qdii_old;
DROP TABLE raw_jisilu_treasury_old;

CREATE INDEX idx_raw_jisilu_etf_snapshot_date ON raw_jisilu_etf(snapshot_date);
CREATE INDEX idx_raw_jisilu_etf_fund_date ON raw_jisilu_etf(fund_id, snapshot_date);
CREATE INDEX idx_raw_jisilu_etf_index_nm ON raw_jisilu_etf(index_nm);
CREATE INDEX idx_raw_jisilu_gold_snapshot_date ON raw_jisilu_gold(snapshot_date);
CREATE INDEX idx_raw_jisilu_gold_fund_date ON raw_jisilu_gold(fund_id, snapshot_date);
CREATE INDEX idx_raw_jisilu_gold_index_nm ON raw_jisilu_gold(index_nm);
CREATE INDEX idx_raw_jisilu_money_snapshot_date ON raw_jisilu_money(snapshot_date);
CREATE INDEX idx_raw_jisilu_money_fund_date ON raw_jisilu_money(fund_id, snapshot_date);
CREATE INDEX idx_raw_jisilu_money_index_nm ON raw_jisilu_money(index_nm);
CREATE INDEX idx_raw_jisilu_qdii_snapshot_date ON raw_jisilu_qdii(snapshot_date);
CREATE INDEX idx_raw_jisilu_qdii_market_date ON raw_jisilu_qdii(market, snapshot_date);
CREATE INDEX idx_raw_jisilu_qdii_fund_date ON raw_jisilu_qdii(fund_id, snapshot_date);
CREATE INDEX idx_raw_jisilu_qdii_index_nm ON raw_jisilu_qdii(index_nm);
CREATE INDEX idx_raw_jisilu_qdii_money_cd ON raw_jisilu_qdii(money_cd);
CREATE INDEX idx_raw_jisilu_treasury_snapshot_date ON raw_jisilu_treasury(snapshot_date);
CREATE INDEX idx_raw_jisilu_treasury_ytm ON raw_jisilu_treasury(ytm, snapshot_date);
CREATE INDEX idx_raw_jisilu_treasury_volume ON raw_jisilu_treasury(volume_wan, snapshot_date);

COMMIT;
