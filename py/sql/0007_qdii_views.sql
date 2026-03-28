BEGIN;

DROP VIEW IF EXISTS vw_qdii_snapshot_enriched;
CREATE VIEW vw_qdii_snapshot_enriched AS
SELECT
    snapshot_date,
    fetched_at,
    market,
    CASE market
        WHEN 'europe_america' THEN '欧美'
        WHEN 'commodity' THEN '商品'
        WHEN 'asia' THEN '亚洲'
        ELSE market
    END AS market_label,
    market_code,
    fund_id,
    COALESCE(NULLIF(fund_nm_display, ''), fund_nm) AS display_name,
    fund_nm,
    fund_nm_display,
    issuer_nm,
    index_nm,
    index_id,
    qtype,
    money_cd,
    lof_type,
    t0,
    has_iopv,
    has_us_ref,
    apply_status,
    redeem_status,
    price_dt,
    nav_dt,
    iopv_dt,
    last_est_dt,
    last_time,
    last_est_time,
    price,
    pre_close,
    increase_rt,
    fund_nav,
    iopv,
    estimate_value,
    volume_wan,
    stock_volume_wan,
    amount_yi,
    amount_incr,
    amount_increase_rt,
    turnover_rt,
    ref_price,
    ref_increase_rt,
    est_val_increase_rt,
    discount_rt AS est_discount_rt,
    nav_discount_rt,
    iopv_discount_rt,
    COALESCE(iopv_discount_rt, nav_discount_rt, discount_rt) AS premium_discount_rt,
    ABS(COALESCE(iopv_discount_rt, nav_discount_rt, discount_rt)) AS abs_premium_discount_rt,
    m_fee,
    t_fee,
    mt_fee,
    asset_ratio,
    eval_show,
    notes,
    source_url,
    records_total
FROM raw_jisilu_qdii
WHERE COALESCE(fund_id, '') <> '';

DROP VIEW IF EXISTS vw_qdii_latest;
CREATE VIEW vw_qdii_latest AS
SELECT e.*
FROM vw_qdii_snapshot_enriched AS e
JOIN (
    SELECT MAX(snapshot_date) AS latest_snapshot_date
    FROM raw_jisilu_qdii
) AS d
    ON e.snapshot_date = d.latest_snapshot_date;

COMMIT;
