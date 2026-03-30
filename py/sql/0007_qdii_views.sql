BEGIN;

DROP VIEW IF EXISTS vw_qdii_enriched;
CREATE VIEW vw_qdii_enriched AS
WITH latest_snapshot AS (
    SELECT
        snapshot_date,
        MAX(fetched_at) AS fetched_at
    FROM raw_jisilu_qdii
    WHERE COALESCE(fund_id, '') <> ''
    GROUP BY snapshot_date
)
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
    COALESCE(
        NULLIF(fund_nm_display, ''),
        NULLIF(fund_nm, ''),
        (
            SELECT COALESCE(NULLIF(r.fund_nm_display, ''), NULLIF(r.fund_nm, ''))
            FROM raw_jisilu_qdii AS r
            WHERE r.fund_id = raw_jisilu_qdii.fund_id
              AND COALESCE(NULLIF(r.fund_nm_display, ''), NULLIF(r.fund_nm, '')) IS NOT NULL
            ORDER BY COALESCE(NULLIF(TRIM(r.nav_dt), ''), r.snapshot_date) DESC, r.snapshot_date DESC, r.fetched_at DESC
            LIMIT 1
        ),
        fund_id
    ) AS display_name,
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
    snapshot_date AS source_snapshot_date,
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
    amount_yi AS total_share_wan,
    volume_wan AS trade_volume_wan,
    CASE
        WHEN COALESCE(amount_yi, 0) > 0 AND COALESCE(pre_close, 0) > 0 THEN (amount_yi * pre_close) / 10000.0
        ELSE unit_total_yi
    END AS total_scale_yi,
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
WHERE COALESCE(fund_id, '') <> ''
  AND (snapshot_date, fetched_at) IN (
    SELECT snapshot_date, fetched_at
    FROM latest_snapshot
  );

DROP VIEW IF EXISTS vw_qdii_latest;
CREATE VIEW vw_qdii_latest AS
WITH latest_nav_dt AS (
    SELECT MAX(COALESCE(NULLIF(TRIM(nav_dt), ''), snapshot_date)) AS nav_dt
    FROM vw_qdii_enriched
)
SELECT e.*
FROM vw_qdii_enriched AS e
JOIN latest_nav_dt AS d
    ON COALESCE(NULLIF(TRIM(e.nav_dt), ''), e.snapshot_date) = d.nav_dt;

COMMIT;
