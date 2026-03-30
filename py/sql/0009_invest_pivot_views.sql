BEGIN;

DROP VIEW IF EXISTS vw_invest_pivot_daily;
CREATE VIEW vw_invest_pivot_daily AS
WITH latest_etf_date AS (
    SELECT MAX(COALESCE(NULLIF(TRIM(nav_dt), ''), snapshot_date)) AS nav_dt
    FROM raw_jisilu_etf
    WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
),
latest_etf_batch AS (
    SELECT snapshot_date, MAX(fetched_at) AS fetched_at
    FROM raw_jisilu_etf
    WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
      AND COALESCE(NULLIF(TRIM(nav_dt), ''), snapshot_date) = (SELECT nav_dt FROM latest_etf_date)
    GROUP BY snapshot_date
),
latest_qdii_date AS (
    SELECT MAX(COALESCE(NULLIF(TRIM(nav_dt), ''), snapshot_date)) AS nav_dt
    FROM raw_jisilu_qdii
    WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
),
latest_qdii_batch AS (
    SELECT snapshot_date, market, MAX(fetched_at) AS fetched_at
    FROM raw_jisilu_qdii
    WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
      AND COALESCE(NULLIF(TRIM(nav_dt), ''), snapshot_date) = (SELECT nav_dt FROM latest_qdii_date)
    GROUP BY snapshot_date, market
),
latest_gold_date AS (
    SELECT MAX(COALESCE(NULLIF(TRIM(nav_dt), ''), snapshot_date)) AS nav_dt
    FROM raw_jisilu_gold
    WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
),
latest_gold_batch AS (
    SELECT snapshot_date, MAX(fetched_at) AS fetched_at
    FROM raw_jisilu_gold
    WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
      AND COALESCE(NULLIF(TRIM(nav_dt), ''), snapshot_date) = (SELECT nav_dt FROM latest_gold_date)
    GROUP BY snapshot_date
),
unified AS (
    SELECT
        snapshot_date,
        fetched_at,
        NULLIF(TRIM(nav_dt), '') AS nav_dt,
        'etf' AS source_kind,
        COALESCE(
            NULLIF(TRIM(index_nm), ''),
            NULLIF(TRIM(fund_nm), ''),
            'ETF'
        ) AS exposure_name,
        LOWER(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            COALESCE(
                                NULLIF(TRIM(index_nm), ''),
                                NULLIF(TRIM(fund_nm), ''),
                                'ETF'
                            ),
                            ' ',
                            ''
                        ),
                        'ETF',
                        ''
                    ),
                    'etf',
                    ''
                ),
                '-',
                ''
            )
        ) AS exposure_key,
        'equity' AS asset_class,
        amount_yi AS total_amount_wan,
        volume_wan,
        unit_total_yi AS total_unit_amount_yi,
        fund_id
    FROM raw_jisilu_etf
    WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
      AND (snapshot_date, fetched_at) IN (
        SELECT snapshot_date, fetched_at
        FROM latest_etf_batch
      )

    UNION ALL

    SELECT
        snapshot_date,
        fetched_at,
        NULLIF(TRIM(nav_dt), '') AS nav_dt,
        'qdii' AS source_kind,
        COALESCE(
            NULLIF(TRIM(index_nm), ''),
            NULLIF(TRIM(fund_nm_display), ''),
            NULLIF(TRIM(fund_nm), ''),
            'QDII'
        ) AS exposure_name,
        LOWER(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            COALESCE(
                                NULLIF(TRIM(index_nm), ''),
                                NULLIF(TRIM(fund_nm_display), ''),
                                NULLIF(TRIM(fund_nm), ''),
                                'QDII'
                            ),
                            ' ',
                            ''
                        ),
                        'ETF',
                        ''
                    ),
                    'etf',
                    ''
                ),
                '-',
                ''
            )
        ) AS exposure_key,
        CASE
            WHEN market = 'commodity' THEN 'commodity'
            ELSE 'equity'
        END AS asset_class,
        amount_yi AS total_amount_wan,
        volume_wan,
        CASE
            WHEN COALESCE(amount_yi, 0) > 0 AND COALESCE(pre_close, 0) > 0 THEN (amount_yi * pre_close) / 10000.0
            ELSE unit_total_yi
        END AS total_unit_amount_yi,
        fund_id
    FROM raw_jisilu_qdii
    WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
      AND (snapshot_date, fetched_at, market) IN (
        SELECT snapshot_date, fetched_at, market
        FROM latest_qdii_batch
      )

    UNION ALL

    SELECT
        snapshot_date,
        fetched_at,
        NULLIF(TRIM(nav_dt), '') AS nav_dt,
        'gold' AS source_kind,
        '黄金' AS exposure_name,
        'gold' AS exposure_key,
        'commodity' AS asset_class,
        amount_yi AS total_amount_wan,
        volume_wan,
        unit_total_yi AS total_unit_amount_yi,
        fund_id
    FROM raw_jisilu_gold
    WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
      AND (snapshot_date, fetched_at) IN (
        SELECT snapshot_date, fetched_at
        FROM latest_gold_batch
      )

    UNION ALL

    SELECT
        snapshot_date,
        fetched_at,
        COALESCE(NULLIF(TRIM(nav_dt), ''), snapshot_date) AS nav_dt,
        'money' AS source_kind,
        '货币ETF' AS exposure_name,
        'money' AS exposure_key,
        'cash' AS asset_class,
        amount_yi AS total_amount_wan,
        volume_wan,
        unit_total_yi AS total_unit_amount_yi,
        fund_id
    FROM raw_jisilu_money
),
deduped AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY source_kind, fund_id, nav_dt
                ORDER BY fetched_at DESC, snapshot_date DESC
            ) AS rn
        FROM unified
    )
    WHERE rn = 1
),
aggregated AS (
    SELECT
        snapshot_date,
        nav_dt,
        exposure_key,
        MIN(exposure_name) AS exposure_name,
        MIN(asset_class) AS asset_class,
        GROUP_CONCAT(DISTINCT source_kind) AS source_kind,
        SUM(COALESCE(total_amount_wan, 0)) AS total_amount_wan,
        SUM(COALESCE(volume_wan, 0)) AS total_volume_wan,
        SUM(COALESCE(total_unit_amount_yi, 0)) AS total_unit_amount_yi,
        COUNT(DISTINCT fund_id) AS etf_count
    FROM deduped
    GROUP BY snapshot_date, nav_dt, exposure_key
)
SELECT
    snapshot_date,
    nav_dt,
    exposure_key,
    exposure_name,
    asset_class,
    source_kind,
    total_amount_wan,
    total_unit_amount_yi,
    total_volume_wan,
    etf_count
FROM aggregated;

COMMIT;
