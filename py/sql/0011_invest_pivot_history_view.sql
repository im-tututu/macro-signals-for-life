BEGIN;

DROP VIEW IF EXISTS vw_invest_pivot_history;
CREATE VIEW vw_invest_pivot_history AS
WITH instrument_lookup AS (
    SELECT
        instrument_code,
        MAX(instrument_name) AS instrument_name,
        MAX(NULLIF(TRIM(raw_exposure_name), '')) AS exposure_name
    FROM vw_invest_instrument_latest
    GROUP BY instrument_code
),
unified AS (
    SELECT
        r.snapshot_date,
        r.fetched_at,
        NULLIF(TRIM(r.nav_dt), '') AS nav_dt,
        'etf' AS source_kind,
        COALESCE(
            NULLIF(TRIM(r.index_nm), ''),
            NULLIF(TRIM(r.fund_nm), ''),
            NULLIF(TRIM(i.exposure_name), ''),
            NULLIF(TRIM(i.instrument_name), ''),
            'ETF'
        ) AS exposure_name,
        LOWER(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            COALESCE(
                                NULLIF(TRIM(r.index_nm), ''),
                                NULLIF(TRIM(r.fund_nm), ''),
                                NULLIF(TRIM(i.exposure_name), ''),
                                NULLIF(TRIM(i.instrument_name), ''),
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
        r.amount_yi AS total_amount_wan,
        r.volume_wan,
        r.unit_total_yi AS total_unit_amount_yi,
        r.fund_id
    FROM raw_jisilu_etf AS r
    LEFT JOIN instrument_lookup AS i
      ON i.instrument_code = r.fund_id
    WHERE COALESCE(NULLIF(TRIM(r.nav_dt), ''), '') <> ''

    UNION ALL

    SELECT
        r.snapshot_date,
        r.fetched_at,
        NULLIF(TRIM(r.nav_dt), '') AS nav_dt,
        'qdii' AS source_kind,
        COALESCE(
            NULLIF(TRIM(r.index_nm), ''),
            NULLIF(TRIM(r.fund_nm_display), ''),
            NULLIF(TRIM(r.fund_nm), ''),
            NULLIF(TRIM(i.exposure_name), ''),
            NULLIF(TRIM(i.instrument_name), ''),
            'QDII'
        ) AS exposure_name,
        LOWER(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            COALESCE(
                                NULLIF(TRIM(r.index_nm), ''),
                                NULLIF(TRIM(r.fund_nm_display), ''),
                                NULLIF(TRIM(r.fund_nm), ''),
                                NULLIF(TRIM(i.exposure_name), ''),
                                NULLIF(TRIM(i.instrument_name), ''),
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
            WHEN r.market = 'commodity' THEN 'commodity'
            ELSE 'equity'
        END AS asset_class,
        r.amount_yi AS total_amount_wan,
        r.volume_wan,
        CASE
            WHEN COALESCE(r.amount_yi, 0) > 0 AND COALESCE(r.pre_close, 0) > 0 THEN (r.amount_yi * r.pre_close) / 10000.0
            ELSE r.unit_total_yi
        END AS total_unit_amount_yi,
        r.fund_id
    FROM raw_jisilu_qdii AS r
    LEFT JOIN instrument_lookup AS i
      ON i.instrument_code = r.fund_id
    WHERE COALESCE(NULLIF(TRIM(r.nav_dt), ''), '') <> ''

    UNION ALL

    SELECT
        r.snapshot_date,
        r.fetched_at,
        NULLIF(TRIM(r.nav_dt), '') AS nav_dt,
        'gold' AS source_kind,
        COALESCE(NULLIF(TRIM(i.exposure_name), ''), NULLIF(TRIM(i.instrument_name), ''), '黄金') AS exposure_name,
        'gold' AS exposure_key,
        'commodity' AS asset_class,
        r.amount_yi AS total_amount_wan,
        r.volume_wan,
        r.unit_total_yi AS total_unit_amount_yi,
        r.fund_id
    FROM raw_jisilu_gold AS r
    LEFT JOIN instrument_lookup AS i
      ON i.instrument_code = r.fund_id
    WHERE COALESCE(NULLIF(TRIM(r.nav_dt), ''), '') <> ''

    UNION ALL

    SELECT
        r.snapshot_date,
        r.fetched_at,
        COALESCE(NULLIF(TRIM(r.nav_dt), ''), r.snapshot_date) AS nav_dt,
        'money' AS source_kind,
        COALESCE(NULLIF(TRIM(i.exposure_name), ''), NULLIF(TRIM(i.instrument_name), ''), '货币ETF') AS exposure_name,
        'money' AS exposure_key,
        'cash' AS asset_class,
        r.amount_yi AS total_amount_wan,
        r.volume_wan,
        r.unit_total_yi AS total_unit_amount_yi,
        r.fund_id
    FROM raw_jisilu_money AS r
    LEFT JOIN instrument_lookup AS i
      ON i.instrument_code = r.fund_id
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
