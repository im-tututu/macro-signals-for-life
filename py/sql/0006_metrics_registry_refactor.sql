BEGIN;

CREATE TABLE IF NOT EXISTS metric_registry (
    code TEXT PRIMARY KEY,
    name_cn TEXT NOT NULL,
    name_en TEXT,
    category TEXT,
    unit TEXT,
    importance TEXT,
    interpret_up TEXT,
    interpret_down TEXT,
    primary_use TEXT,
    note TEXT,
    calc_type TEXT NOT NULL DEFAULT 'precomputed',
    depends_on_json TEXT,
    schedule_group TEXT NOT NULL DEFAULT 'manual',
    is_active INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS metric_snapshot (
    as_of_date TEXT NOT NULL,
    code TEXT NOT NULL,
    history_start TEXT,
    history_end TEXT,
    sample_count INTEGER,
    missing_count INTEGER,
    latest_date TEXT,
    latest_value REAL,
    prev_value REAL,
    change_5d REAL,
    change_20d REAL,
    deviation_ma20 REAL,
    percentile_250 REAL,
    percentile_3y REAL,
    percentile_all REAL,
    zscore_1y REAL,
    distance_median_1y_bp REAL,
    computed_at TEXT,
    PRIMARY KEY (as_of_date, code)
);

CREATE INDEX IF NOT EXISTS idx_metric_snapshot_code_date
ON metric_snapshot(code, as_of_date);

CREATE INDEX IF NOT EXISTS idx_metric_registry_active
ON metric_registry(is_active, code);

COMMIT;
