BEGIN;

CREATE TABLE IF NOT EXISTS metric_daily (
    date TEXT NOT NULL,
    code TEXT NOT NULL,
    value REAL,
    source TEXT,
    fetched_at TEXT,
    updated_at TEXT,
    PRIMARY KEY (date, code)
);

CREATE INDEX IF NOT EXISTS idx_metric_daily_code_date
ON metric_daily(code, date);

CREATE INDEX IF NOT EXISTS idx_metric_daily_date
ON metric_daily(date);

COMMIT;
