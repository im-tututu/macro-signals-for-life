-- Extra indexes placeholder

CREATE INDEX IF NOT EXISTS idx_raw_bond_curves_curve_code
ON raw_bond_curves (curve_code);

CREATE INDEX IF NOT EXISTS idx_metrics_metric_key
ON metrics (metric_key);

CREATE INDEX IF NOT EXISTS idx_signals_signal_key
ON signals (signal_key);
