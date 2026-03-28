from __future__ import annotations

# 兼容层：
# - `daily.py` 是历史文件名，语义已不准确
# - 实际原始抓取实现已迁到 `ingest.py`
# - 保留此层是为了避免一次性改断外部脚本和旧导入路径

from .ingest import (
    fetch_bond_curve_for_date,
    fetch_bond_index_duration,
    fetch_latest_bond_curve,
    fetch_latest_etf_snapshot,
    fetch_latest_futures,
    fetch_latest_life_asset,
    fetch_latest_money_market,
    fetch_latest_overseas_macro,
    fetch_latest_policy_rate,
    fetch_recent_policy_rate_events,
    run_daily_fetcher,
)

__all__ = [
    "run_daily_fetcher",
    "fetch_latest_money_market",
    "fetch_bond_curve_for_date",
    "fetch_latest_bond_curve",
    "fetch_latest_overseas_macro",
    "fetch_recent_policy_rate_events",
    "fetch_latest_policy_rate",
    "fetch_latest_futures",
    "fetch_latest_etf_snapshot",
    "fetch_latest_life_asset",
    "fetch_bond_index_duration",
]
