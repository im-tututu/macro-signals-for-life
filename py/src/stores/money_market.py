from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.config import TABLE_RAW_MONEY_MARKET
from .base import BaseSqliteStore, TableSpec


MONEY_MARKET_RATE_FIELDS = (
    "dr001_weighted_rate",
    "dr001_latest_rate",
    "dr001_avg_prd",
    "dr007_weighted_rate",
    "dr007_latest_rate",
    "dr007_avg_prd",
    "dr014_weighted_rate",
    "dr014_latest_rate",
    "dr014_avg_prd",
    "dr021_weighted_rate",
    "dr021_latest_rate",
    "dr021_avg_prd",
    "dr1m_weighted_rate",
    "dr1m_latest_rate",
    "dr1m_avg_prd",
)

MONEY_MARKET_SPEC = TableSpec(
    table_name=TABLE_RAW_MONEY_MARKET,
    key_fields=("date",),
    date_field="date",
    numeric_fields=MONEY_MARKET_RATE_FIELDS,
    integer_fields=("source_row_num",),
    text_fields=("show_date_cn", "source_url", "source_sheet"),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=(
        "date",
        "show_date_cn",
        "source_url",
        *MONEY_MARKET_RATE_FIELDS,
        "fetched_at",
        "source_sheet",
        "source_row_num",
        "migrated_at",
    ),
    default_order_by=("date",),
)


@dataclass
class MoneyMarketStore(BaseSqliteStore):
    spec: TableSpec = MONEY_MARKET_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    def fetch_latest_rate(self, field_name: str) -> float | None:
        if field_name not in MONEY_MARKET_RATE_FIELDS:
            raise ValueError(f"invalid money market field: {field_name}")
        rows = self.fetch_recent(limit=1)
        if not rows:
            return None
        value = rows[0].get(field_name)
        return None if value is None else float(value)

    def fetch_spread_series(self, long_field: str, short_field: str, limit: int = 250) -> list[dict[str, Any]]:
        if long_field not in MONEY_MARKET_RATE_FIELDS or short_field not in MONEY_MARKET_RATE_FIELDS:
            raise ValueError("invalid money market spread field")
        sql = f"""
            SELECT date,
                   {long_field} AS long_value,
                   {short_field} AS short_value,
                   ({long_field} - {short_field}) AS spread
              FROM {self.spec.table_name}
          ORDER BY date DESC
             LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(sql, (limit,)).fetchall()
            return [dict(row) for row in rows]
