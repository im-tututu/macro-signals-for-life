from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.config import TABLE_RAW_OVERSEAS_MACRO
from .base import BaseSqliteStore, TableSpec


OVERSEAS_NUMERIC_FIELDS = (
    "fed_upper",
    "fed_lower",
    "sofr",
    "ust_2y",
    "ust_10y",
    "us_real_10y",
    "usd_broad",
    "usd_cny",
    "gold",
    "wti",
    "brent",
    "copper",
    "vix",
    "spx",
    "nasdaq_100",
)

OVERSEAS_SPEC = TableSpec(
    table_name=TABLE_RAW_OVERSEAS_MACRO,
    key_fields=("date",),
    date_field="date",
    numeric_fields=OVERSEAS_NUMERIC_FIELDS,
    integer_fields=("source_row_num",),
    text_fields=("source", "source_sheet"),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=(
        "date",
        *OVERSEAS_NUMERIC_FIELDS,
        "source",
        "fetched_at",
        "source_sheet",
        "source_row_num",
        "migrated_at",
    ),
    default_order_by=("date",),
)


@dataclass
class OverseasStore(BaseSqliteStore):
    spec: TableSpec = OVERSEAS_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    def fetch_series(self, field_name: str, limit: int = 250) -> list[dict[str, Any]]:
        if field_name not in OVERSEAS_NUMERIC_FIELDS:
            raise ValueError(f"invalid overseas field: {field_name}")
        sql = f"SELECT date, {field_name} AS value FROM {self.spec.table_name} ORDER BY date DESC LIMIT ?"
        with self._connect() as conn:
            rows = conn.execute(sql, (limit,)).fetchall()
            return [dict(row) for row in rows]

    def fetch_spread_series(self, long_field: str, short_field: str, limit: int = 250) -> list[dict[str, Any]]:
        if long_field not in OVERSEAS_NUMERIC_FIELDS or short_field not in OVERSEAS_NUMERIC_FIELDS:
            raise ValueError("invalid overseas spread field")
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
