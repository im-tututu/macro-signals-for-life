from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.config import CURVE_VALUE_COLUMNS, TABLE_RAW_BOND_CURVE
from .base import BaseSqliteStore, TableSpec


BOND_CURVE_SPEC = TableSpec(
    table_name=TABLE_RAW_BOND_CURVE,
    key_fields=("date", "curve"),
    date_field="date",
    numeric_fields=tuple(CURVE_VALUE_COLUMNS),
    integer_fields=("source_row_num",),
    text_fields=("curve", "source_sheet"),
    datetime_fields=("migrated_at",),
    compare_fields=("date", "curve", *CURVE_VALUE_COLUMNS, "source_sheet", "source_row_num", "migrated_at"),
    default_order_by=("date", "curve"),
)


@dataclass
class BondCurveStore(BaseSqliteStore):
    spec: TableSpec = BOND_CURVE_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    def fetch_curve_window(self, curve: str, limit: int = 250) -> list[dict[str, Any]]:
        return self.fetch_recent(curve=curve, limit=limit)

    def fetch_curve_points(self, curve: str, tenor_col: str, limit: int = 250) -> list[dict[str, Any]]:
        self._require_tenor(tenor_col)
        sql = f"""
            SELECT date, curve, {tenor_col} AS value
              FROM {self.spec.table_name}
             WHERE curve = ?
          ORDER BY date DESC
             LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(sql, (curve, limit)).fetchall()
            return [dict(row) for row in rows]

    def fetch_latest_curve_row(self, curve: str) -> dict[str, Any] | None:
        rows = self.fetch_recent(curve=curve, limit=1)
        return rows[0] if rows else None

    def fetch_latest_curve_value(self, curve: str, tenor_col: str) -> float | None:
        self._require_tenor(tenor_col)
        row = self.fetch_latest_curve_row(curve)
        if row is None:
            return None
        value = row.get(tenor_col)
        return None if value is None else float(value)

    def fetch_term_spread_series(
        self,
        curve: str,
        long_tenor_col: str,
        short_tenor_col: str,
        limit: int = 250,
    ) -> list[dict[str, Any]]:
        self._require_tenor(long_tenor_col)
        self._require_tenor(short_tenor_col)
        sql = f"""
            SELECT date,
                   curve,
                   {long_tenor_col} AS long_value,
                   {short_tenor_col} AS short_value,
                   ({long_tenor_col} - {short_tenor_col}) AS spread
              FROM {self.spec.table_name}
             WHERE curve = ?
          ORDER BY date DESC
             LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(sql, (curve, limit)).fetchall()
            return [dict(row) for row in rows]

    def list_curves(self) -> list[str]:
        sql = f"SELECT DISTINCT curve FROM {self.spec.table_name} WHERE curve IS NOT NULL ORDER BY curve ASC"
        with self._connect() as conn:
            rows = conn.execute(sql).fetchall()
            return [str(row[0]) for row in rows]

    def upsert_curve_map(
        self,
        *,
        date: str,
        curve: str,
        points: dict[str, Any],
        source_sheet: str | None = None,
        source_row_num: int | None = None,
        migrated_at: str | None = None,
    ) -> dict[str, Any]:
        row: dict[str, Any] = {
            "date": date,
            "curve": curve,
            "source_sheet": source_sheet,
            "source_row_num": source_row_num,
            "migrated_at": migrated_at,
        }
        for tenor_col in CURVE_VALUE_COLUMNS:
            if tenor_col in points:
                row[tenor_col] = points[tenor_col]
        return row

    @staticmethod
    def _require_tenor(tenor_col: str) -> None:
        if tenor_col not in CURVE_VALUE_COLUMNS:
            raise ValueError(f"invalid tenor column: {tenor_col}")
