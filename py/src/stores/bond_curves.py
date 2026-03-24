from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True)
class BondCurveStore:
    db_path: Path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def fetch_latest_date(self, curve: Optional[str] = None) -> Optional[str]:
        sql = "SELECT MAX(date) AS latest_date FROM raw_bond_curve"
        params: list[Any] = []
        if curve:
            sql += " WHERE curve = ?"
            params.append(curve)

        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return None if row is None else row["latest_date"]

    def fetch_between(
        self,
        start_date: str,
        end_date: str,
        curve: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT *
            FROM raw_bond_curve
            WHERE date >= ? AND date <= ?
        """
        params: list[Any] = [start_date, end_date]
        if curve:
            sql += " AND curve = ?"
            params.append(curve)
        sql += " ORDER BY date ASC, curve ASC"

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(row) for row in rows]

    def fetch_recent(
        self,
        curve: Optional[str] = None,
        limit: int = 250,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM raw_bond_curve"
        params: list[Any] = []
        if curve:
            sql += " WHERE curve = ?"
            params.append(curve)
        sql += " ORDER BY date DESC LIMIT ?"
        params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(row) for row in rows]

    def fetch_curve_window(
        self,
        curve: str,
        limit: int = 250,
    ) -> list[dict[str, Any]]:
        return self.fetch_recent(curve=curve, limit=limit)

    def fetch_curve_points(
        self,
        curve: str,
        tenor_col: str,
        limit: int = 250,
    ) -> list[dict[str, Any]]:
        allowed_cols = self._get_tenor_columns()
        if tenor_col not in allowed_cols:
            raise ValueError(f"Invalid tenor column: {tenor_col}")

        sql = f"""
            SELECT date, curve, {tenor_col} AS value
            FROM raw_bond_curve
            WHERE curve = ?
            ORDER BY date DESC
            LIMIT ?
        """

        with self._connect() as conn:
            rows = conn.execute(sql, (curve, limit)).fetchall()
            return [dict(row) for row in rows]

    def fetch_latest_curve_row(self, curve: str) -> Optional[dict[str, Any]]:
        sql = """
            SELECT *
            FROM raw_bond_curve
            WHERE curve = ?
            ORDER BY date DESC
            LIMIT 1
        """
        with self._connect() as conn:
            row = conn.execute(sql, (curve,)).fetchone()
            return None if row is None else dict(row)

    def fetch_latest_curve_value(self, curve: str, tenor_col: str) -> Optional[float]:
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
        allowed_cols = self._get_tenor_columns()
        for col in (long_tenor_col, short_tenor_col):
            if col not in allowed_cols:
                raise ValueError(f"Invalid tenor column: {col}")

        sql = f"""
            SELECT
                date,
                curve,
                {long_tenor_col} AS long_value,
                {short_tenor_col} AS short_value,
                ({long_tenor_col} - {short_tenor_col}) AS spread
            FROM raw_bond_curve
            WHERE curve = ?
            ORDER BY date DESC
            LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(sql, (curve, limit)).fetchall()
            return [dict(row) for row in rows]

    def list_curves(self) -> list[str]:
        sql = "SELECT DISTINCT curve FROM raw_bond_curve WHERE curve IS NOT NULL ORDER BY curve ASC"
        with self._connect() as conn:
            rows = conn.execute(sql).fetchall()
            return [str(row[0]) for row in rows]

    def count_rows(self, curve: Optional[str] = None) -> int:
        sql = "SELECT COUNT(*) FROM raw_bond_curve"
        params: list[Any] = []
        if curve:
            sql += " WHERE curve = ?"
            params.append(curve)
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return 0 if row is None else int(row[0])

    def _get_tenor_columns(self) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute("PRAGMA table_info(raw_bond_curve)").fetchall()
        cols = {str(row[1]) for row in rows}
        return {col for col in cols if col.startswith("y_")}
