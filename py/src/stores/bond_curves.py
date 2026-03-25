from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.models import CurveSnapshot
from src.core.config import CURVE_VALUE_COLUMNS, CURVES, TABLE_RAW_BOND_CURVE
from src.sources.base import FetchResult
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
    """收益率曲线原始表 store。

    职责分工：
    - source 负责抓取中债曲线并产出 CurveSnapshot 列表
    - store 负责把 snapshot 映射成 raw_bond_curve 多行
    - job 负责定义抓取日期与增量写入策略
    """

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

    def count_rows_for_date(self, date: str) -> int:
        return self.count_rows(date=date)

    def has_complete_curves_for_date(
        self,
        date: str,
        *,
        expected_curves: list[str] | None = None,
        min_filled_terms: int = 3,
    ) -> bool:
        """判断某日是否已经有一整组可用曲线。"""

        expected = expected_curves or [curve.name for curve in CURVES]
        rows = self.fetch_between(date, date)
        if len(rows) < len(expected):
            return False

        row_map = {str(row.get("curve") or ""): row for row in rows}
        for curve_name in expected:
            row = row_map.get(curve_name)
            if not row:
                return False
            filled_terms = sum(1 for tenor_col in CURVE_VALUE_COLUMNS if row.get(tenor_col) is not None)
            if filled_terms < min_filled_terms:
                return False
        return True

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
    def build_row_from_snapshot(snapshot: CurveSnapshot) -> dict[str, Any]:
        """把单条曲线快照转成表行。"""

        row: dict[str, Any] = {
            "date": snapshot.date,
            "curve": snapshot.curve_name,
        }
        for tenor_col in CURVE_VALUE_COLUMNS:
            row[tenor_col] = None
        for raw_term, value in snapshot.points.items():
            if float(raw_term).is_integer():
                normalized = str(int(raw_term))
            else:
                normalized = str(raw_term).replace(".", "_")
            tenor_col = f"y_{normalized}"
            if tenor_col in row:
                row[tenor_col] = value
        return row

    @classmethod
    def build_rows_from_fetch_result(
        cls,
        fetch_result: FetchResult[list[CurveSnapshot]],
    ) -> list[dict[str, Any]]:
        """把 source 返回的曲线抓取结果转成多条表行。"""

        return [cls.build_row_from_snapshot(snapshot) for snapshot in fetch_result.payload]

    @staticmethod
    def _require_tenor(tenor_col: str) -> None:
        if tenor_col not in CURVE_VALUE_COLUMNS:
            raise ValueError(f"invalid tenor column: {tenor_col}")
