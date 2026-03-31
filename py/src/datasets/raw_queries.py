from __future__ import annotations

from pathlib import Path

from src.core.config import CURVE_VALUE_COLUMNS, CURVES, TABLE_RAW_BOND_CURVE
from src.core.db import connect


def has_complete_chinabond_curves_for_date(
    date: str,
    *,
    db_path: Path | None = None,
    expected_curves: list[str] | None = None,
    min_filled_terms: int = 3,
) -> bool:
    expected = expected_curves or [curve.name for curve in CURVES]
    placeholders = ", ".join("?" for _ in expected)
    sql = f"""
        SELECT curve, {", ".join(CURVE_VALUE_COLUMNS)}
          FROM {TABLE_RAW_BOND_CURVE}
         WHERE date = ?
           AND curve IN ({placeholders})
    """
    params = [date, *expected]
    with connect(db_path) as conn:
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
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
