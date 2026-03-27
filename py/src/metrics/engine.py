from __future__ import annotations

import math
import sqlite3
from pathlib import Path
from statistics import median
from typing import Any

from src.core.db import connect, transaction
from src.core.utils import now_text, today_ymd


def list_metric_registry(*, db_path: str | None = None) -> list[dict[str, Any]]:
    resolved_db_path = Path(db_path) if db_path else None
    conn = connect(resolved_db_path)
    try:
        rows = conn.execute(
            """
            SELECT code, name_cn, name_en, category, unit, importance,
                   interpret_up, interpret_down, primary_use, note,
                   calc_type, depends_on_json, schedule_group, is_active
            FROM metric_registry
            WHERE is_active = 1
            ORDER BY code ASC
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _query_metric_daily_series(
    conn: sqlite3.Connection,
    *,
    code: str,
    as_of_date: str | None,
) -> list[tuple[str, float]]:
    rows = conn.execute(
        """
        SELECT date, value
        FROM metric_daily
        WHERE code = ?
          AND value IS NOT NULL
          AND (? IS NULL OR date <= ?)
        ORDER BY date ASC
        """,
        (code, as_of_date, as_of_date),
    ).fetchall()
    out: list[tuple[str, float]] = []
    for row in rows:
        d = str(row["date"] or "")
        if not d:
            continue
        try:
            out.append((d, float(row["value"])))
        except (TypeError, ValueError):
            continue
    return out


def _lag(values: list[float], n: int) -> float | None:
    if len(values) <= n:
        return None
    return values[-1 - n]


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _std(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    m = _mean(values)
    if m is None:
        return None
    var = sum((x - m) ** 2 for x in values) / (len(values) - 1)
    if var <= 0:
        return None
    return math.sqrt(var)


def _percentile_rank(window: list[float], latest: float) -> float | None:
    if not window:
        return None
    min_value = min(window)
    max_value = max(window)
    if min_value == max_value:
        return 0.5
    less = sum(1 for x in window if x < latest)
    equal = sum(1 for x in window if x == latest)
    return (less + 0.5 * equal) / len(window)


def _bp_factor(unit: str, values: list[float]) -> float:
    if "0-1" in unit:
        max_abs = max((abs(v) for v in values), default=0.0)
        return 10000.0 if max_abs <= 1.5 else 100.0
    if unit == "bp":
        return 1.0
    return 0.0


def _snapshot_from_series(
    *,
    code: str,
    unit: str,
    rows: list[tuple[str, float]],
    baseline_count: int,
    as_of_date: str,
) -> dict[str, Any]:
    values = [v for _, v in rows]
    dates = [d for d, _ in rows]

    latest_value = values[-1] if values else None
    prev_value = values[-2] if len(values) >= 2 else None

    lag5 = _lag(values, 5)
    lag20 = _lag(values, 20)
    win20 = values[-20:] if values else []
    win250 = values[-250:] if values else []
    win750 = values[-750:] if values else []
    win_all = values
    has_20 = len(win20) >= 20
    has_250 = len(win250) >= 250
    has_750 = len(win750) >= 750

    change_5d = None if latest_value is None or lag5 is None else latest_value - lag5
    change_20d = None if latest_value is None or lag20 is None else latest_value - lag20
    ma20 = _mean(win20)
    deviation_ma20 = None if latest_value is None or ma20 is None else latest_value - ma20
    if not has_20:
        change_20d = None
        deviation_ma20 = None
    pct250 = None if latest_value is None or not has_250 else _percentile_rank(win250, latest_value)
    pct3y = None if latest_value is None or not has_750 else _percentile_rank(win750, latest_value)
    pctall = None if latest_value is None else _percentile_rank(win_all, latest_value)
    std250 = _std(win250) if has_250 else None
    mean250 = _mean(win250) if has_250 else None
    z1y = None if latest_value is None or std250 in (None, 0) or mean250 is None else (latest_value - mean250) / std250
    med250 = median(win250) if has_250 else None
    bp_factor = _bp_factor(unit, values)
    dist_median_bp = None if latest_value is None or med250 is None or bp_factor <= 0 else (latest_value - med250) * bp_factor

    if bp_factor > 0:
        change_5d = None if change_5d is None else change_5d * bp_factor
        change_20d = None if change_20d is None else change_20d * bp_factor
        deviation_ma20 = None if deviation_ma20 is None else deviation_ma20 * bp_factor
    else:
        dist_median_bp = None

    sample_count = len(values)
    missing_count = max(0, baseline_count - sample_count)
    return {
        "as_of_date": as_of_date,
        "code": code,
        "history_start": dates[0] if dates else None,
        "history_end": dates[-1] if dates else None,
        "sample_count": sample_count,
        "missing_count": missing_count,
        "latest_date": dates[-1] if dates else None,
        "latest_value": latest_value,
        "prev_value": prev_value,
        "change_5d": change_5d,
        "change_20d": change_20d,
        "deviation_ma20": deviation_ma20,
        "percentile_250": pct250,
        "percentile_3y": pct3y,
        "percentile_all": pctall,
        "zscore_1y": z1y,
        "distance_median_1y_bp": dist_median_bp,
        "computed_at": now_text(),
    }


def build_metric_snapshots(*, db_path: str | None = None, as_of_date: str | None = None) -> list[dict[str, Any]]:
    target_date = as_of_date or today_ymd()
    registry = list_metric_registry(db_path=db_path)
    resolved_db_path = Path(db_path) if db_path else None
    conn = connect(resolved_db_path)
    try:
        series_by_code: dict[str, list[tuple[str, float]]] = {}
        for item in registry:
            code = str(item["code"])
            series_by_code[code] = _query_metric_daily_series(conn, code=code, as_of_date=target_date)

        baseline = len(series_by_code.get("gov_1y", []))
        if baseline <= 0:
            baseline = max((len(v) for v in series_by_code.values()), default=0)

        snapshots: list[dict[str, Any]] = []
        for item in registry:
            code = str(item["code"])
            unit = str(item["unit"] or "原值")
            rows = series_by_code.get(code, [])
            snapshots.append(
                _snapshot_from_series(
                    code=code,
                    unit=unit,
                    rows=rows,
                    baseline_count=baseline,
                    as_of_date=target_date,
                )
            )
        return snapshots
    finally:
        conn.close()


def upsert_metric_registry_rows(
    rows: list[dict[str, Any]],
    *,
    db_path: str | None = None,
    dry_run: bool = False,
) -> int:
    resolved_db_path = Path(db_path) if db_path else None
    conn = connect(resolved_db_path)
    try:
        payload: list[dict[str, Any]] = []
        ts = now_text()
        for row in rows:
            code = str(row.get("code") or "").strip()
            if not code:
                continue
            payload.append(
                {
                    "code": code,
                    "name_cn": str(row.get("name_cn") or code),
                    "name_en": str(row.get("name_en") or code),
                    "category": str(row.get("category") or "未分类"),
                    "unit": str(row.get("unit") or "原值"),
                    "importance": str(row.get("importance") or "观察"),
                    "interpret_up": str(row.get("interpret_up") or ""),
                    "interpret_down": str(row.get("interpret_down") or ""),
                    "primary_use": str(row.get("primary_use") or ""),
                    "note": str(row.get("note") or ""),
                    "calc_type": str(row.get("calc_type") or "precomputed"),
                    "depends_on_json": row.get("depends_on_json"),
                    "schedule_group": str(row.get("schedule_group") or "manual"),
                    "is_active": int(row.get("is_active") or 1),
                    "updated_at": ts,
                }
            )
        sql = """
            INSERT INTO metric_registry (
                code, name_cn, name_en, category, unit, importance,
                interpret_up, interpret_down, primary_use, note,
                calc_type, depends_on_json, schedule_group, is_active, updated_at
            )
            VALUES (
                :code, :name_cn, :name_en, :category, :unit, :importance,
                :interpret_up, :interpret_down, :primary_use, :note,
                :calc_type, :depends_on_json, :schedule_group, :is_active, :updated_at
            )
            ON CONFLICT(code) DO UPDATE SET
                name_cn = excluded.name_cn,
                name_en = excluded.name_en,
                category = excluded.category,
                unit = excluded.unit,
                importance = excluded.importance,
                interpret_up = excluded.interpret_up,
                interpret_down = excluded.interpret_down,
                primary_use = excluded.primary_use,
                note = excluded.note,
                calc_type = excluded.calc_type,
                depends_on_json = excluded.depends_on_json,
                schedule_group = excluded.schedule_group,
                is_active = excluded.is_active,
                updated_at = excluded.updated_at
        """
        with transaction(conn, dry_run=dry_run):
            conn.executemany(sql, payload)
        return len(payload)
    finally:
        conn.close()


def upsert_metric_daily_rows(
    rows: list[dict[str, Any]],
    *,
    db_path: str | None = None,
    dry_run: bool = False,
) -> int:
    resolved_db_path = Path(db_path) if db_path else None
    conn = connect(resolved_db_path)
    try:
        payload: list[dict[str, Any]] = []
        ts = now_text()
        for row in rows:
            date_value = str(row.get("date") or "").strip()
            code = str(row.get("code") or "").strip()
            if not date_value or not code:
                continue
            try:
                value_float = float(row["value"])
            except (TypeError, ValueError, KeyError):
                continue
            fetched_at = str(row.get("fetched_at") or ts)
            source = str(row.get("source") or "metric_daily_import")
            payload.append(
                {
                    "date": date_value,
                    "code": code,
                    "value": value_float,
                    "source": source,
                    "fetched_at": fetched_at,
                    "updated_at": ts,
                }
            )
        sql = """
            INSERT INTO metric_daily (date, code, value, source, fetched_at, updated_at)
            VALUES (:date, :code, :value, :source, :fetched_at, :updated_at)
            ON CONFLICT(date, code) DO UPDATE SET
                value = excluded.value,
                source = excluded.source,
                fetched_at = excluded.fetched_at,
                updated_at = excluded.updated_at
        """
        with transaction(conn, dry_run=dry_run):
            conn.executemany(sql, payload)
        return len(payload)
    finally:
        conn.close()


def upsert_metric_snapshots(
    rows: list[dict[str, Any]],
    *,
    db_path: str | None = None,
    dry_run: bool = False,
) -> int:
    resolved_db_path = Path(db_path) if db_path else None
    conn = connect(resolved_db_path)
    try:
        sql = """
            INSERT OR REPLACE INTO metric_snapshot (
                as_of_date, code,
                history_start, history_end, sample_count, missing_count,
                latest_date, latest_value, prev_value, change_5d, change_20d, deviation_ma20,
                percentile_250, percentile_3y, percentile_all, zscore_1y, distance_median_1y_bp,
                computed_at
            )
            VALUES (
                :as_of_date, :code,
                :history_start, :history_end, :sample_count, :missing_count,
                :latest_date, :latest_value, :prev_value, :change_5d, :change_20d, :deviation_ma20,
                :percentile_250, :percentile_3y, :percentile_all, :zscore_1y, :distance_median_1y_bp,
                :computed_at
            )
        """
        with transaction(conn, dry_run=dry_run):
            conn.executemany(sql, rows)
        return len(rows)
    finally:
        conn.close()
