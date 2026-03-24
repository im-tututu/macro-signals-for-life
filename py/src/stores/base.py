from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

from src.core.config import AppConfig, default_sql_paths, ensure_runtime_dirs
from src.core.db import connect, list_columns, run_sql_files, table_exists, transaction
from src.core.runtime import RunContext, WriteStats


@dataclass(frozen=True)
class TableSpec:
    table_name: str
    key_fields: tuple[str, ...]
    date_field: str | None = None
    numeric_fields: tuple[str, ...] = ()
    integer_fields: tuple[str, ...] = ()
    text_fields: tuple[str, ...] = ()
    datetime_fields: tuple[str, ...] = ()
    compare_fields: tuple[str, ...] = ()
    default_order_by: tuple[str, ...] = ()

    @property
    def known_fields(self) -> tuple[str, ...]:
        merged = list(self.key_fields)
        for chunk in (
            self.numeric_fields,
            self.integer_fields,
            self.text_fields,
            self.datetime_fields,
            self.compare_fields,
            self.default_order_by,
        ):
            for field_name in chunk:
                if field_name not in merged:
                    merged.append(field_name)
        return tuple(merged)


@dataclass
class ReviewReport:
    table_name: str
    total_rows: int
    duplicate_key_groups: int
    duplicate_key_rows: int
    blank_key_rows: int
    null_date_rows: int
    min_date: str | None
    max_date: str | None


class BaseSqliteStore:
    """Generic table store for raw data tables.

    The raw GAS code follows a consistent pattern:
    1. ensure header / table structure
    2. insert or update one row
    3. batch append or range backfill
    4. skip or dedupe existing business keys

    This base class centralizes that workflow for SQLite so each concrete store
    only needs to declare its table spec and its domain-specific query helpers.
    """

    spec: TableSpec

    def __init__(
        self,
        db_path: Path | None = None,
        *,
        auto_init: bool = True,
    ) -> None:
        ensure_runtime_dirs()
        cfg = AppConfig.load()
        self.db_path = Path(db_path or cfg.db_path)
        self._table_columns: list[str] | None = None
        if auto_init:
            self.initialize()

    def initialize(self) -> None:
        conn = connect(self.db_path)
        try:
            run_sql_files(conn, default_sql_paths())
            if not table_exists(conn, self.spec.table_name):
                raise RuntimeError(f"table does not exist after init: {self.spec.table_name}")
            conn.commit()
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return connect(self.db_path)

    @property
    def table_columns(self) -> list[str]:
        if self._table_columns is None:
            with self._connect() as conn:
                self._table_columns = list_columns(conn, self.spec.table_name)
        return self._table_columns

    def reset_column_cache(self) -> None:
        self._table_columns = None

    # -----------------------------
    # Read helpers
    # -----------------------------
    def fetch_latest_date(self, **filters: object) -> str | None:
        if not self.spec.date_field:
            return None
        where_sql, params = self._build_where(filters)
        sql = f"SELECT MAX({self.spec.date_field}) AS latest_date FROM {self.spec.table_name}{where_sql}"
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return None if row is None else row["latest_date"]

    def fetch_between(self, start_date: str, end_date: str, **filters: object) -> list[dict[str, Any]]:
        if not self.spec.date_field:
            raise ValueError(f"{self.spec.table_name} does not have date_field")
        where_parts = [f"{self.spec.date_field} >= ?", f"{self.spec.date_field} <= ?"]
        params: list[Any] = [start_date, end_date]
        extra_where, extra_params = self._build_where(filters, leading_where=False)
        if extra_where:
            where_parts.append(extra_where)
            params.extend(extra_params)
        order_by = self._order_by_sql()
        sql = f"SELECT * FROM {self.spec.table_name} WHERE {' AND '.join(where_parts)}{order_by}"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(row) for row in rows]

    def fetch_recent(self, limit: int = 250, **filters: object) -> list[dict[str, Any]]:
        where_sql, params = self._build_where(filters)
        order_by = self._order_by_sql(desc=True)
        sql = f"SELECT * FROM {self.spec.table_name}{where_sql}{order_by} LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(row) for row in rows]

    def get_by_key(self, **key_values: object) -> dict[str, Any] | None:
        self._require_complete_key(key_values)
        where_sql, params = self._build_key_where(key_values)
        sql = f"SELECT * FROM {self.spec.table_name} WHERE {where_sql} LIMIT 1"
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return None if row is None else dict(row)

    def count_rows(self, **filters: object) -> int:
        where_sql, params = self._build_where(filters)
        sql = f"SELECT COUNT(*) AS cnt FROM {self.spec.table_name}{where_sql}"
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return 0 if row is None else int(row["cnt"])

    # -----------------------------
    # Write helpers
    # -----------------------------
    def upsert_one(
        self,
        row: dict[str, Any],
        *,
        dry_run: bool = False,
        run: RunContext | None = None,
    ) -> WriteStats:
        return self.upsert_many([row], dry_run=dry_run, run=run)

    def upsert_many(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        dry_run: bool = False,
        run: RunContext | None = None,
    ) -> WriteStats:
        stats = WriteStats()
        materialized = list(rows)
        stats.incoming = len(materialized)
        deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
        for raw_row in materialized:
            try:
                normalized = self.normalize_row(raw_row)
                key = self._business_key(normalized)
                deduped[key] = normalized
            except Exception as exc:
                stats.failed += 1
                if run:
                    run.record_error(
                        "normalize_row_failed",
                        str(exc),
                        source_name=self.spec.table_name,
                        object_name=self.spec.table_name,
                        row_payload=raw_row,
                    )
        with self._connect() as conn, transaction(conn, dry_run=dry_run) as tx:
            for normalized in deduped.values():
                existing = self._get_existing(tx, normalized)
                if existing is None:
                    self._insert_row(tx, normalized)
                    stats.inserted += 1
                    stats.changed += 1
                    continue
                if self._rows_equal(existing, normalized):
                    stats.skipped += 1
                    continue
                self._update_row(tx, normalized)
                stats.updated += 1
                stats.changed += 1
        return stats

    def delete_between(
        self,
        start_date: str,
        end_date: str,
        *,
        dry_run: bool = True,
        run: RunContext | None = None,
        **filters: object,
    ) -> WriteStats:
        if not self.spec.date_field:
            raise ValueError(f"{self.spec.table_name} does not have date_field")
        stats = WriteStats()
        where_parts = [f"{self.spec.date_field} >= ?", f"{self.spec.date_field} <= ?"]
        params: list[Any] = [start_date, end_date]
        extra_where, extra_params = self._build_where(filters, leading_where=False)
        if extra_where:
            where_parts.append(extra_where)
            params.extend(extra_params)
        sql = f"DELETE FROM {self.spec.table_name} WHERE {' AND '.join(where_parts)}"
        with self._connect() as conn, transaction(conn, dry_run=dry_run) as tx:
            cursor = tx.execute(sql, params)
            stats.deleted = cursor.rowcount if cursor.rowcount >= 0 else 0
            stats.changed = stats.deleted
        return stats

    # -----------------------------
    # Review / clean
    # -----------------------------
    def review_data(self) -> ReviewReport:
        with self._connect() as conn:
            total_rows = self.count_rows()
            duplicate_info = self._duplicate_key_info(conn)
            blank_key_rows = self._blank_key_rows(conn)
            null_date_rows = self._null_date_rows(conn)
            min_date = max_date = None
            if self.spec.date_field:
                row = conn.execute(
                    f"SELECT MIN({self.spec.date_field}) AS min_date, MAX({self.spec.date_field}) AS max_date FROM {self.spec.table_name}"
                ).fetchone()
                if row is not None:
                    min_date = row["min_date"]
                    max_date = row["max_date"]
            return ReviewReport(
                table_name=self.spec.table_name,
                total_rows=total_rows,
                duplicate_key_groups=duplicate_info[0],
                duplicate_key_rows=duplicate_info[1],
                blank_key_rows=blank_key_rows,
                null_date_rows=null_date_rows,
                min_date=min_date,
                max_date=max_date,
            )

    def clean_data(
        self,
        *,
        dry_run: bool = True,
        delete_invalid_rows: bool = False,
        run: RunContext | None = None,
    ) -> WriteStats:
        """Normalize in-table rows.

        Cleaning is conservative:
        - whitespace and empty-string normalization
        - numeric parsing
        - date/datetime normalization
        - optional deletion of rows whose business key becomes incomplete
        """
        stats = WriteStats()
        with self._connect() as conn, transaction(conn, dry_run=dry_run) as tx:
            rows = tx.execute(f"SELECT rowid AS _rowid_, * FROM {self.spec.table_name}").fetchall()
            stats.reviewed = len(rows)
            for row in rows:
                current = dict(row)
                rowid = int(current.pop("_rowid_"))
                try:
                    normalized = self.normalize_row(current)
                except Exception as exc:
                    if delete_invalid_rows:
                        tx.execute(f"DELETE FROM {self.spec.table_name} WHERE rowid = ?", (rowid,))
                        stats.deleted += 1
                        stats.changed += 1
                    else:
                        stats.failed += 1
                    if run:
                        run.record_error(
                            "clean_row_failed",
                            str(exc),
                            source_name=self.spec.table_name,
                            object_name=self.spec.table_name,
                            row_payload=current,
                        )
                    continue
                if self._rows_equal(current, normalized):
                    stats.skipped += 1
                    continue
                current_key = self._business_key(current)
                new_key = self._business_key(normalized)
                if current_key == new_key:
                    self._update_row_by_rowid(tx, rowid, normalized)
                    stats.updated += 1
                    stats.changed += 1
                    continue
                if any(part in (None, "") for part in new_key):
                    if delete_invalid_rows:
                        tx.execute(f"DELETE FROM {self.spec.table_name} WHERE rowid = ?", (rowid,))
                        stats.deleted += 1
                        stats.changed += 1
                    else:
                        stats.failed += 1
                    continue
                existing = self._get_existing(tx, normalized)
                if existing is None:
                    self._insert_row(tx, normalized)
                else:
                    self._update_row(tx, normalized)
                tx.execute(f"DELETE FROM {self.spec.table_name} WHERE rowid = ?", (rowid,))
                stats.updated += 1
                stats.changed += 1
        return stats

    # -----------------------------
    # Internals
    # -----------------------------
    def normalize_row(self, row: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        allowed_columns = set(self.table_columns)
        for field_name, raw_value in row.items():
            if field_name not in allowed_columns:
                continue
            if field_name in self.spec.numeric_fields:
                normalized[field_name] = self._to_float(raw_value)
            elif field_name in self.spec.integer_fields:
                normalized[field_name] = self._to_int(raw_value)
            elif field_name in self.spec.datetime_fields:
                normalized[field_name] = self._to_datetime_text(raw_value)
            elif field_name == self.spec.date_field:
                normalized[field_name] = self._to_date_text(raw_value)
            elif field_name in self.spec.text_fields or field_name in self.spec.key_fields:
                normalized[field_name] = self._to_text(raw_value)
            else:
                normalized[field_name] = raw_value
        for key_name in self.spec.key_fields:
            if normalized.get(key_name) in (None, ""):
                raise ValueError(f"missing key field: {key_name}")
        return normalized

    def _order_by_sql(self, *, desc: bool = False) -> str:
        order_by = self.spec.default_order_by or self.spec.key_fields
        direction = " DESC" if desc else " ASC"
        return " ORDER BY " + ", ".join(f"{field_name}{direction}" for field_name in order_by)

    def _build_where(
        self,
        filters: dict[str, object],
        *,
        leading_where: bool = True,
    ) -> tuple[str, list[Any]]:
        parts: list[str] = []
        params: list[Any] = []
        for field_name, value in filters.items():
            if value is None:
                continue
            if field_name not in self.table_columns:
                raise ValueError(f"invalid filter field: {field_name}")
            parts.append(f"{field_name} = ?")
            params.append(value)
        if not parts:
            return ("", params)
        prefix = " WHERE " if leading_where else " AND ".join(parts)
        return (prefix + " AND ".join(parts) if leading_where else " AND ".join(parts), params)

    def _build_key_where(self, key_values: dict[str, object]) -> tuple[str, list[Any]]:
        self._require_complete_key(key_values)
        where_sql = " AND ".join(f"{field_name} = ?" for field_name in self.spec.key_fields)
        params = [key_values[field_name] for field_name in self.spec.key_fields]
        return where_sql, params

    def _require_complete_key(self, key_values: dict[str, object]) -> None:
        missing = [field_name for field_name in self.spec.key_fields if field_name not in key_values]
        if missing:
            raise ValueError(f"missing key fields: {missing}")

    def _business_key(self, row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(row.get(field_name) for field_name in self.spec.key_fields)

    def _get_existing(self, conn: sqlite3.Connection, row: dict[str, Any]) -> dict[str, Any] | None:
        where_sql, params = self._build_key_where({field_name: row[field_name] for field_name in self.spec.key_fields})
        sql = f"SELECT * FROM {self.spec.table_name} WHERE {where_sql} LIMIT 1"
        found = conn.execute(sql, params).fetchone()
        return None if found is None else dict(found)

    def _insert_row(self, conn: sqlite3.Connection, row: dict[str, Any]) -> None:
        fields = [field_name for field_name in self.table_columns if field_name in row]
        placeholders = ", ".join("?" for _ in fields)
        sql = f"INSERT INTO {self.spec.table_name} ({', '.join(fields)}) VALUES ({placeholders})"
        conn.execute(sql, [row.get(field_name) for field_name in fields])

    def _update_row(self, conn: sqlite3.Connection, row: dict[str, Any]) -> None:
        update_fields = [
            field_name
            for field_name in self.table_columns
            if field_name in row and field_name not in self.spec.key_fields
        ]
        assignments = ", ".join(f"{field_name} = ?" for field_name in update_fields)
        where_sql, key_params = self._build_key_where({field_name: row[field_name] for field_name in self.spec.key_fields})
        sql = f"UPDATE {self.spec.table_name} SET {assignments} WHERE {where_sql}"
        conn.execute(sql, [row.get(field_name) for field_name in update_fields] + key_params)

    def _update_row_by_rowid(self, conn: sqlite3.Connection, rowid: int, row: dict[str, Any]) -> None:
        update_fields = [field_name for field_name in self.table_columns if field_name in row]
        assignments = ", ".join(f"{field_name} = ?" for field_name in update_fields)
        sql = f"UPDATE {self.spec.table_name} SET {assignments} WHERE rowid = ?"
        conn.execute(sql, [row.get(field_name) for field_name in update_fields] + [rowid])

    def _rows_equal(self, left: dict[str, Any], right: dict[str, Any]) -> bool:
        candidate_fields = self.spec.compare_fields or tuple(right.keys())
        for field_name in candidate_fields:
            if field_name not in self.table_columns:
                continue
            if field_name not in right:
                continue
            if left.get(field_name) != right.get(field_name):
                return False
        return True

    def _duplicate_key_info(self, conn: sqlite3.Connection) -> tuple[int, int]:
        key_expr = ", ".join(self.spec.key_fields)
        sql = (
            f"SELECT COUNT(*) AS groups_count, COALESCE(SUM(cnt), 0) AS rows_count FROM ("
            f"SELECT COUNT(*) AS cnt FROM {self.spec.table_name} GROUP BY {key_expr} HAVING COUNT(*) > 1"
            f")"
        )
        row = conn.execute(sql).fetchone()
        if row is None:
            return (0, 0)
        return (int(row["groups_count"] or 0), int(row["rows_count"] or 0))

    def _blank_key_rows(self, conn: sqlite3.Connection) -> int:
        parts = [f"({field_name} IS NULL OR TRIM(CAST({field_name} AS TEXT)) = '')" for field_name in self.spec.key_fields]
        sql = f"SELECT COUNT(*) AS cnt FROM {self.spec.table_name} WHERE {' OR '.join(parts)}"
        row = conn.execute(sql).fetchone()
        return 0 if row is None else int(row["cnt"])

    def _null_date_rows(self, conn: sqlite3.Connection) -> int:
        if not self.spec.date_field:
            return 0
        sql = f"SELECT COUNT(*) AS cnt FROM {self.spec.table_name} WHERE {self.spec.date_field} IS NULL OR TRIM(CAST({self.spec.date_field} AS TEXT)) = ''"
        row = conn.execute(sql).fetchone()
        return 0 if row is None else int(row["cnt"])

    @staticmethod
    def _to_text(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            return text or None
        text = str(value).strip()
        return text or None

    def _to_date_text(self, value: Any) -> str | None:
        text = self._to_text(value)
        if text is None:
            return None
        text = text.replace("/", "-").replace(".", "-")
        if len(text) >= 10:
            return text[:10]
        return text

    def _to_datetime_text(self, value: Any) -> str | None:
        text = self._to_text(value)
        if text is None:
            return None
        text = text.replace("T", " ")
        return text[:19] if len(text) >= 19 else text

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        if text.endswith("%"):
            text = text[:-1]
        if text in {"-", "--", "N/A", "null", "None"}:
            return None
        return float(text)

    @staticmethod
    def _to_int(value: Any) -> int | None:
        if value is None or value == "":
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        return int(float(text))
