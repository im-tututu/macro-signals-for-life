from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


@dataclass(frozen=True)
class TableCheckSpec:
    table_name: str
    date_col: Optional[str] = None
    key_cols: tuple[str, ...] = ()
    important_cols: tuple[str, ...] = ()
    sample_order_by: Optional[str] = None


DEFAULT_SPECS: tuple[TableCheckSpec, ...] = (
    TableCheckSpec(
        table_name="raw_bond_curve",
        date_col="date",
        key_cols=("date", "curve"),
        important_cols=("date", "curve", "y_1", "y_10"),
        sample_order_by="date DESC, curve ASC",
    ),
    TableCheckSpec(
        table_name="raw_money_market",
        date_col="date",
        key_cols=("date",),
        important_cols=("date", "dr007_weighted_rate"),
        sample_order_by="date DESC",
    ),
    TableCheckSpec(
        table_name="raw_policy_rate",
        date_col="date",
        key_cols=("date", "type", "term"),
        important_cols=("date", "type", "term", "rate"),
        sample_order_by="date DESC, type ASC, term ASC",
    ),
    TableCheckSpec(
        table_name="raw_overseas_macro",
        date_col="date",
        key_cols=("date",),
        important_cols=("date", "ust_10y", "dxy", "gold"),
        sample_order_by="date DESC",
    ),
    TableCheckSpec(
        table_name="raw_bond_index",
        date_col="trade_date",
        key_cols=("trade_date", "index_name"),
        important_cols=("trade_date", "index_name", "index_code", "y", "d"),
        sample_order_by="trade_date DESC, index_name ASC",
    ),
    TableCheckSpec(
        table_name="cfg_bond_index_list",
        important_cols=("index_name", "index_code"),
        sample_order_by="index_name ASC",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check imported raw tables in SQLite.")
    parser.add_argument("--db", required=True, help="Path to sqlite db file.")
    parser.add_argument(
        "--samples",
        type=int,
        default=3,
        help="Number of sample rows to print for each table.",
    )
    return parser.parse_args()


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def get_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [str(r[1]) for r in rows]


def scalar(conn: sqlite3.Connection, sql: str, params: Iterable[object] = ()) -> object:
    row = conn.execute(sql, tuple(params)).fetchone()
    return None if row is None else row[0]


def count_rows(conn: sqlite3.Connection, table_name: str) -> int:
    return int(scalar(conn, f"SELECT COUNT(*) FROM {table_name}") or 0)


def min_date(conn: sqlite3.Connection, table_name: str, date_col: str) -> Optional[str]:
    return scalar(conn, f"SELECT MIN({date_col}) FROM {table_name}")  # type: ignore[return-value]


def max_date(conn: sqlite3.Connection, table_name: str, date_col: str) -> Optional[str]:
    return scalar(conn, f"SELECT MAX({date_col}) FROM {table_name}")  # type: ignore[return-value]


def duplicate_count(conn: sqlite3.Connection, table_name: str, key_cols: tuple[str, ...]) -> int:
    if not key_cols:
        return 0
    group_by = ", ".join(key_cols)
    sql = f"""
        SELECT COALESCE(SUM(cnt - 1), 0)
        FROM (
            SELECT COUNT(*) AS cnt
            FROM {table_name}
            GROUP BY {group_by}
            HAVING COUNT(*) > 1
        ) t
    """
    return int(scalar(conn, sql) or 0)


def null_count(conn: sqlite3.Connection, table_name: str, col: str) -> int:
    sql = f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL OR TRIM(CAST({col} AS TEXT)) = ''"
    return int(scalar(conn, sql) or 0)


def fetch_samples(
    conn: sqlite3.Connection,
    table_name: str,
    limit: int,
    order_by: Optional[str],
) -> list[sqlite3.Row]:
    sql = f"SELECT * FROM {table_name}"
    if order_by:
        sql += f" ORDER BY {order_by}"
    sql += " LIMIT ?"
    return conn.execute(sql, (limit,)).fetchall()


def format_row(row: sqlite3.Row, keep_cols: list[str], max_len: int = 80) -> str:
    parts: list[str] = []
    for col in keep_cols:
        value = row[col]
        text = "" if value is None else str(value)
        if len(text) > max_len:
            text = text[: max_len - 3] + "..."
        parts.append(f"{col}={text}")
    return " | ".join(parts)


def choose_sample_columns(all_cols: list[str], important_cols: tuple[str, ...]) -> list[str]:
    chosen = [c for c in important_cols if c in all_cols]
    if chosen:
        return chosen
    return all_cols[: min(6, len(all_cols))]


def check_table(conn: sqlite3.Connection, spec: TableCheckSpec, sample_limit: int) -> None:
    print(f"\n=== {spec.table_name} ===")
    if not table_exists(conn, spec.table_name):
        print("missing table")
        return

    cols = get_columns(conn, spec.table_name)
    print(f"columns={len(cols)}")

    total = count_rows(conn, spec.table_name)
    print(f"rows={total}")

    if spec.date_col and spec.date_col in cols and total > 0:
        print(f"date_min={min_date(conn, spec.table_name, spec.date_col)}")
        print(f"date_max={max_date(conn, spec.table_name, spec.date_col)}")

    if spec.key_cols:
        valid_keys = tuple(c for c in spec.key_cols if c in cols)
        if valid_keys:
            print(f"duplicate_rows_by_key={duplicate_count(conn, spec.table_name, valid_keys)}")
        else:
            print("duplicate_rows_by_key=skip(no key columns found)")

    for col in spec.important_cols:
        if col in cols:
            print(f"null_or_blank[{col}]={null_count(conn, spec.table_name, col)}")

    sample_cols = choose_sample_columns(cols, spec.important_cols)
    rows = fetch_samples(conn, spec.table_name, sample_limit, spec.sample_order_by)
    if not rows:
        print("samples=none")
        return

    print("samples:")
    for idx, row in enumerate(rows, start=1):
        print(f"  {idx}. {format_row(row, sample_cols)}")


def main() -> None:
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    with connect(db_path) as conn:
        for spec in DEFAULT_SPECS:
            check_table(conn, spec, sample_limit=args.samples)


if __name__ == "__main__":
    main()
