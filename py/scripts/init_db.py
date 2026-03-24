#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize SQLite database from SQL schema.")
    parser.add_argument(
        "--db",
        default="py/data/db.sqlite",
        help="Path to SQLite database file. Default: py/data/db.sqlite",
    )
    parser.add_argument(
        "--sql",
        default="py/sql/0001_init_import_first.sql",
        help="Path to SQL init script. Default: py/sql/0001_init_import_first.sql",
    )
    return parser.parse_args()


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def init_db(db_path: Path, sql_path: Path) -> None:
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    ensure_parent_dir(db_path)
    sql_text = sql_path.read_text(encoding="utf-8")

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.executescript(sql_text)

        tables = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()

        print(f"[OK] Initialized database: {db_path}")
        print(f"[OK] Applied schema: {sql_path}")
        print(f"[OK] Table count: {len(tables)}")
        for (name,) in tables:
            print(f"  - {name}")
    finally:
        conn.close()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    sql_path = Path(args.sql).expanduser().resolve()
    init_db(db_path, sql_path)


if __name__ == "__main__":
    main()
