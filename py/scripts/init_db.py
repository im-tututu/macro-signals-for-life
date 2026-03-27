#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

try:
    from src.core.config import AppConfig, load_local_env  # type: ignore
except Exception:
    CURRENT_DIR = Path(__file__).resolve().parent
    PY_ROOT = CURRENT_DIR.parent
    SRC_DIR = PY_ROOT / "src"
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    from core.config import AppConfig, load_local_env  # type: ignore


def parse_args() -> argparse.Namespace:
    load_local_env()
    cfg = AppConfig.load()

    parser = argparse.ArgumentParser(description="根据 SQL schema 初始化 SQLite 数据库。")
    parser.add_argument(
        "--db",
        default=str(cfg.db_path),
        help="SQLite 数据库文件路径。默认读取 .env 中的 DB_PATH，或使用 runtime/db/app.sqlite。",
    )
    parser.add_argument(
        "--sql",
        default=str(cfg.init_sql_path),
        help="初始化 SQL 文件路径。默认读取 .env 中的 INIT_SQL_PATH，或使用 py/sql/0001_init.sql。",
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

        print(f"[OK] 已初始化数据库：{db_path}")
        print(f"[OK] 已应用 schema：{sql_path}")
        print(f"[OK] 数据表数量：{len(tables)}")
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
