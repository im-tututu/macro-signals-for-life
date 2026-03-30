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
    if str(PY_ROOT) not in sys.path:
        sys.path.insert(0, str(PY_ROOT))
    if str(CURRENT_DIR) not in sys.path:
        sys.path.insert(0, str(CURRENT_DIR))
    from src.core.config import AppConfig, load_local_env  # type: ignore


ASSET_SQL = {
    "etf": (
        "raw_jisilu_etf",
        """
        SELECT COUNT(*) AS cnt
        FROM raw_jisilu_etf
        WHERE (unit_total_yi IS NULL OR unit_total_yi = 0)
          AND amount_yi IS NOT NULL
          AND fund_nav IS NOT NULL
        """,
        """
        UPDATE raw_jisilu_etf
        SET unit_total_yi = (amount_yi * fund_nav) / 10000.0
        WHERE (unit_total_yi IS NULL OR unit_total_yi = 0)
          AND amount_yi IS NOT NULL
          AND fund_nav IS NOT NULL
        """,
    ),
    "qdii": (
        "raw_jisilu_qdii",
        """
        SELECT COUNT(*) AS cnt
        FROM raw_jisilu_qdii
        WHERE (unit_total_yi IS NULL OR unit_total_yi = 0)
          AND amount_yi IS NOT NULL
          AND price IS NOT NULL
        """,
        """
        UPDATE raw_jisilu_qdii
        SET unit_total_yi = (amount_yi * price) / 10000.0
        WHERE (unit_total_yi IS NULL OR unit_total_yi = 0)
          AND amount_yi IS NOT NULL
          AND price IS NOT NULL
        """,
    ),
    "gold": (
        "raw_jisilu_gold",
        """
        SELECT COUNT(*) AS cnt
        FROM raw_jisilu_gold
        WHERE (unit_total_yi IS NULL OR unit_total_yi = 0)
          AND amount_yi IS NOT NULL
          AND fund_nav IS NOT NULL
        """,
        """
        UPDATE raw_jisilu_gold
        SET unit_total_yi = (amount_yi * fund_nav) / 10000.0
        WHERE (unit_total_yi IS NULL OR unit_total_yi = 0)
          AND amount_yi IS NOT NULL
          AND fund_nav IS NOT NULL
        """,
    ),
}


def build_parser(cfg: AppConfig) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="统一回填 Jisilu 原始表的 unit_total_yi 缺失值。")
    parser.add_argument(
        "--db",
        default=str(cfg.db_path),
        help="SQLite 数据库路径。默认读取 .env 中的 DB_PATH，或使用 runtime/db/app.sqlite。",
    )
    parser.add_argument(
        "--asset",
        choices=("etf", "qdii", "gold", "all"),
        default="all",
        help="要回填的资产类型。默认 all。",
    )
    parser.add_argument("--dry-run", action="store_true", help="只统计不更新。")
    return parser


def connect_rw(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def backfill_one(conn: sqlite3.Connection, asset: str, *, dry_run: bool) -> int:
    table_name, count_sql, update_sql = ASSET_SQL[asset]
    pending = int(conn.execute(count_sql).fetchone()["cnt"])
    print(f"{asset}: pending={pending}")
    if dry_run or pending == 0:
        return pending
    cur = conn.execute(update_sql)
    conn.commit()
    print(f"{asset}: updated={cur.rowcount}")
    return pending


def main() -> None:
    load_local_env()
    cfg = AppConfig.load()
    args = build_parser(cfg).parse_args()
    db_path = Path(args.db).expanduser()
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    assets = ("etf", "qdii", "gold") if args.asset == "all" else (args.asset,)
    with connect_rw(db_path) as conn:
        conn.execute("PRAGMA busy_timeout=5000;")
        for asset in assets:
            backfill_one(conn, asset, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
