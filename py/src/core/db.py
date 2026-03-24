from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from .config import AppConfig, ensure_runtime_dirs


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    """Create a sqlite connection with Row factory and FK enabled."""
    ensure_runtime_dirs()
    cfg = AppConfig.load()
    final_path = Path(db_path or cfg.db_path)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(final_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection, *, dry_run: bool = False) -> Iterator[sqlite3.Connection]:
    """Provide a transaction boundary that rolls back when dry_run=True."""
    try:
        conn.execute("BEGIN")
        yield conn
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
    except Exception:
        conn.rollback()
        raise


def run_sql_files(conn: sqlite3.Connection, sql_files: Sequence[Path]) -> None:
    for path in sql_files:
        if not path.exists():
            continue
        conn.executescript(path.read_text(encoding="utf-8"))


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def index_exists(conn: sqlite3.Connection, index_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name = ? LIMIT 1",
        (index_name,),
    ).fetchone()
    return row is not None


def list_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [str(row[1]) for row in rows]


def list_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [str(row[0]) for row in rows]


def executemany_named(
    conn: sqlite3.Connection,
    sql: str,
    rows: Iterable[dict[str, object]],
) -> None:
    conn.executemany(sql, list(rows))
