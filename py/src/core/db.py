"""SQLite connection helpers."""

import sqlite3
from .config import SQLITE_PATH


def get_conn() -> sqlite3.Connection:
    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn
