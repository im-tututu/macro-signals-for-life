from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path
from typing import Sequence

try:
    from src.outputs.sheets import GoogleSheetsWriter, getenv_first, load_local_env  # type: ignore
except Exception:
    try:
        from sheets import GoogleSheetsWriter, getenv_first, load_local_env  # type: ignore
    except Exception:
        CURRENT_DIR = Path(__file__).resolve().parent
        if str(CURRENT_DIR) not in sys.path:
            sys.path.insert(0, str(CURRENT_DIR))
        from sheets import GoogleSheetsWriter, getenv_first, load_local_env  # type: ignore


DEFAULT_WORKSHEET_MAP = {
    "raw_bond_curve": "原始_收益率曲线",
    "raw_bond_index": "原始_债券指数特征",
    "raw_policy_rate": "原始_政策利率",
    "raw_money_market": "原始_资金面",
    "raw_futures": "原始_国债期货",
    "raw_overseas_macro": "原始_海外宏观",
    "raw_life_asset": "原始_民生与资产价格",
    "raw_jisilu_etf": "原始_指数ETF",
}


def parse_args() -> argparse.Namespace:
    load_local_env()

    parser = argparse.ArgumentParser(
        description="Export one SQLite table or SELECT query to Google Sheets."
    )
    parser.add_argument(
        "--db",
        default=getenv_first("DB_PATH", "SQLITE_DB_PATH") or "py/data/db.sqlite",
        help="Path to SQLite database. Default comes from DB_PATH or py/data/db.sqlite.",
    )
    parser.add_argument(
        "--creds",
        default=getenv_first("GOOGLE_APPLICATION_CREDENTIALS"),
        help="Optional override for Google service account JSON. Default reads env/.env.",
    )
    parser.add_argument(
        "--spreadsheet-id",
        default=getenv_first("GOOGLE_SPREADSHEET_ID"),
        help="Optional override for target spreadsheet id. Default reads env/.env.",
    )
    parser.add_argument(
        "--worksheet",
        default=None,
        help="Target worksheet/tab title. If omitted and --table is used, a default name mapping is applied.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--table", help="SQLite table name to export.")
    group.add_argument("--query", help="Custom SELECT query to export.")
    parser.add_argument("--limit", type=int, default=None, help="Optional LIMIT if using --table.")
    parser.add_argument(
        "--order-by",
        default=None,
        help="Optional ORDER BY clause without the keyword, e.g. 'date DESC'. Only for --table.",
    )
    parser.add_argument(
        "--csv",
        default=None,
        help="Optional CSV path for local preview/export before writing to sheet.",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional explicit .env path. Normally auto-detected.",
    )
    return parser.parse_args()


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def build_table_query(table: str, order_by: str | None, limit: int | None) -> str:
    sql = f"SELECT * FROM {quote_ident(table)}"
    if order_by:
        sql += f" ORDER BY {order_by}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return sql


def fetch_rows(conn: sqlite3.Connection, sql: str) -> list[list[object]]:
    cur = conn.execute(sql)
    headers = [d[0] for d in cur.description]
    rows = cur.fetchall()
    out = [headers]
    out.extend([list(row) for row in rows])
    return out


def write_csv(path: str, rows: Sequence[Sequence[object]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def resolve_worksheet_title(table: str | None, worksheet: str | None) -> str:
    if worksheet:
        return worksheet
    if table and table in DEFAULT_WORKSHEET_MAP:
        return DEFAULT_WORKSHEET_MAP[table]
    if table:
        return table
    raise ValueError("worksheet title is required when using --query")


def main() -> None:
    args = parse_args()
    if args.env_file:
        load_local_env(env_file=args.env_file)

    db_path = Path(args.db).expanduser()
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    sql = args.query or build_table_query(args.table, args.order_by, args.limit)
    worksheet_title = resolve_worksheet_title(args.table, args.worksheet)

    conn = sqlite3.connect(str(db_path))
    try:
        rows = fetch_rows(conn, sql)
    finally:
        conn.close()

    if args.csv:
        write_csv(args.csv, rows)

    writer = GoogleSheetsWriter(
        credentials_path=args.creds,
        spreadsheet_id=args.spreadsheet_id,
        env_file=args.env_file,
    )
    writer.replace_all(worksheet_title, rows)

    print(
        f"Exported {max(len(rows) - 1, 0)} data rows "
        f"to worksheet={worksheet_title!r} in spreadsheet={writer.spreadsheet_id!r}"
    )


if __name__ == "__main__":
    main()
