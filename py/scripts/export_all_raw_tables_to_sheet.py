from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
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


# 正式 raw 表 -> 目标 worksheet 名
RAW_TABLE_TITLE_MAP = {
    "raw_bond_curve": "原始_收益率曲线",
    "raw_bond_index": "原始_债券指数特征",
    "raw_policy_rate": "原始_政策利率",
    "raw_money_market": "原始_资金面",
    "raw_futures": "原始_国债期货",
    "raw_overseas_macro": "原始_海外宏观",
    "raw_life_asset": "原始_民生与资产价格",
    "raw_jisilu_etf": "原始_指数ETF",
}

# 需要额外一并导出的“原始*”历史兼容表
# 这里不是 raw_*，但用户当前仍希望在表格里看到
EXTRA_EXPORT_TABLES = {
}

ORDER_CANDIDATES = [
    "date",
    "trade_date",
    "snapshot_date",
    "data_date",
    "show_date_cn",
    "ts",
    "fetched_at",
]


@dataclass
class ExportResult:
    table_name: str
    worksheet_title: str
    row_count: int
    order_by: str | None
    status: str
    note: str = ""


def parse_args() -> argparse.Namespace:
    load_local_env()

    parser = argparse.ArgumentParser(
        description="Export current raw tables from SQLite into one Google Spreadsheet."
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
        "--prefix",
        default=getenv_first("GOOGLE_WORKSHEET_PREFIX") or "",
        help="Optional prefix added to every worksheet title, e.g. '验证_'.",
    )
    parser.add_argument(
        "--index-worksheet",
        default="导出目录",
        help="Worksheet name for export summary/index.",
    )
    parser.add_argument(
        "--limit-per-table",
        type=int,
        default=int(getenv_first("EXPORT_LIMIT_PER_TABLE") or 0) or None,
        help="Optional LIMIT applied to each table. Default exports all rows.",
    )
    parser.add_argument(
        "--include-empty",
        action="store_true",
        default=(getenv_first("EXPORT_INCLUDE_EMPTY") or "").strip().lower() in {"1", "true", "yes", "y", "on"},
        help="Also create worksheets for empty tables.",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        help="Optional explicit subset of table names. Default exports all current raw_* plus configured extras.",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional explicit .env path. Normally auto-detected.",
    )
    return parser.parse_args()


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def list_existing_tables(conn: sqlite3.Connection) -> list[str]:
    sql = '''
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
    ORDER BY name
    '''
    return [row[0] for row in conn.execute(sql).fetchall()]


def list_export_tables(conn: sqlite3.Connection) -> list[str]:
    existing = set(list_existing_tables(conn))
    raw_tables = sorted([t for t in existing if t.startswith("raw_")])
    extra_tables = [t for t in EXTRA_EXPORT_TABLES.keys() if t in existing]
    # raw_* 在前，额外兼容表在后
    return raw_tables + extra_tables


def table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    sql = f"PRAGMA table_info({quote_ident(table_name)})"
    rows = conn.execute(sql).fetchall()
    return [row[1] for row in rows]


def table_row_count(conn: sqlite3.Connection, table_name: str) -> int:
    sql = f"SELECT COUNT(*) FROM {quote_ident(table_name)}"
    return int(conn.execute(sql).fetchone()[0])


def choose_order_by(columns: Sequence[str]) -> str | None:
    for col in ORDER_CANDIDATES:
        if col in columns:
            return f'{quote_ident(col)} DESC'
    return None


def build_export_query(table_name: str, columns: Sequence[str], limit: int | None) -> str:
    sql = f"SELECT * FROM {quote_ident(table_name)}"
    order_by = choose_order_by(columns)
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


def worksheet_title_for(table_name: str, prefix: str = "") -> str:
    if table_name in RAW_TABLE_TITLE_MAP:
        base = RAW_TABLE_TITLE_MAP[table_name]
    elif table_name in EXTRA_EXPORT_TABLES:
        base = EXTRA_EXPORT_TABLES[table_name]
    else:
        base = table_name
    title = f"{prefix}{base}" if prefix else base
    return title[:100]


def export_table(
    writer: GoogleSheetsWriter,
    conn: sqlite3.Connection,
    table_name: str,
    *,
    prefix: str = "",
    limit: int | None = None,
    include_empty: bool = False,
) -> ExportResult:
    columns = table_columns(conn, table_name)
    if not columns:
        return ExportResult(
            table_name=table_name,
            worksheet_title=worksheet_title_for(table_name, prefix),
            row_count=0,
            order_by=None,
            status="skipped",
            note="no columns",
        )

    row_count = table_row_count(conn, table_name)
    worksheet_title = worksheet_title_for(table_name, prefix)
    order_by = choose_order_by(columns)

    if row_count == 0 and not include_empty:
        return ExportResult(
            table_name=table_name,
            worksheet_title=worksheet_title,
            row_count=0,
            order_by=order_by,
            status="skipped",
            note="empty table",
        )

    sql = build_export_query(table_name, columns, limit)
    rows = fetch_rows(conn, sql)
    if row_count == 0:
        rows = [list(columns)]

    writer.replace_all(worksheet_title, rows)
    exported_data_rows = max(len(rows) - 1, 0)
    note = f"exported_rows={exported_data_rows}"
    if limit is not None and row_count > limit:
        note += f" (limited from total={row_count})"

    return ExportResult(
        table_name=table_name,
        worksheet_title=worksheet_title,
        row_count=row_count,
        order_by=order_by,
        status="exported",
        note=note,
    )


def write_index_sheet(
    writer: GoogleSheetsWriter,
    worksheet_title: str,
    results: Sequence[ExportResult],
) -> None:
    rows: list[list[object]] = [[
        "table_name",
        "worksheet_title",
        "row_count_total",
        "order_by",
        "status",
        "note",
    ]]
    for r in results:
        rows.append([
            r.table_name,
            r.worksheet_title,
            r.row_count,
            r.order_by or "",
            r.status,
            r.note,
        ])
    writer.replace_all(worksheet_title, rows)


def main() -> None:
    args = parse_args()
    if args.env_file:
        load_local_env(env_file=args.env_file)

    db_path = Path(args.db).expanduser()
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        existing_export_tables = list_export_tables(conn)

        if args.tables:
            tables = [t for t in args.tables if t in existing_export_tables]
            missing = [t for t in args.tables if t not in existing_export_tables]
            if missing:
                print(f"Warning: these tables were not found and will be skipped: {missing}")
        else:
            tables = existing_export_tables

        if not tables:
            print("No exportable raw/legacy raw tables found in database.")
            return

        writer = GoogleSheetsWriter(
            credentials_path=args.creds,
            spreadsheet_id=args.spreadsheet_id,
            env_file=args.env_file,
        )

        results: list[ExportResult] = []
        for table_name in tables:
            result = export_table(
                writer,
                conn,
                table_name,
                prefix=args.prefix,
                limit=args.limit_per_table,
                include_empty=args.include_empty,
            )
            results.append(result)
            print(
                f"[{result.status.upper()}] table={result.table_name} "
                f"worksheet={result.worksheet_title} rows_total={result.row_count} note={result.note}"
            )

        index_title = f"{args.prefix}{args.index_worksheet}"[:100] if args.prefix else args.index_worksheet[:100]
        write_index_sheet(writer, index_title, results)
        print(
            f"Done. Exported summary index to worksheet={index_title!r} "
            f"in spreadsheet={writer.spreadsheet_id!r}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
