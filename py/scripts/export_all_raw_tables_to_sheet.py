from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

try:
    from src.core.config import AppConfig  # type: ignore
    from src.outputs.sheets import GoogleSheetsWriter, getenv_first, load_local_env  # type: ignore
except Exception:
    try:
        from core.config import AppConfig  # type: ignore
        from sheets import GoogleSheetsWriter, getenv_first, load_local_env  # type: ignore
    except Exception:
        CURRENT_DIR = Path(__file__).resolve().parent
        PY_ROOT = CURRENT_DIR.parent
        SRC_DIR = PY_ROOT / "src"
        if str(SRC_DIR) not in sys.path:
            sys.path.insert(0, str(SRC_DIR))
        if str(CURRENT_DIR) not in sys.path:
            sys.path.insert(0, str(CURRENT_DIR))
        from core.config import AppConfig  # type: ignore
        from sheets import GoogleSheetsWriter, getenv_first, load_local_env  # type: ignore


# 正式 raw 表 -> 目标 worksheet 名
RAW_TABLE_TITLE_MAP = {
    "raw_bond_curve": "原始_收益率曲线",
    "raw_chinabond_bond_index": "原始_中债债券指数特征",
    "raw_csindex_bond_index": "原始_中证债券指数特征",
    "raw_cnindex_bond_index": "原始_国证债券指数特征",
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
    cfg = AppConfig.load()

    parser = argparse.ArgumentParser(
        description="将当前 SQLite 中的 raw 表批量导出到一个 Google Spreadsheet。"
    )
    parser.add_argument(
        "--db",
        default=str(cfg.db_path),
        help="SQLite 数据库路径。默认读取 .env 中的 DB_PATH，或使用 runtime/db/app.sqlite。",
    )
    parser.add_argument(
        "--creds",
        default=str(cfg.google_credentials_path) if cfg.google_credentials_path else None,
        help="可选的 Google service account JSON 路径覆盖值。默认从 .env 中读取。",
    )
    parser.add_argument(
        "--spreadsheet-id",
        default=cfg.spreadsheet_id or getenv_first("GOOGLE_SPREADSHEET_ID"),
        help="可选的目标 Spreadsheet ID 覆盖值。默认从 .env 中读取。",
    )
    parser.add_argument(
        "--prefix",
        default=cfg.worksheet_prefix,
        help="可选的工作表名前缀，例如 '验证_'。",
    )
    parser.add_argument(
        "--index-worksheet",
        default="导出目录",
        help="导出汇总目录所使用的工作表名称。",
    )
    parser.add_argument(
        "--limit-per-table",
        type=int,
        default=cfg.export_limit_per_table,
        help="每张表可选的 LIMIT；默认导出全部行。",
    )
    parser.add_argument(
        "--include-empty",
        action="store_true",
        default=cfg.export_include_empty,
        help="空表也创建对应工作表。",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        help="可选的显式表名子集。默认导出所有当前 raw_* 表以及额外配置表。",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="可选的 .env 文件路径；默认会自动探测。",
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
            note="无列信息",
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
            note="空表",
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
                print(f"警告：以下表不存在，已跳过：{missing}")
        else:
            tables = existing_export_tables

        if not tables:
            print("数据库中未找到可导出的 raw / 历史兼容原始表。")
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
            f"已完成导出，并将汇总目录写入 spreadsheet={writer.spreadsheet_id!r} "
            f"中的 worksheet={index_title!r}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
