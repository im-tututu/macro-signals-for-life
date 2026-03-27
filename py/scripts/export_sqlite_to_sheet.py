from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
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
    cfg = AppConfig.load()

    parser = argparse.ArgumentParser(
        description="将单张 SQLite 表或一条 SELECT 查询结果导出到 Google Sheets。"
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
        "--worksheet",
        default=None,
        help="目标工作表名称。若省略且使用 --table，则按默认映射自动推断。",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--table", help="要导出的 SQLite 表名。")
    group.add_argument("--query", help="要导出的自定义 SELECT 查询。")
    parser.add_argument("--limit", type=int, default=None, help="使用 --table 时可附加的 LIMIT。")
    parser.add_argument(
        "--order-by",
        default=None,
        help="使用 --table 时可附加的 ORDER BY 子句，不要写 ORDER BY 关键字，例如 'date DESC'。",
    )
    parser.add_argument(
        "--csv",
        default=None,
        help="可选的本地 CSV 输出路径，可在写入 Sheet 前先导出预览。",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="可选的 .env 文件路径；默认会自动探测。",
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
    raise ValueError("使用 --query 时必须显式提供 worksheet 名称")


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
        f"已导出 {max(len(rows) - 1, 0)} 行数据，"
        f"写入 spreadsheet={writer.spreadsheet_id!r} 中的 worksheet={worksheet_title!r}"
    )


if __name__ == "__main__":
    main()
