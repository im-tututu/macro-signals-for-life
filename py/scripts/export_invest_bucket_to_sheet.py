#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

try:
    from src.core.config import AppConfig, getenv_first, load_local_env  # type: ignore
except Exception:
    try:
        from core.config import AppConfig, getenv_first, load_local_env  # type: ignore
    except Exception:
        CURRENT_DIR = Path(__file__).resolve().parent
        PY_ROOT = CURRENT_DIR.parent
        if str(PY_ROOT) not in sys.path:
            sys.path.insert(0, str(PY_ROOT))
        if str(CURRENT_DIR) not in sys.path:
            sys.path.insert(0, str(CURRENT_DIR))
        from src.core.config import AppConfig, getenv_first, load_local_env  # type: ignore


RESULT_HEADERS = [
    "snapshot_date",
    "nav_dt",
    "instrument_id",
    "instrument_type",
    "instrument_code",
    "instrument_name",
    "issuer_name",
    "source_table",
    "exposure_name",
    "exposure_key",
    "exposure_method",
    "duration_band",
    "bucket_match_key",
    "bucket_id",
    "bucket_name_cn",
    "asset_class",
    "issuer_type",
    "bucket_duration_band",
    "region_tag",
    "style_tag",
    "bucket_match_method",
    "bucket_match_confidence",
    "needs_bucket_review",
    "price",
    "amount_yi",
    "volume_wan",
    "unit_total_yi",
    "premium_discount_rt",
    "apply_status",
    "redeem_status",
    "duration",
    "ytm",
]

RULE_HEADERS = [
    "exposure_key",
    "exposure_name",
    "bucket_id",
    "match_method",
    "match_confidence",
    "note",
    "updated_at",
]


def build_parser(cfg: AppConfig) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="导出投资侧归桶结果与人工修正规则到 Google Sheets。")
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
        help="目标 Spreadsheet ID。默认从 .env 中读取。",
    )
    parser.add_argument(
        "--result-worksheet",
        default="投资侧_最新归桶结果",
        help="归桶结果工作表名称。",
    )
    parser.add_argument(
        "--rule-worksheet",
        default="投资侧_人工修正规则",
        help="人工修正规则工作表名称。",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="可选的 .env 文件路径；默认会自动探测。",
    )
    return parser


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    db_uri = f"file:{db_path.resolve().as_posix()}?mode=ro"
    conn = sqlite3.connect(db_uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_rows(conn: sqlite3.Connection, sql: str) -> list[sqlite3.Row]:
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn.execute(sql).fetchall()


def build_result_rows(conn: sqlite3.Connection) -> list[list[object]]:
    rows = fetch_rows(
        conn,
        """
        SELECT
            snapshot_date,
            nav_dt,
            instrument_id,
            instrument_type,
            instrument_code,
            instrument_name,
            issuer_name,
            source_table,
            exposure_name,
            exposure_key,
            exposure_method,
            duration_band,
            bucket_match_key,
            bucket_id,
            bucket_name_cn,
            asset_class,
            issuer_type,
            bucket_duration_band,
            region_tag,
            style_tag,
            bucket_match_method,
            bucket_match_confidence,
            needs_bucket_review,
            price,
            amount_yi,
            volume_wan,
            unit_total_yi,
            premium_discount_rt,
            apply_status,
            redeem_status,
            duration,
            ytm
        FROM vw_invest_bucket_latest
        ORDER BY
            COALESCE(bucket_id, 'zz_unknown'),
            instrument_type,
            exposure_name,
            instrument_name
        """,
    )
    out: list[list[object]] = [RESULT_HEADERS]
    for row in rows:
        out.append([row[header] if header in row.keys() else "" for header in RESULT_HEADERS])
    return out


def build_rule_rows(conn: sqlite3.Connection) -> list[list[object]]:
    rows = fetch_rows(
        conn,
        """
        SELECT
            exposure_key,
            exposure_name,
            bucket_id,
            match_method,
            match_confidence,
            note,
            updated_at
        FROM map_invest_exposure_bucket
        ORDER BY exposure_key
        """,
    )
    out: list[list[object]] = [RULE_HEADERS]
    for row in rows:
        out.append([row[header] if header in row.keys() else "" for header in RULE_HEADERS])
    return out


def main() -> None:
    load_local_env()
    cfg = AppConfig.load()
    args = build_parser(cfg).parse_args()
    if args.env_file:
        load_local_env(env_file=args.env_file)

    db_path = Path(args.db).expanduser()
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    from src.outputs.sheets import GoogleSheetsWriter  # type: ignore

    with connect_readonly(db_path) as conn:
        result_rows = build_result_rows(conn)
        rule_rows = build_rule_rows(conn)

    writer = GoogleSheetsWriter(
        credentials_path=args.creds,
        spreadsheet_id=args.spreadsheet_id,
        env_file=args.env_file,
    )
    writer.replace_all(args.result_worksheet, result_rows)
    writer.replace_all(args.rule_worksheet, rule_rows)

    print(
        f"已导出投资侧结果 {max(len(result_rows) - 1, 0)} 行，"
        f"规则 {max(len(rule_rows) - 1, 0)} 行，"
        f"写入 spreadsheet={writer.spreadsheet_id!r}"
    )


if __name__ == "__main__":
    main()
