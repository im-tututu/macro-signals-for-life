#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.core.config import default_sql_paths
from src.core.db import connect, run_sql_files
from src.metrics import build_metric_snapshots, list_metric_registry, upsert_metric_snapshots


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="生成并写入指标快照（metric_snapshot）。")
    parser.add_argument("--db", dest="db_path", type=Path, default=None, help="可选 SQLite 路径覆盖。")
    parser.add_argument("--as-of-date", default=None, help="快照日期，格式 YYYY-MM-DD，默认今天。")
    parser.add_argument("--dry-run", action="store_true", help="仅演练，不提交数据库事务。")
    parser.add_argument("--print-sample", type=int, default=5, help="打印前 N 条样例。")
    return parser


def _validate_date_text(value: str | None) -> str | None:
    if value is None:
        return None
    datetime.strptime(value, "%Y-%m-%d")
    return value


def _resolve_default_data_date(conn: Any) -> str:
    row = conn.execute("SELECT MAX(date) FROM metric_daily").fetchone()
    value = "" if row is None else str(row[0] or "").strip()
    if not value:
        raise RuntimeError("metric_daily 无可用数据，无法推断默认数据日期，请显式传 --as-of-date。")
    datetime.strptime(value, "%Y-%m-%d")
    return value


def _resolve_db_path(conn: Any) -> str:
    row = conn.execute("PRAGMA database_list").fetchone()
    if row is None:
        return ""
    return str(row[2] or "")


def _collect_data_warnings(snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    no_data = [str(s["code"]) for s in snapshots if s.get("latest_value") is None]
    weak_5d = [str(s["code"]) for s in snapshots if s.get("latest_value") is not None and s.get("change_5d") is None]
    weak_20d = [str(s["code"]) for s in snapshots if s.get("latest_value") is not None and s.get("change_20d") is None]
    weak_250 = [str(s["code"]) for s in snapshots if int(s.get("sample_count") or 0) < 250]
    return {
        "no_data_count": len(no_data),
        "insufficient_for_5d_change_count": len(weak_5d),
        "insufficient_for_20d_change_count": len(weak_20d),
        "insufficient_for_250d_stats_count": len(weak_250),
        "no_data_sample_codes": no_data[:10],
        "insufficient_for_5d_change_sample_codes": weak_5d[:10],
        "insufficient_for_20d_change_sample_codes": weak_20d[:10],
        "insufficient_for_250d_stats_sample_codes": weak_250[:10],
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    target_date = _validate_date_text(args.as_of_date)

    with connect(args.db_path) as conn:
        run_sql_files(conn, default_sql_paths())
        conn.commit()
        if target_date is None:
            target_date = _resolve_default_data_date(conn)
        resolved_db_path = _resolve_db_path(conn)
        existing_rows = conn.execute(
            "SELECT COUNT(*) FROM metric_snapshot WHERE as_of_date = ?",
            (target_date,),
        ).fetchone()
        existing_for_date = int((existing_rows[0] if existing_rows else 0) or 0)

    snapshots = build_metric_snapshots(
        db_path=str(args.db_path) if args.db_path else None,
        as_of_date=target_date,
    )
    registry_count = len(list_metric_registry(db_path=str(args.db_path) if args.db_path else None))
    snapshot_count = upsert_metric_snapshots(
        snapshots,
        db_path=str(args.db_path) if args.db_path else None,
        dry_run=args.dry_run,
    )
    warnings = _collect_data_warnings(snapshots)
    mode = "update" if existing_for_date > 0 else "insert"

    sample = snapshots[: max(args.print_sample, 0)]
    print(
        json.dumps(
            {
                "db_path": resolved_db_path,
                "as_of_date": target_date,
                "write_mode": mode,
                "existing_rows_for_date_before_write": existing_for_date,
                "registry_rows": registry_count,
                "snapshot_rows": snapshot_count,
                "dry_run": args.dry_run,
                "data_warnings": warnings,
                "sample": sample,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
