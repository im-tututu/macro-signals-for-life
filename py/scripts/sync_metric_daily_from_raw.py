#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.core.config import default_sql_paths
from src.core.db import connect, run_sql_files
from src.metrics import sync_metric_daily_from_raw


def _validate_date_text(value: str | None) -> str | None:
    if value is None:
        return None
    datetime.strptime(value, "%Y-%m-%d")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="根据 raw_* 原始表同步 metric_daily。")
    parser.add_argument("--db", dest="db_path", type=Path, default=None, help="可选 SQLite 路径覆盖。")
    parser.add_argument("--start-date", default=None, help="起始日期，格式 YYYY-MM-DD。")
    parser.add_argument("--end-date", default=None, help="结束日期，格式 YYYY-MM-DD；默认取最新国债曲线日期。")
    parser.add_argument("--dry-run", action="store_true", help="仅演练，不提交事务。")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    start_date = _validate_date_text(args.start_date)
    end_date = _validate_date_text(args.end_date)

    with connect(args.db_path) as conn:
        run_sql_files(conn, default_sql_paths())
        conn.commit()

    stats = sync_metric_daily_from_raw(
        db_path=str(args.db_path) if args.db_path else None,
        start_date=start_date,
        end_date=end_date,
        dry_run=args.dry_run,
    )
    print(json.dumps(stats, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
