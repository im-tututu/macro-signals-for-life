#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.core.trading_calendar import DEFAULT_TRADING_DAYS_CSV, sync_trading_days_csv
from src.jobs.daily import (
    fetch_bond_index_duration,
    fetch_bond_curve_for_date,
    fetch_latest_bond_curve,
    fetch_latest_etf_snapshot,
    fetch_latest_futures,
    fetch_latest_life_asset,
    fetch_latest_money_market,
    fetch_latest_overseas_macro,
    fetch_latest_policy_rate,
    fetch_recent_policy_rate_events,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="运行单条 Python daily job。")
    parser.add_argument(
        "job",
        choices=[
            "money_market",
            "bond_curve",
            "bond_index",
            "overseas_macro",
            "policy_rate",
            "policy_rate_recent",
            "futures",
            "etf",
            "life_asset",
            "trading_days_update",
        ],
        help="要执行的 daily job。",
    )
    parser.add_argument("--db", dest="db_path", type=Path, default=None, help="可选 SQLite 路径覆盖。")
    parser.add_argument("--dry-run", action="store_true", help="仅演练，不提交数据库事务。")
    parser.add_argument("--date", default=None, help="bond_curve 使用的抓取日期，格式 YYYY-MM-DD。")
    parser.add_argument("--limit", type=int, default=20, help="policy_rate_recent 每类事件抓取上限。")
    parser.add_argument("--index-id", default=None, help="bond_index 使用的 index id。")
    parser.add_argument("--index-name", default=None, help="bond_index 可选 index name。")
    parser.add_argument("--index-code", default=None, help="bond_index 可选 index code。")
    parser.add_argument("--rows-per-page", type=int, default=500, help="etf 每页抓取条数。")
    parser.add_argument("--max-pages", type=int, default=20, help="etf 最大抓取页数。")
    parser.add_argument("--source-csv", type=Path, default=None, help="trading_days_update 可选源文件。")
    parser.add_argument("--target-csv", type=Path, default=DEFAULT_TRADING_DAYS_CSV, help="trading_days_update 目标文件。")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.job == "money_market":
            stats = fetch_latest_money_market(dry_run=args.dry_run, db_path=args.db_path)
        elif args.job == "bond_curve":
            if args.date:
                stats = fetch_bond_curve_for_date(args.date, dry_run=args.dry_run, db_path=args.db_path)
            else:
                stats = fetch_latest_bond_curve(dry_run=args.dry_run, db_path=args.db_path)
        elif args.job == "bond_index":
            if not args.index_id:
                parser.error("bond_index 需要传 --index-id")
            stats = fetch_bond_index_duration(
                args.index_id,
                dry_run=args.dry_run,
                db_path=args.db_path,
                index_name=args.index_name,
                index_code=args.index_code,
            )
        elif args.job == "overseas_macro":
            stats = fetch_latest_overseas_macro(dry_run=args.dry_run, db_path=args.db_path)
        elif args.job == "policy_rate":
            stats = fetch_latest_policy_rate(dry_run=args.dry_run, db_path=args.db_path)
        elif args.job == "policy_rate_recent":
            stats = fetch_recent_policy_rate_events(dry_run=args.dry_run, db_path=args.db_path, limit=args.limit)
        elif args.job == "futures":
            stats = fetch_latest_futures(dry_run=args.dry_run, db_path=args.db_path)
        elif args.job == "etf":
            stats = fetch_latest_etf_snapshot(
                dry_run=args.dry_run,
                db_path=args.db_path,
                rows_per_page=args.rows_per_page,
                max_pages=args.max_pages,
            )
        elif args.job == "life_asset":
            stats = fetch_latest_life_asset(dry_run=args.dry_run, db_path=args.db_path)
        elif args.job == "trading_days_update":
            result = sync_trading_days_csv(
                source_path=args.source_csv,
                target_path=args.target_csv,
                dry_run=args.dry_run,
            )
            print(
                json.dumps(
                    {
                        "target_path": str(result.target_path),
                        "source_path": str(result.source_path) if result.source_path else None,
                        "coverage_start": result.coverage_start,
                        "coverage_end": result.coverage_end,
                        "row_count": result.row_count,
                        "changed": result.changed,
                        "created": result.created,
                        "dry_run": result.dry_run,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        else:
            parser.error(f"unsupported job: {args.job}")
            return
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)

    print(json.dumps(stats.__dict__, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
