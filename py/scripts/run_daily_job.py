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

from src.core.trading_calendar import DEFAULT_TRADING_DAYS_CSV
from src.jobs.executor import execute_daily_job
from src.jobs.registry import DAILY_JOB_REGISTRY
from src.sources.akshare import AKSHARE_BOND_GB_US_SINA_SYMBOLS


def _build_output_payload(result: dict[str, object], args: argparse.Namespace) -> dict[str, object]:
    payload: dict[str, object] = {
        "job": result.get("job", args.job),
        "status": result.get("status", "success"),
        "table": result.get("table"),
        "dry_run": args.dry_run,
    }
    if args.snapshot_date:
        payload["snapshot_date"] = args.snapshot_date
    if args.date:
        payload["date"] = args.date
    if args.index_id:
        payload["index_id"] = args.index_id
    if args.symbol:
        payload["symbol"] = args.symbol
    if args.start_date:
        payload["start_date"] = args.start_date
    if "stats" in result:
        payload["stats"] = result["stats"]
    else:
        for key, value in result.items():
            if key not in payload:
                payload[key] = value
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="运行单条 Python daily job。")
    parser.add_argument(
        "job",
        choices=sorted(DAILY_JOB_REGISTRY.keys()),
        help="要执行的 daily job。",
    )
    parser.add_argument("--db", dest="db_path", type=Path, default=None, help="可选 SQLite 路径覆盖。")
    parser.add_argument("--dry-run", action="store_true", help="仅演练，不提交数据库事务。")
    parser.add_argument("--force", action="store_true", help="强制重跑当天快照，覆盖已存在的同日数据。")
    parser.add_argument("--date", default=None, help="bond_curve 使用的抓取日期，格式 YYYY-MM-DD。")
    parser.add_argument("--limit", type=int, default=20, help="policy_rate_recent 每类事件抓取上限。")
    parser.add_argument("--index-id", default=None, help="债券指数 job 使用的 index id / code。")
    parser.add_argument("--index-name", default=None, help="债券指数 job 可选 index name。")
    parser.add_argument("--index-code", default=None, help="债券指数 job 可选 index code。")
    parser.add_argument(
        "--symbol",
        default=None,
        choices=AKSHARE_BOND_GB_US_SINA_SYMBOLS,
        help="akshare_bond_gb_us_sina 可选单一美国国债期限；不传则默认批量抓全部期限。",
    )
    parser.add_argument("--start-date", default=None, help="akshare_bond_zh_us_rate 可选历史起始日期，格式 YYYYMMDD。")
    parser.add_argument("--snapshot-date", default=None, help="etf 可选快照日期，格式 YYYY-MM-DD。")
    parser.add_argument("--rows-per-page", type=int, default=500, help="etf 每页抓取条数。")
    parser.add_argument("--max-pages", type=int, default=20, help="etf 最大抓取页数。")
    parser.add_argument("--source-csv", type=Path, default=None, help="trading_days_update 可选源文件。")
    parser.add_argument("--target-csv", type=Path, default=DEFAULT_TRADING_DAYS_CSV, help="trading_days_update 目标文件。")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        results = execute_daily_job(
            args.job,
            dry_run=args.dry_run,
            force=args.force,
            db_path=args.db_path,
            date=args.date,
            limit=args.limit,
            index_id=args.index_id,
            index_name=args.index_name,
            index_code=args.index_code,
            start_date=args.start_date,
            symbol=args.symbol,
            snapshot_date=args.snapshot_date,
            rows_per_page=args.rows_per_page,
            max_pages=args.max_pages,
            source_csv=args.source_csv,
            target_csv=args.target_csv,
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)

    if not results:
        parser.error(f"job {args.job} 返回空结果")
        return

    if args.job in {"chinabond_index", "csindex_bond_index", "cnindex_bond_index"}:
        payload = _build_output_payload(results[0], args)
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        return

    payload = _build_output_payload(results[0], args)
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
