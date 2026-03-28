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

from src.jobs.backfill import backfill_chinabond_curve_window


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="按交易日窗口回填 chinabond_curve。")
    parser.add_argument("--start-date", required=True, help="回填起始日，格式 YYYY-MM-DD。")
    parser.add_argument("--end-date", required=True, help="回填结束日，格式 YYYY-MM-DD。")
    parser.add_argument("--db", dest="db_path", type=Path, default=None, help="可选 SQLite 路径覆盖。")
    parser.add_argument("--dry-run", action="store_true", help="仅演练，不提交数据库事务。")
    parser.add_argument("--review", action="store_true", help="回填后顺手执行一次 review。")
    parser.add_argument("--trading-days-csv", type=Path, default=None, help="可选交易日 CSV，优先用于历史回填。")
    parser.add_argument("--skip-weekends", action="store_true", help="缺少交易日 CSV 时，fallback 逻辑里跳过周六周日。")
    parser.add_argument("--force-fetch", action="store_true", help="即使数据库里已存在完整日期，也仍然发请求抓取。")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        result = backfill_chinabond_curve_window(
            start_date=args.start_date,
            end_date=args.end_date,
            db_path=args.db_path,
            dry_run=args.dry_run,
            review=args.review,
            trading_days_csv=args.trading_days_csv,
            skip_weekends=args.skip_weekends,
            force_fetch=args.force_fetch,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
