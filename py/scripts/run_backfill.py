#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Iterable

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.core.runtime import WriteStats
from src.jobs.backfill import backfill_chinabond_curve_window
from src.jobs.ingest import fetch_bond_index_duration, fetch_recent_policy_rate_events
from src.jobs.manual import review_table


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="运行 Python 侧原始表 backfill。")
    parser.add_argument(
        "target",
        choices=["bond_curve", "policy_rate", "bond_index"],
        help="要执行的 backfill 目标。",
    )
    parser.add_argument("--db", dest="db_path", type=Path, default=None, help="可选 SQLite 路径覆盖。")
    parser.add_argument("--dry-run", action="store_true", help="仅演练，不提交数据库事务。")
    parser.add_argument("--review", action="store_true", help="回填后顺手执行一次 review。")

    # bond_curve
    parser.add_argument("--start-date", default=None, help="bond_curve 回填起始日，格式 YYYY-MM-DD。")
    parser.add_argument("--end-date", default=None, help="bond_curve 回填结束日，格式 YYYY-MM-DD。")
    parser.add_argument("--skip-weekends", action="store_true", help="bond_curve 回填时跳过周六周日。")
    parser.add_argument("--trading-days-csv", type=Path, default=None, help="bond_curve 可选交易日 CSV，优先用于历史回填。")
    parser.add_argument("--force-fetch", action="store_true", help="即使数据库里已存在完整日期，也仍然发请求抓取。")

    # policy_rate
    parser.add_argument("--limit", type=int, default=60, help="policy_rate 近期事件回填上限。")

    # bond_index
    parser.add_argument("--index-id", action="append", default=[], help="bond_index 的中债 index id，可重复传入。")
    parser.add_argument("--index-name", action="append", default=[], help="bond_index 可选 index name，顺序需与 --index-id 对齐。")
    parser.add_argument("--index-code", action="append", default=[], help="bond_index 可选 index code，顺序需与 --index-id 对齐。")

    return parser.parse_args()

def _merge_stats(stats_list: Iterable[WriteStats]) -> WriteStats:
    merged = WriteStats()
    for stats in stats_list:
        merged.merge(stats)
    return merged


def _maybe_review(store_name: str, *, enabled: bool, db_path: Path | None) -> dict | None:
    if not enabled:
        return None
    return review_table(store_name, db_path=db_path)


def _run_bond_curve_backfill(args: argparse.Namespace) -> dict[str, object]:
    if not args.start_date or not args.end_date:
        raise ValueError("bond_curve backfill 需要同时传 --start-date 和 --end-date。")

    return backfill_chinabond_curve_window(
        start_date=args.start_date,
        end_date=args.end_date,
        db_path=args.db_path,
        dry_run=args.dry_run,
        review=args.review,
        trading_days_csv=args.trading_days_csv,
        skip_weekends=args.skip_weekends,
        force_fetch=args.force_fetch,
    )


def _run_policy_rate_backfill(args: argparse.Namespace) -> dict[str, object]:
    failures: list[dict[str, str]] = []
    try:
        stats = fetch_recent_policy_rate_events(dry_run=args.dry_run, db_path=args.db_path, limit=args.limit)
    except Exception as exc:  # noqa: BLE001
        failures.append({"scope": f"recent:{args.limit}", "error": str(exc)})
        stats = WriteStats()
    return {
        "target": "policy_rate",
        "limit": args.limit,
        "stats": asdict(stats),
        "failures": failures,
        "review": _maybe_review("raw_policy_rate", enabled=args.review, db_path=args.db_path),
    }


def _align_optional(values: list[str], total: int) -> list[str | None]:
    if not values:
        return [None] * total
    if len(values) != total:
        raise ValueError("可选参数数量需与 --index-id 数量一致。")
    return [value or None for value in values]


def _run_bond_index_backfill(args: argparse.Namespace) -> dict[str, object]:
    if not args.index_id:
        raise ValueError("bond_index backfill 至少需要一个 --index-id。")

    names = _align_optional(args.index_name, len(args.index_id))
    codes = _align_optional(args.index_code, len(args.index_id))

    stats_list: list[WriteStats] = []
    items: list[dict[str, str | None]] = []
    failures: list[dict[str, str | None]] = []
    for index_id, index_name, index_code in zip(args.index_id, names, codes):
        items.append({"index_id": index_id, "index_name": index_name, "index_code": index_code})
        try:
            stats_list.append(
                fetch_bond_index_duration(
                    index_id,
                    dry_run=args.dry_run,
                    db_path=args.db_path,
                    index_name=index_name,
                    index_code=index_code,
                )
            )
        except Exception as exc:  # noqa: BLE001
            failures.append(
                {
                    "index_id": index_id,
                    "index_name": index_name,
                    "index_code": index_code,
                    "error": str(exc),
                }
            )

    return {
        "target": "bond_index",
        "items": items,
        "stats": asdict(_merge_stats(stats_list)),
        "failures": failures,
        "review": _maybe_review("raw_bond_index", enabled=args.review, db_path=args.db_path),
    }


def main() -> None:
    args = _parse_args()

    try:
        if args.target == "bond_curve":
            result = _run_bond_curve_backfill(args)
        elif args.target == "policy_rate":
            result = _run_policy_rate_backfill(args)
        elif args.target == "bond_index":
            result = _run_bond_index_backfill(args)
        else:
            raise ValueError(f"unsupported target: {args.target}")
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
