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

from src.core.trading_calendar import DEFAULT_TRADING_DAYS_CSV, sync_trading_days_csv
from src.jobs.daily import (
    fetch_bond_index_duration,
    fetch_latest_bond_curve,
    fetch_latest_etf_snapshot,
    fetch_latest_futures,
    fetch_latest_life_asset,
    fetch_latest_money_market,
    fetch_latest_overseas_macro,
    fetch_latest_policy_rate,
    fetch_recent_policy_rate_events,
)
from src.jobs.registry import DAILY_JOB_REGISTRY
from src.metrics import build_metric_snapshots, list_metric_registry, sync_metric_daily_from_raw, upsert_metric_snapshots
from export_latest_metric_snapshot_to_sheet import export_metric_snapshot_to_sheet


JOB_GROUPS: dict[str, tuple[str, ...]] = {
    "cn_night": (
        "money_market",
        "bond_curve",
        "policy_rate",
        "futures",
        "etf",
        "life_asset",
        "bond_index",
    ),
    "us_morning": (
        "overseas_macro",
    ),
    "manual": (
        "policy_rate_recent",
        "trading_days_update",
    ),
}


def _resolve_latest_metric_daily_date(db_path: Path | None) -> str:
    from src.core.db import connect

    with connect(db_path) as conn:
        row = conn.execute("SELECT MAX(date) AS max_date FROM metric_daily").fetchone()
    value = "" if row is None else str(row["max_date"] or "").strip()
    if not value:
        raise RuntimeError("metric_daily 无可用数据，无法继续生成 metric_snapshot / Sheet 导出。")
    datetime.strptime(value, "%Y-%m-%d")
    return value


def _build_metric_snapshot_stats(db_path: Path | None, as_of_date: str, dry_run: bool) -> dict[str, object]:
    db_path_text = str(db_path) if db_path else None
    snapshots = build_metric_snapshots(db_path=db_path_text, as_of_date=as_of_date)
    registry_count = len(list_metric_registry(db_path=db_path_text))
    snapshot_count = upsert_metric_snapshots(snapshots, db_path=db_path_text, dry_run=dry_run)
    return {
        "as_of_date": as_of_date,
        "registry_rows": registry_count,
        "snapshot_rows": snapshot_count,
        "dry_run": dry_run,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="按分组执行 Python daily jobs。")
    parser.add_argument("group", choices=sorted(JOB_GROUPS.keys()), help="要执行的 job 分组。")
    parser.add_argument("--db", dest="db_path", type=Path, default=None, help="可选 SQLite 路径覆盖。")
    parser.add_argument("--dry-run", action="store_true", help="仅演练，不提交数据库事务。")
    parser.add_argument("--limit", type=int, default=20, help="manual 组里 policy_rate_recent 的抓取上限。")
    parser.add_argument("--snapshot-date", default=None, help="cn_night 组里 etf 可选快照日期，格式 YYYY-MM-DD。")
    parser.add_argument("--rows-per-page", type=int, default=500, help="cn_night 组里 etf 每页抓取条数。")
    parser.add_argument("--max-pages", type=int, default=20, help="cn_night 组里 etf 最大抓取页数。")
    parser.add_argument("--source-csv", type=Path, default=None, help="manual 组里 trading_days_update 可选源文件。")
    parser.add_argument("--target-csv", type=Path, default=DEFAULT_TRADING_DAYS_CSV, help="manual 组里 trading_days_update 目标文件。")
    parser.add_argument(
        "--bond-index-id",
        action="append",
        default=[],
        help="cn_night 组里可重复传入的 bond_index index_id；未提供则跳过 bond_index。",
    )
    return parser


def run_group(args: argparse.Namespace) -> dict[str, object]:
    jobs = JOB_GROUPS[args.group]
    results: list[dict[str, object]] = []

    for job_name in jobs:
        spec = DAILY_JOB_REGISTRY[job_name]
        if job_name == "money_market":
            stats = fetch_latest_money_market(dry_run=args.dry_run, db_path=args.db_path)
            results.append({"job": job_name, "status": "success", "stats": stats.__dict__, "table": spec.target_table})
        elif job_name == "bond_curve":
            stats = fetch_latest_bond_curve(dry_run=args.dry_run, db_path=args.db_path)
            results.append({"job": job_name, "status": "success", "stats": stats.__dict__, "table": spec.target_table})
        elif job_name == "policy_rate":
            stats = fetch_latest_policy_rate(dry_run=args.dry_run, db_path=args.db_path)
            results.append({"job": job_name, "status": "success", "stats": stats.__dict__, "table": spec.target_table})
        elif job_name == "futures":
            stats = fetch_latest_futures(dry_run=args.dry_run, db_path=args.db_path)
            results.append({"job": job_name, "status": "success", "stats": stats.__dict__, "table": spec.target_table})
        elif job_name == "etf":
            stats = fetch_latest_etf_snapshot(
                dry_run=args.dry_run,
                db_path=args.db_path,
                snapshot_date=args.snapshot_date,
                rows_per_page=args.rows_per_page,
                max_pages=args.max_pages,
            )
            results.append({"job": job_name, "status": "success", "stats": stats.__dict__, "table": spec.target_table})
        elif job_name == "life_asset":
            stats = fetch_latest_life_asset(dry_run=args.dry_run, db_path=args.db_path)
            results.append({"job": job_name, "status": "success", "stats": stats.__dict__, "table": spec.target_table})
        elif job_name == "bond_index":
            if not args.bond_index_id:
                results.append(
                    {
                        "job": job_name,
                        "status": "skipped",
                        "reason": "missing --bond-index-id",
                        "table": spec.target_table,
                    }
                )
                continue
            for index_id in args.bond_index_id:
                stats = fetch_bond_index_duration(index_id, dry_run=args.dry_run, db_path=args.db_path)
                results.append(
                    {
                        "job": job_name,
                        "index_id": index_id,
                        "status": "success",
                        "stats": stats.__dict__,
                        "table": spec.target_table,
                    }
                )
        elif job_name == "overseas_macro":
            stats = fetch_latest_overseas_macro(dry_run=args.dry_run, db_path=args.db_path)
            results.append({"job": job_name, "status": "success", "stats": stats.__dict__, "table": spec.target_table})
        elif job_name == "policy_rate_recent":
            stats = fetch_recent_policy_rate_events(dry_run=args.dry_run, db_path=args.db_path, limit=args.limit)
            results.append({"job": job_name, "status": "success", "stats": stats.__dict__, "table": spec.target_table})
        elif job_name == "trading_days_update":
            result = sync_trading_days_csv(
                source_path=args.source_csv,
                target_path=args.target_csv,
                dry_run=args.dry_run,
            )
            results.append(
                {
                    "job": job_name,
                    "status": "success",
                    "table": spec.target_table,
                    "stats": {
                        "target_path": str(result.target_path),
                        "source_path": str(result.source_path) if result.source_path else None,
                        "coverage_start": result.coverage_start,
                        "coverage_end": result.coverage_end,
                        "row_count": result.row_count,
                        "changed": result.changed,
                        "created": result.created,
                        "dry_run": result.dry_run,
                    },
                }
            )
        else:
            raise ValueError(f"unsupported job in group: {job_name}")

    if args.group in {"cn_night", "us_morning"}:
        sync_stats = sync_metric_daily_from_raw(
            db_path=str(args.db_path) if args.db_path else None,
            dry_run=args.dry_run,
        )
        results.append(
            {
                "job": "metric_daily_sync",
                "status": "success",
                "table": "metric_daily",
                "stats": sync_stats,
            }
        )
        snapshot_date = _resolve_latest_metric_daily_date(args.db_path)
        snapshot_stats = _build_metric_snapshot_stats(args.db_path, snapshot_date, args.dry_run)
        results.append(
            {
                "job": "metric_snapshot",
                "status": "success",
                "table": "metric_snapshot",
                "stats": snapshot_stats,
            }
        )
        if not args.dry_run:
            export_stats = export_metric_snapshot_to_sheet(
                db=args.db_path or "runtime/db/app.sqlite",
                as_of_date=snapshot_date,
            )
            results.append(
                {
                    "job": "metric_snapshot_export",
                    "status": "success",
                    "table": "google_sheet",
                    "stats": export_stats,
                }
            )

    return {
        "group": args.group,
        "jobs": list(jobs),
        "results": results,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        output = run_group(args)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
