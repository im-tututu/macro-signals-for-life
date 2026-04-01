#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.core.trading_calendar import DEFAULT_TRADING_DAYS_CSV
from src.core.runtime import build_logger
from src.core.notify import build_notifier
from src.jobs.executor import execute_daily_job
from src.metrics import build_metric_snapshots, list_metric_registry, sync_metric_daily_from_raw, upsert_metric_snapshots
from export_latest_metric_snapshot_to_sheet import export_metric_snapshot_to_sheet


JOB_GROUPS: dict[str, tuple[str, ...]] = {
    "cn_night": (
        "money_market",
        "bond_curve",
        "policy_rate",
        "etf",
        "jisilu_gold",
        "jisilu_money",
        "qdii",
        "treasury",
        "sse_lively_bond",
        "life_asset",
        "chinabond_index",
        "csindex_bond_index",
        "cnindex_bond_index",
    ),
    "us_morning": (
        "fred",
        "alpha_vantage",
    ),
    "manual": (
        "policy_rate_recent",
        "trading_days_update",
    ),
}

JOB_DELAY_RULES: dict[str, tuple[float, float]] = {
    "money_market": (0.8, 1.8),
    "bond_curve": (1.0, 2.2),
    "policy_rate": (0.8, 1.6),
    "etf": (2.0, 4.0),
    "jisilu_gold": (2.0, 4.0),
    "jisilu_money": (2.0, 4.0),
    "qdii": (2.0, 4.0),
    "treasury": (2.0, 4.0),
    "sse_lively_bond": (1.2, 2.5),
    "life_asset": (0.8, 1.8),
    "chinabond_index": (1.2, 2.5),
    "csindex_bond_index": (1.2, 2.5),
    "cnindex_bond_index": (1.2, 2.5),
    "fred": (0.5, 1.2),
    "alpha_vantage": (0.8, 1.8),
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
        help="cn_night 组里可重复传入的债券指数 index_id；未提供则按各 provider 默认名单抓取。",
    )
    return parser


def run_group(args: argparse.Namespace) -> dict[str, object]:
    jobs = JOB_GROUPS[args.group]
    results: list[dict[str, object]] = []
    failed_jobs: list[str] = []
    previous_disable_success_bark = os.environ.get("MSFL_DISABLE_SUCCESS_BARK")
    os.environ["MSFL_DISABLE_SUCCESS_BARK"] = "1"

    try:
        for job_name in jobs:
            _sleep_before_job(args.group, job_name)
            try:
                results.extend(
                    execute_daily_job(
                        job_name,
                        dry_run=args.dry_run,
                        db_path=args.db_path,
                        limit=args.limit,
                        bond_index_ids=args.bond_index_id,
                        snapshot_date=args.snapshot_date,
                        rows_per_page=args.rows_per_page,
                        max_pages=args.max_pages,
                        source_csv=args.source_csv,
                        target_csv=args.target_csv,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                failed_jobs.append(job_name)
                results.append(
                    {
                        "job": job_name,
                        "status": "failed",
                        "table": None,
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                        "traceback": traceback.format_exc(),
                    }
                )

        if args.group in {"cn_night", "us_morning"}:
            try:
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
            except Exception as exc:  # noqa: BLE001
                failed_jobs.append("metric_pipeline")
                results.append(
                    {
                        "job": "metric_pipeline",
                        "status": "failed",
                        "table": None,
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                        "traceback": traceback.format_exc(),
                    }
                )
    finally:
        if previous_disable_success_bark is None:
            os.environ.pop("MSFL_DISABLE_SUCCESS_BARK", None)
        else:
            os.environ["MSFL_DISABLE_SUCCESS_BARK"] = previous_disable_success_bark

    _notify_group_summary(args.group, results, args.dry_run)

    return {
        "group": args.group,
        "jobs": list(jobs),
        "failed_jobs": failed_jobs,
        "results": results,
    }


def _sleep_before_job(group: str, job_name: str) -> None:
    delay_range = JOB_DELAY_RULES.get(job_name)
    if delay_range is None:
        return
    low, high = delay_range
    if high <= 0 or high < low:
        return
    seconds = random.uniform(low, high)
    logger = build_logger(f"job_group_{group}")
    logger.info("任务 %s 执行前等待 %.2fs，降低连续请求频率。", job_name, seconds)
    time.sleep(seconds)


def _notify_group_summary(group: str, results: list[dict[str, object]], dry_run: bool) -> None:
    if dry_run:
        return
    logger = build_logger(f"job_group_{group}")
    notifier = build_notifier(logger)
    inserted_parts: list[str] = []
    updated_parts: list[str] = []
    changed_parts: list[str] = []
    skipped_parts: list[str] = []
    failed_parts: list[str] = []
    for item in results:
        job = str(item.get("job") or "")
        status = str(item.get("status") or "")
        if status == "failed":
            failed_parts.append(job)
            continue
        stats = item.get("stats")
        if not isinstance(stats, dict):
            continue
        inserted = int(stats.get("inserted") or 0)
        updated = int(stats.get("updated") or 0)
        changed = int(stats.get("changed") or 0)
        skipped = int(stats.get("skipped") or 0)
        if inserted > 0:
            inserted_parts.append(f"{job}:{inserted}")
        if updated > 0:
            updated_parts.append(f"{job}:{updated}")
        if changed > 0:
            changed_parts.append(f"{job}:{changed}")
        elif skipped > 0:
            skipped_parts.append(job)

    if failed_parts:
        title = f"{group} WARNING"
        parts = [f"失败 {len(failed_parts)} 项: {', '.join(failed_parts[:8])}"]
        if inserted_parts:
            parts.append(f"新增 {len(inserted_parts)} 项: {', '.join(inserted_parts[:6])}")
        if updated_parts:
            parts.append(f"更新 {len(updated_parts)} 项: {', '.join(updated_parts[:6])}")
        if changed_parts:
            parts.append(f"总变化 {len(changed_parts)} 项: {', '.join(changed_parts[:8])}")
        if skipped_parts:
            parts.append(f"跳过 {len(skipped_parts)} 项")
        notifier.notify(title, "\n".join(parts), level="WARNING")
        return

    if changed_parts:
        title = f"{group} SUCCESS"
        parts = []
        if inserted_parts:
            parts.append(f"新增 {len(inserted_parts)} 项: {', '.join(inserted_parts[:6])}")
        if updated_parts:
            parts.append(f"更新 {len(updated_parts)} 项: {', '.join(updated_parts[:6])}")
        parts.append(f"总变化 {len(changed_parts)} 项: {', '.join(changed_parts[:10])}")
        if skipped_parts:
            parts.append(f"跳过 {len(skipped_parts)} 项")
        notifier.notify(title, "\n".join(parts), level="INFO")


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
