from __future__ import annotations

from pathlib import Path
from typing import Sequence

from src.core.runtime import WriteStats
from src.core.trading_calendar import DEFAULT_TRADING_DAYS_CSV, sync_trading_days_csv

from .daily import (
    fetch_bond_curve_for_date,
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
from .registry import get_daily_job_spec


def _stats_payload(stats: WriteStats) -> dict[str, int]:
    return stats.__dict__.copy()


def _result(job_name: str, *, status: str, table: str, stats: dict | None = None, **extra: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "job": job_name,
        "status": status,
        "table": table,
    }
    if stats is not None:
        payload["stats"] = stats
    payload.update(extra)
    return payload


def execute_daily_job(
    job_name: str,
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    date: str | None = None,
    limit: int = 20,
    index_id: str | None = None,
    index_name: str | None = None,
    index_code: str | None = None,
    bond_index_ids: Sequence[str] | None = None,
    snapshot_date: str | None = None,
    rows_per_page: int = 500,
    max_pages: int = 20,
    source_csv: Path | None = None,
    target_csv: Path = DEFAULT_TRADING_DAYS_CSV,
) -> list[dict[str, object]]:
    spec = get_daily_job_spec(job_name)

    if job_name == "money_market":
        stats = fetch_latest_money_market(dry_run=dry_run, db_path=db_path)
        return [_result(job_name, status="success", table=spec.target_table, stats=_stats_payload(stats))]

    if job_name == "bond_curve":
        if date:
            stats = fetch_bond_curve_for_date(date, dry_run=dry_run, db_path=db_path)
        else:
            stats = fetch_latest_bond_curve(dry_run=dry_run, db_path=db_path)
        return [_result(job_name, status="success", table=spec.target_table, stats=_stats_payload(stats))]

    if job_name == "bond_index":
        all_index_ids = list(bond_index_ids or ())
        if index_id:
            all_index_ids.append(index_id)
        if not all_index_ids:
            return [_result(job_name, status="skipped", table=spec.target_table, reason="missing --index-id")]
        out: list[dict[str, object]] = []
        for item in all_index_ids:
            stats = fetch_bond_index_duration(
                item,
                dry_run=dry_run,
                db_path=db_path,
                index_name=index_name,
                index_code=index_code,
            )
            out.append(
                _result(
                    job_name,
                    status="success",
                    table=spec.target_table,
                    stats=_stats_payload(stats),
                    index_id=item,
                )
            )
        return out

    if job_name == "overseas_macro":
        stats = fetch_latest_overseas_macro(dry_run=dry_run, db_path=db_path)
        return [_result(job_name, status="success", table=spec.target_table, stats=_stats_payload(stats))]

    if job_name == "policy_rate":
        stats = fetch_latest_policy_rate(dry_run=dry_run, db_path=db_path)
        return [_result(job_name, status="success", table=spec.target_table, stats=_stats_payload(stats))]

    if job_name == "policy_rate_recent":
        stats = fetch_recent_policy_rate_events(dry_run=dry_run, db_path=db_path, limit=limit)
        return [_result(job_name, status="success", table=spec.target_table, stats=_stats_payload(stats))]

    if job_name == "futures":
        stats = fetch_latest_futures(dry_run=dry_run, db_path=db_path)
        return [_result(job_name, status="success", table=spec.target_table, stats=_stats_payload(stats))]

    if job_name == "etf":
        stats = fetch_latest_etf_snapshot(
            dry_run=dry_run,
            db_path=db_path,
            snapshot_date=snapshot_date,
            rows_per_page=rows_per_page,
            max_pages=max_pages,
        )
        return [_result(job_name, status="success", table=spec.target_table, stats=_stats_payload(stats))]

    if job_name == "life_asset":
        stats = fetch_latest_life_asset(dry_run=dry_run, db_path=db_path)
        return [_result(job_name, status="success", table=spec.target_table, stats=_stats_payload(stats))]

    if job_name == "trading_days_update":
        sync_result = sync_trading_days_csv(
            source_path=source_csv,
            target_path=target_csv,
            dry_run=dry_run,
        )
        return [
            _result(
                job_name,
                status="success",
                table=spec.target_table,
                stats={
                    "target_path": str(sync_result.target_path),
                    "source_path": str(sync_result.source_path) if sync_result.source_path else None,
                    "coverage_start": sync_result.coverage_start,
                    "coverage_end": sync_result.coverage_end,
                    "row_count": sync_result.row_count,
                    "changed": sync_result.changed,
                    "created": sync_result.created,
                    "dry_run": sync_result.dry_run,
                },
            )
        ]

    raise ValueError(f"unsupported daily job: {job_name}")
