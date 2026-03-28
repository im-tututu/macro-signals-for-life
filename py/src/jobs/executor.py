from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

from src.core.runtime import WriteStats
from src.core.trading_calendar import DEFAULT_TRADING_DAYS_CSV, sync_trading_days_csv

from .ingest import (
    fetch_bond_curve_for_date,
    fetch_bond_index_duration,
    fetch_latest_etf_snapshot,
    fetch_latest_futures,
    fetch_latest_life_asset,
    fetch_latest_money_market,
    fetch_latest_qdii_snapshot,
    fetch_latest_sse_lively_bond_snapshot,
    fetch_latest_treasury_snapshot,
    fetch_latest_overseas_macro,
    fetch_latest_policy_rate,
    fetch_recent_policy_rate_events,
)
from .latest import fetch_latest_chinabond_curve
from .registry import DailyJobSpec, get_daily_job_spec


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


SimpleLatestHandler = Callable[..., WriteStats]
ExecutorHandler = Callable[["ExecutionContext"], list[dict[str, object]]]


SIMPLE_LATEST_HANDLERS: dict[str, SimpleLatestHandler] = {
    "money_market": fetch_latest_money_market,
    "policy_rate": fetch_latest_policy_rate,
    "futures": fetch_latest_futures,
    "life_asset": fetch_latest_life_asset,
    "overseas_macro": fetch_latest_overseas_macro,
}


@dataclass(frozen=True)
class ExecutionContext:
    spec: DailyJobSpec
    dry_run: bool
    db_path: Path | None
    date: str | None
    limit: int
    index_id: str | None
    index_name: str | None
    index_code: str | None
    bond_index_ids: Sequence[str] | None
    snapshot_date: str | None
    rows_per_page: int
    max_pages: int
    source_csv: Path | None
    target_csv: Path


def _run_simple_latest_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    """执行“无额外参数分支”的标准 latest job。

    这些 job 都遵循同一模式：
    - 从 registry 取目标表等元信息
    - 调用一个 latest handler
    - 把 WriteStats 包装成统一返回结构
    """

    handler = SIMPLE_LATEST_HANDLERS[ctx.spec.job_name]
    stats = handler(dry_run=ctx.dry_run, db_path=ctx.db_path)
    return [_result(ctx.spec.job_name, status="success", table=ctx.spec.target_table, stats=_stats_payload(stats))]


def _run_bond_curve_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    if ctx.date:
        stats = fetch_bond_curve_for_date(ctx.date, dry_run=ctx.dry_run, db_path=ctx.db_path)
    else:
        stats = fetch_latest_chinabond_curve(dry_run=ctx.dry_run, db_path=ctx.db_path)
    return [_result(ctx.spec.job_name, status="success", table=ctx.spec.target_table, stats=_stats_payload(stats))]


def _run_bond_index_batch_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    all_index_ids = list(ctx.bond_index_ids or ())
    if ctx.index_id:
        all_index_ids.append(ctx.index_id)
    if not all_index_ids:
        return [_result(ctx.spec.job_name, status="skipped", table=ctx.spec.target_table, reason="missing --index-id")]

    out: list[dict[str, object]] = []
    for item in all_index_ids:
        stats = fetch_bond_index_duration(
            item,
            dry_run=ctx.dry_run,
            db_path=ctx.db_path,
            index_name=ctx.index_name,
            index_code=ctx.index_code,
        )
        out.append(
            _result(
                ctx.spec.job_name,
                status="success",
                table=ctx.spec.target_table,
                stats=_stats_payload(stats),
                index_id=item,
            )
        )
    return out


def _run_policy_rate_recent_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    stats = fetch_recent_policy_rate_events(dry_run=ctx.dry_run, db_path=ctx.db_path, limit=ctx.limit)
    return [_result(ctx.spec.job_name, status="success", table=ctx.spec.target_table, stats=_stats_payload(stats))]


def _run_etf_snapshot_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    stats = fetch_latest_etf_snapshot(
        dry_run=ctx.dry_run,
        db_path=ctx.db_path,
        snapshot_date=ctx.snapshot_date,
        rows_per_page=ctx.rows_per_page,
        max_pages=ctx.max_pages,
    )
    return [_result(ctx.spec.job_name, status="success", table=ctx.spec.target_table, stats=_stats_payload(stats))]


def _run_qdii_snapshot_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    stats = fetch_latest_qdii_snapshot(
        dry_run=ctx.dry_run,
        db_path=ctx.db_path,
        snapshot_date=ctx.snapshot_date,
        rows_per_page=ctx.rows_per_page,
        max_pages=ctx.max_pages,
    )
    return [_result(ctx.spec.job_name, status="success", table=ctx.spec.target_table, stats=_stats_payload(stats))]


def _run_treasury_snapshot_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    stats = fetch_latest_treasury_snapshot(
        dry_run=ctx.dry_run,
        db_path=ctx.db_path,
        snapshot_date=ctx.snapshot_date,
    )
    return [_result(ctx.spec.job_name, status="success", table=ctx.spec.target_table, stats=_stats_payload(stats))]


def _run_sse_lively_bond_snapshot_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    stats = fetch_latest_sse_lively_bond_snapshot(
        dry_run=ctx.dry_run,
        db_path=ctx.db_path,
        page_size=ctx.rows_per_page,
        max_pages=ctx.max_pages,
    )
    return [_result(ctx.spec.job_name, status="success", table=ctx.spec.target_table, stats=_stats_payload(stats))]


def _run_trading_days_update_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    sync_result = sync_trading_days_csv(
        source_path=ctx.source_csv,
        target_path=ctx.target_csv,
        dry_run=ctx.dry_run,
    )
    return [
        _result(
            ctx.spec.job_name,
            status="success",
            table=ctx.spec.target_table,
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


EXECUTION_HANDLERS: dict[str, ExecutorHandler] = {
    "simple_latest": _run_simple_latest_job,
    "bond_curve": _run_bond_curve_job,
    "bond_index_batch": _run_bond_index_batch_job,
    "policy_rate_recent": _run_policy_rate_recent_job,
    "etf_snapshot": _run_etf_snapshot_job,
    "qdii_snapshot": _run_qdii_snapshot_job,
    "treasury_snapshot": _run_treasury_snapshot_job,
    "sse_lively_bond_snapshot": _run_sse_lively_bond_snapshot_job,
    "trading_days_update": _run_trading_days_update_job,
}


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
    handler = EXECUTION_HANDLERS.get(spec.execution_key)
    if handler is None:
        raise ValueError(f"unsupported execution key for daily job {job_name}: {spec.execution_key}")

    ctx = ExecutionContext(
        spec=spec,
        dry_run=dry_run,
        db_path=db_path,
        date=date,
        limit=limit,
        index_id=index_id,
        index_name=index_name,
        index_code=index_code,
        bond_index_ids=bond_index_ids,
        snapshot_date=snapshot_date,
        rows_per_page=rows_per_page,
        max_pages=max_pages,
        source_csv=source_csv,
        target_csv=target_csv,
    )
    return handler(ctx)
