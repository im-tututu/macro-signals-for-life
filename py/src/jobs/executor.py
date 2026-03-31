from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

from src.core.db import connect
from src.core.runtime import WriteStats
from src.core.trading_calendar import DEFAULT_TRADING_DAYS_CSV, sync_trading_days_csv

from .ingest import (
    fetch_akshare_bond_gb_us_sina,
    fetch_akshare_bond_zh_us_rate,
    fetch_bond_curve_for_date,
    fetch_chinabond_bond_index,
    fetch_cnindex_bond_index,
    fetch_csindex_bond_index,
    fetch_latest_etf_snapshot,
    fetch_latest_jisilu_gold_snapshot,
    fetch_latest_jisilu_money_snapshot,
    fetch_latest_alpha_vantage,
    fetch_latest_futures,
    fetch_latest_fred,
    fetch_latest_money_market,
    fetch_latest_qdii_snapshot,
    fetch_latest_sse_lively_bond_snapshot,
    fetch_latest_treasury_snapshot,
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
    "jisilu_gold": fetch_latest_jisilu_gold_snapshot,
    "jisilu_money": fetch_latest_jisilu_money_snapshot,
    "policy_rate": fetch_latest_policy_rate,
    "futures": fetch_latest_futures,
    "fred": fetch_latest_fred,
    "alpha_vantage": fetch_latest_alpha_vantage,
    "akshare_bond_gb_us_sina": fetch_akshare_bond_gb_us_sina,
    "akshare_bond_zh_us_rate": fetch_akshare_bond_zh_us_rate,
}


@dataclass(frozen=True)
class ExecutionContext:
    spec: DailyJobSpec
    dry_run: bool
    force: bool
    db_path: Path | None
    date: str | None
    limit: int
    index_id: str | None
    index_name: str | None
    index_code: str | None
    symbol: str | None
    start_date: str | None
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
    kwargs: dict[str, object] = {"dry_run": ctx.dry_run, "db_path": ctx.db_path}
    if ctx.spec.job_name == "akshare_bond_gb_us_sina":
        kwargs["symbol"] = ctx.symbol
    if ctx.spec.job_name == "akshare_bond_zh_us_rate" and ctx.start_date:
        kwargs["start_date"] = ctx.start_date
    stats = handler(**kwargs)
    return [_result(ctx.spec.job_name, status="success", table=ctx.spec.target_table, stats=_stats_payload(stats))]


def _run_bond_curve_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    if ctx.date:
        stats = fetch_bond_curve_for_date(ctx.date, dry_run=ctx.dry_run, db_path=ctx.db_path)
    else:
        stats = fetch_latest_chinabond_curve(dry_run=ctx.dry_run, db_path=ctx.db_path)
    return [_result(ctx.spec.job_name, status="success", table=ctx.spec.target_table, stats=_stats_payload(stats))]


def _load_default_bond_index_items(db_path: Path | None, provider: str) -> list[dict[str, str]]:
    """从配置表按 provider 加载默认可抓的债券指数名单。"""

    conn = connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT index_name, index_code
            FROM cfg_bond_index_list
            WHERE provider = ?
              AND COALESCE(index_code, '') <> ''
            ORDER BY index_name
            """,
            (provider,),
        ).fetchall()
        return [
            {
                "index_id": str(row["index_code"]),
                "index_name": str(row["index_name"]),
                "index_code": str(row["index_code"]),
            }
            for row in rows
        ]
    finally:
        conn.close()


def _run_bond_index_batch_job(
    ctx: ExecutionContext,
    *,
    provider: str,
    fetcher: Callable[..., WriteStats],
) -> list[dict[str, object]]:
    items: list[dict[str, str | None]] = []
    explicit_ids = list(ctx.bond_index_ids or ())
    if ctx.index_id:
        explicit_ids.append(ctx.index_id)

    if explicit_ids:
        for item in explicit_ids:
            items.append(
                {
                    "index_id": item,
                    "index_name": ctx.index_name,
                    "index_code": ctx.index_code,
                }
            )
    else:
        defaults = _load_default_bond_index_items(ctx.db_path, provider)
        if not defaults:
            return [_result(ctx.spec.job_name, status="skipped", table=ctx.spec.target_table, reason="missing index ids")]
        items.extend(defaults)

    out: list[dict[str, object]] = []
    for item in items:
        stats = fetcher(
            str(item["index_id"]),
            dry_run=ctx.dry_run,
            db_path=ctx.db_path,
            index_name=str(item["index_name"]) if item["index_name"] else None,
            index_code=str(item["index_code"]) if item["index_code"] else None,
        )
        out.append(
            _result(
                ctx.spec.job_name,
                status="success",
                table=ctx.spec.target_table,
                stats=_stats_payload(stats),
                index_id=item["index_id"],
                index_name=item["index_name"],
            )
        )
    return out


def _run_chinabond_bond_index_batch_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    return _run_bond_index_batch_job(ctx, provider="中债", fetcher=fetch_chinabond_bond_index)


def _run_csindex_bond_index_batch_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    return _run_bond_index_batch_job(ctx, provider="中证", fetcher=fetch_csindex_bond_index)


def _run_cnindex_bond_index_batch_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    return _run_bond_index_batch_job(ctx, provider="国证", fetcher=fetch_cnindex_bond_index)


def _run_policy_rate_recent_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    stats = fetch_recent_policy_rate_events(dry_run=ctx.dry_run, db_path=ctx.db_path, limit=ctx.limit)
    return [_result(ctx.spec.job_name, status="success", table=ctx.spec.target_table, stats=_stats_payload(stats))]


def _run_etf_snapshot_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    stats = fetch_latest_etf_snapshot(
        dry_run=ctx.dry_run,
        force=ctx.force,
        db_path=ctx.db_path,
        snapshot_date=ctx.snapshot_date,
        rows_per_page=ctx.rows_per_page,
        max_pages=ctx.max_pages,
    )
    return [_result(ctx.spec.job_name, status="success", table=ctx.spec.target_table, stats=_stats_payload(stats))]


def _run_qdii_snapshot_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    stats = fetch_latest_qdii_snapshot(
        dry_run=ctx.dry_run,
        force=ctx.force,
        db_path=ctx.db_path,
        snapshot_date=ctx.snapshot_date,
        rows_per_page=ctx.rows_per_page,
        max_pages=ctx.max_pages,
    )
    return [_result(ctx.spec.job_name, status="success", table=ctx.spec.target_table, stats=_stats_payload(stats))]


def _run_treasury_snapshot_job(ctx: ExecutionContext) -> list[dict[str, object]]:
    stats = fetch_latest_treasury_snapshot(
        dry_run=ctx.dry_run,
        force=ctx.force,
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
    "chinabond_bond_index_batch": _run_chinabond_bond_index_batch_job,
    "csindex_bond_index_batch": _run_csindex_bond_index_batch_job,
    "cnindex_bond_index_batch": _run_cnindex_bond_index_batch_job,
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
    force: bool = False,
    db_path: Path | None = None,
    date: str | None = None,
    limit: int = 20,
    index_id: str | None = None,
    index_name: str | None = None,
    index_code: str | None = None,
    bond_index_ids: Sequence[str] | None = None,
    symbol: str | None = None,
    start_date: str | None = None,
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
        force=force,
        db_path=db_path,
        date=date,
        limit=limit,
        index_id=index_id,
        index_name=index_name,
        index_code=index_code,
        symbol=symbol,
        start_date=start_date,
        bond_index_ids=bond_index_ids,
        snapshot_date=snapshot_date,
        rows_per_page=rows_per_page,
        max_pages=max_pages,
        source_csv=source_csv,
        target_csv=target_csv,
    )
    return handler(ctx)
