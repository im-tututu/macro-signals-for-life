from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Callable, Iterable, Sequence, TypeVar

from src.core.runtime import RunContext, WriteStats
from src.datasets.raw_registry import RAW_TABLE_REGISTRY, get_raw_spec_by_table_name
from src.sources._base import FetchResult
from src.stores import (
    MetricsStore,
    RunLogStore,
    SignalsStore,
)
from src.stores._base import BaseSqliteStore
from src.stores.raw import RawStore

T = TypeVar("T")

STORE_REGISTRY: dict[str, type[BaseSqliteStore]] = {
    "run_log": RunLogStore,
    "metrics": MetricsStore,
    "signal_main": SignalsStore,
}


def get_store(store_name: str, db_path: Path | None = None) -> BaseSqliteStore:
    if store_name in RAW_TABLE_REGISTRY:
        return RawStore(get_raw_spec_by_table_name(store_name).table_spec, db_path=db_path)
    try:
        return STORE_REGISTRY[store_name](db_path=db_path)
    except KeyError as exc:
        raise ValueError(f"unknown store: {store_name}") from exc


def run_upsert_job(
    *,
    store: BaseSqliteStore,
    rows: Iterable[dict],
    job_name: str,
    source_type: str,
    dry_run: bool = False,
) -> WriteStats:
    ctx = RunContext.create(job_name=job_name, source_type=source_type, dry_run=dry_run, db_path=store.db_path)
    stats = store.upsert_many(rows, dry_run=dry_run, run=ctx)
    ctx.finish(
        status="success",
        stats=stats,
        message=f"{store.spec.table_name}: {stats.as_message()}",
        detail=f"job={job_name} source={source_type}",
    )
    return stats


def run_single_row_job(
    *,
    store: BaseSqliteStore,
    row: dict,
    job_name: str,
    source_type: str,
    dry_run: bool = False,
) -> WriteStats:
    return run_upsert_job(store=store, rows=[row], job_name=job_name, source_type=source_type, dry_run=dry_run)


def run_window_job(
    *,
    store: BaseSqliteStore,
    rows: Iterable[dict],
    start_date: str,
    end_date: str,
    job_name: str,
    source_type: str,
    dry_run: bool = False,
) -> WriteStats:
    if not store.spec.date_field:
        raise ValueError(f"store {store.spec.table_name} does not support date windows")
    kept = [
        row
        for row in rows
        if row.get(store.spec.date_field) is not None and start_date <= str(row[store.spec.date_field]) <= end_date
    ]
    return run_upsert_job(store=store, rows=kept, job_name=job_name, source_type=source_type, dry_run=dry_run)


def run_incremental_job(
    *,
    store: BaseSqliteStore,
    rows: Iterable[dict],
    job_name: str,
    source_type: str,
    dry_run: bool = False,
    inclusive: bool = False,
    **latest_filters: object,
) -> WriteStats:
    if not store.spec.date_field:
        raise ValueError(f"store {store.spec.table_name} does not support incremental windows")
    latest = store.fetch_latest_date(**latest_filters)
    if latest is None:
        kept = list(rows)
    else:
        if inclusive:
            kept = [row for row in rows if row.get(store.spec.date_field) is not None and str(row[store.spec.date_field]) >= latest]
        else:
            kept = [row for row in rows if row.get(store.spec.date_field) is not None and str(row[store.spec.date_field]) > latest]
    return run_upsert_job(store=store, rows=kept, job_name=job_name, source_type=source_type, dry_run=dry_run)


def run_fetch_transform_job(
    *,
    store: BaseSqliteStore,
    fetch: Callable[[], FetchResult[T]],
    row_builder: Callable[[FetchResult[T]], dict],
    job_name: str,
    source_type: str,
    dry_run: bool = False,
    incremental: bool = True,
    inclusive: bool = False,
    **latest_filters: object,
) -> WriteStats:
    """通用编排 helper。

    适用场景：
    1. source 先抓取并返回 FetchResult
    2. store 负责把 FetchResult / payload 映射成表行
    3. job 只需要声明“抓什么、如何写入”
    """

    fetch_result = fetch()
    row = row_builder(fetch_result)
    if incremental:
        return run_incremental_job(
            store=store,
            rows=[row],
            job_name=job_name,
            source_type=source_type,
            dry_run=dry_run,
            inclusive=inclusive,
            **latest_filters,
        )
    return run_upsert_job(
        store=store,
        rows=[row],
        job_name=job_name,
        source_type=source_type,
        dry_run=dry_run,
    )


def run_fetch_transform_many_job(
    *,
    store: BaseSqliteStore,
    fetch: Callable[[], FetchResult[T]],
    rows_builder: Callable[[FetchResult[T]], Iterable[dict]],
    job_name: str,
    source_type: str,
    dry_run: bool = False,
    incremental: bool = True,
    inclusive: bool = False,
    **latest_filters: object,
) -> WriteStats:
    """通用多行编排 helper。

    适用场景：
    - 一个来源一次抓取会产生多条业务行
    - 例如收益率曲线、政策事件、ETF 快照等
    """

    fetch_result = fetch()
    rows = list(rows_builder(fetch_result))
    if incremental:
        return run_incremental_job(
            store=store,
            rows=rows,
            job_name=job_name,
            source_type=source_type,
            dry_run=dry_run,
            inclusive=inclusive,
            **latest_filters,
        )
    return run_upsert_job(
        store=store,
        rows=rows,
        job_name=job_name,
        source_type=source_type,
        dry_run=dry_run,
    )


def run_review_job(
    *,
    store: BaseSqliteStore,
    job_name: str,
    dry_run: bool = True,
) -> dict:
    ctx = RunContext.create(job_name=job_name, source_type=store.spec.table_name, dry_run=dry_run, db_path=store.db_path)
    report = store.review_data()
    stats = WriteStats(reviewed=report.total_rows)
    ctx.finish(
        status="success",
        stats=stats,
        message=(
            f"{report.table_name}: rows={report.total_rows} blank_keys={report.blank_key_rows} "
            f"dup_groups={report.duplicate_key_groups} dup_rows={report.duplicate_key_rows} "
            f"date_range={report.min_date}..{report.max_date}"
        ),
        detail="review only",
    )
    return asdict(report)


def run_clean_job(
    *,
    store: BaseSqliteStore,
    job_name: str,
    dry_run: bool = True,
    delete_invalid_rows: bool = False,
) -> WriteStats:
    ctx = RunContext.create(job_name=job_name, source_type=store.spec.table_name, dry_run=dry_run, db_path=store.db_path)
    stats = store.clean_data(dry_run=dry_run, delete_invalid_rows=delete_invalid_rows, run=ctx)
    ctx.finish(
        status="success",
        stats=stats,
        message=f"{store.spec.table_name}: {stats.as_message()}",
        detail=f"clean delete_invalid_rows={delete_invalid_rows}",
    )
    return stats
