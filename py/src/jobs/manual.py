from __future__ import annotations

from pathlib import Path
from typing import Iterable

from .common import get_store, run_clean_job, run_incremental_job, run_review_job, run_single_row_job, run_upsert_job, run_window_job


def upsert_single(store_name: str, row: dict, *, dry_run: bool = False, db_path: Path | None = None):
    store = get_store(store_name, db_path=db_path)
    return run_single_row_job(store=store, row=row, job_name=f"manual_{store_name}_single", source_type="manual", dry_run=dry_run)


def upsert_batch(store_name: str, rows: Iterable[dict], *, dry_run: bool = False, db_path: Path | None = None):
    store = get_store(store_name, db_path=db_path)
    return run_upsert_job(store=store, rows=rows, job_name=f"manual_{store_name}_batch", source_type="manual", dry_run=dry_run)


def upsert_window(store_name: str, rows: Iterable[dict], *, start_date: str, end_date: str, dry_run: bool = False, db_path: Path | None = None):
    store = get_store(store_name, db_path=db_path)
    return run_window_job(
        store=store,
        rows=rows,
        start_date=start_date,
        end_date=end_date,
        job_name=f"manual_{store_name}_window",
        source_type="manual",
        dry_run=dry_run,
    )


def upsert_incremental(store_name: str, rows: Iterable[dict], *, dry_run: bool = False, db_path: Path | None = None, inclusive: bool = False, **latest_filters: object):
    store = get_store(store_name, db_path=db_path)
    return run_incremental_job(
        store=store,
        rows=rows,
        job_name=f"manual_{store_name}_incremental",
        source_type="manual",
        dry_run=dry_run,
        inclusive=inclusive,
        **latest_filters,
    )


def review_table(store_name: str, *, db_path: Path | None = None):
    store = get_store(store_name, db_path=db_path)
    return run_review_job(store=store, job_name=f"review_{store_name}")


def clean_table(store_name: str, *, dry_run: bool = True, delete_invalid_rows: bool = False, db_path: Path | None = None):
    store = get_store(store_name, db_path=db_path)
    return run_clean_job(
        store=store,
        job_name=f"clean_{store_name}",
        dry_run=dry_run,
        delete_invalid_rows=delete_invalid_rows,
    )
