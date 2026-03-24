from __future__ import annotations

from pathlib import Path

from .common import STORE_REGISTRY, get_store, run_clean_job, run_review_job


def review_all_raw_tables(*, db_path: Path | None = None) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for name in STORE_REGISTRY:
        if name in {"metrics", "signal_main"}:
            continue
        store = get_store(name, db_path=db_path)
        out[name] = run_review_job(store=store, job_name=f"review_{name}")
    return out


def clean_all_raw_tables(*, db_path: Path | None = None, dry_run: bool = True, delete_invalid_rows: bool = False):
    out = {}
    for name in STORE_REGISTRY:
        if name in {"metrics", "signal_main"}:
            continue
        store = get_store(name, db_path=db_path)
        out[name] = run_clean_job(
            store=store,
            job_name=f"clean_{name}",
            dry_run=dry_run,
            delete_invalid_rows=delete_invalid_rows,
        )
    return out
