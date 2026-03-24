from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable

from .common import get_store, run_incremental_job


# 说明：
# 当前仓库里的 py/src/sources 仍大多是占位文件；所以 daily 这里先提供
# “调度骨架”，后续只要把 fetcher 补齐即可直接接入。

def run_daily_fetcher(
    store_name: str,
    fetcher: Callable[[], Iterable[dict]],
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    source_type: str = "daily_fetcher",
    **latest_filters: object,
):
    store = get_store(store_name, db_path=db_path)
    rows = list(fetcher())
    return run_incremental_job(
        store=store,
        rows=rows,
        job_name=f"daily_{store_name}",
        source_type=source_type,
        dry_run=dry_run,
        **latest_filters,
    )
