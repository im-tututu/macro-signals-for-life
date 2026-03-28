from __future__ import annotations

from datetime import date as date_cls
from datetime import datetime, timedelta
from pathlib import Path

from src.core.runtime import WriteStats
from src.datasets.registry import get_dataset_spec
from src.jobs.common import run_incremental_job


def _to_ymd(value: str | date_cls | datetime | None = None) -> str:
    if value is None:
        return date_cls.today().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date_cls):
        return value.isoformat()
    return str(value)


def _iter_recent_dates(start_date: str | date_cls | datetime | None = None, *, lookback_days: int = 7) -> list[str]:
    current = datetime.strptime(_to_ymd(start_date), "%Y-%m-%d").date()
    return [(current - timedelta(days=offset)).isoformat() for offset in range(max(lookback_days, 1))]


def _require_chinabond_curve_latest_spec() -> None:
    """确认中债曲线 latest 语义与 dataset spec 一致。"""

    spec = get_dataset_spec("chinabond_curve")
    if not spec.supports_latest:
        raise RuntimeError("dataset chinabond_curve does not support latest sync")
    if spec.latest_mode != "latest_available_with_lookback":
        raise RuntimeError(f"unexpected latest mode for chinabond_curve: {spec.latest_mode}")
    if not spec.trading_day_sensitive:
        raise RuntimeError("dataset chinabond_curve is expected to be trading-day-sensitive")


def fetch_latest_chinabond_curve(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    start_date: str | None = None,
    lookback_days: int = 7,
) -> WriteStats:
    """抓取最近一个可用日期的中债收益率曲线。

    这是 `chinabond_curve` 的 latest job 实现。

    语义上它不同于“按指定日期抓一次”：
    - latest 会以 start_date / 今天为起点
    - 若当天无可用曲线，则按 lookback 窗口向前回看
    - 一旦找到首个有 payload 的日期，就写入该日期的整组曲线
    """

    _require_chinabond_curve_latest_spec()

    from src.sources.chinabond import ChinaBondSource
    from src.stores.bond_curves import BondCurveStore

    store = BondCurveStore(db_path=db_path)
    source = ChinaBondSource()

    picked_date = _to_ymd(start_date)
    fetch_result = None
    for candidate_date in _iter_recent_dates(start_date, lookback_days=lookback_days):
        candidate_result = source.fetch_daily_wide_result(candidate_date)
        if candidate_result.payload:
            picked_date = candidate_date
            fetch_result = candidate_result
            break

    if fetch_result is None:
        fetch_result = source.fetch_daily_wide_result(picked_date)

    rows = store.build_rows_from_fetch_result(fetch_result)
    source_type = f"chinabond_yc_detail_auto:{picked_date}"
    return run_incremental_job(
        store=store,
        rows=rows,
        job_name="daily_raw_bond_curve",
        source_type=source_type,
        dry_run=dry_run,
        inclusive=True,
    )
