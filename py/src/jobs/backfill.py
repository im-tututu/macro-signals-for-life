from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from src.core.config import CURVES
from src.core.runtime import WriteStats
from src.core.trading_calendar import load_trading_day_window
from src.datasets.raw_queries import has_complete_chinabond_curves_for_date
from src.datasets.registry import get_dataset_spec
from src.jobs.ingest import fetch_bond_curve_for_date
from src.jobs.manual import review_table


def _require_chinabond_curve_backfill_spec() -> None:
    """确认中债曲线回填逻辑与 dataset spec 一致。"""

    spec = get_dataset_spec("chinabond_curve")
    if not spec.supports_backfill:
        raise ValueError("dataset chinabond_curve does not support backfill")
    if spec.backfill_mode != "trading_day_range":
        raise ValueError(f"unexpected backfill mode for chinabond_curve: {spec.backfill_mode}")
    if not spec.trading_day_sensitive:
        raise ValueError("dataset chinabond_curve is expected to be trading-day-sensitive")
    if not spec.prefer_trading_day_window:
        raise ValueError("dataset chinabond_curve is expected to prefer trading day windows")


def _merge_stats(stats_list: list[WriteStats]) -> WriteStats:
    merged = WriteStats()
    for stats in stats_list:
        merged.merge(stats)
    return merged


def backfill_chinabond_curve_window(
    *,
    start_date: str,
    end_date: str,
    db_path: Path | None = None,
    dry_run: bool = False,
    review: bool = False,
    trading_days_csv: Path | None = None,
    skip_weekends: bool = False,
    force_fetch: bool = False,
) -> dict[str, object]:
    """按交易日窗口回填 `chinabond_curve`。

    这是 `run_backfill.py` 中 `bond_curve` 分支的核心实现。

    之所以单独抽出来，是为了把“脚本参数解析”和“中债曲线回填逻辑”分开：
    - 脚本层只负责 CLI
    - jobs/backfill 层负责 dataset-driven 的历史抓取编排
    """

    _require_chinabond_curve_backfill_spec()

    stats_list: list[WriteStats] = []
    attempted_dates: list[str] = []
    precheck_skipped_dates: list[str] = []
    failures: list[dict[str, str]] = []
    expected_curves = [curve.name for curve in CURVES]
    window = load_trading_day_window(
        start_date=start_date,
        end_date=end_date,
        csv_path=trading_days_csv,
        skip_weekends_on_fallback=skip_weekends,
    )

    for current_date in window.dates:
        current = datetime.strptime(current_date, "%Y-%m-%d").date()
        if skip_weekends and window.csv_path is None and current.weekday() >= 5:
            continue
        if not force_fetch and has_complete_chinabond_curves_for_date(
            current_date,
            db_path=db_path,
            expected_curves=expected_curves,
        ):
            precheck_skipped_dates.append(current_date)
            continue
        attempted_dates.append(current_date)
        try:
            stats_list.append(fetch_bond_curve_for_date(current_date, dry_run=dry_run, db_path=db_path))
        except Exception as exc:  # noqa: BLE001
            failures.append({"date": current_date, "error": str(exc)})

    return {
        "target": "bond_curve",
        "dataset_id": "chinabond_curve",
        "dates": attempted_dates,
        "trading_days_csv": str(window.csv_path) if window.csv_path else None,
        "calendar_coverage_start": window.coverage_start,
        "calendar_coverage_end": window.coverage_end,
        "fallback_dates": window.fallback_dates or [],
        "precheck_skipped_dates": precheck_skipped_dates,
        "stats": asdict(_merge_stats(stats_list)),
        "failures": failures,
        "review": review_table("raw_bond_curve", db_path=db_path) if review else None,
    }
