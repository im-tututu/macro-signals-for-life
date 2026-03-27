from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from .config import IMPORT_DIR, REFERENCE_DIR, REPO_ROOT, ensure_runtime_dirs, getenv_first

ENV_TRADING_DAYS_CSV_KEYS = ("TRADING_DAYS_CSV", "MSFL_TRADING_DAYS_CSV", "BOND_TRADING_DAYS_CSV")
ENV_TRADING_DAYS_SOURCE_CSV_KEYS = ("TRADING_DAYS_SOURCE_CSV", "MSFL_TRADING_DAYS_SOURCE_CSV")

DEFAULT_TRADING_DAYS_CSV = REFERENCE_DIR / "trading_days.csv"

TRADING_DAYS_CANDIDATES = [
    DEFAULT_TRADING_DAYS_CSV,
    IMPORT_DIR / "trading_days.csv",
    IMPORT_DIR / "interbank_bond_trading_days_2006-03-08_to_2026-03-08_enriched.csv",
    REPO_ROOT / "interbank_bond_trading_days_2006-03-08_to_2026-03-08_enriched.csv",
]

TRADING_DAYS_SOURCE_CANDIDATES = [
    IMPORT_DIR / "interbank_bond_trading_days_2006-03-08_to_2026-03-08_enriched.csv",
    IMPORT_DIR / "trading_days.csv",
    REPO_ROOT / "interbank_bond_trading_days_2006-03-08_to_2026-03-08_enriched.csv",
]

# 依据中国政府网《国务院办公厅关于2026年部分节假日安排的通知》
# https://www.gov.cn/zhengce/content/202511/content_7047090.htm
HOLIDAY_DATES_2026 = {
    "2026-01-01",
    "2026-01-02",
    "2026-01-03",
    "2026-02-15",
    "2026-02-16",
    "2026-02-17",
    "2026-02-18",
    "2026-02-19",
    "2026-02-20",
    "2026-02-21",
    "2026-02-22",
    "2026-02-23",
    "2026-04-04",
    "2026-04-05",
    "2026-04-06",
    "2026-05-01",
    "2026-05-02",
    "2026-05-03",
    "2026-05-04",
    "2026-05-05",
    "2026-06-19",
    "2026-06-20",
    "2026-06-21",
    "2026-09-25",
    "2026-09-26",
    "2026-09-27",
    "2026-10-01",
    "2026-10-02",
    "2026-10-03",
    "2026-10-04",
    "2026-10-05",
    "2026-10-06",
    "2026-10-07",
}

WORKDAY_WEEKEND_DATES_2026 = {
    "2026-01-04",
    "2026-02-14",
    "2026-02-28",
    "2026-05-09",
    "2026-09-20",
    "2026-10-10",
}


@dataclass(frozen=True)
class TradingDayWindow:
    csv_path: Path | None
    dates: list[str]
    coverage_start: str | None = None
    coverage_end: str | None = None
    fallback_dates: list[str] | None = None


@dataclass(frozen=True)
class TradingCalendarSyncResult:
    target_path: Path
    source_path: Path | None
    coverage_start: str | None
    coverage_end: str | None
    row_count: int
    changed: bool
    created: bool
    dry_run: bool = False


def _parse_ymd(value: str) -> datetime.date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _iter_natural_dates(start_date: str, end_date: str, *, skip_weekends: bool = False) -> list[str]:
    start = _parse_ymd(start_date)
    end = _parse_ymd(end_date)
    if start > end:
        return []
    out: list[str] = []
    for offset in range((end - start).days + 1):
        current = start + timedelta(days=offset)
        if skip_weekends and current.weekday() >= 5:
            continue
        out.append(current.isoformat())
    return out


def _enriched_row_for_date(date_text: str) -> dict[str, str]:
    current = _parse_ymd(date_text)
    weekday = current.strftime("%A")
    weekday_cn = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][current.weekday()]
    return {
        "date": date_text,
        "year": str(current.year),
        "month": str(current.month),
        "day": str(current.day),
        "weekday": weekday,
        "weekday_cn": weekday_cn,
    }


def _is_trading_day_2026(date_text: str) -> bool:
    if date_text in WORKDAY_WEEKEND_DATES_2026:
        return True
    if date_text in HOLIDAY_DATES_2026:
        return False
    return _parse_ymd(date_text).weekday() < 5


def _read_trading_days_rows(csv_path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        if "date" not in fieldnames:
            raise ValueError(f"交易日文件缺少 date 列: {csv_path}")
        for row in reader:
            normalized = {str(key): str(value or "").strip() for key, value in row.items() if key}
            date_text = normalized.get("date", "")
            if not date_text:
                continue
            _parse_ymd(date_text)
            rows.append(normalized)
    rows.sort(key=lambda item: item["date"])
    deduped: dict[str, dict[str, str]] = {}
    for row in rows:
        deduped[row["date"]] = row
    return [deduped[key] for key in sorted(deduped)]


def _extend_rows_to_2026_end(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """用官方 2026 节假日安排把交易日文件补到 2026-12-31。"""

    if not rows:
        return rows
    if rows[-1]["date"] >= "2026-12-31":
        return rows

    existing_dates = {row["date"] for row in rows}
    start_date = max("2026-01-01", (_parse_ymd(rows[-1]["date"]) + timedelta(days=1)).isoformat())
    current = _parse_ymd(start_date)
    end = _parse_ymd("2026-12-31")
    additions: list[dict[str, str]] = []
    while current <= end:
        date_text = current.isoformat()
        if date_text not in existing_dates and _is_trading_day_2026(date_text):
            additions.append(_enriched_row_for_date(date_text))
        current += timedelta(days=1)

    merged = rows + additions
    deduped = {row["date"]: row for row in merged}
    return [deduped[key] for key in sorted(deduped)]


def find_default_trading_days_csv() -> Path | None:
    env_path = getenv_first(*ENV_TRADING_DAYS_CSV_KEYS)
    if env_path:
        candidate = Path(env_path).expanduser().resolve()
        if candidate.exists():
            return candidate
    for candidate in TRADING_DAYS_CANDIDATES:
        if candidate.exists():
            return candidate.resolve()
    return None


def find_trading_days_source_csv() -> Path | None:
    env_path = getenv_first(*ENV_TRADING_DAYS_SOURCE_CSV_KEYS)
    if env_path:
        candidate = Path(env_path).expanduser().resolve()
        if candidate.exists():
            return candidate
    for candidate in TRADING_DAYS_SOURCE_CANDIDATES:
        if candidate.exists():
            return candidate.resolve()
    return None


def sync_trading_days_csv(
    *,
    source_path: Path | None = None,
    target_path: Path | None = None,
    dry_run: bool = False,
) -> TradingCalendarSyncResult:
    """把外部交易日文件同步到项目内标准位置，并补齐到 2026 年底。"""

    ensure_runtime_dirs()
    resolved_target = (target_path or DEFAULT_TRADING_DAYS_CSV).expanduser().resolve()
    resolved_source = source_path.resolve() if source_path else find_trading_days_source_csv()

    if resolved_source is None or not resolved_source.exists():
        raise ValueError("未找到交易日更新源文件，请传 --source-csv 或配置 TRADING_DAYS_SOURCE_CSV。")

    rows = _read_trading_days_rows(resolved_source)
    rows = _extend_rows_to_2026_end(rows)
    if not rows:
        raise ValueError(f"交易日源文件为空: {resolved_source}")

    fieldnames = list(rows[0].keys())
    existing_rows = _read_trading_days_rows(resolved_target) if resolved_target.exists() else []
    changed = existing_rows != rows
    created = not resolved_target.exists()

    if changed and not dry_run:
        resolved_target.parent.mkdir(parents=True, exist_ok=True)
        with resolved_target.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    return TradingCalendarSyncResult(
        target_path=resolved_target,
        source_path=resolved_source,
        coverage_start=rows[0]["date"],
        coverage_end=rows[-1]["date"],
        row_count=len(rows),
        changed=changed,
        created=created,
        dry_run=dry_run,
    )


def load_trading_day_window(
    *,
    start_date: str,
    end_date: str,
    csv_path: Path | None = None,
    skip_weekends_on_fallback: bool = True,
) -> TradingDayWindow:
    """读取交易日窗口。

    规则：
    - 优先使用交易日 CSV 覆盖的日期
    - 若请求区间超出 CSV 覆盖上限，则尾部回退到自然日
    """

    resolved_csv = csv_path.resolve() if csv_path else find_default_trading_days_csv()
    if resolved_csv is None or not resolved_csv.exists():
        fallback_dates = _iter_natural_dates(start_date, end_date, skip_weekends=skip_weekends_on_fallback)
        return TradingDayWindow(csv_path=None, dates=fallback_dates, fallback_dates=fallback_dates)

    loaded_dates = [row["date"] for row in _read_trading_days_rows(resolved_csv)]
    if not loaded_dates:
        fallback_dates = _iter_natural_dates(start_date, end_date, skip_weekends=skip_weekends_on_fallback)
        return TradingDayWindow(csv_path=resolved_csv, dates=fallback_dates, fallback_dates=fallback_dates)

    coverage_start = loaded_dates[0]
    coverage_end = loaded_dates[-1]
    selected = [date_text for date_text in loaded_dates if start_date <= date_text <= min(end_date, coverage_end)]

    fallback_dates: list[str] = []
    if end_date > coverage_end:
        tail_start = (_parse_ymd(coverage_end) + timedelta(days=1)).isoformat()
        fallback_dates = _iter_natural_dates(tail_start, end_date, skip_weekends=skip_weekends_on_fallback)

    merged_dates = sorted(set(selected + fallback_dates))
    return TradingDayWindow(
        csv_path=resolved_csv,
        dates=merged_dates,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        fallback_dates=fallback_dates,
    )


def is_trading_day(date_text: str, *, csv_path: Path | None = None) -> bool:
    window = load_trading_day_window(start_date=date_text, end_date=date_text, csv_path=csv_path, skip_weekends_on_fallback=False)
    return date_text in window.dates
