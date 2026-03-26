from __future__ import annotations

from datetime import date as date_cls
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable

from src.core.runtime import WriteStats
from src.core.trading_calendar import is_trading_day
from src.core.utils import today_ymd

from .common import get_store, run_fetch_transform_job, run_fetch_transform_many_job, run_incremental_job, run_upsert_job


# 说明：
# jobs 层只负责“编排”。
# - source: 抓取并解析
# - store: 负责表结构映射与 upsert
# - job: 只定义本次任务抓什么、怎么写


def _to_ymd(value: str | date_cls | datetime | None = None) -> str:
    """把输入统一转成 YYYY-MM-DD。"""

    if value is None:
        return date_cls.today().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date_cls):
        return value.isoformat()
    return str(value)


def _iter_recent_dates(start_date: str | date_cls | datetime | None = None, *, lookback_days: int = 7) -> list[str]:
    """生成从近到远的候选日期列表，用于回退寻找最近可用收益率曲线。"""

    current = datetime.strptime(_to_ymd(start_date), "%Y-%m-%d").date()
    return [(current - timedelta(days=offset)).isoformat() for offset in range(max(lookback_days, 1))]

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


def fetch_latest_money_market(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
):
    """抓取 ChinaMoney 最新资金面快照并增量写入 SQLite。"""

    from src.sources.chinamoney import ChinaMoneySource
    from src.stores.money_market import MoneyMarketStore

    store = MoneyMarketStore(db_path=db_path)
    source = ChinaMoneySource()
    return run_fetch_transform_job(
        store=store,
        fetch=source.fetch_money_market,
        row_builder=store.build_row_from_fetch_result,
        job_name="daily_raw_money_market",
        source_type="chinamoney_prr_md",
        dry_run=dry_run,
        incremental=True,
        inclusive=True,
    )


def fetch_bond_curve_for_date(
    date: str,
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
):
    """抓取指定日期的中债收益率曲线并按 date+curve 增量写入。"""

    from src.sources.chinabond import ChinaBondSource
    from src.stores.bond_curves import BondCurveStore

    store = BondCurveStore(db_path=db_path)
    source = ChinaBondSource()
    return run_fetch_transform_many_job(
        store=store,
        fetch=lambda: source.fetch_daily_wide_result(date),
        rows_builder=store.build_rows_from_fetch_result,
        job_name="daily_raw_bond_curve",
        source_type="chinabond_yc_detail",
        dry_run=dry_run,
        incremental=True,
        inclusive=True,
    )


def fetch_latest_bond_curve(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    start_date: str | None = None,
    lookback_days: int = 7,
):
    """抓取最近一个可用日期的中债收益率曲线。

    业务背景：
    - 中债曲线通常不是白天实时可得
    - 默认模式下若当天为空，向前回退最近可用日更符合实际使用方式
    - 若调用方明确传入 date，则仍应走严格按日抓取的入口
    """

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


def fetch_latest_overseas_macro(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
):
    """抓取 FRED + Alpha Vantage，并合并写入海外宏观原始表。"""

    from src.sources.external_misc import ExternalMiscSource
    from src.sources.fred import FredSource
    from src.stores.overseas import OverseasStore

    store = OverseasStore(db_path=db_path)
    fred_source = FredSource()
    alpha_source = ExternalMiscSource()

    fred_result = fred_source.fetch_overseas_macro_result()
    alpha_result = alpha_source.fetch_overseas_macro_from_alpha_vantage_result()
    row = store.build_row_from_fetch_results(fred_result, alpha_result)

    return run_upsert_job(
        store=store,
        rows=[row],
        job_name="daily_raw_overseas_macro",
        source_type="fred_alpha_vantage",
        dry_run=dry_run,
    )


def fetch_recent_policy_rate_events(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    limit: int = 20,
):
    """抓取近期央行政策事件并写入原始政策利率表。

    当前包含：
    - OMO
    - MLF
    - LPR
    """

    from src.sources.pbc import PbcSource
    from src.stores.policy_rates import PolicyRateStore

    store = PolicyRateStore(db_path=db_path)
    source = PbcSource()
    return run_fetch_transform_many_job(
        store=store,
        fetch=lambda: source.fetch_recent_policy_rate_events_result(limit=limit),
        rows_builder=store.build_rows_from_fetch_result,
        job_name="daily_raw_policy_rate",
        source_type="pbc_policy_rate",
        dry_run=dry_run,
        incremental=True,
        inclusive=True,
    )


def fetch_latest_policy_rate(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
):
    """抓取最新一组政策事件并写入原始政策利率表。"""

    from src.sources.pbc import PbcSource
    from src.stores.policy_rates import PolicyRateStore

    store = PolicyRateStore(db_path=db_path)
    source = PbcSource()
    return run_fetch_transform_many_job(
        store=store,
        fetch=source.fetch_latest_policy_rate_events_result,
        rows_builder=store.build_rows_from_fetch_result,
        job_name="daily_raw_policy_rate_latest",
        source_type="pbc_policy_rate_latest",
        dry_run=dry_run,
        incremental=True,
        inclusive=True,
    )


def fetch_latest_futures(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
):
    """抓取最新国债期货快照并写入原始期货表。"""

    from src.sources.external_misc import ExternalMiscSource
    from src.stores.futures import FuturesStore

    store = FuturesStore(db_path=db_path)
    source = ExternalMiscSource()
    return run_fetch_transform_job(
        store=store,
        fetch=source.fetch_bond_futures_result,
        row_builder=store.build_row_from_fetch_result,
        job_name="daily_raw_futures",
        source_type="sina_futures",
        dry_run=dry_run,
        incremental=True,
        inclusive=True,
    )


def fetch_latest_etf_snapshot(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    snapshot_date: str | None = None,
    rows_per_page: int = 500,
    max_pages: int = 20,
    min_unit_total_yi: float | int | str = 2,
    min_volume_wan: float | int | str = "",
):
    """抓取最新指数 ETF 快照并写入原始 ETF 表。

    策略：
    - 仅交易日抓取
    - 同一 snapshot_date 若已抓过则跳过
    - 按 snapshot_date + fund_id 累积历史快照
    """

    from src.sources.jisilu import JisiluSource
    from src.stores.etf import EtfStore

    store = EtfStore(db_path=db_path)
    source = JisiluSource()
    target_snapshot_date = snapshot_date or today_ymd()

    if not is_trading_day(target_snapshot_date):
        return WriteStats(skipped=1)

    if store.count_rows(snapshot_date=target_snapshot_date) > 0:
        return WriteStats(skipped=1)

    return run_fetch_transform_many_job(
        store=store,
        fetch=lambda: source.fetch_etf_index_all_result(
            snapshot_date=target_snapshot_date,
            rows_per_page=rows_per_page,
            max_pages=max_pages,
            min_unit_total_yi=min_unit_total_yi,
            min_volume_wan=min_volume_wan,
        ),
        rows_builder=store.build_rows_from_fetch_result,
        job_name="daily_raw_jisilu_etf",
        source_type="jisilu_etf",
        dry_run=dry_run,
        incremental=False,
    )


def fetch_latest_life_asset(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
):
    """抓取民生与资产价格快照。

    当前先统一占位链路，保证主流程可安全落表。
    """

    from src.sources.external_misc import ExternalMiscSource
    from src.sources.stats_gov import StatsGovSource
    from src.stores.life_asset import LifeAssetStore

    store = LifeAssetStore(db_path=db_path)
    stats_source = StatsGovSource()
    misc_source = ExternalMiscSource()
    row = store.build_row_from_fetch_results(
        stats_source.fetch_placeholder_house_price_result(),
        misc_source.fetch_sge_gold_result(),
        misc_source.fetch_boc_deposit_1y_result(),
        misc_source.fetch_money_fund_7d_result(),
    )
    return run_upsert_job(
        store=store,
        rows=[row],
        job_name="daily_raw_life_asset",
        source_type="life_asset_placeholder",
        dry_run=dry_run,
    )


def fetch_bond_index_duration(
    index_id: str,
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    index_name: str | None = None,
    index_code: str | None = None,
):
    """抓取单个中债指数特征并写入原始债券指数表。"""

    from src.sources.chinabond_index import ChinaBondIndexSource
    from src.stores.bond_index import BondIndexStore

    store = BondIndexStore(db_path=db_path)
    source = ChinaBondIndexSource()
    return run_fetch_transform_job(
        store=store,
        fetch=lambda: source.fetch_duration_snapshot_result(index_id, index_name=index_name, index_code=index_code),
        row_builder=store.build_row_from_fetch_result,
        job_name="daily_raw_bond_index",
        source_type="chinabond_index_single",
        dry_run=dry_run,
        incremental=True,
        inclusive=True,
    )
