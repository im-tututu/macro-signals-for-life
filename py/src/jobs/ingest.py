from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable

from src.core.runtime import WriteStats
from src.core.trading_calendar import is_trading_day
from src.core.utils import today_ymd
from src.datasets.registry import get_dataset_spec

from .common import get_store, run_fetch_transform_job, run_fetch_transform_many_job, run_incremental_job, run_upsert_job
from .latest import fetch_latest_chinabond_curve


# 说明：
# - 本文件承载“原始数据抓取并写入 raw 表”的 job 实现
# - 与 `latest.py` / `backfill.py` 的区别是：
#   - `latest.py` 放语义很明确、值得单独抽出的 latest jobs
#   - `backfill.py` 放历史回填 jobs
#   - `ingest.py` 放其余原始层 ingest jobs，以及尚未完全拆分的兼容入口
# - 当前 `chinabond_curve` 的 latest / backfill 已开始迁出，
#   但单日抓取 helper 与多数普通 raw jobs 仍保留在这里


def _get_chinabond_curve_spec():
    """读取 `chinabond_curve` 的数据集定义。"""

    return get_dataset_spec("chinabond_curve")


def _require_chinabond_curve_backfill_spec() -> None:
    """校验中债曲线历史抓取语义仍与当前实现一致。

    注意：
    - `fetch_bond_curve_for_date` 本身只是“按指定日期抓取一次”的底层 helper
    - 真正的历史回填会在 `run_backfill.py` / `jobs/backfill.py` 中按交易日序列反复调用它
    """

    spec = _get_chinabond_curve_spec()
    if not spec.supports_backfill:
        raise RuntimeError("dataset chinabond_curve does not support backfill")
    if spec.backfill_mode != "trading_day_range":
        raise RuntimeError(f"unexpected backfill mode for chinabond_curve: {spec.backfill_mode}")


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

    _require_chinabond_curve_backfill_spec()

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
    """兼容入口：实际 latest 实现已迁到 `jobs/latest.py`。"""

    return fetch_latest_chinabond_curve(
        dry_run=dry_run,
        db_path=db_path,
        start_date=start_date,
        lookback_days=lookback_days,
    )


def fetch_latest_overseas_macro(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
):
    """抓取 FRED + Alpha Vantage，并合并写入海外宏观原始表。"""

    from src.sources.alpha_vantage import AlphaVantageSource
    from src.sources.fred import FredSource
    from src.stores.overseas import OverseasStore

    store = OverseasStore(db_path=db_path)
    fred_source = FredSource()
    alpha_source = AlphaVantageSource()

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
    """抓取近期央行政策事件并写入原始政策利率表。"""

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

    from src.sources.sina_futures import SinaFuturesSource
    from src.stores.futures import FuturesStore

    store = FuturesStore(db_path=db_path)
    source = SinaFuturesSource()
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
    """抓取最新指数 ETF 快照并写入原始 ETF 表。"""

    from src.sources.jisilu import JisiluEtfSource
    from src.stores.etf import EtfStore

    store = EtfStore(db_path=db_path)
    source = JisiluEtfSource()
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


def fetch_latest_qdii_snapshot(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    snapshot_date: str | None = None,
    rows_per_page: int = 50,
    max_pages: int = 20,
):
    """抓取最新 QDII 分市场快照并写入原始 QDII 表。"""

    from src.sources.jisilu import JisiluQdiiSource
    from src.stores.qdii import QdiiStore

    store = QdiiStore(db_path=db_path)
    source = JisiluQdiiSource()
    target_snapshot_date = snapshot_date or today_ymd()

    if not is_trading_day(target_snapshot_date):
        return WriteStats(skipped=1)

    markets = ("europe_america", "commodity", "asia")
    stats_total = WriteStats()
    for market in markets:
        if store.count_rows(snapshot_date=target_snapshot_date, market=market) > 0:
            stats_total.skipped += 1
            continue
        stats = run_fetch_transform_many_job(
            store=store,
            fetch=lambda market=market: source.fetch_qdii_all_result(
                market=market,
                snapshot_date=target_snapshot_date,
                rows_per_page=rows_per_page,
                max_pages=max_pages,
            ),
            rows_builder=store.build_rows_from_fetch_result,
            job_name=f"daily_raw_jisilu_qdii_{market}",
            source_type=f"jisilu_qdii:{market}",
            dry_run=dry_run,
            incremental=False,
        )
        stats_total.merge(stats)
    return stats_total


def fetch_latest_treasury_snapshot(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    snapshot_date: str | None = None,
):
    """抓取最新国债现券全量表格并写入原始国债表。"""

    from src.sources.jisilu import JisiluTreasurySource
    from src.stores.treasury import TreasuryStore

    store = TreasuryStore(db_path=db_path)
    source = JisiluTreasurySource()
    target_snapshot_date = snapshot_date or today_ymd()

    if not is_trading_day(target_snapshot_date):
        return WriteStats(skipped=1)

    if store.count_rows(snapshot_date=target_snapshot_date) > 0:
        return WriteStats(skipped=1)

    return run_fetch_transform_many_job(
        store=store,
        fetch=lambda: source.fetch_treasury_result(snapshot_date=target_snapshot_date),
        rows_builder=store.build_rows_from_fetch_result,
        job_name="daily_raw_jisilu_treasury",
        source_type="jisilu_treasury",
        dry_run=dry_run,
        incremental=False,
    )


def fetch_latest_sse_lively_bond_snapshot(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    page_size: int = 100,
    max_pages: int = 20,
):
    """抓取上交所活跃国债榜单并写入原始表。"""

    from src.sources.sse import SseLivelyBondSource
    from src.stores.sse_lively_bond import SseLivelyBondStore

    store = SseLivelyBondStore(db_path=db_path)
    source = SseLivelyBondSource()
    return run_fetch_transform_many_job(
        store=store,
        fetch=lambda: source.fetch_lively_bond_all_result(page_size=page_size, max_pages=max_pages),
        rows_builder=store.build_rows_from_fetch_result,
        job_name="daily_raw_sse_lively_bond",
        source_type="sse_lively_bond",
        dry_run=dry_run,
        incremental=True,
        inclusive=True,
    )


def fetch_latest_life_asset(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
):
    """抓取民生与资产价格快照。"""

    from src.sources.boc import BocSource
    from src.sources.eastmoney import EastMoneySource
    from src.sources.sge import SgeSource
    from src.sources.stats_gov import StatsGovSource
    from src.stores.life_asset import LifeAssetStore

    store = LifeAssetStore(db_path=db_path)
    stats_source = StatsGovSource()
    sge_source = SgeSource()
    boc_source = BocSource()
    eastmoney_source = EastMoneySource()
    row = store.build_row_from_fetch_results(
        stats_source.fetch_placeholder_house_price_result(),
        sge_source.fetch_gold_result(),
        boc_source.fetch_deposit_1y_result(),
        eastmoney_source.fetch_money_fund_7d_result(),
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

    from src.sources.chinabond import ChinaBondIndexSource
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
