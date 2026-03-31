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

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.chinamoney import ChinaMoneySource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("money_market")
    if spec.build_row is None:
        raise RuntimeError("raw dataset money_market 缺少 build_row")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = ChinaMoneySource()
    return run_fetch_transform_job(
        store=store,
        fetch=source.fetch_money_market,
        row_builder=spec.build_row,
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

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.chinabond import ChinaBondSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("chinabond_curve")
    if spec.build_rows is None:
        raise RuntimeError("raw dataset chinabond_curve 缺少 build_rows")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = ChinaBondSource()
    return run_fetch_transform_many_job(
        store=store,
        fetch=lambda: source.fetch_daily_wide_result(date),
        rows_builder=spec.build_rows,
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


def fetch_etf_detail_history(
    fund_id: str,
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    rows_per_page: int = 50,
    max_pages: int = 2,
):
    """抓取单只 ETF 的 detail_hists 历史明细，并补到原始 ETF 表。

    这个入口适合回补近 50 天左右的历史数据。字段缺失时会留空，
    但会尽量保留 `snapshot_date / fund_id / price / volume / amount / unit_total`
    这些核心字段。
    """

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.datasets.raw_schema import build_jisilu_etf_history_row
    from src.sources.jisilu import JisiluEtfSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("jisilu_etf")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = JisiluEtfSource()
    fetch_result = source.fetch_etf_detail_history_result(
        fund_id=fund_id,
        rows_per_page=rows_per_page,
        max_pages=max_pages,
    )

    rows: list[dict[str, object]] = []
    for page_payload in fetch_result.payload.get("pages", []):
        for row in page_payload.get("rows", []) or []:
            cell = row.get("cell", row) if isinstance(row, dict) else {}
            snapshot_date = source._extract_history_snapshot_date(row)
            if not snapshot_date:
                continue
            rows.append(
                build_jisilu_etf_history_row(
                    snapshot_date=snapshot_date,
                    fetched_at=str(fetch_result.meta.get("fetched_at") or ""),
                    fund_id=fund_id,
                    cell=dict(cell),
                    source_url=fetch_result.source_url,
                )
            )

    if not rows:
        raise ValueError(f"ETF detail history returned no rows for fund_id={fund_id}")

    return run_upsert_job(
        store=store,
        rows=rows,
        job_name="daily_raw_jisilu_etf_detail_history",
        source_type="jisilu_etf_detail_hists",
        dry_run=dry_run,
    )


def fetch_latest_fred(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
):
    """抓取 FRED，并写入原始 FRED 表。"""

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.fred import FredSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("fred")
    if spec.build_row is None:
        raise RuntimeError("raw dataset fred 缺少 build_row")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = FredSource()
    return run_fetch_transform_job(
        store=store,
        fetch=source.fetch_fred_result,
        row_builder=spec.build_row,
        job_name="daily_raw_fred",
        source_type="fred",
        dry_run=dry_run,
        incremental=True,
        inclusive=True,
    )


def fetch_latest_alpha_vantage(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
):
    """抓取 Alpha Vantage，并写入原始 Alpha Vantage 表。"""

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.alpha_vantage import AlphaVantageSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("alpha_vantage")
    if spec.build_row is None:
        raise RuntimeError("raw dataset alpha_vantage 缺少 build_row")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = AlphaVantageSource()
    return run_fetch_transform_job(
        store=store,
        fetch=source.fetch_alpha_vantage_result,
        row_builder=spec.build_row,
        job_name="daily_raw_alpha_vantage",
        source_type="alpha_vantage",
        dry_run=dry_run,
        incremental=True,
        inclusive=True,
    )


def fetch_akshare_bond_gb_us_sina(
    *,
    symbol: str | None = None,
    dry_run: bool = False,
    db_path: Path | None = None,
    incremental: bool = True,
):
    """抓取 akshare 封装的新浪美国国债收益率历史行情。"""

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.akshare import AKSHARE_BOND_GB_US_SINA_SYMBOLS, AkshareBondGbUsSinaSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("akshare_bond_gb_us_sina")
    if spec.build_rows is None:
        raise RuntimeError("raw dataset akshare_bond_gb_us_sina 缺少 build_rows")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = AkshareBondGbUsSinaSource()
    return run_fetch_transform_many_job(
        store=store,
        fetch=(
            (lambda: source.fetch_history_result(symbol=symbol))
            if symbol
            else (lambda: source.fetch_histories_result(symbols=AKSHARE_BOND_GB_US_SINA_SYMBOLS))
        ),
        rows_builder=spec.build_rows,
        job_name="daily_raw_akshare_bond_gb_us_sina",
        source_type="akshare_bond_gb_us_sina",
        dry_run=dry_run,
        incremental=incremental,
        inclusive=True,
        **({"symbol": symbol} if symbol else {}),
    )


def fetch_akshare_bond_zh_us_rate(
    *,
    start_date: str = "19901219",
    dry_run: bool = False,
    db_path: Path | None = None,
    incremental: bool = True,
):
    """抓取 akshare 的中美国债收益率与 GDP 年增率对比历史序列。"""

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.akshare import AkshareBondZhUsRateSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("akshare_bond_zh_us_rate")
    if spec.build_rows is None:
        raise RuntimeError("raw dataset akshare_bond_zh_us_rate 缺少 build_rows")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = AkshareBondZhUsRateSource()
    return run_fetch_transform_many_job(
        store=store,
        fetch=lambda: source.fetch_history_result(start_date=start_date),
        rows_builder=spec.build_rows,
        job_name="daily_raw_akshare_bond_zh_us_rate",
        source_type="akshare_bond_zh_us_rate",
        dry_run=dry_run,
        incremental=incremental,
        inclusive=True,
    )


def fetch_recent_policy_rate_events(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    limit: int = 20,
):
    """抓取近期央行政策事件并写入原始政策利率表。"""

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.pbc import PbcSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("policy_rate")
    if spec.build_rows is None:
        raise RuntimeError("raw dataset policy_rate 缺少 build_rows")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = PbcSource()
    return run_fetch_transform_many_job(
        store=store,
        fetch=lambda: source.fetch_recent_policy_rate_events_result(limit=limit),
        rows_builder=spec.build_rows,
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

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.pbc import PbcSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("policy_rate")
    if spec.build_rows is None:
        raise RuntimeError("raw dataset policy_rate 缺少 build_rows")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = PbcSource()
    return run_fetch_transform_many_job(
        store=store,
        fetch=source.fetch_latest_policy_rate_events_result,
        rows_builder=spec.build_rows,
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

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.sina_futures import SinaFuturesSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("futures")
    if spec.build_row is None:
        raise RuntimeError("raw dataset futures 缺少 build_row")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = SinaFuturesSource()
    return run_fetch_transform_job(
        store=store,
        fetch=source.fetch_bond_futures_result,
        row_builder=spec.build_row,
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
    min_unit_total_yi: float | int | str = "",
    min_volume_wan: float | int | str = "",
    force: bool = False,
):
    """抓取最新指数 ETF 快照并写入原始 ETF 表。"""

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.jisilu import JisiluEtfSource

    spec = get_raw_dataset_spec("jisilu_etf")
    if spec.build_rows is None:
        raise RuntimeError("raw dataset jisilu_etf 缺少 build_rows")
    store = get_store("raw_jisilu_etf", db_path=db_path)
    source = JisiluEtfSource()
    target_snapshot_date = snapshot_date or today_ymd()

    if not is_trading_day(target_snapshot_date):
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
        rows_builder=spec.build_rows,
        job_name="daily_raw_jisilu_etf",
        source_type="jisilu_etf",
        dry_run=dry_run,
        incremental=False,
    )


def fetch_latest_jisilu_gold_snapshot(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    snapshot_date: str | None = None,
    rows_per_page: int = 25,
    max_pages: int = 20,
    min_unit_total_yi: float | int | str = "",
    min_volume_wan: float | int | str = "",
    force: bool = False,
):
    """抓取最新集思录黄金列表快照并写入原始黄金表。"""

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.jisilu import JisiluGoldEtfSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("jisilu_gold_etf")
    if spec.build_rows is None:
        raise RuntimeError("raw dataset jisilu_gold_etf 缺少 build_rows")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = JisiluGoldEtfSource()
    target_snapshot_date = snapshot_date or today_ymd()

    if not is_trading_day(target_snapshot_date):
        return WriteStats(skipped=1)

    return run_fetch_transform_many_job(
        store=store,
        fetch=lambda: source.fetch_gold_result(
            snapshot_date=target_snapshot_date,
            rows_per_page=rows_per_page,
            max_pages=max_pages,
            min_unit_total_yi=min_unit_total_yi,
            min_volume_wan=min_volume_wan,
        ),
        rows_builder=spec.build_rows,
        job_name="daily_raw_jisilu_gold",
        source_type="jisilu_gold",
        dry_run=dry_run,
        incremental=False,
    )


def fetch_latest_jisilu_money_snapshot(
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    snapshot_date: str | None = None,
    rows_per_page: int = 25,
    max_pages: int = 20,
    min_unit_total_yi: float | int | str = "",
    min_volume_wan: float | int | str = "",
    force: bool = False,
):
    """抓取最新集思录货币列表快照并写入原始货币表。"""

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.jisilu import JisiluMoneyEtfSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("jisilu_money_etf")
    if spec.build_rows is None:
        raise RuntimeError("raw dataset jisilu_money_etf 缺少 build_rows")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = JisiluMoneyEtfSource()
    target_snapshot_date = snapshot_date or today_ymd()

    if not is_trading_day(target_snapshot_date):
        return WriteStats(skipped=1)

    return run_fetch_transform_many_job(
        store=store,
        fetch=lambda: source.fetch_money_result(
            snapshot_date=target_snapshot_date,
            rows_per_page=rows_per_page,
            max_pages=max_pages,
            min_unit_total_yi=min_unit_total_yi,
            min_volume_wan=min_volume_wan,
        ),
        rows_builder=spec.build_rows,
        job_name="daily_raw_jisilu_money",
        source_type="jisilu_money",
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
    force: bool = False,
):
    """抓取最新 QDII 分市场快照并写入原始 QDII 表。"""

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.jisilu import JisiluQdiiEtfSource

    spec = get_raw_dataset_spec("jisilu_qdii")
    if spec.build_rows is None:
        raise RuntimeError("raw dataset jisilu_qdii 缺少 build_rows")
    store = get_store("raw_jisilu_qdii", db_path=db_path)
    source = JisiluQdiiEtfSource()
    target_snapshot_date = snapshot_date or today_ymd()

    if not is_trading_day(target_snapshot_date):
        return WriteStats(skipped=1)

    markets = ("europe_america", "commodity", "asia")
    stats_total = WriteStats()
    for market in markets:
        stats = run_fetch_transform_many_job(
            store=store,
            fetch=lambda market=market: source.fetch_qdii_all_result(
                market=market,
                snapshot_date=target_snapshot_date,
                rows_per_page=rows_per_page,
                max_pages=max_pages,
            ),
            rows_builder=spec.build_rows,
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
    force: bool = False,
):
    """抓取最新国债现券全量表格并写入原始国债表。"""

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.jisilu import JisiluTreasurySource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("jisilu_treasury")
    if spec.build_rows is None:
        raise RuntimeError("raw dataset jisilu_treasury 缺少 build_rows")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = JisiluTreasurySource()
    target_snapshot_date = snapshot_date or today_ymd()

    if not is_trading_day(target_snapshot_date):
        return WriteStats(skipped=1)

    return run_fetch_transform_many_job(
        store=store,
        fetch=lambda: source.fetch_treasury_result(snapshot_date=target_snapshot_date),
        rows_builder=spec.build_rows,
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

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.sse import SseLivelyBondSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("sse_lively_bond")
    if spec.build_rows is None:
        raise RuntimeError("raw dataset sse_lively_bond 缺少 build_rows")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = SseLivelyBondSource()
    return run_fetch_transform_many_job(
        store=store,
        fetch=lambda: source.fetch_lively_bond_all_result(page_size=page_size, max_pages=max_pages),
        rows_builder=spec.build_rows,
        job_name="daily_raw_sse_lively_bond",
        source_type="sse_lively_bond",
        dry_run=dry_run,
        incremental=True,
        inclusive=True,
    )


def fetch_chinabond_bond_index(
    index_id: str,
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    index_name: str | None = None,
    index_code: str | None = None,
):
    """抓取单个中债指数特征并写入原始债券指数表。"""

    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.chinabond import ChinaBondIndexSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("chinabond_bond_index")
    if spec.build_row is None:
        raise RuntimeError("raw dataset chinabond_bond_index 缺少 build_row")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = ChinaBondIndexSource()
    return run_fetch_transform_job(
        store=store,
        fetch=lambda: source.fetch_duration_snapshot_result(index_id, index_name=index_name, index_code=index_code),
        row_builder=spec.build_row,
        job_name="daily_raw_chinabond_bond_index",
        source_type="chinabond_index_single",
        dry_run=dry_run,
        incremental=True,
        inclusive=True,
    )


def fetch_csindex_bond_index(
    index_id: str,
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    index_name: str | None = None,
    index_code: str | None = None,
):
    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.csindex import CsindexBondSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("csindex_bond_index")
    if spec.build_row is None:
        raise RuntimeError("raw dataset csindex_bond_index 缺少 build_row")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = CsindexBondSource()
    return run_fetch_transform_job(
        store=store,
        fetch=lambda: source.fetch_feature_snapshot_result(index_id, index_name=index_name, index_code=index_code),
        row_builder=spec.build_row,
        job_name="daily_raw_csindex_bond_index",
        source_type="csindex_bond_feature",
        dry_run=dry_run,
        incremental=True,
        inclusive=True,
    )


def fetch_cnindex_bond_index(
    index_id: str,
    *,
    dry_run: bool = False,
    db_path: Path | None = None,
    index_name: str | None = None,
    index_code: str | None = None,
):
    from src.datasets.raw_registry import get_raw_dataset_spec
    from src.sources.cnindex import CnindexBondSource
    from src.stores.raw import RawStore

    spec = get_raw_dataset_spec("cnindex_bond_index")
    if spec.build_row is None:
        raise RuntimeError("raw dataset cnindex_bond_index 缺少 build_row")
    store = RawStore(spec.table_spec, db_path=db_path)
    source = CnindexBondSource()
    return run_fetch_transform_job(
        store=store,
        fetch=lambda: source.fetch_feature_snapshot_result(index_id, index_name=index_name, index_code=index_code),
        row_builder=spec.build_row,
        job_name="daily_raw_cnindex_bond_index",
        source_type="cnindex_bond_feature",
        dry_run=dry_run,
        incremental=True,
        inclusive=True,
    )
