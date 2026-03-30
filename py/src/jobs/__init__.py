from .manual import clean_table, review_table, upsert_batch, upsert_incremental, upsert_single, upsert_window
from .rebuild import clean_all_raw_tables, review_all_raw_tables
from .registry import DAILY_JOB_REGISTRY, get_daily_job_spec


def run_daily_fetcher(*args, **kwargs):
    from .ingest import run_daily_fetcher as _run_daily_fetcher

    return _run_daily_fetcher(*args, **kwargs)


def fetch_latest_money_market(*args, **kwargs):
    from .ingest import fetch_latest_money_market as _fetch_latest_money_market

    return _fetch_latest_money_market(*args, **kwargs)


def fetch_latest_jisilu_gold_snapshot(*args, **kwargs):
    from .ingest import fetch_latest_jisilu_gold_snapshot as _fetch_latest_jisilu_gold_snapshot

    return _fetch_latest_jisilu_gold_snapshot(*args, **kwargs)


def fetch_latest_jisilu_money_snapshot(*args, **kwargs):
    from .ingest import fetch_latest_jisilu_money_snapshot as _fetch_latest_jisilu_money_snapshot

    return _fetch_latest_jisilu_money_snapshot(*args, **kwargs)


def fetch_bond_curve_for_date(*args, **kwargs):
    from .ingest import fetch_bond_curve_for_date as _fetch_bond_curve_for_date

    return _fetch_bond_curve_for_date(*args, **kwargs)


def fetch_latest_bond_curve(*args, **kwargs):
    from .ingest import fetch_latest_bond_curve as _fetch_latest_bond_curve

    return _fetch_latest_bond_curve(*args, **kwargs)


def fetch_latest_chinabond_curve(*args, **kwargs):
    from .latest import fetch_latest_chinabond_curve as _fetch_latest_chinabond_curve

    return _fetch_latest_chinabond_curve(*args, **kwargs)


def fetch_latest_fred(*args, **kwargs):
    from .ingest import fetch_latest_fred as _fetch_latest_fred

    return _fetch_latest_fred(*args, **kwargs)


def fetch_latest_alpha_vantage(*args, **kwargs):
    from .ingest import fetch_latest_alpha_vantage as _fetch_latest_alpha_vantage

    return _fetch_latest_alpha_vantage(*args, **kwargs)


def fetch_recent_policy_rate_events(*args, **kwargs):
    from .ingest import fetch_recent_policy_rate_events as _fetch_recent_policy_rate_events

    return _fetch_recent_policy_rate_events(*args, **kwargs)


def fetch_latest_policy_rate(*args, **kwargs):
    from .ingest import fetch_latest_policy_rate as _fetch_latest_policy_rate

    return _fetch_latest_policy_rate(*args, **kwargs)


def fetch_latest_futures(*args, **kwargs):
    from .ingest import fetch_latest_futures as _fetch_latest_futures

    return _fetch_latest_futures(*args, **kwargs)


def fetch_latest_etf_snapshot(*args, **kwargs):
    from .ingest import fetch_latest_etf_snapshot as _fetch_latest_etf_snapshot

    return _fetch_latest_etf_snapshot(*args, **kwargs)


def fetch_etf_detail_history(*args, **kwargs):
    from .ingest import fetch_etf_detail_history as _fetch_etf_detail_history

    return _fetch_etf_detail_history(*args, **kwargs)


def fetch_latest_qdii_snapshot(*args, **kwargs):
    from .ingest import fetch_latest_qdii_snapshot as _fetch_latest_qdii_snapshot

    return _fetch_latest_qdii_snapshot(*args, **kwargs)


def fetch_latest_treasury_snapshot(*args, **kwargs):
    from .ingest import fetch_latest_treasury_snapshot as _fetch_latest_treasury_snapshot

    return _fetch_latest_treasury_snapshot(*args, **kwargs)


def fetch_latest_sse_lively_bond_snapshot(*args, **kwargs):
    from .ingest import fetch_latest_sse_lively_bond_snapshot as _fetch_latest_sse_lively_bond_snapshot

    return _fetch_latest_sse_lively_bond_snapshot(*args, **kwargs)


def fetch_chinabond_bond_index(*args, **kwargs):
    from .ingest import fetch_chinabond_bond_index as _fetch_chinabond_bond_index

    return _fetch_chinabond_bond_index(*args, **kwargs)


def fetch_csindex_bond_index(*args, **kwargs):
    from .ingest import fetch_csindex_bond_index as _fetch_csindex_bond_index

    return _fetch_csindex_bond_index(*args, **kwargs)


def fetch_cnindex_bond_index(*args, **kwargs):
    from .ingest import fetch_cnindex_bond_index as _fetch_cnindex_bond_index

    return _fetch_cnindex_bond_index(*args, **kwargs)

__all__ = [
    "run_daily_fetcher",
    "fetch_latest_money_market",
    "fetch_latest_jisilu_gold_snapshot",
    "fetch_latest_jisilu_money_snapshot",
    "fetch_bond_curve_for_date",
    "fetch_latest_bond_curve",
    "fetch_latest_chinabond_curve",
    "fetch_latest_fred",
    "fetch_latest_alpha_vantage",
    "fetch_recent_policy_rate_events",
    "fetch_latest_policy_rate",
    "fetch_latest_futures",
    "fetch_latest_etf_snapshot",
    "fetch_etf_detail_history",
    "fetch_latest_qdii_snapshot",
    "fetch_latest_treasury_snapshot",
    "fetch_latest_sse_lively_bond_snapshot",
    "fetch_chinabond_bond_index",
    "fetch_csindex_bond_index",
    "fetch_cnindex_bond_index",
    "DAILY_JOB_REGISTRY",
    "get_daily_job_spec",
    "clean_table",
    "review_table",
    "upsert_batch",
    "upsert_incremental",
    "upsert_single",
    "upsert_window",
    "clean_all_raw_tables",
    "review_all_raw_tables",
]
