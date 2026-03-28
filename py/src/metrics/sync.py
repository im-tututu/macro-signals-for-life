from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.db import connect
from src.core.utils import now_text

from .engine import _mean, _percentile_rank, upsert_metric_daily_rows


@dataclass(frozen=True)
class SourceMetricSpec:
    code: str
    table: str
    key_sql: str
    value_sql: str
    scale: float = 1.0


CURVE_SOURCE_SPECS: tuple[SourceMetricSpec, ...] = (
    SourceMetricSpec("gov_1y", "raw_bond_curve", "curve = '国债'", "y_1", 0.01),
    SourceMetricSpec("gov_2y", "raw_bond_curve", "curve = '国债'", "y_2", 0.01),
    SourceMetricSpec("gov_3y", "raw_bond_curve", "curve = '国债'", "y_3", 0.01),
    SourceMetricSpec("gov_5y", "raw_bond_curve", "curve = '国债'", "y_5", 0.01),
    SourceMetricSpec("gov_10y", "raw_bond_curve", "curve = '国债'", "y_10", 0.01),
    SourceMetricSpec("gov_30y", "raw_bond_curve", "curve = '国债'", "y_30", 0.01),
    SourceMetricSpec("cdb_3y", "raw_bond_curve", "curve = '国开债'", "y_3", 0.01),
    SourceMetricSpec("cdb_5y", "raw_bond_curve", "curve = '国开债'", "y_5", 0.01),
    SourceMetricSpec("cdb_10y", "raw_bond_curve", "curve = '国开债'", "y_10", 0.01),
    SourceMetricSpec("local_gov_1y", "raw_bond_curve", "curve = '地方债'", "y_1", 0.01),
    SourceMetricSpec("local_gov_5y", "raw_bond_curve", "curve = '地方债'", "y_5", 0.01),
    SourceMetricSpec("local_gov_10y", "raw_bond_curve", "curve = '地方债'", "y_10", 0.01),
    SourceMetricSpec("aa_plus_credit_1y", "raw_bond_curve", "curve = 'AA+信用'", "y_1", 0.01),
    SourceMetricSpec("aaa_credit_1y", "raw_bond_curve", "curve = 'AAA信用'", "y_1", 0.01),
    SourceMetricSpec("aaa_credit_3y", "raw_bond_curve", "curve = 'AAA信用'", "y_3", 0.01),
    SourceMetricSpec("aaa_credit_5y", "raw_bond_curve", "curve = 'AAA信用'", "y_5", 0.01),
    SourceMetricSpec("aaa_lgfv_1y", "raw_bond_curve", "curve = 'AAA城投'", "y_1", 0.01),
    SourceMetricSpec("aaa_mtn_1y", "raw_bond_curve", "curve = 'AAA中票'", "y_1", 0.01),
    SourceMetricSpec("aaa_mtn_3y", "raw_bond_curve", "curve = 'AAA中票'", "y_3", 0.01),
    SourceMetricSpec("aaa_mtn_5y", "raw_bond_curve", "curve = 'AAA中票'", "y_5", 0.01),
    SourceMetricSpec("aaa_ncd_1y", "raw_bond_curve", "curve = 'AAA存单'", "y_1", 0.01),
    SourceMetricSpec("aaa_plus_mtn_1y", "raw_bond_curve", "curve = 'AAA+中票'", "y_1", 0.01),
    SourceMetricSpec("aaa_bank_bond_1y", "raw_bond_curve", "curve = 'AAA银行债'", "y_1", 0.01),
    SourceMetricSpec("aaa_bank_bond_3y", "raw_bond_curve", "curve = 'AAA银行债'", "y_3", 0.01),
    SourceMetricSpec("aaa_bank_bond_5y", "raw_bond_curve", "curve = 'AAA银行债'", "y_5", 0.01),
)

DIRECT_SOURCE_SPECS: tuple[SourceMetricSpec, ...] = (
    SourceMetricSpec("dr007_weighted_rate", "raw_money_market", "1 = 1", "dr007_weighted_rate", 0.01),
    SourceMetricSpec("omo_7d", "raw_policy_rate", "type = 'OMO' AND term = '7D'", "rate", 0.01),
    SourceMetricSpec("mlf_1y", "raw_policy_rate", "type = 'MLF' AND term = '1Y'", "rate", 0.01),
    SourceMetricSpec("lpr_1y", "raw_policy_rate", "type = 'LPR' AND term = '1Y'", "rate", 0.01),
    SourceMetricSpec("lpr_5y", "raw_policy_rate", "type = 'LPR' AND term = '5Y+'", "rate", 0.01),
    SourceMetricSpec("ust_2y", "raw_fred", "1 = 1", "ust_2y", 0.01),
    SourceMetricSpec("ust_10y", "raw_fred", "1 = 1", "ust_10y", 0.01),
    SourceMetricSpec("usd_broad", "raw_fred", "1 = 1", "usd_broad"),
    SourceMetricSpec("usd_cny", "raw_fred", "1 = 1", "usd_cny"),
    SourceMetricSpec("gold", "raw_alpha_vantage", "1 = 1", "gold"),
    SourceMetricSpec("wti", "raw_alpha_vantage", "1 = 1", "wti"),
    SourceMetricSpec("brent", "raw_alpha_vantage", "1 = 1", "brent"),
    SourceMetricSpec("copper", "raw_alpha_vantage", "1 = 1", "copper"),
    SourceMetricSpec("vix", "raw_fred", "1 = 1", "vix"),
    SourceMetricSpec("spx", "raw_fred", "1 = 1", "spx"),
    SourceMetricSpec("nasdaq_100", "raw_fred", "1 = 1", "nasdaq_100"),
)

RATE_COUNT_CODES: tuple[str, ...] = (
    "gov_1y",
    "gov_2y",
    "gov_3y",
    "gov_5y",
    "gov_10y",
    "gov_30y",
    "cdb_3y",
    "cdb_5y",
    "cdb_10y",
    "local_gov_1y",
    "local_gov_5y",
    "local_gov_10y",
    "aaa_ncd_1y",
    "dr007_weighted_rate",
    "omo_7d",
    "mlf_1y",
    "lpr_1y",
    "lpr_5y",
    "ust_2y",
    "ust_10y",
)

CURVE_COUNT_CODES: tuple[str, ...] = (
    "slope_gov_10y_1y_bp",
    "slope_gov_10y_3y_bp",
    "slope_gov_30y_10y_bp",
    "slope_cdb_10y_3y_bp",
    "slope_ust_10y_2y_bp",
    "bfly_gov_1y_5y_10y_bp",
    "bfly_gov_3y_5y_10y_bp",
    "bfly_gov_5y_10y_30y_bp",
    "bfly_cdb_3y_5y_10y_bp",
    "spread_cn_us_curve_slope_bp",
)

CREDIT_SPREAD_COUNT_CODES: tuple[str, ...] = (
    "spread_aa_plus_credit_aaa_credit_1y_bp",
    "spread_aaa_bank_bond_aaa_credit_1y_bp",
    "spread_aaa_bank_bond_aaa_credit_3y_bp",
    "spread_aaa_bank_bond_aaa_credit_5y_bp",
    "spread_aaa_credit_cdb_3y_bp",
    "spread_aaa_credit_cdb_5y_bp",
    "spread_aaa_credit_gov_1y_bp",
    "spread_aaa_credit_gov_3y_bp",
    "spread_aaa_credit_gov_5y_bp",
    "spread_aaa_credit_ncd_1y_bp",
    "spread_aaa_lgfv_aaa_credit_1y_bp",
    "spread_aaa_lgfv_gov_1y_bp",
    "spread_aaa_lgfv_local_gov_1y_bp",
    "spread_aaa_mtn_aaa_plus_mtn_1y_bp",
    "spread_aaa_plus_mtn_gov_1y_bp",
)

ALL_METRIC_CODES: tuple[str, ...] = (
    tuple(spec.code for spec in CURVE_SOURCE_SPECS)
    + tuple(spec.code for spec in DIRECT_SOURCE_SPECS)
    + (
        "gov_10y_ma20",
        "gov_10y_ma60",
        "gov_10y_ma120",
        "gov_10y_prank250",
        "aaa_ncd_1y_ma20",
        "aaa_ncd_1y_prank250",
        "gold_ma20",
        "usd_broad_ma20",
        "usd_cny_ma20",
        "usd_cny_prank250",
        "ust_10y_prank250",
        "bfly_cdb_3y_5y_10y_bp",
        "bfly_gov_1y_5y_10y_bp",
        "bfly_gov_3y_5y_10y_bp",
        "bfly_gov_5y_10y_30y_bp",
        "slope_cdb_10y_3y_bp",
        "slope_gov_10y_1y_bp",
        "slope_gov_10y_3y_bp",
        "slope_gov_30y_10y_bp",
        "slope_ust_10y_2y_bp",
        "spread_aa_plus_credit_aaa_credit_1y_bp",
        "spread_aa_plus_credit_aaa_credit_1y_bp_ma20",
        "spread_aa_plus_credit_aaa_credit_1y_bp_prank250",
        "spread_aaa_bank_bond_aaa_credit_1y_bp",
        "spread_aaa_bank_bond_aaa_credit_3y_bp",
        "spread_aaa_bank_bond_aaa_credit_5y_bp",
        "spread_aaa_credit_cdb_3y_bp",
        "spread_aaa_credit_cdb_5y_bp",
        "spread_aaa_credit_dr007_bp",
        "spread_aaa_credit_gov_1y_bp",
        "spread_aaa_credit_gov_3y_bp",
        "spread_aaa_credit_gov_5y_bp",
        "spread_aaa_credit_gov_5y_bp_ma20",
        "spread_aaa_credit_gov_5y_bp_prank250",
        "spread_aaa_credit_ncd_1y_bp",
        "spread_aaa_lgfv_aaa_credit_1y_bp",
        "spread_aaa_lgfv_gov_1y_bp",
        "spread_aaa_lgfv_local_gov_1y_bp",
        "spread_aaa_mtn_aaa_plus_mtn_1y_bp",
        "spread_aaa_ncd_dr007_bp",
        "spread_aaa_plus_mtn_gov_1y_bp",
        "spread_cdb_gov_10y_bp",
        "spread_cdb_gov_10y_bp_ma20",
        "spread_cdb_gov_10y_bp_prank250",
        "spread_cdb_gov_3y_bp",
        "spread_cdb_gov_5y_bp",
        "spread_cn_us_curve_slope_bp",
        "spread_dr007_omo_7d_bp",
        "spread_gov_1y_dr007_bp",
        "spread_gov_mlf_1y_bp",
        "spread_gov_ust_10y_bp",
        "spread_gov_ust_2y_bp",
        "spread_local_gov_cdb_10y_bp",
        "spread_local_gov_cdb_5y_bp",
        "spread_local_gov_gov_10y_bp",
        "spread_local_gov_gov_5y_bp",
        "spread_lpr1y_gov1y_bp",
        "spread_lpr5y_lpr1y_bp",
        "spread_lpr5y_ncd1y_bp",
        "spread_lpr_gov_5y_bp",
        "spread_lpr_mlf_1y_bp",
        "spread_ncd_mlf_1y_bp",
        "ratio_brent_gold",
        "ratio_copper_gold",
        "ratio_spx_gold",
        "ratio_spx_vix",
        "count_credit_spreads_bottom_decile",
        "count_credit_spreads_top_decile",
        "count_curve_metrics_bottom_decile",
        "count_curve_metrics_top_decile",
        "count_rates_bottom_decile",
        "count_rates_top_decile",
    )
)


def _rate_diff_bp(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return (left - right) * 10000.0


def _butterfly_bp(short: float | None, mid: float | None, long: float | None) -> float | None:
    if short is None or mid is None or long is None:
        return None
    return (2.0 * mid - short - long) * 10000.0


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def _rolling_mean(history: list[float], current: float | None, window: int) -> float | None:
    if current is None:
        return None
    values = history[-(window - 1) :] + [current]
    if len(values) < window:
        return None
    return _mean(values)


def _rolling_percentile_rank(history: list[float], current: float | None, window: int) -> float | None:
    if current is None:
        return None
    values = history[-(window - 1) :] + [current]
    if len(values) < window:
        return None
    return _percentile_rank(values, current)


def _count_deciles(
    history_by_code: dict[str, list[float]],
    current_values: dict[str, float | None],
    codes: tuple[str, ...],
) -> tuple[float, float]:
    top = 0
    bottom = 0
    for code in codes:
        prank = _rolling_percentile_rank(history_by_code.get(code, []), current_values.get(code), 250)
        if prank is None:
            continue
        if prank >= 0.9:
            top += 1
        if prank <= 0.1:
            bottom += 1
    return float(top), float(bottom)


def _load_target_dates(conn: Any, *, end_date: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT date
        FROM raw_bond_curve
        WHERE curve = '国债'
          AND date <= ?
        ORDER BY date ASC
        """,
        (end_date,),
    ).fetchall()
    return [str(row["date"] or "") for row in rows if str(row["date"] or "").strip()]


def _load_observed_series(conn: Any, spec: SourceMetricSpec, *, end_date: str) -> dict[str, float]:
    rows = conn.execute(
        f"""
        SELECT date, {spec.value_sql} AS value
        FROM {spec.table}
        WHERE {spec.key_sql}
          AND date <= ?
          AND {spec.value_sql} IS NOT NULL
        ORDER BY date ASC
        """,
        (end_date,),
    ).fetchall()
    out: dict[str, float] = {}
    for row in rows:
        date_value = str(row["date"] or "").strip()
        if not date_value:
            continue
        try:
            out[date_value] = float(row["value"]) * spec.scale
        except (TypeError, ValueError):
            continue
    return out


def _forward_fill(target_dates: list[str], observed: dict[str, float]) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    current: float | None = None
    for date_value in target_dates:
        if date_value in observed:
            current = observed[date_value]
        out[date_value] = current
    return out


def resolve_latest_metric_date(*, db_path: str | None = None) -> str:
    resolved_db_path = Path(db_path) if db_path else None
    conn = connect(resolved_db_path)
    try:
        row = conn.execute(
            """
            SELECT MAX(date) AS max_date
            FROM raw_bond_curve
            WHERE curve = '国债'
            """
        ).fetchone()
        value = "" if row is None else str(row["max_date"] or "").strip()
        if not value:
            raise RuntimeError("raw_bond_curve 无可用国债日期，无法推断 metric_daily 同步日期。")
        return value
    finally:
        conn.close()


def build_metric_daily_rows_from_raw(
    *,
    db_path: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, Any]]:
    resolved_db_path = Path(db_path) if db_path else None
    target_end_date = end_date or resolve_latest_metric_date(db_path=db_path)
    conn = connect(resolved_db_path)
    try:
        target_dates = _load_target_dates(conn, end_date=target_end_date)
        if not target_dates:
            return []

        filled_by_code: dict[str, dict[str, float | None]] = {}
        for spec in CURVE_SOURCE_SPECS + DIRECT_SOURCE_SPECS:
            observed = _load_observed_series(conn, spec, end_date=target_end_date)
            filled_by_code[spec.code] = _forward_fill(target_dates, observed)
    finally:
        conn.close()

    fetched_at = now_text()
    history_by_code: dict[str, list[float]] = defaultdict(list)
    rows: list[dict[str, Any]] = []
    selected_dates = {date_value for date_value in target_dates if start_date is None or date_value >= start_date}

    for date_value in target_dates:
        values: dict[str, float | None] = {code: series.get(date_value) for code, series in filled_by_code.items()}

        values["slope_gov_10y_1y_bp"] = _rate_diff_bp(values.get("gov_10y"), values.get("gov_1y"))
        values["slope_gov_10y_3y_bp"] = _rate_diff_bp(values.get("gov_10y"), values.get("gov_3y"))
        values["slope_gov_30y_10y_bp"] = _rate_diff_bp(values.get("gov_30y"), values.get("gov_10y"))
        values["slope_cdb_10y_3y_bp"] = _rate_diff_bp(values.get("cdb_10y"), values.get("cdb_3y"))
        values["slope_ust_10y_2y_bp"] = _rate_diff_bp(values.get("ust_10y"), values.get("ust_2y"))

        values["bfly_gov_1y_5y_10y_bp"] = _butterfly_bp(values.get("gov_1y"), values.get("gov_5y"), values.get("gov_10y"))
        values["bfly_gov_3y_5y_10y_bp"] = _butterfly_bp(values.get("gov_3y"), values.get("gov_5y"), values.get("gov_10y"))
        values["bfly_gov_5y_10y_30y_bp"] = _butterfly_bp(values.get("gov_5y"), values.get("gov_10y"), values.get("gov_30y"))
        values["bfly_cdb_3y_5y_10y_bp"] = _butterfly_bp(values.get("cdb_3y"), values.get("cdb_5y"), values.get("cdb_10y"))

        values["spread_aa_plus_credit_aaa_credit_1y_bp"] = _rate_diff_bp(values.get("aa_plus_credit_1y"), values.get("aaa_credit_1y"))
        values["spread_aaa_bank_bond_aaa_credit_1y_bp"] = _rate_diff_bp(values.get("aaa_bank_bond_1y"), values.get("aaa_credit_1y"))
        values["spread_aaa_bank_bond_aaa_credit_3y_bp"] = _rate_diff_bp(values.get("aaa_bank_bond_3y"), values.get("aaa_credit_3y"))
        values["spread_aaa_bank_bond_aaa_credit_5y_bp"] = _rate_diff_bp(values.get("aaa_bank_bond_5y"), values.get("aaa_credit_5y"))
        values["spread_aaa_credit_cdb_3y_bp"] = _rate_diff_bp(values.get("aaa_credit_3y"), values.get("cdb_3y"))
        values["spread_aaa_credit_cdb_5y_bp"] = _rate_diff_bp(values.get("aaa_credit_5y"), values.get("cdb_5y"))
        values["spread_aaa_credit_dr007_bp"] = _rate_diff_bp(values.get("aaa_credit_1y"), values.get("dr007_weighted_rate"))
        values["spread_aaa_credit_gov_1y_bp"] = _rate_diff_bp(values.get("aaa_credit_1y"), values.get("gov_1y"))
        values["spread_aaa_credit_gov_3y_bp"] = _rate_diff_bp(values.get("aaa_credit_3y"), values.get("gov_3y"))
        values["spread_aaa_credit_gov_5y_bp"] = _rate_diff_bp(values.get("aaa_credit_5y"), values.get("gov_5y"))
        values["spread_aaa_credit_ncd_1y_bp"] = _rate_diff_bp(values.get("aaa_credit_1y"), values.get("aaa_ncd_1y"))
        values["spread_aaa_lgfv_aaa_credit_1y_bp"] = _rate_diff_bp(values.get("aaa_lgfv_1y"), values.get("aaa_credit_1y"))
        values["spread_aaa_lgfv_gov_1y_bp"] = _rate_diff_bp(values.get("aaa_lgfv_1y"), values.get("gov_1y"))
        values["spread_aaa_lgfv_local_gov_1y_bp"] = _rate_diff_bp(values.get("aaa_lgfv_1y"), values.get("local_gov_1y"))
        values["spread_aaa_mtn_aaa_plus_mtn_1y_bp"] = _rate_diff_bp(values.get("aaa_mtn_1y"), values.get("aaa_plus_mtn_1y"))
        values["spread_aaa_ncd_dr007_bp"] = _rate_diff_bp(values.get("aaa_ncd_1y"), values.get("dr007_weighted_rate"))
        values["spread_aaa_plus_mtn_gov_1y_bp"] = _rate_diff_bp(values.get("aaa_plus_mtn_1y"), values.get("gov_1y"))
        values["spread_cdb_gov_10y_bp"] = _rate_diff_bp(values.get("cdb_10y"), values.get("gov_10y"))
        values["spread_cdb_gov_3y_bp"] = _rate_diff_bp(values.get("cdb_3y"), values.get("gov_3y"))
        values["spread_cdb_gov_5y_bp"] = _rate_diff_bp(values.get("cdb_5y"), values.get("gov_5y"))
        gov_curve_slope = None
        if values.get("gov_10y") is not None and values.get("gov_1y") is not None:
            gov_curve_slope = values["gov_10y"] - values["gov_1y"]
        us_curve_slope = None
        if values.get("ust_10y") is not None and values.get("ust_2y") is not None:
            us_curve_slope = values["ust_10y"] - values["ust_2y"]
        values["spread_cn_us_curve_slope_bp"] = _rate_diff_bp(gov_curve_slope, us_curve_slope)
        values["spread_dr007_omo_7d_bp"] = _rate_diff_bp(values.get("dr007_weighted_rate"), values.get("omo_7d"))
        values["spread_gov_1y_dr007_bp"] = _rate_diff_bp(values.get("gov_1y"), values.get("dr007_weighted_rate"))
        values["spread_gov_mlf_1y_bp"] = _rate_diff_bp(values.get("gov_1y"), values.get("mlf_1y"))
        values["spread_gov_ust_10y_bp"] = _rate_diff_bp(values.get("gov_10y"), values.get("ust_10y"))
        values["spread_gov_ust_2y_bp"] = _rate_diff_bp(values.get("gov_2y"), values.get("ust_2y"))
        values["spread_local_gov_cdb_10y_bp"] = _rate_diff_bp(values.get("local_gov_10y"), values.get("cdb_10y"))
        values["spread_local_gov_cdb_5y_bp"] = _rate_diff_bp(values.get("local_gov_5y"), values.get("cdb_5y"))
        values["spread_local_gov_gov_10y_bp"] = _rate_diff_bp(values.get("local_gov_10y"), values.get("gov_10y"))
        values["spread_local_gov_gov_5y_bp"] = _rate_diff_bp(values.get("local_gov_5y"), values.get("gov_5y"))
        values["spread_lpr1y_gov1y_bp"] = _rate_diff_bp(values.get("lpr_1y"), values.get("gov_1y"))
        values["spread_lpr5y_lpr1y_bp"] = _rate_diff_bp(values.get("lpr_5y"), values.get("lpr_1y"))
        values["spread_lpr5y_ncd1y_bp"] = _rate_diff_bp(values.get("lpr_5y"), values.get("aaa_ncd_1y"))
        values["spread_lpr_gov_5y_bp"] = _rate_diff_bp(values.get("lpr_5y"), values.get("gov_5y"))
        values["spread_lpr_mlf_1y_bp"] = _rate_diff_bp(values.get("lpr_1y"), values.get("mlf_1y"))
        values["spread_ncd_mlf_1y_bp"] = _rate_diff_bp(values.get("aaa_ncd_1y"), values.get("mlf_1y"))

        values["ratio_brent_gold"] = _ratio(values.get("brent"), values.get("gold"))
        values["ratio_copper_gold"] = _ratio(values.get("copper"), values.get("gold"))
        values["ratio_spx_gold"] = _ratio(values.get("spx"), values.get("gold"))
        values["ratio_spx_vix"] = _ratio(values.get("spx"), values.get("vix"))

        values["gov_10y_ma20"] = _rolling_mean(history_by_code.get("gov_10y", []), values.get("gov_10y"), 20)
        values["gov_10y_ma60"] = _rolling_mean(history_by_code.get("gov_10y", []), values.get("gov_10y"), 60)
        values["gov_10y_ma120"] = _rolling_mean(history_by_code.get("gov_10y", []), values.get("gov_10y"), 120)
        values["gov_10y_prank250"] = _rolling_percentile_rank(history_by_code.get("gov_10y", []), values.get("gov_10y"), 250)

        values["aaa_ncd_1y_ma20"] = _rolling_mean(history_by_code.get("aaa_ncd_1y", []), values.get("aaa_ncd_1y"), 20)
        values["aaa_ncd_1y_prank250"] = _rolling_percentile_rank(history_by_code.get("aaa_ncd_1y", []), values.get("aaa_ncd_1y"), 250)
        values["gold_ma20"] = _rolling_mean(history_by_code.get("gold", []), values.get("gold"), 20)
        values["usd_broad_ma20"] = _rolling_mean(history_by_code.get("usd_broad", []), values.get("usd_broad"), 20)
        values["usd_cny_ma20"] = _rolling_mean(history_by_code.get("usd_cny", []), values.get("usd_cny"), 20)
        values["usd_cny_prank250"] = _rolling_percentile_rank(history_by_code.get("usd_cny", []), values.get("usd_cny"), 250)
        values["ust_10y_prank250"] = _rolling_percentile_rank(history_by_code.get("ust_10y", []), values.get("ust_10y"), 250)
        values["spread_cdb_gov_10y_bp_ma20"] = _rolling_mean(history_by_code.get("spread_cdb_gov_10y_bp", []), values.get("spread_cdb_gov_10y_bp"), 20)
        values["spread_cdb_gov_10y_bp_prank250"] = _rolling_percentile_rank(
            history_by_code.get("spread_cdb_gov_10y_bp", []),
            values.get("spread_cdb_gov_10y_bp"),
            250,
        )
        values["spread_aaa_credit_gov_5y_bp_ma20"] = _rolling_mean(
            history_by_code.get("spread_aaa_credit_gov_5y_bp", []),
            values.get("spread_aaa_credit_gov_5y_bp"),
            20,
        )
        values["spread_aaa_credit_gov_5y_bp_prank250"] = _rolling_percentile_rank(
            history_by_code.get("spread_aaa_credit_gov_5y_bp", []),
            values.get("spread_aaa_credit_gov_5y_bp"),
            250,
        )
        values["spread_aa_plus_credit_aaa_credit_1y_bp_ma20"] = _rolling_mean(
            history_by_code.get("spread_aa_plus_credit_aaa_credit_1y_bp", []),
            values.get("spread_aa_plus_credit_aaa_credit_1y_bp"),
            20,
        )
        values["spread_aa_plus_credit_aaa_credit_1y_bp_prank250"] = _rolling_percentile_rank(
            history_by_code.get("spread_aa_plus_credit_aaa_credit_1y_bp", []),
            values.get("spread_aa_plus_credit_aaa_credit_1y_bp"),
            250,
        )

        rate_top, rate_bottom = _count_deciles(history_by_code, values, RATE_COUNT_CODES)
        curve_top, curve_bottom = _count_deciles(history_by_code, values, CURVE_COUNT_CODES)
        credit_top, credit_bottom = _count_deciles(history_by_code, values, CREDIT_SPREAD_COUNT_CODES)
        values["count_rates_top_decile"] = rate_top
        values["count_rates_bottom_decile"] = rate_bottom
        values["count_curve_metrics_top_decile"] = curve_top
        values["count_curve_metrics_bottom_decile"] = curve_bottom
        values["count_credit_spreads_top_decile"] = credit_top
        values["count_credit_spreads_bottom_decile"] = credit_bottom

        for code in ALL_METRIC_CODES:
            current_value = values.get(code)
            if current_value is not None:
                history_by_code[code].append(current_value)
            if date_value in selected_dates and current_value is not None:
                rows.append(
                    {
                        "date": date_value,
                        "code": code,
                        "value": current_value,
                        "source": "raw_metric_sync",
                        "fetched_at": fetched_at,
                    }
                )

    return rows


def sync_metric_daily_from_raw(
    *,
    db_path: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    target_end_date = end_date or resolve_latest_metric_date(db_path=db_path)
    rows = build_metric_daily_rows_from_raw(
        db_path=db_path,
        start_date=start_date,
        end_date=target_end_date,
    )
    written = upsert_metric_daily_rows(rows, db_path=db_path, dry_run=dry_run)
    dates = sorted({str(row["date"]) for row in rows})
    return {
        "target_end_date": target_end_date,
        "target_start_date": start_date or (dates[0] if dates else None),
        "date_count": len(dates),
        "rows_built": len(rows),
        "rows_written": written,
        "dry_run": dry_run,
        "sample": rows[:5],
    }
