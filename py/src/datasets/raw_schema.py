from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from src.core.config import (
    CURVE_VALUE_COLUMNS,
    TABLE_RAW_ALPHA_VANTAGE,
    TABLE_RAW_BOND_CURVE,
    TABLE_RAW_CHINABOND_BOND_INDEX,
    TABLE_RAW_CNINDEX_BOND_INDEX,
    TABLE_RAW_CSINDEX_BOND_INDEX,
    TABLE_RAW_FRED,
    TABLE_RAW_FUTURES,
    TABLE_RAW_JISILU_ETF,
    TABLE_RAW_JISILU_GOLD,
    TABLE_RAW_JISILU_MONEY,
    TABLE_RAW_JISILU_QDII,
    TABLE_RAW_JISILU_TREASURY,
    TABLE_RAW_MONEY_MARKET,
    TABLE_RAW_POLICY_RATE,
    TABLE_RAW_SSE_LIVELY_BOND,
)
from src.core.models import (
    BondIndexSnapshot,
    CurveSnapshot,
    FuturesSnapshot,
    JisiluEtfSnapshot,
    JisiluGoldSnapshot,
    JisiluMoneySnapshot,
    JisiluQdiiSnapshot,
    JisiluTreasurySnapshot,
    MoneyMarketSnapshot,
    Observation,
    PolicyRateEvent,
    SseLivelyBondSnapshot,
)
from src.core.utils import norm_ymd, strip_tags, to_float
from src.sources._base import FetchResult
from src.stores._base import TableSpec

FieldType = str
ColumnTypes = dict[str, FieldType]
ColumnMapping = dict[str, object]
ColumnMappings = dict[str, ColumnMapping]
RowContext = dict[str, Any]
RawFetchResult = FetchResult[Any]
RowBuilder = Callable[[RawFetchResult], dict[str, Any]]
RowsBuilder = Callable[[RawFetchResult], list[dict[str, Any]]]
ContextBuilder = Callable[[RawFetchResult], RowContext]
ContextsBuilder = Callable[[RawFetchResult], Iterable[RowContext]]
TransformSpec = str | Callable[[Any], Any]

_UNSET = object()
_MISSING = object()


@dataclass(frozen=True)
class ColumnDef:
    type: FieldType
    source: str | None = None
    fallbacks: tuple[str, ...] = ()
    value: Any = _UNSET
    transform: TransformSpec | None = None
    default: Any = _UNSET
    required: bool = False

    @property
    def mapping(self) -> ColumnMapping:
        mapping: ColumnMapping = {}
        if self.source is not None:
            mapping["source"] = self.source
        if self.fallbacks:
            mapping["fallback_sources"] = self.fallbacks
        if self.value is not _UNSET:
            mapping["value"] = self.value
        if self.transform is not None:
            mapping["transform"] = self.transform
        if self.default is not _UNSET:
            mapping["default"] = self.default
        if self.required:
            mapping["required"] = True
        return mapping


ColumnDefs = dict[str, ColumnDef]


def col(
    type: FieldType,
    source: str | None = None,
    *,
    fallbacks: tuple[str, ...] = (),
    value: Any = _UNSET,
    transform: TransformSpec | None = None,
    default: Any = _UNSET,
    required: bool = False,
) -> ColumnDef:
    return ColumnDef(
        type=type,
        source=source,
        fallbacks=fallbacks,
        value=value,
        transform=transform,
        default=default,
        required=required,
    )


@dataclass(frozen=True)
class RawSchemaDef:
    table_name: str
    key_fields: tuple[str, ...]
    columns: ColumnDefs
    date_field: str | None = None
    default_order_by: tuple[str, ...] = ()
    row_context_builder: ContextBuilder | None = None
    rows_context_builder: ContextsBuilder | None = None
    custom_build_row: RowBuilder | None = None
    custom_build_rows: RowsBuilder | None = None

    @property
    def fields(self) -> ColumnTypes:
        return {name: column.type for name, column in self.columns.items()}

    @property
    def column_mappings(self) -> ColumnMappings:
        return {name: column.mapping for name, column in self.columns.items()}

    @property
    def table_spec(self) -> TableSpec:
        return make_table_spec_from_def(self)

    @property
    def build_row(self) -> RowBuilder | None:
        if self.custom_build_row is not None:
            return self.custom_build_row
        if self.row_context_builder is None or not self.columns:
            return None
        return lambda fetch_result: build_mapped_row(self, self.row_context_builder(fetch_result))

    @property
    def build_rows(self) -> RowsBuilder | None:
        if self.custom_build_rows is not None:
            return self.custom_build_rows
        if self.rows_context_builder is None or not self.columns:
            return None
        return lambda fetch_result: build_mapped_rows(self, self.rows_context_builder(fetch_result))


def make_table_spec_from_def(defn: RawSchemaDef) -> TableSpec:
    return TableSpec(
        table_name=defn.table_name,
        key_fields=defn.key_fields,
        date_field=defn.date_field,
        numeric_fields=tuple(name for name, typ in defn.fields.items() if typ == "float"),
        integer_fields=tuple(name for name, typ in defn.fields.items() if typ == "int"),
        text_fields=tuple(name for name, typ in defn.fields.items() if typ == "text"),
        datetime_fields=tuple(name for name, typ in defn.fields.items() if typ == "datetime"),
        compare_fields=tuple(defn.fields.keys()),
        default_order_by=defn.default_order_by,
    )


def _pick_obs_date(obs: Observation | None) -> str:
    if obs is None:
        return ""
    return str(obs.date or "")


def _pick_obs_value(obs: Observation | None) -> float | None:
    if obs is None:
        return None
    return obs.value


def _int_from_numeric_text(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    return int(float(text))


def _to_text(value: Any) -> str:
    return str(value or "")


def _strip_tags_text(value: Any) -> str:
    return strip_tags(str(value or ""))


_TRANSFORMS: dict[str, Callable[[Any], Any]] = {
    "int_from_numeric_text": _int_from_numeric_text,
    "norm_ymd": norm_ymd,
    "obs_date": _pick_obs_date,
    "obs_value": _pick_obs_value,
    "strip_tags": _strip_tags_text,
    "text": _to_text,
    "to_float": to_float,
}


def _is_blank(value: Any) -> bool:
    return value is _MISSING or value is None or (isinstance(value, str) and not value.strip())


def _resolve_path(context: RowContext, path: str) -> Any:
    current: Any = context
    for part in path.split("."):
        if isinstance(current, dict):
            if part not in current:
                return _MISSING
            current = current[part]
            continue
        if not hasattr(current, part):
            return _MISSING
        current = getattr(current, part)
    return current


def _apply_transform(transform: TransformSpec, value: Any) -> Any:
    if callable(transform):
        return transform(value)
    try:
        fn = _TRANSFORMS[transform]
    except KeyError as exc:
        raise ValueError(f"unknown transform: {transform}") from exc
    return fn(value)


def _resolve_mapping_value(mapping: ColumnMapping, context: RowContext) -> Any:
    if "value" in mapping:
        value = mapping["value"]
    else:
        value = _MISSING
        sources: list[str] = []
        source = mapping.get("source")
        if isinstance(source, str) and source:
            sources.append(source)
        for fallback in mapping.get("fallback_sources", ()):
            if isinstance(fallback, str) and fallback:
                sources.append(fallback)
        for path in sources:
            candidate = _resolve_path(context, path)
            if not _is_blank(candidate):
                value = candidate
                break
        if value is _MISSING and "default" in mapping:
            value = mapping["default"]

    transform = mapping.get("transform")
    if value is not _MISSING and transform is not None:
        value = _apply_transform(transform, value)

    if mapping.get("required") and _is_blank(value):
        label = mapping.get("source") or mapping.get("fallback_sources") or "value"
        raise ValueError(f"missing required mapped value: {label}")

    if value is _MISSING:
        return None
    return value


def build_mapped_row(defn: RawSchemaDef, context: RowContext) -> dict[str, Any]:
    return {column: _resolve_mapping_value(mapping, context) for column, mapping in defn.column_mappings.items()}


def build_mapped_rows(defn: RawSchemaDef, contexts: Iterable[RowContext]) -> list[dict[str, Any]]:
    return [build_mapped_row(defn, context) for context in contexts]


def _build_fetch_context(fetch_result: RawFetchResult) -> RowContext:
    return {
        "meta": fetch_result.meta,
        "payload": fetch_result.payload,
        "source_url": fetch_result.source_url,
    }


def _require_text(value: Any, message: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(message)
    return text


def _require_rows(rows: list[Any], message: str) -> list[Any]:
    if not rows:
        raise ValueError(message)
    return rows


def _build_policy_rate_contexts(fetch_result: FetchResult[list[PolicyRateEvent]]) -> list[RowContext]:
    return [
        {"event": event}
        for event in fetch_result.payload
        if event.date and event.type and event.term
    ]


def build_chinabond_curve_rows(fetch_result: FetchResult[list[CurveSnapshot]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for snapshot in fetch_result.payload:
        row: dict[str, Any] = {
            "date": snapshot.date,
            "curve": snapshot.curve_name,
        }
        for tenor_col in CURVE_VALUE_COLUMNS:
            row[tenor_col] = None
        for raw_term, value in snapshot.points.items():
            if float(raw_term).is_integer():
                normalized = str(int(raw_term))
            else:
                normalized = str(raw_term).replace(".", "_")
            tenor_col = f"y_{normalized}"
            if tenor_col in row:
                row[tenor_col] = value
        out.append(row)
    return out


def _build_sse_lively_bond_contexts(fetch_result: FetchResult[SseLivelyBondSnapshot]) -> list[RowContext]:
    payload = fetch_result.payload
    fetched_at = _require_text(payload.fetched_at or fetch_result.meta.get("fetched_at"), "SSE lively bond snapshot missing fetched_at")
    rows = _require_rows(list(payload.rows or []), "SSE lively bond snapshot rows are empty")
    source_url = str(payload.source_url or fetch_result.source_url or "")
    return [
        {
            "bond_id": row.bond_id,
            "fetched_at": fetched_at,
            "fields": row.fields,
            "source_url": source_url,
            "trade_date": row.trade_date,
        }
        for row in rows
    ]


def _build_jisilu_treasury_contexts(fetch_result: FetchResult[JisiluTreasurySnapshot]) -> list[RowContext]:
    payload = fetch_result.payload
    snapshot_date = _require_text(payload.snapshot_date, "Treasury snapshot missing snapshot_date")
    fetched_at = _require_text(payload.fetched_at, "Treasury snapshot missing fetched_at")
    rows = _require_rows(list(payload.rows or []), "Treasury snapshot rows are empty")
    source_url = str(payload.source_url or fetch_result.source_url or "")
    return [
        {
            "bond_id": row.bond_id,
            "fetched_at": fetched_at,
            "fields": row.fields,
            "snapshot_date": snapshot_date,
            "source_url": source_url,
        }
        for row in rows
    ]


def _build_jisilu_etf_like_contexts(
    fetch_result: FetchResult[JisiluEtfSnapshot | JisiluGoldSnapshot | JisiluMoneySnapshot],
    *,
    label: str,
) -> list[RowContext]:
    payload = fetch_result.payload
    snapshot_date = _require_text(payload.snapshot_date, f"{label} snapshot missing snapshot_date")
    fetched_at = _require_text(payload.fetched_at, f"{label} snapshot missing fetched_at")
    rows = _require_rows(list(payload.rows or []), f"{label} snapshot rows are empty")
    source_url = str(payload.source_url or fetch_result.source_url or "")
    records_total = payload.records_total
    return [
        {
            "cell": row.cell,
            "fetched_at": fetched_at,
            "fund_id": row.fund_id,
            "records_total": records_total,
            "snapshot_date": snapshot_date,
            "source_url": source_url,
        }
        for row in rows
    ]


def _build_jisilu_etf_contexts(fetch_result: FetchResult[JisiluEtfSnapshot]) -> list[RowContext]:
    return _build_jisilu_etf_like_contexts(fetch_result, label="ETF")


def _build_jisilu_gold_contexts(fetch_result: FetchResult[JisiluGoldSnapshot]) -> list[RowContext]:
    return _build_jisilu_etf_like_contexts(fetch_result, label="Gold")


def _build_jisilu_money_contexts(fetch_result: FetchResult[JisiluMoneySnapshot]) -> list[RowContext]:
    return _build_jisilu_etf_like_contexts(fetch_result, label="Money")


def _build_jisilu_qdii_contexts(fetch_result: FetchResult[JisiluQdiiSnapshot]) -> list[RowContext]:
    payload = fetch_result.payload
    snapshot_date = _require_text(payload.snapshot_date, "QDII snapshot missing snapshot_date")
    fetched_at = _require_text(payload.fetched_at, "QDII snapshot missing fetched_at")
    market = _require_text(payload.market, "QDII snapshot missing market identity")
    market_code = _require_text(payload.market_code, "QDII snapshot missing market identity")
    rows = _require_rows(list(payload.rows or []), "QDII snapshot rows are empty")
    source_url = str(payload.source_url or fetch_result.source_url or "")
    records_total = payload.records_total
    return [
        {
            "cell": row.cell,
            "fetched_at": fetched_at,
            "fund_id": row.fund_id,
            "market": market,
            "market_code": market_code,
            "records_total": records_total,
            "snapshot_date": snapshot_date,
            "source_url": source_url,
        }
        for row in rows
    ]


RAW_FRED_DEF = RawSchemaDef(
    table_name=TABLE_RAW_FRED,
    key_fields=("date",),
    date_field="date",
    default_order_by=("date",),
    columns={
        "date": col(
            "text",
            "payload.spx",
            fallbacks=("payload.nasdaq_100", "payload.ust_10y", "payload.sofr"),
            transform="obs_date",
            required=True,
        ),
        "fed_upper": col("float", "payload.fed_upper", transform="obs_value"),
        "fed_lower": col("float", "payload.fed_lower", transform="obs_value"),
        "sofr": col("float", "payload.sofr", transform="obs_value"),
        "ust_2y": col("float", "payload.ust_2y", transform="obs_value"),
        "ust_10y": col("float", "payload.ust_10y", transform="obs_value"),
        "us_real_10y": col("float", "payload.us_real_10y", transform="obs_value"),
        "usd_broad": col("float", "payload.usd_broad", transform="obs_value"),
        "usd_cny": col("float", "payload.usd_cny", transform="obs_value"),
        "vix": col("float", "payload.vix", transform="obs_value"),
        "spx": col("float", "payload.spx", transform="obs_value"),
        "nasdaq_100": col("float", "payload.nasdaq_100", transform="obs_value"),
        "source": col("text", value="FRED"),
        "fetched_at": col("datetime", "meta.fetched_at", required=True),
    },
    row_context_builder=_build_fetch_context,
)

RAW_ALPHA_VANTAGE_DEF = RawSchemaDef(
    table_name=TABLE_RAW_ALPHA_VANTAGE,
    key_fields=("date",),
    date_field="date",
    default_order_by=("date",),
    columns={
        "date": col(
            "text",
            "payload.gold",
            fallbacks=("payload.wti", "payload.brent", "payload.copper"),
            transform="obs_date",
            required=True,
        ),
        "gold": col("float", "payload.gold", transform="obs_value"),
        "wti": col("float", "payload.wti", transform="obs_value"),
        "brent": col("float", "payload.brent", transform="obs_value"),
        "copper": col("float", "payload.copper", transform="obs_value"),
        "source": col("text", value="ALPHA_VANTAGE"),
        "fetched_at": col("datetime", "meta.fetched_at", required=True),
    },
    row_context_builder=_build_fetch_context,
)

RAW_FUTURES_DEF = RawSchemaDef(
    table_name=TABLE_RAW_FUTURES,
    key_fields=("date",),
    date_field="date",
    default_order_by=("date",),
    columns={
        "date": col("text", "payload.date", required=True),
        "t0_last": col("float", "payload.values.t0_last"),
        "tf0_last": col("float", "payload.values.tf0_last"),
        "source": col("text", "payload.source", default=""),
        "fetched_at": col("datetime", "payload.fetched_at", required=True),
    },
    row_context_builder=_build_fetch_context,
)

RAW_MONEY_MARKET_DEF = RawSchemaDef(
    table_name=TABLE_RAW_MONEY_MARKET,
    key_fields=("date",),
    date_field="date",
    default_order_by=("date",),
    columns={
        "date": col("text", "payload.date", required=True),
        "show_date_cn": col("text", "meta.show_date_cn", default=""),
        "source_url": col("text", "source_url", fallbacks=("payload.source",), default=""),
        "dr001_weighted_rate": col("float", "payload.fields.DR001_weightedRate", transform="to_float"),
        "dr001_latest_rate": col("float", "payload.fields.DR001_latestRate", transform="to_float"),
        "dr001_avg_prd": col("float", "payload.fields.DR001_avgPrd", transform="to_float"),
        "dr007_weighted_rate": col("float", "payload.fields.DR007_weightedRate", transform="to_float"),
        "dr007_latest_rate": col("float", "payload.fields.DR007_latestRate", transform="to_float"),
        "dr007_avg_prd": col("float", "payload.fields.DR007_avgPrd", transform="to_float"),
        "dr014_weighted_rate": col("float", "payload.fields.DR014_weightedRate", transform="to_float"),
        "dr014_latest_rate": col("float", "payload.fields.DR014_latestRate", transform="to_float"),
        "dr014_avg_prd": col("float", "payload.fields.DR014_avgPrd", transform="to_float"),
        "dr021_weighted_rate": col("float", "payload.fields.DR021_weightedRate", transform="to_float"),
        "dr021_latest_rate": col("float", "payload.fields.DR021_latestRate", transform="to_float"),
        "dr021_avg_prd": col("float", "payload.fields.DR021_avgPrd", transform="to_float"),
        "dr1m_weighted_rate": col("float", "payload.fields.DR1M_weightedRate", transform="to_float"),
        "dr1m_latest_rate": col("float", "payload.fields.DR1M_latestRate", transform="to_float"),
        "dr1m_avg_prd": col("float", "payload.fields.DR1M_avgPrd", transform="to_float"),
        "fetched_at": col("datetime", "payload.fetched_at", required=True),
    },
    row_context_builder=_build_fetch_context,
)

RAW_CHINABOND_CURVE_DEF = RawSchemaDef(
    table_name=TABLE_RAW_BOND_CURVE,
    key_fields=("date", "curve"),
    date_field="date",
    default_order_by=("date", "curve"),
    columns={
        "date": col("text"),
        "curve": col("text"),
        **{tenor_col: col("float") for tenor_col in CURVE_VALUE_COLUMNS},
        "source_sheet": col("text"),
        "source_row_num": col("int"),
        "migrated_at": col("datetime"),
    },
    custom_build_rows=build_chinabond_curve_rows,
)

RAW_POLICY_RATE_DEF = RawSchemaDef(
    table_name=TABLE_RAW_POLICY_RATE,
    key_fields=("date", "type", "term"),
    date_field="date",
    default_order_by=("date", "type", "term"),
    columns={
        "date": col("text", "event.date", required=True),
        "type": col("text", "event.type", required=True),
        "term": col("text", "event.term", required=True),
        "rate": col("float", "event.rate"),
        "amount": col("float", "event.amount"),
        "source": col("text", "event.source", default=""),
        "fetched_at": col("datetime", "event.fetched_at", required=True),
        "note": col("text", "event.note", default=""),
        "source_sheet": col("text"),
        "source_row_num": col("int"),
        "migrated_at": col("datetime"),
    },
    rows_context_builder=_build_policy_rate_contexts,
)

RAW_SSE_LIVELY_BOND_DEF = RawSchemaDef(
    table_name=TABLE_RAW_SSE_LIVELY_BOND,
    key_fields=("trade_date", "bond_id"),
    date_field="trade_date",
    default_order_by=("trade_date", "rank_num", "bond_id"),
    columns={
        "trade_date": col("text", "trade_date", required=True),
        "bond_id": col("text", "bond_id", required=True),
        "bond_nm": col("text", "fields.SEC_NAME", default=""),
        "bond_nm_full": col("text", "fields.SECURITY_ABBR_FULL", default=""),
        "rank_num": col("int", "fields.NUM", transform="int_from_numeric_text"),
        "open_price": col("float", "fields.OPEN_PRICE", transform="to_float"),
        "close_price": col("float", "fields.CLOSE_PRICE", transform="to_float"),
        "change_ratio": col("float", "fields.SUM_CHANGE_RATIO", transform="to_float"),
        "amplitude": col("float", "fields.QJZF", transform="to_float"),
        "volume_hand": col("float", "fields.SUM_TRADE_VOL", transform="to_float"),
        "amount_wanyuan": col("float", "fields.SUM_TRADE_AMT", transform="to_float"),
        "ytm": col("float", "fields.SUM_TO_RATE", transform="to_float"),
        "source_url": col("text", "source_url", default=""),
        "source_sheet": col("text"),
        "fetched_at": col("datetime", "fetched_at", required=True),
        "source_row_num": col("int"),
        "migrated_at": col("datetime"),
    },
    rows_context_builder=_build_sse_lively_bond_contexts,
)

RAW_JISILU_TREASURY_DEF = RawSchemaDef(
    table_name=TABLE_RAW_JISILU_TREASURY,
    key_fields=("snapshot_date", "fetched_at", "bond_id"),
    date_field="snapshot_date",
    default_order_by=("snapshot_date", "fetched_at", "bond_id"),
    columns={
        "snapshot_date": col("text", "snapshot_date", required=True),
        "fetched_at": col("datetime", "fetched_at", required=True),
        "bond_id": col("text", "bond_id", required=True),
        "bond_nm": col("text", "fields.bond_nm", default=""),
        "price": col("float", "fields.price", transform="to_float"),
        "full_price": col("float", "fields.full_price", transform="to_float"),
        "increase_rt": col("float", "fields.increase_rt", transform="to_float"),
        "volume_wan": col("float", "fields.volume_wan", transform="to_float"),
        "ask_1": col("float", "fields.ask_1", transform="to_float"),
        "bid_1": col("float", "fields.bid_1", transform="to_float"),
        "days_to_coupon": col("float", "fields.days_to_coupon", transform="to_float"),
        "years_left": col("float", "fields.years_left", transform="to_float"),
        "duration": col("float", "fields.duration", transform="to_float"),
        "ytm": col("float", "fields.ytm", transform="to_float"),
        "coupon_rt": col("float", "fields.coupon_rt", transform="to_float"),
        "repo_ratio": col("float", "fields.repo_ratio", transform="to_float"),
        "repo_usage_rt": col("float", "fields.repo_usage_rt", transform="to_float"),
        "maturity_dt": col("text", "fields.maturity_dt", transform="norm_ymd"),
        "size_yi": col("float", "fields.size_yi", transform="to_float"),
        "source_url": col("text", "source_url", default=""),
        "source_sheet": col("text"),
        "source_row_num": col("int"),
        "migrated_at": col("datetime"),
    },
    rows_context_builder=_build_jisilu_treasury_contexts,
)

JISILU_ETF_BASE_COLUMNS: ColumnDefs = {
    "snapshot_date": col("text", "snapshot_date", required=True),
    "fetched_at": col("datetime", "fetched_at", required=True),
    "fund_id": col("text", "fund_id", required=True),
    "fund_nm": col("text", "cell.fund_nm", fallbacks=("cell.fund_name",), default=""),
    "index_nm": col("text", "cell.index_nm", fallbacks=("cell.index_name", "cell.ref_nm"), default=""),
    "issuer_nm": col("text", "cell.issuer_nm", fallbacks=("cell.company_nm",), default=""),
    "price": col("float", "cell.price", transform="to_float"),
    "increase_rt": col("float", "cell.increase_rt", transform="to_float"),
    "volume_wan": col("float", "cell.volume", transform="to_float"),
    "amount_yi": col("float", "cell.amount", transform="to_float"),
    "unit_total_yi": col("float", "cell.unit_total", transform="to_float"),
    "discount_rt": col("float", "cell.discount_rt", transform="to_float"),
    "fund_nav": col("float", "cell.fund_nav", transform="to_float"),
    "nav_dt": col("text", "cell.nav_dt", default=""),
    "estimate_value": col("float", "cell.estimate_value", transform="to_float"),
    "creation_unit": col("float", "cell.creation_unit", transform="to_float"),
    "pe": col("float", "cell.pe", transform="to_float"),
    "pb": col("float", "cell.pb", transform="to_float"),
    "m_fee": col("float", "cell.m_fee", transform="to_float"),
    "t_fee": col("float", "cell.t_fee", transform="to_float"),
    "mt_fee": col("float", "cell.mt_fee", transform="to_float"),
    "last_time": col("text", "cell.last_time", default=""),
    "last_est_time": col("text", "cell.last_est_time", default=""),
    "is_qdii": col("text", "cell.is_qdii", default=""),
    "is_t0": col("text", "cell.is_t0", default=""),
    "apply_fee": col("float", "cell.apply_fee", transform="to_float"),
    "redeem_fee": col("float", "cell.redeem_fee", transform="to_float"),
    "records_total": col("float", "records_total", transform="to_float"),
    "source_url": col("text", "source_url", default=""),
    "source_sheet": col("text"),
    "source_row_num": col("int"),
    "migrated_at": col("datetime"),
}

RAW_JISILU_GOLD_ETF_DEF = RawSchemaDef(
    table_name=TABLE_RAW_JISILU_GOLD,
    key_fields=("snapshot_date", "fetched_at", "fund_id"),
    date_field="snapshot_date",
    default_order_by=("snapshot_date", "fetched_at", "fund_id"),
    columns=dict(JISILU_ETF_BASE_COLUMNS),
    rows_context_builder=_build_jisilu_gold_contexts,
)

RAW_JISILU_MONEY_ETF_DEF = RawSchemaDef(
    table_name=TABLE_RAW_JISILU_MONEY,
    key_fields=("snapshot_date", "fetched_at", "fund_id"),
    date_field="snapshot_date",
    default_order_by=("snapshot_date", "fetched_at", "fund_id"),
    columns=dict(JISILU_ETF_BASE_COLUMNS),
    rows_context_builder=_build_jisilu_money_contexts,
)

RAW_JISILU_ETF_DEF = RawSchemaDef(
    table_name=TABLE_RAW_JISILU_ETF,
    key_fields=("snapshot_date", "fetched_at", "fund_id"),
    date_field="snapshot_date",
    default_order_by=("snapshot_date", "fetched_at", "fund_id"),
    columns=dict(JISILU_ETF_BASE_COLUMNS),
    rows_context_builder=_build_jisilu_etf_contexts,
)

RAW_JISILU_QDII_DEF = RawSchemaDef(
    table_name=TABLE_RAW_JISILU_QDII,
    key_fields=("snapshot_date", "fetched_at", "market", "fund_id"),
    date_field="snapshot_date",
    default_order_by=("snapshot_date", "fetched_at", "market", "fund_id"),
    columns={
        "snapshot_date": col("text", "snapshot_date", required=True),
        "fetched_at": col("datetime", "fetched_at", required=True),
        "market": col("text", "market", required=True),
        "market_code": col("text", "market_code", required=True),
        "fund_id": col("text", "fund_id", required=True),
        "fund_nm": col("text", "cell.fund_nm", fallbacks=("cell.fund_name",), default=""),
        "fund_nm_display": col(
            "text",
            "cell.fund_nm_color",
            fallbacks=("cell.fund_nm", "cell.fund_name"),
            transform="strip_tags",
            default="",
        ),
        "qtype": col("text", "cell.qtype", default=""),
        "index_nm": col("text", "cell.index_nm", fallbacks=("cell.index_name", "cell.ref_nm"), default=""),
        "index_id": col("text", "cell.index_id", default=""),
        "issuer_nm": col("text", "cell.issuer_nm", fallbacks=("cell.company_nm",), default=""),
        "price": col("float", "cell.price", transform="to_float"),
        "pre_close": col("float", "cell.pre_close", transform="to_float"),
        "increase_rt": col("float", "cell.increase_rt", transform="to_float"),
        "volume_wan": col("float", "cell.volume", transform="to_float"),
        "stock_volume_wan": col("float", "cell.stock_volume", transform="to_float"),
        "amount_yi": col("float", "cell.amount", transform="to_float"),
        "amount_incr": col("float", "cell.amount_incr", transform="to_float"),
        "amount_increase_rt": col("float", "cell.amount_increase_rt", transform="to_float"),
        "unit_total_yi": col("float", "cell.unit_total", transform="to_float"),
        "discount_rt": col("float", "cell.discount_rt", transform="to_float"),
        "fund_nav": col("float", "cell.fund_nav", transform="to_float"),
        "iopv": col("float", "cell.iopv", transform="to_float"),
        "estimate_value": col("float", "cell.estimate_value", transform="to_float"),
        "ref_price": col("float", "cell.ref_price", transform="to_float"),
        "ref_increase_rt": col("float", "cell.ref_increase_rt", transform="to_float"),
        "est_val_increase_rt": col("float", "cell.est_val_increase_rt", transform="to_float"),
        "m_fee": col("float", "cell.m_fee", transform="to_float"),
        "t_fee": col("float", "cell.t_fee", transform="to_float"),
        "mt_fee": col("float", "cell.mt_fee", transform="to_float"),
        "nav_discount_rt": col("float", "cell.nav_discount_rt", transform="to_float"),
        "iopv_discount_rt": col("float", "cell.iopv_discount_rt", transform="to_float"),
        "turnover_rt": col("float", "cell.turnover_rt", transform="to_float"),
        "records_total": col("float", "records_total", transform="to_float"),
        "price_dt": col("text", "cell.price_dt", default=""),
        "nav_dt": col("text", "cell.nav_dt", default=""),
        "iopv_dt": col("text", "cell.iopv_dt", default=""),
        "last_est_dt": col("text", "cell.last_est_dt", default=""),
        "last_time": col("text", "cell.last_time", default=""),
        "last_est_time": col("text", "cell.last_est_time", default=""),
        "apply_fee": col("text", "cell.apply_fee", default=""),
        "apply_status": col("text", "cell.apply_status", default=""),
        "apply_fee_tips": col("text", "cell.apply_fee_tips", default=""),
        "redeem_fee": col("text", "cell.redeem_fee", default=""),
        "redeem_status": col("text", "cell.redeem_status", default=""),
        "redeem_fee_tips": col("text", "cell.redeem_fee_tips", default=""),
        "money_cd": col("text", "cell.money_cd", default=""),
        "asset_ratio": col("text", "cell.asset_ratio", default=""),
        "lof_type": col("text", "cell.lof_type", default=""),
        "t0": col("text", "cell.t0", default=""),
        "eval_show": col("text", "cell.eval_show", default=""),
        "notes": col("text", "cell.notes", default=""),
        "has_iopv": col("text", "cell.has_iopv", default=""),
        "has_us_ref": col("text", "cell.has_us_ref", default=""),
        "source_url": col("text", "source_url", default=""),
        "source_sheet": col("text"),
        "source_row_num": col("int"),
        "migrated_at": col("datetime"),
    },
    rows_context_builder=_build_jisilu_qdii_contexts,
)

BOND_INDEX_COLUMNS: ColumnDefs = {
    "trade_date": col("text", "payload.date", required=True),
    "index_name": col("text", "meta.index_name", fallbacks=("payload.index_id",), required=True),
    "index_code": col("text", "meta.index_code", fallbacks=("payload.index_id",), required=True),
    "source_url": col("text", "source_url", default=""),
    "dm": col("float", "payload.duration"),
    "y": col("float", "payload.ytm"),
    "cons_number": col("float", "payload.cons_number"),
    "d": col("float", "payload.modified_duration"),
    "v": col("float", "payload.convexity"),
    "total_market_value": col("float", "payload.total_market_value"),
    "avg_compensation_period": col("float", "payload.avg_compensation_period"),
    "fetch_status": col("text", value="OK"),
    "fetched_at": col("datetime", "meta.fetched_at", required=True),
    "error": col("text", value=""),
    "source_sheet": col("text"),
    "source_row_num": col("int"),
    "migrated_at": col("datetime"),
}


def make_bond_index_def(table_name: str) -> RawSchemaDef:
    return RawSchemaDef(
        table_name=table_name,
        key_fields=("trade_date", "index_name"),
        date_field="trade_date",
        default_order_by=("trade_date", "index_name"),
        columns=dict(BOND_INDEX_COLUMNS),
        row_context_builder=_build_fetch_context,
    )


RAW_CHINABOND_BOND_INDEX_DEF = make_bond_index_def(TABLE_RAW_CHINABOND_BOND_INDEX)
RAW_CSINDEX_BOND_INDEX_DEF = make_bond_index_def(TABLE_RAW_CSINDEX_BOND_INDEX)
RAW_CNINDEX_BOND_INDEX_DEF = make_bond_index_def(TABLE_RAW_CNINDEX_BOND_INDEX)


def build_jisilu_etf_history_row(
    *,
    snapshot_date: str,
    fetched_at: str,
    fund_id: str,
    cell: dict[str, Any],
    source_url: str = "",
) -> dict[str, Any]:
    return build_mapped_row(
        RAW_JISILU_ETF_DEF,
        {
            "cell": cell,
            "fetched_at": fetched_at,
            "fund_id": fund_id,
            "records_total": cell.get("records_total"),
            "snapshot_date": snapshot_date,
            "source_url": source_url,
        },
    )


def build_jisilu_gold_etf_history_row(
    *,
    snapshot_date: str,
    fetched_at: str,
    fund_id: str,
    cell: dict[str, Any],
    source_url: str = "",
) -> dict[str, Any]:
    return build_mapped_row(
        RAW_JISILU_GOLD_ETF_DEF,
        {
            "cell": cell,
            "fetched_at": fetched_at,
            "fund_id": fund_id,
            "records_total": cell.get("records_total"),
            "snapshot_date": snapshot_date,
            "source_url": source_url,
        },
    )


def build_jisilu_qdii_history_row(
    *,
    snapshot_date: str,
    fetched_at: str,
    market: str,
    market_code: str,
    fund_id: str,
    cell: dict[str, Any],
    source_url: str = "",
) -> dict[str, Any]:
    return build_mapped_row(
        RAW_JISILU_QDII_DEF,
        {
            "cell": cell,
            "fetched_at": fetched_at,
            "fund_id": fund_id,
            "market": market,
            "market_code": market_code,
            "records_total": cell.get("records_total"),
            "snapshot_date": snapshot_date,
            "source_url": source_url,
        },
    )
