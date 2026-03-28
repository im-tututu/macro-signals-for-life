from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.core.models import (
    BondIndexSnapshot,
    CurveSnapshot,
    FuturesSnapshot,
    JisiluEtfSnapshot,
    JisiluQdiiSnapshot,
    JisiluTreasurySnapshot,
    MoneyMarketSnapshot,
    PolicyRateEvent,
    SseLivelyBondSnapshot,
)

from .alpha_vantage import AlphaVantageSource
from ._base import AccessKind, BaseSource
from .chinabond import ChinaBondIndexSource, ChinaBondSource
from .chinamoney import ChinaMoneySource
from .fred import FredSource
from .jisilu import JisiluEtfSource, JisiluQdiiSource, JisiluTreasurySource
from .pbc import PbcSource
from .sina_futures import SinaFuturesSource
from .sse import SseLivelyBondSource


@dataclass(frozen=True)
class SourceSpec:
    source_id: str
    dataset_id: str | None
    source_class: type[BaseSource]
    access_kind: AccessKind
    snapshot_type: object
    fetch_method: str
    supports_latest: bool = True
    supports_backfill: bool = False
    notes: tuple[str, ...] = ()


SOURCE_REGISTRY: dict[str, SourceSpec] = {
    "chinamoney_money_market": SourceSpec(
        source_id="chinamoney_money_market",
        dataset_id="money_market",
        source_class=ChinaMoneySource,
        access_kind="api",
        snapshot_type=MoneyMarketSnapshot,
        fetch_method="fetch_money_market",
        notes=("日频资金面快照。",),
    ),
    "chinabond_curve": SourceSpec(
        source_id="chinabond_curve",
        dataset_id="chinabond_curve",
        source_class=ChinaBondSource,
        access_kind="xhr_html",
        snapshot_type=list[CurveSnapshot],
        fetch_method="fetch_daily_wide_result",
        supports_backfill=True,
    ),
    "chinabond_index": SourceSpec(
        source_id="chinabond_index",
        dataset_id="bond_index",
        source_class=ChinaBondIndexSource,
        access_kind="xhr_json",
        snapshot_type=BondIndexSnapshot,
        fetch_method="fetch_duration_snapshot_result",
    ),
    "pbc_policy_rate": SourceSpec(
        source_id="pbc_policy_rate",
        dataset_id="policy_rate",
        source_class=PbcSource,
        access_kind="page_html",
        snapshot_type=list[PolicyRateEvent],
        fetch_method="fetch_latest_policy_rate_events_result",
        supports_backfill=True,
    ),
    "sina_futures": SourceSpec(
        source_id="sina_futures",
        dataset_id="futures",
        source_class=SinaFuturesSource,
        access_kind="api",
        snapshot_type=FuturesSnapshot,
        fetch_method="fetch_bond_futures_result",
    ),
    "jisilu_etf": SourceSpec(
        source_id="jisilu_etf",
        dataset_id="etf",
        source_class=JisiluEtfSource,
        access_kind="xhr_json",
        snapshot_type=JisiluEtfSnapshot,
        fetch_method="fetch_etf_index_all_result",
    ),
    "jisilu_qdii": SourceSpec(
        source_id="jisilu_qdii",
        dataset_id="qdii",
        source_class=JisiluQdiiSource,
        access_kind="xhr_json",
        snapshot_type=JisiluQdiiSnapshot,
        fetch_method="fetch_qdii_all_result",
    ),
    "jisilu_treasury": SourceSpec(
        source_id="jisilu_treasury",
        dataset_id="treasury",
        source_class=JisiluTreasurySource,
        access_kind="page_html",
        snapshot_type=JisiluTreasurySnapshot,
        fetch_method="fetch_treasury_result",
    ),
    "sse_lively_bond": SourceSpec(
        source_id="sse_lively_bond",
        dataset_id="sse_lively_bond",
        source_class=SseLivelyBondSource,
        access_kind="xhr_json",
        snapshot_type=SseLivelyBondSnapshot,
        fetch_method="fetch_lively_bond_all_result",
    ),
    "fred": SourceSpec(
        source_id="fred",
        dataset_id="fred",
        source_class=FredSource,
        access_kind="api",
        snapshot_type=dict[str, Any],
        fetch_method="fetch_fred_result",
    ),
    "alpha_vantage": SourceSpec(
        source_id="alpha_vantage",
        dataset_id="alpha_vantage",
        source_class=AlphaVantageSource,
        access_kind="api",
        snapshot_type=dict[str, Any],
        fetch_method="fetch_alpha_vantage_result",
    ),
}


def get_source_spec(source_id: str) -> SourceSpec:
    try:
        return SOURCE_REGISTRY[source_id]
    except KeyError as exc:
        raise ValueError(f"unknown source: {source_id}") from exc
