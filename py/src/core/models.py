from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(slots=True)
class Observation:
    date: str
    value: Optional[float]
    source: str
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AkshareBondUsSinaRow:
    trade_date: str
    symbol: str
    close: Optional[float]
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    change: Optional[float] = None
    pct_change: Optional[float] = None
    volume: Optional[float] = None
    amount: Optional[float] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AkshareBondZhUsRateRow:
    trade_date: str
    cn_2y: Optional[float] = None
    cn_5y: Optional[float] = None
    cn_10y: Optional[float] = None
    cn_30y: Optional[float] = None
    cn_10y_2y: Optional[float] = None
    cn_gdp_yoy: Optional[float] = None
    us_2y: Optional[float] = None
    us_5y: Optional[float] = None
    us_10y: Optional[float] = None
    us_30y: Optional[float] = None
    us_10y_2y: Optional[float] = None
    us_gdp_yoy: Optional[float] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CurveSpec:
    name: str
    id: str
    tier: str = "main"
    fetch_separately: bool = False
    aliases: List[str] = field(default_factory=list)


@dataclass(slots=True)
class CurveBlock:
    title: str
    title_key: str
    points: Dict[float, float]


@dataclass(slots=True)
class CurveSnapshot:
    date: str
    curve_name: str
    curve_id: str
    tier: str
    source_title: str
    points: Dict[float, float]


@dataclass(slots=True)
class PolicyRateEvent:
    date: str
    type: str
    term: str
    rate: Optional[float]
    amount: Optional[float]
    source: str
    fetched_at: str
    note: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MoneyMarketSnapshot:
    date: str
    source: str
    fields: Dict[str, Any]
    fetched_at: str


@dataclass(slots=True)
class FuturesSnapshot:
    date: str
    source: str
    values: Dict[str, Optional[float]]
    fetched_at: str


@dataclass(slots=True)
class BondIndexSnapshot:
    date: str
    index_id: str
    duration: Optional[float]
    ytm: Optional[float]
    cons_number: Optional[float]
    modified_duration: Optional[float]
    convexity: Optional[float]
    total_market_value: Optional[float]
    avg_compensation_period: Optional[float]
    source: str
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JisiluEtfRowSnapshot:
    fund_id: str
    cell: Dict[str, Any]
    raw_row: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JisiluEtfSnapshot:
    snapshot_date: str
    fetched_at: str
    source_url: str
    records_total: Any
    rows: List[JisiluEtfRowSnapshot]
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JisiluGoldRowSnapshot:
    fund_id: str
    cell: Dict[str, Any]
    raw_row: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JisiluGoldSnapshot:
    snapshot_date: str
    fetched_at: str
    source_url: str
    records_total: Any
    rows: List[JisiluGoldRowSnapshot]
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JisiluMoneyRowSnapshot:
    fund_id: str
    cell: Dict[str, Any]
    raw_row: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JisiluMoneySnapshot:
    snapshot_date: str
    fetched_at: str
    source_url: str
    records_total: Any
    rows: List[JisiluMoneyRowSnapshot]
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JisiluQdiiRowSnapshot:
    market: str
    market_code: str
    fund_id: str
    cell: Dict[str, Any]
    raw_row: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JisiluQdiiSnapshot:
    snapshot_date: str
    fetched_at: str
    source_url: str
    market: str
    market_code: str
    records_total: Any
    rows: List[JisiluQdiiRowSnapshot]
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class JisiluTreasuryRowSnapshot:
    bond_id: str
    fields: Dict[str, Any]


@dataclass(slots=True)
class JisiluTreasurySnapshot:
    snapshot_date: str
    fetched_at: str
    source_url: str
    rows: List[JisiluTreasuryRowSnapshot]
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SseLivelyBondRowSnapshot:
    trade_date: str
    bond_id: str
    fields: Dict[str, Any]


@dataclass(slots=True)
class SseLivelyBondSnapshot:
    snapshot_date: str
    fetched_at: str
    source_url: str
    rows: List[SseLivelyBondRowSnapshot]
    total_count: int
    meta: Dict[str, Any] = field(default_factory=dict)
