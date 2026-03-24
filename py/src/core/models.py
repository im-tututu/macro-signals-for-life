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
    convexity: Optional[float]
    source: str
    meta: Dict[str, Any] = field(default_factory=dict)
