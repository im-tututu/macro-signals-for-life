"""Source adapters."""

from .alpha_vantage import AlphaVantageSource
from .base import BaseSource, FetchResult
from .boc import BocSource
from .chinabond import ChinaBondIndexSource, ChinaBondSource
from .chinamoney import ChinaMoneySource
from .eastmoney import EastMoneySource
from .fred import FredSource
from .jisilu import JisiluEtfSource, JisiluTreasurySource
from .pbc import PbcSource
from .sge import SgeSource
from .sina_futures import SinaFuturesSource
from .sse import SseLivelyBondSource
from .stats_gov import StatsGovSource

__all__ = [
    "AlphaVantageSource",
    "BaseSource",
    "BocSource",
    "ChinaBondIndexSource",
    "ChinaBondSource",
    "ChinaMoneySource",
    "EastMoneySource",
    "FetchResult",
    "FredSource",
    "JisiluEtfSource",
    "JisiluTreasurySource",
    "PbcSource",
    "SgeSource",
    "SinaFuturesSource",
    "SseLivelyBondSource",
    "StatsGovSource",
]
