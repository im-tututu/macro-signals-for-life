"""Source adapters."""

from .alpha_vantage import AlphaVantageSource
from ._base import BaseSource, FetchResult
from .chinabond import ChinaBondIndexSource, ChinaBondSource
from .chinamoney import ChinaMoneySource
from .fred import FredSource
from .jisilu import JisiluEtfSource, JisiluQdiiSource, JisiluTreasurySource
from .pbc import PbcSource
from ._registry import SOURCE_REGISTRY, SourceSpec, get_source_spec
from .sina_futures import SinaFuturesSource
from .sse import SseLivelyBondSource

__all__ = [
    "AlphaVantageSource",
    "BaseSource",
    "ChinaBondIndexSource",
    "ChinaBondSource",
    "ChinaMoneySource",
    "FetchResult",
    "FredSource",
    "JisiluEtfSource",
    "JisiluQdiiSource",
    "JisiluTreasurySource",
    "PbcSource",
    "SOURCE_REGISTRY",
    "SinaFuturesSource",
    "SourceSpec",
    "SseLivelyBondSource",
    "get_source_spec",
]
