"""Source adapters."""

from .alpha_vantage import AlphaVantageSource
from .akshare import (
    AKSHARE_BOND_GB_US_SINA_SYMBOLS,
    AKSHARE_BOND_ZH_US_RATE_DEFAULT_START_DATE,
    AkshareBondGbUsSinaSource,
    AkshareBondZhUsRateSource,
)
from ._base import BaseSource, FetchResult
from .chinabond import ChinaBondIndexSource, ChinaBondSource
from .chinamoney import ChinaMoneySource
from .cnindex import CnindexBondSource
from .csindex import CsindexBondSource
from .fred import FredSource
from .jisilu import (
    JisiluEtfSource,
    JisiluGoldEtfSource,
    JisiluGoldSource,
    JisiluMoneyEtfSource,
    JisiluMoneySource,
    JisiluQdiiEtfSource,
    JisiluQdiiSource,
    JisiluTreasurySource,
)
from .pbc import PbcSource
from ._registry import SOURCE_REGISTRY, SourceSpec, get_source_spec
from .sina_futures import SinaFuturesSource
from .sse import SseLivelyBondSource

__all__ = [
    "AlphaVantageSource",
    "AkshareBondGbUsSinaSource",
    "AkshareBondZhUsRateSource",
    "AKSHARE_BOND_GB_US_SINA_SYMBOLS",
    "AKSHARE_BOND_ZH_US_RATE_DEFAULT_START_DATE",
    "BaseSource",
    "ChinaBondIndexSource",
    "ChinaBondSource",
    "ChinaMoneySource",
    "CnindexBondSource",
    "CsindexBondSource",
    "FetchResult",
    "FredSource",
    "JisiluEtfSource",
    "JisiluGoldEtfSource",
    "JisiluGoldSource",
    "JisiluMoneyEtfSource",
    "JisiluMoneySource",
    "JisiluQdiiEtfSource",
    "JisiluQdiiSource",
    "JisiluTreasurySource",
    "PbcSource",
    "SOURCE_REGISTRY",
    "SinaFuturesSource",
    "SourceSpec",
    "SseLivelyBondSource",
    "get_source_spec",
]
