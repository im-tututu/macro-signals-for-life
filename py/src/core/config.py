from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv

from .models import CurveSpec

BASE_DIR = Path(__file__).resolve().parents[2]
ENV_CANDIDATES = [
    Path.cwd() / ".env",
    BASE_DIR / ".env",
    BASE_DIR.parent / ".env",
]
for env_path in ENV_CANDIDATES:
    if env_path.exists():
        load_dotenv(env_path, override=True)

TERMS: List[float] = [
    0, 0.08, 0.17, 0.25, 0.5, 0.75, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 40, 50,
]

CURVES: List[CurveSpec] = [
    CurveSpec(name="国债", id="2c9081e50a2f9606010a3068cae70001", tier="main", aliases=["国债收益率曲线", "Government Bond"]),
    CurveSpec(name="国开债", id="8a8b2ca037a7ca910137bfaa94fa5057", tier="main", aliases=["国开债收益率曲线", "政策性金融债", "Policy Bank"]),
    CurveSpec(name="AAA信用", id="2c9081e50a2f9606010a309f4af50111", tier="main", aliases=["企业债收益率曲线(AAA)", "企业债AAA", "Enterprise Bond AAA"]),
    CurveSpec(name="AA+信用", id="2c908188138b62cd01139a2ee6b51e25", tier="main", aliases=["企业债收益率曲线(AA+)", "企业债收益率曲线(AA＋)", "企业债AA+", "Enterprise Bond AA+"]),
    CurveSpec(name="AAA+中票", id="2c9081e9257ddf2a012590efdded1d35", tier="main", aliases=["中短期票据收益率曲线(AAA+)", "中票AAA+", "CP&Note AAA+"]),
    CurveSpec(name="AAA中票", id="2c9081880fa9d507010fb8505b393fe7", tier="main", aliases=["中短期票据收益率曲线(AAA)", "中票AAA", "CP&Note AAA"]),
    CurveSpec(name="AAA存单", id="8308218D1D030E0DE0540010E03EE6DA", tier="main", aliases=["同业存单收益率曲线(AAA)", "存单AAA", "NCD AAA", "Negotiable CD AAA"]),
    CurveSpec(name="AAA城投", id="2c9081e91b55cc84011be3c53b710598", tier="extended", aliases=["城投债收益率曲线(AAA)", "城投AAA", "LGFV AAA"]),
    CurveSpec(name="AAA银行债", id="2c9081e9259b766a0125be8b5115149f", tier="extended", aliases=["商业银行普通债收益率曲线(AAA)", "商业银行债收益率曲线(AAA)", "银行债AAA", "Financial Bond of Commercial Bank AAA"]),
    CurveSpec(name="地方债", id="998183ff8c00f640018c32d4721a0d16", tier="short_history", fetch_separately=True, aliases=["地方政府债收益率曲线", "地方政府债", "Local Government"]),
]

OVERSEAS_MACRO_FRED_SERIES: Dict[str, str] = {
    "fed_upper": "DFEDTARU",
    "fed_lower": "DFEDTARL",
    "sofr": "SOFR",
    "ust_2y": "DGS2",
    "ust_10y": "DGS10",
    "us_real_10y": "DFII10",
    "usd_broad": "DTWEXBGS",
    "usd_cny": "DEXCHUS",
    "vix": "VIXCLS",
    "spx": "SP500",
    "nasdaq_100": "NASDAQ100",
}

OVERSEAS_MACRO_ALPHA_SERIES: Dict[str, Dict[str, str]] = {
    "gold": {"fn": "GOLD_SILVER_HISTORY", "symbol": "GOLD", "interval": "daily"},
    "wti": {"fn": "WTI", "interval": "daily"},
    "brent": {"fn": "BRENT", "interval": "daily"},
    "copper": {"fn": "COPPER", "interval": "monthly"},
}


@dataclass(slots=True)
class Settings:
    fred_api_key: str = field(default_factory=lambda: os.getenv("FRED_API_KEY", ""))
    alpha_vantage_api_key: str = field(default_factory=lambda: os.getenv("ALPHA_VANTAGE_API_KEY", ""))
    clasprc_json: str = field(default_factory=lambda: os.getenv("CLASPRC_JSON", ""))
    gas_script_id: str = field(default_factory=lambda: os.getenv("GAS_SCRIPT_ID", ""))
    google_application_credentials: str = field(default_factory=lambda: os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""))
    google_service_account_json: str = field(default_factory=lambda: os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", ""))
    google_sheet_id: str = field(default_factory=lambda: os.getenv("GOOGLE_SHEET_ID", ""))
    sqlite_path: str = field(default_factory=lambda: os.getenv("SQLITE_PATH", "./data/macro_signals.db"))
    bark_device_key: str = field(default_factory=lambda: os.getenv("BARK_DEVICE_KEY", ""))
    jisilu_cookie: str = field(default_factory=lambda: os.getenv("JISILU_COOKIE", ""))
    jisilu_user_agent: str = field(default_factory=lambda: os.getenv(
        "JISILU_USER_AGENT",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    ))
    http_timeout_seconds: float = field(default_factory=lambda: float(os.getenv("HTTP_TIMEOUT_SECONDS", "30")))
    http_retry_count: int = field(default_factory=lambda: int(os.getenv("HTTP_RETRY_COUNT", "3")))
    http_retry_sleep_seconds: float = field(default_factory=lambda: float(os.getenv("HTTP_RETRY_SLEEP_SECONDS", "1.2")))
    tz: str = field(default_factory=lambda: os.getenv("TZ", "Asia/Shanghai"))


settings = Settings()
