from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, Iterable, Optional

from src.core.config import OVERSEAS_MACRO_ALPHA_SERIES, settings
from src.core.models import FuturesSnapshot, Observation
from src.core.utils import now_text, norm_ymd, to_float, today_ymd

from .base import BaseSource, FetchResult

ALPHA_VANTAGE_QUERY_URL = "https://www.alphavantage.co/query"
SINA_FUTURES_QUOTE_URL = "https://hq.sinajs.cn/list="
SGE_HOME_URL = "https://www.sge.com.cn/"
BOC_DEPOSIT_RATE_URL = "https://www.bankofchina.com/fimarkets/lilv/fd31/"
MONEY_FUND_PROXY_URL = "https://fundf10.eastmoney.com/jjjz_000198.html"


class ExternalMiscSource(BaseSource):
    """零散外部来源。

    当前重点覆盖：
    - Alpha Vantage 商品数据
    - 新浪国债期货报价
    """

    def build_alpha_vantage_url(self, spec: Dict[str, str], api_key: str | None = None) -> str:
        api_key = api_key or settings.alpha_vantage_api_key
        if not api_key:
            raise ValueError("Missing ALPHA_VANTAGE_API_KEY")
        parts = [f"function={spec['fn']}", f"apikey={api_key}"]
        if spec.get("symbol"):
            parts.append(f"symbol={spec['symbol']}")
        if spec.get("interval"):
            parts.append(f"interval={spec['interval']}")
        return f"{ALPHA_VANTAGE_QUERY_URL}?{'&'.join(parts)}"

    @staticmethod
    def extract_alpha_numeric_value(obs: Dict[str, Any]) -> Optional[float]:
        for key in ("value", "price", "close", "gold", "silver"):
            value = to_float(obs.get(key))
            if value is not None:
                return value
        return None

    def fetch_alpha_vantage_latest_observation(self, spec: Dict[str, str], api_key: str | None = None) -> Observation:
        url = self.build_alpha_vantage_url(spec, api_key)
        data = self.http.get_json(url)
        if data.get("Error Message"):
            raise ValueError(f"Alpha Vantage error: {spec['fn']} | {data['Error Message']}")
        if data.get("Information"):
            raise ValueError(f"Alpha Vantage information: {spec['fn']} | {data['Information']}")
        if data.get("Note"):
            raise ValueError(f"Alpha Vantage note: {spec['fn']} | {data['Note']}")
        rows = data.get("data", [])
        if not rows:
            raise ValueError(f"Alpha Vantage data empty: {spec['fn']}")
        for obs in rows:
            value = self.extract_alpha_numeric_value(obs)
            if value is None:
                continue
            return Observation(
                date=norm_ymd(obs.get("date")),
                value=value,
                source=f"ALPHA_VANTAGE:{spec['fn']}:{spec.get('interval', '')}".rstrip(":"),
                meta={"raw": obs},
            )
        sample = rows[0] if rows else {}
        raise ValueError(f"Alpha Vantage no valid observation: {spec['fn']} | sample_keys={list(sample.keys())}")

    def fetch_overseas_macro_from_alpha_vantage(self) -> Dict[str, Observation | None]:
        out: Dict[str, Observation | None] = {}
        items = list(OVERSEAS_MACRO_ALPHA_SERIES.items())
        for idx, (field, spec) in enumerate(items):
            try:
                out[field] = self.fetch_alpha_vantage_latest_observation(spec)
            except Exception:  # noqa: BLE001
                out[field] = None
            if idx < len(items) - 1:
                time.sleep(1.2)
        return out

    def fetch_overseas_macro_from_alpha_vantage_result(self) -> FetchResult[Dict[str, Observation | None]]:
        """统一返回 FetchResult，供 job/store 层复用。"""

        return FetchResult(
            payload=self.fetch_overseas_macro_from_alpha_vantage(),
            source_url=ALPHA_VANTAGE_QUERY_URL,
            meta={"provider": "ALPHA_VANTAGE"},
        )

    def fetch_sina_price(self, symbol: str | Iterable[str]) -> Optional[float]:
        candidates = [symbol] if isinstance(symbol, str) else list(symbol)
        expanded: list[str] = []
        for item in candidates:
            expanded.extend([item, f"nf_{item}"])
        for candidate in expanded:
            try:
                text = self.http.get_text(
                    SINA_FUTURES_QUOTE_URL + candidate,
                    headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn/"},
                )
            except Exception:  # noqa: BLE001
                continue
            m = re.search(r'="([^"]*)"', text)
            if not m:
                continue
            arr = m.group(1).split(",")
            if len(arr) > 3:
                value = to_float(arr[3])
                if value and value > 0:
                    return value
            for item in arr:
                value = to_float(item)
                if value and value > 0:
                    return value
        return None

    def fetch_bond_futures_snapshot(self) -> FuturesSnapshot:
        return FuturesSnapshot(
            date=today_ymd(),
            source="hq.sinajs.cn",
            values={"t0_last": self.fetch_sina_price("T0"), "tf0_last": self.fetch_sina_price("TF0")},
            fetched_at=now_text(),
        )

    def fetch_bond_futures_result(self) -> FetchResult[FuturesSnapshot]:
        """统一返回 FetchResult，供 store/job 层复用。"""

        return FetchResult(
            payload=self.fetch_bond_futures_snapshot(),
            source_url=SINA_FUTURES_QUOTE_URL,
            meta={"provider": "SINA_FUTURES"},
        )

    def fetch_sge_gold_snapshot(self) -> Dict[str, Any]:
        return {"date": "", "gold_cny": None, "source": SGE_HOME_URL, "implemented": False}

    def fetch_boc_deposit_1y_snapshot(self) -> Dict[str, Any]:
        return {"date": "", "deposit_1y": None, "source": BOC_DEPOSIT_RATE_URL, "implemented": False}

    def fetch_money_fund_7d_snapshot(self) -> Dict[str, Any]:
        return {"date": "", "money_fund_7d": None, "source": MONEY_FUND_PROXY_URL, "implemented": False}

    def fetch_sge_gold_result(self) -> FetchResult[Dict[str, Any]]:
        return FetchResult(payload=self.fetch_sge_gold_snapshot(), source_url=SGE_HOME_URL, meta={"provider": "SGE", "placeholder": True})

    def fetch_boc_deposit_1y_result(self) -> FetchResult[Dict[str, Any]]:
        return FetchResult(payload=self.fetch_boc_deposit_1y_snapshot(), source_url=BOC_DEPOSIT_RATE_URL, meta={"provider": "BOC", "placeholder": True})

    def fetch_money_fund_7d_result(self) -> FetchResult[Dict[str, Any]]:
        return FetchResult(payload=self.fetch_money_fund_7d_snapshot(), source_url=MONEY_FUND_PROXY_URL, meta={"provider": "MONEY_FUND", "placeholder": True})
