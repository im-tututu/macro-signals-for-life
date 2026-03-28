from __future__ import annotations

import time
from typing import Any

from src.core.config import OVERSEAS_MACRO_ALPHA_SERIES, settings
from src.core.models import Observation
from src.core.utils import norm_ymd, now_text, to_float

from .base import BaseSource, FetchResult

ALPHA_VANTAGE_QUERY_URL = "https://www.alphavantage.co/query"


class AlphaVantageSource(BaseSource):
    """Alpha Vantage 来源。

    当前主要承接海外宏观里那部分由 Alpha Vantage 提供的商品/市场数据。

    它和 FRED 的分工是互补而不是替代：
    - FRED 更偏利率/官方经济序列；
    - Alpha Vantage 更适合补商品、汇率和部分市场价格。
    """

    def build_query_url(self, spec: dict[str, str], api_key: str | None = None) -> str:
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
    def extract_numeric_value(obs: dict[str, Any]) -> float | None:
        for key in ("value", "price", "close", "gold", "silver"):
            value = to_float(obs.get(key))
            if value is not None:
                return value
        return None

    def fetch_latest_observation(self, spec: dict[str, str], api_key: str | None = None) -> Observation:
        url = self.build_query_url(spec, api_key)
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

        # 同一函数返回的字段名并不完全统一，所以先走“抽取第一个可解释数值”的策略。
        for obs in rows:
            value = self.extract_numeric_value(obs)
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

    def fetch_overseas_macro(self) -> dict[str, Observation | None]:
        out: dict[str, Observation | None] = {}
        items = list(OVERSEAS_MACRO_ALPHA_SERIES.items())
        for idx, (field, spec) in enumerate(items):
            try:
                out[field] = self.fetch_latest_observation(spec)
            except Exception:  # noqa: BLE001
                # 这里故意降级为 None，而不是整批失败；
                # 海外宏观拼表时，个别商品源失败不应拖垮整张表。
                out[field] = None
            if idx < len(items) - 1:
                # Alpha Vantage 免费额度比较紧，来源层主动节流比让 job 层硬等更自然。
                time.sleep(1.2)
        return out

    def fetch_overseas_macro_result(self) -> FetchResult[dict[str, Observation | None]]:
        payload = self.fetch_overseas_macro()
        return FetchResult(
            payload=payload,
            source_url=ALPHA_VANTAGE_QUERY_URL,
            meta=self.build_fetch_meta(
                provider="ALPHA_VANTAGE",
                fetched_at=now_text(),
                params={"series_count": len(OVERSEAS_MACRO_ALPHA_SERIES)},
                page_info={"resolved_count": sum(1 for item in payload.values() if item is not None)},
                raw_sample={
                    key: value.meta.get("raw")
                    for key, value in payload.items()
                    if value is not None and value.meta.get("raw") is not None
                },
            ),
        )
