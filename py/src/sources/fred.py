from __future__ import annotations

from typing import Dict

from src.core.config import OVERSEAS_MACRO_FRED_SERIES, settings
from src.core.models import Observation
from src.core.utils import norm_ymd, to_float

from .base import BaseSource, FetchResult

FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"


class FredSource(BaseSource):
    """FRED 来源。

    只负责：
    - 调 FRED API
    - 解析 observation
    - 返回字段 -> Observation 的结果
    """

    def fetch_latest_observation(self, series_id: str, api_key: str | None = None) -> Observation:
        api_key = api_key or settings.fred_api_key
        if not api_key:
            raise ValueError("Missing FRED_API_KEY")
        params = {
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 10,
        }
        data = self.http.get_json(FRED_OBSERVATIONS_URL, params=params)
        observations = data.get("observations", [])
        if not observations:
            raise ValueError(f"FRED observations empty: {series_id}")
        for obs in observations:
            value = to_float(obs.get("value"))
            if value is None:
                continue
            return Observation(date=norm_ymd(obs.get("date")), value=value, source=f"FRED:{series_id}")
        raise ValueError(f"FRED no valid observation: {series_id}")

    def fetch_overseas_macro(self) -> Dict[str, Observation]:
        return {
            field: self.fetch_latest_observation(series_id)
            for field, series_id in OVERSEAS_MACRO_FRED_SERIES.items()
        }

    def fetch_overseas_macro_result(self) -> FetchResult[Dict[str, Observation]]:
        """统一返回 FetchResult，供 job/store 层复用。"""

        return FetchResult(
            payload=self.fetch_overseas_macro(),
            source_url=FRED_OBSERVATIONS_URL,
            meta={"provider": "FRED"},
        )
