from __future__ import annotations

from typing import Dict

from core.config import OVERSEAS_MACRO_FRED_SERIES, settings
from core.models import Observation
from core.utils import norm_ymd, to_float

from .base import BaseSource

FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"


class FredSource(BaseSource):
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
