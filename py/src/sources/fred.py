from __future__ import annotations

from typing import Dict

from src.core.config import OVERSEAS_MACRO_FRED_SERIES, settings
from src.core.models import Observation
from src.core.utils import norm_ymd, now_text, to_float

from ._base import BaseSource, FetchResult

FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"


class FredSource(BaseSource):
    """FRED 来源。

    这是海外宏观数据里最稳定的一组官方/准官方时间序列来源。
    在系统里它承担“海外宏观基础骨架”的角色，和 Alpha Vantage 这种补商品/
    市场数据的来源是并列关系，不应该混成一个类。

    access kind:
    - `api`
    - 典型特征是参数明确、返回 JSON 结构稳定、主要风险集中在 API key 和配额

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
        # FRED 偶尔会在最新几条里给 "." 之类空值，所以会向后找第一个可用观测。
        for obs in observations:
            value = to_float(obs.get("value"))
            if value is None:
                continue
            return Observation(date=norm_ymd(obs.get("date")), value=value, source=f"FRED:{series_id}")
        raise ValueError(f"FRED no valid observation: {series_id}")

    def fetch_fred_snapshot(self) -> Dict[str, Observation]:
        return {
            field: self.fetch_latest_observation(series_id)
            for field, series_id in OVERSEAS_MACRO_FRED_SERIES.items()
        }

    def fetch_fred_result(self) -> FetchResult[Dict[str, Observation]]:
        """统一返回 FetchResult，供 job/store 层复用。"""

        payload = self.fetch_fred_snapshot()
        return FetchResult(
            payload=payload,
            source_url=FRED_OBSERVATIONS_URL,
            meta=self.build_fetch_meta(
                provider="FRED",
                fetched_at=now_text(),
                params={"series_count": len(OVERSEAS_MACRO_FRED_SERIES)},
                page_info={"resolved_count": len(payload)},
                raw_sample={key: value.date for key, value in payload.items()},
            ),
        )
