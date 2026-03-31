from __future__ import annotations

from typing import Any

from src.core.models import AkshareBondZhUsRateRow
from src.core.utils import norm_ymd, now_text, to_float

from .._base import BaseSource, FetchResult

AKSHARE_BOND_ZH_US_RATE_URL = "https://www.akshare.xyz/data/bond/bond.html"
AKSHARE_BOND_ZH_US_RATE_DEFAULT_START_DATE = "19901219"


class AkshareBondZhUsRateSource(BaseSource):
    """通过 akshare 抓取中美利率与 GDP 年增率对比序列。"""

    COLUMN_MAP: dict[str, str] = {
        "中国国债收益率2年": "cn_2y",
        "中国国债收益率5年": "cn_5y",
        "中国国债收益率10年": "cn_10y",
        "中国国债收益率30年": "cn_30y",
        "中国国债收益率10年-2年": "cn_10y_2y",
        "中国GDP年增率": "cn_gdp_yoy",
        "美国国债收益率2年": "us_2y",
        "美国国债收益率5年": "us_5y",
        "美国国债收益率10年": "us_10y",
        "美国国债收益率30年": "us_30y",
        "美国国债收益率10年-2年": "us_10y_2y",
        "美国GDP年增率": "us_gdp_yoy",
    }

    @staticmethod
    def _pick_value(record: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in record and record[key] not in (None, ""):
                return record[key]
        return None

    def fetch_history(
        self,
        *,
        start_date: str = AKSHARE_BOND_ZH_US_RATE_DEFAULT_START_DATE,
    ) -> list[AkshareBondZhUsRateRow]:
        try:
            import akshare as ak
        except ImportError as exc:
            raise RuntimeError("akshare is required for akshare_bond_zh_us_rate") from exc

        df = ak.bond_zh_us_rate(start_date=start_date)
        rows: list[AkshareBondZhUsRateRow] = []
        for raw in df.to_dict(orient="records"):
            trade_date = norm_ymd(self._pick_value(raw, "日期", "date", "Date"))
            if not trade_date:
                continue
            payload: dict[str, Any] = {"trade_date": trade_date, "meta": {"raw": raw}}
            for source_name, target_name in self.COLUMN_MAP.items():
                payload[target_name] = to_float(raw.get(source_name))
            rows.append(AkshareBondZhUsRateRow(**payload))
        return rows

    def fetch_history_result(
        self,
        *,
        start_date: str = AKSHARE_BOND_ZH_US_RATE_DEFAULT_START_DATE,
    ) -> FetchResult[list[AkshareBondZhUsRateRow]]:
        rows = self.fetch_history(start_date=start_date)
        return FetchResult(
            payload=rows,
            source_url=AKSHARE_BOND_ZH_US_RATE_URL,
            meta=self.build_fetch_meta(
                provider="AKSHARE",
                fetched_at=now_text(),
                params={"start_date": start_date, "adapter": "AKSHARE", "api": "bond_zh_us_rate"},
                page_info={"row_count": len(rows)},
                raw_sample=[row.meta.get("raw") for row in rows[:3]],
                extra={"adapter": "AKSHARE"},
            ),
        )
