from __future__ import annotations

from typing import Any, Dict

from src.core.models import BondIndexSnapshot
from src.core.utils import now_text, norm_ymd, to_float, today_ymd

from ._base import BaseSource
from ._base import FetchResult

CNI_BOND_INCOME_URL = "https://www.cnindex.com.cn/index-income?indexcode={code}"
CNI_BOND_ANALYSIS_URL = "https://www.cnindex.com.cn/indexAnalysis/getIndexAnalysis?indexCode={code}"


class CnindexBondSource(BaseSource):
    """国证公司债券指数特征来源。

    国证债券指数的“日期/收益表现”和“分析指标”分散在两个 JSON 接口：
    - `index-income`: 近期收益表现、波动率、calDate/deadline
    - `getIndexAnalysis`: 到期收益率、修正久期、凸性、平均剩余期限、市值

    因此这里改成组合抓取，再统一映射成一条 `BondIndexSnapshot`。
    """

    def fetch_income_payload(self, code: str) -> Dict[str, Any]:
        url = CNI_BOND_INCOME_URL.format(code=code)
        data = self.http.get_json(url, headers={"Accept": "application/json, text/plain, */*"})
        return data.get("data", data)

    def fetch_analysis_payload(self, code: str) -> Dict[str, Any]:
        url = CNI_BOND_ANALYSIS_URL.format(code=code)
        data = self.http.get_json(url, headers={"Accept": "application/json, text/plain, */*"})
        return data.get("data", data)

    def fetch_feature_snapshot(self, code: str) -> BondIndexSnapshot:
        income_payload = self.fetch_income_payload(code)
        analysis_payload = self.fetch_analysis_payload(code)

        source_date = norm_ymd(
            income_payload.get("calDate")
            or income_payload.get("deadline")
            or analysis_payload.get("genDate")
            or analysis_payload.get("date")
            or analysis_payload.get("tradeDate")
            or analysis_payload.get("trade_date")
        )
        date = source_date or today_ymd()

        return BondIndexSnapshot(
            date=date,
            index_id=code,
            duration=to_float(analysis_payload.get("avgResidualMaturity")),
            ytm=to_float(analysis_payload.get("yieldMaturity")),
            cons_number=to_float(
                analysis_payload.get("consNumber")
                or analysis_payload.get("cons_number")
                or income_payload.get("consNumber")
                or income_payload.get("cons_number")
            ),
            modified_duration=to_float(analysis_payload.get("modifiedDuration")),
            convexity=to_float(analysis_payload.get("convexity")),
            total_market_value=to_float(analysis_payload.get("totalMarketValue")),
            avg_compensation_period=None,
            source=CNI_BOND_ANALYSIS_URL,
            meta={
                "income_payload": income_payload,
                "analysis_payload": analysis_payload,
            },
        )

    def fetch_feature_snapshot_result(
        self,
        code: str,
        *,
        index_name: str | None = None,
        index_code: str | None = None,
    ) -> FetchResult[BondIndexSnapshot]:
        snapshot = self.fetch_feature_snapshot(code)
        fetched_at = now_text()
        return FetchResult(
            payload=snapshot,
            source_url=CNI_BOND_ANALYSIS_URL.format(code=code),
            meta=self.build_fetch_meta(
                provider="CNINDEX",
                biz_date=snapshot.date,
                fetched_at=fetched_at,
                params={
                    "index_id": code,
                    "index_name": index_name or code,
                    "index_code": index_code or code,
                },
                raw_sample=snapshot.meta,
                extra={
                    "index_name": index_name or code,
                    "index_code": index_code or code,
                },
            ),
        )
