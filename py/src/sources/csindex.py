from __future__ import annotations

from typing import Any, Dict

from src.core.models import BondIndexSnapshot
from src.core.utils import now_text, norm_ymd, to_float, today_ymd

from ._base import BaseSource
from ._base import FetchResult

CSI_BOND_FEATURE_URL = "https://www.csindex.com.cn/csindex-home/perf/get-bond-index-feature/{code}"


class CsindexBondSource(BaseSource):
    """中证公司债券指数特征来源。"""

    def fetch_bond_feature(self, code: str) -> Dict[str, Any]:
        url = CSI_BOND_FEATURE_URL.format(code=code)
        data = self.http.get_json(url, headers={"Accept": "application/json, text/plain, */*"})
        return data.get("data", data)

    def fetch_feature_snapshot(self, code: str) -> BondIndexSnapshot:
        payload = self.fetch_bond_feature(code)
        source_date = norm_ymd(payload.get("date") or payload.get("tradeDate") or payload.get("trade_date"))
        date = source_date or today_ymd()
        return BondIndexSnapshot(
            date=date,
            index_id=code,
            duration=to_float(payload.get("dm")),
            ytm=to_float(payload.get("y")),
            cons_number=to_float(payload.get("consNumber") or payload.get("cons_number")),
            modified_duration=to_float(payload.get("d")),
            convexity=to_float(payload.get("v")),
            source=CSI_BOND_FEATURE_URL,
            meta={
                "source_date": source_date,
                "payload": payload if isinstance(payload, dict) else {"raw": payload},
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
            source_url=CSI_BOND_FEATURE_URL.format(code=code),
            meta=self.build_fetch_meta(
                provider="CSINDEX",
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
