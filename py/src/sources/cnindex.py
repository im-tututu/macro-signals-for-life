from __future__ import annotations

import json
from typing import Any, Dict

from src.core.models import BondIndexSnapshot
from src.core.utils import now_text, norm_ymd, to_float, today_ymd

from ._base import BaseSource
from ._base import FetchResult

CNI_BOND_FEATURE_URL = "https://www.cnindex.com.cn/module/index-detail.html?act_menu=1&indexCode={code}"


class CnindexBondSource(BaseSource):
    """国证公司债券指数特征来源。"""

    def fetch_bond_feature(self, code: str) -> Dict[str, Any]:
        url = CNI_BOND_FEATURE_URL.format(code=code)
        text = self.http.get_text(url, headers={"Accept": "application/json, text/plain, */*"})
        try:
            parsed = json.loads(text)
        except Exception:  # noqa: BLE001
            return {"raw": text}
        return parsed.get("data", parsed)

    def fetch_feature_snapshot(self, code: str) -> BondIndexSnapshot:
        payload = self.fetch_bond_feature(code)
        source_date = norm_ymd(payload.get("date") or payload.get("tradeDate") or payload.get("trade_date"))
        date = source_date or today_ymd()
        normalized_payload = payload if isinstance(payload, dict) else {"raw": payload}
        return BondIndexSnapshot(
            date=date,
            index_id=code,
            duration=to_float(normalized_payload.get("dm")),
            ytm=to_float(normalized_payload.get("y")),
            cons_number=to_float(normalized_payload.get("consNumber") or normalized_payload.get("cons_number")),
            modified_duration=to_float(normalized_payload.get("d")),
            convexity=to_float(normalized_payload.get("v")),
            source=CNI_BOND_FEATURE_URL,
            meta={
                "source_date": source_date,
                "payload": normalized_payload,
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
            source_url=CNI_BOND_FEATURE_URL.format(code=code),
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
