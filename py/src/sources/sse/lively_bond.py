from __future__ import annotations

import json
import re
import time
from typing import Any

from src.core.models import SseLivelyBondRowSnapshot, SseLivelyBondSnapshot
from src.core.utils import norm_ymd
from src.core.utils import now_text, today_ymd

from ..base import BaseSource, FetchResult

SSE_LIVELY_BOND_REFERER_URL = "https://www.sse.com.cn/market/bonddata/livelybond/"
SSE_COMMON_QUERY_URL = "https://query.sse.com.cn/commonQuery.do"
SSE_LIVELY_BOND_SQL_ID = "COMMON_SSE_SJ_ZQSJ_ZQHYPZ_L"


def _parse_jsonp(text: str) -> dict[str, Any]:
    match = re.search(r"^[^(]+\((.*)\)\s*;?\s*$", text.strip(), flags=re.S)
    if not match:
        raise RuntimeError("SSE lively bond response is not valid JSONP")
    return json.loads(match.group(1))


class SseLivelyBondSource(BaseSource):
    """上交所国债活跃品种来源。

    access kind:
    - `xhr_json`
    - 实际返回为 JSONP，需要先剥离回调包装后再解析 JSON
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.http.session.headers["Referer"] = SSE_LIVELY_BOND_REFERER_URL
        self.http.session.headers["User-Agent"] = "Mozilla/5.0"
        self.http.session.headers["Accept"] = "*/*"

    @staticmethod
    def build_query_params(*, page_no: int = 1, page_size: int = 25) -> dict[str, Any]:
        callback_name = "jsonpCallback"
        return {
            "jsonCallBack": callback_name,
            "isPagination": "true",
            "pageHelp.pageSize": page_size,
            "pageHelp.pageNo": page_no,
            "pageHelp.beginPage": 1,
            "pageHelp.cacheSize": 1,
            "pageHelp.endPage": 1,
            "rankCondition": "",
            "sqlId": SSE_LIVELY_BOND_SQL_ID,
            "START_DATE": "",
            "END_DATE": "",
            "MAX_TRADE_DATE": 1,
            "SUM_TRADE_VOL_DESC": 1,
            "_": int(time.time() * 1000),
        }

    def fetch_lively_bond_page(
        self,
        *,
        page_no: int = 1,
        page_size: int = 25,
    ) -> dict[str, Any]:
        text = self.http.get_text(
            SSE_COMMON_QUERY_URL,
            params=self.build_query_params(page_no=page_no, page_size=page_size),
        )
        return _parse_jsonp(text)

    @staticmethod
    def _payload_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
        for key in ("result", "pageHelp", "data", "rows"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
            if isinstance(value, dict):
                for nested_key in ("data", "result", "rows"):
                    nested_value = value.get(nested_key)
                    if isinstance(nested_value, list):
                        return [item for item in nested_value if isinstance(item, dict)]
        return []

    @staticmethod
    def _payload_total(payload: dict[str, Any], default: int) -> int:
        candidates: list[Any] = [
            payload.get("total"),
            payload.get("totalCount"),
            payload.get("resultNum"),
        ]
        page_help = payload.get("pageHelp")
        if isinstance(page_help, dict):
            candidates.extend(
                [
                    page_help.get("total"),
                    page_help.get("totalCount"),
                    page_help.get("dataCount"),
                ]
            )
        for value in candidates:
            try:
                return int(float(value))
            except (TypeError, ValueError):
                continue
        return default

    def fetch_lively_bond_all(self, *, page_size: int = 100, max_pages: int = 20) -> SseLivelyBondSnapshot:
        all_rows: list[dict[str, Any]] = []
        total_count = 0
        last_payload: dict[str, Any] = {}
        for page_no in range(1, max_pages + 1):
            payload = self.fetch_lively_bond_page(page_no=page_no, page_size=page_size)
            last_payload = payload
            rows = self._payload_rows(payload)
            if not rows:
                break
            all_rows.extend(rows)
            total_count = self._payload_total(payload, len(all_rows))
            if len(all_rows) >= total_count:
                break
            if len(rows) < page_size:
                break

        fetched_at = now_text()
        row_snapshots: list[SseLivelyBondRowSnapshot] = []
        for row in self.require_rows(all_rows):
            trade_date = self.require_text(norm_ymd(row.get("TRADE_DATE")), field_name="trade_date")
            bond_id = self.require_text(str(row.get("SEC_CODE") or ""), field_name="bond_id")
            row_snapshots.append(
                SseLivelyBondRowSnapshot(
                    trade_date=trade_date,
                    bond_id=bond_id,
                    fields=dict(row),
                )
            )
        snapshot_date = max(item.trade_date for item in row_snapshots)
        return SseLivelyBondSnapshot(
            snapshot_date=snapshot_date,
            fetched_at=fetched_at,
            source_url=SSE_COMMON_QUERY_URL,
            rows=row_snapshots,
            total_count=total_count or len(row_snapshots),
            meta={
                "raw": last_payload,
                "page_size": page_size,
                "row_count": len(row_snapshots),
            },
        )

    def fetch_lively_bond_all_result(self, *, page_size: int = 100, max_pages: int = 20) -> FetchResult[SseLivelyBondSnapshot]:
        payload = self.fetch_lively_bond_all(page_size=page_size, max_pages=max_pages)
        return FetchResult(
            payload=payload,
            source_url=SSE_COMMON_QUERY_URL,
            meta=self.build_fetch_meta(
                provider="SSE",
                biz_date=payload.snapshot_date,
                fetched_at=payload.fetched_at,
                params={
                    "category": "lively_bond",
                    "page_size": page_size,
                    "max_pages": max_pages,
                },
                page_info={
                    "row_count": len(payload.rows),
                    "total_count": payload.total_count,
                },
                raw_sample=payload.meta.get("raw"),
                extra={"category": "lively_bond"},
            ),
        )
