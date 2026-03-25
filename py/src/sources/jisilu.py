from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from src.core.config import settings
from src.core.utils import now_text, today_ymd

from .base import BaseSource, FetchResult

JISILU_ETF_LIST_URL = "https://www.jisilu.cn/data/etf/etf_list/"


class JisiluSource(BaseSource):
    """集思录 ETF 来源。"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if settings.jisilu_cookie:
            self.http.session.headers["Cookie"] = settings.jisilu_cookie
        self.http.session.headers["User-Agent"] = settings.jisilu_user_agent
        self.http.session.headers["Referer"] = "https://www.jisilu.cn/data/etf/etf_list/"
        self.http.session.headers["X-Requested-With"] = "XMLHttpRequest"

    def fetch_etf_index_page(
        self,
        *,
        page: int = 1,
        rows_per_page: int = 500,
        min_unit_total_yi: float | int | str = 2,
        min_volume_wan: float | int | str = "",
        extra_query_string: str = "",
    ) -> Dict[str, Any]:
        params = {
            "___jsl": f"LST___t={int(time.time() * 1000)}",
            "rp": rows_per_page,
            "page": page,
            "unit_total": min_unit_total_yi,
            "volume": min_volume_wan,
        }
        if extra_query_string:
            params["extra"] = extra_query_string
        return self.http.get_json(JISILU_ETF_LIST_URL, params=params)

    def fetch_etf_index_all(
        self,
        *,
        rows_per_page: int = 500,
        max_pages: int = 20,
        min_unit_total_yi: float | int | str = 2,
        min_volume_wan: float | int | str = "",
        extra_query_string: str = "",
    ) -> Dict[str, Any]:
        all_rows: List[Dict[str, Any]] = []
        records_total: Any = ""
        last_payload: Dict[str, Any] = {}
        seen_fund_ids: set[str] = set()
        for page in range(1, max_pages + 1):
            payload = self.fetch_etf_index_page(
                page=page,
                rows_per_page=rows_per_page,
                min_unit_total_yi=min_unit_total_yi,
                min_volume_wan=min_volume_wan,
                extra_query_string=extra_query_string,
            )
            last_payload = payload
            rows = payload.get("rows", [])
            records_total = payload.get("records", records_total)
            if not rows:
                break

            page_unique_rows: List[Dict[str, Any]] = []
            page_new_ids = 0
            for row in rows:
                cell = row.get("cell", row) if isinstance(row, dict) else {}
                fund_id = str(cell.get("fund_id") or row.get("id") or "").strip()
                dedupe_key = fund_id or str(row)
                if dedupe_key in seen_fund_ids:
                    continue
                seen_fund_ids.add(dedupe_key)
                page_unique_rows.append(row)
                page_new_ids += 1

            all_rows.extend(page_unique_rows)

            # 集思录偶尔会重复返回前一页数据；若当前页没有任何新增 id，则提前停止。
            if page_new_ids == 0:
                break

            try:
                expected_total = int(float(records_total))
            except (TypeError, ValueError):
                expected_total = None

            if expected_total is not None and len(seen_fund_ids) >= expected_total:
                break

            if len(rows) < rows_per_page:
                break
        return {
            "snapshot_date": today_ymd(),
            "fetched_at": now_text(),
            "records": records_total,
            "rows": all_rows,
            "last_url": JISILU_ETF_LIST_URL,
            "raw": last_payload,
            "unique_rows": len(all_rows),
        }

    def fetch_etf_index_all_result(
        self,
        *,
        rows_per_page: int = 500,
        max_pages: int = 20,
        min_unit_total_yi: float | int | str = 2,
        min_volume_wan: float | int | str = "",
        extra_query_string: str = "",
    ) -> FetchResult[Dict[str, Any]]:
        """统一返回 FetchResult，供 store/job 层复用。"""

        payload = self.fetch_etf_index_all(
            rows_per_page=rows_per_page,
            max_pages=max_pages,
            min_unit_total_yi=min_unit_total_yi,
            min_volume_wan=min_volume_wan,
            extra_query_string=extra_query_string,
        )
        return FetchResult(
            payload=payload,
            source_url=JISILU_ETF_LIST_URL,
            meta={
                "rows_per_page": rows_per_page,
                "max_pages": max_pages,
                "min_unit_total_yi": min_unit_total_yi,
                "min_volume_wan": min_volume_wan,
            },
        )
