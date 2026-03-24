from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from core.config import settings
from core.utils import now_text, today_ymd

from .base import BaseSource

JISILU_ETF_LIST_URL = "https://www.jisilu.cn/data/etf/etf_list/"


class JisiluSource(BaseSource):
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
            all_rows.extend(rows)
            if len(rows) < rows_per_page:
                break
        return {
            "snapshot_date": today_ymd(),
            "fetched_at": now_text(),
            "records": records_total,
            "rows": all_rows,
            "last_url": JISILU_ETF_LIST_URL,
            "raw": last_payload,
        }
