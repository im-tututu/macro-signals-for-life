from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

from src.core.config import settings
from src.core.notify import build_notifier
from src.core.utils import now_text, today_ymd

from .base import BaseSource, FetchResult

JISILU_ETF_LIST_URL = "https://www.jisilu.cn/data/etf/etf_list/"


class JisiluSource(BaseSource):
    """集思录 ETF 来源。"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._cookie = settings.jisilu_cookie
        self._notifier = build_notifier(logging.getLogger(__name__))
        self._cookie_expired_notified = False
        self._apply_cookie()
        self.http.session.headers["User-Agent"] = settings.jisilu_user_agent
        self.http.session.headers["Referer"] = "https://www.jisilu.cn/data/etf/etf_list/"
        self.http.session.headers["X-Requested-With"] = "XMLHttpRequest"

    def _apply_cookie(self) -> None:
        if self._cookie:
            self.http.session.headers["Cookie"] = self._cookie
        elif "Cookie" in self.http.session.headers:
            del self.http.session.headers["Cookie"]

    @staticmethod
    def _detect_tourist_limit(raw_text: str, payload: Dict[str, Any]) -> bool:
        parts: list[str] = []
        if raw_text.strip():
            parts.append(raw_text)
        for key in ("err_msg", "message", "msg", "notice"):
            value = payload.get(key)
            if value not in (None, ""):
                parts.append(str(value))
        merged = " | ".join(parts)
        return "游客仅显示前 20 条" in merged or "请登录查看完整列表数据" in merged

    @staticmethod
    def _looks_like_tourist_rows(rows: List[Dict[str, Any]], records: Any) -> bool:
        if not rows or len(rows) > 20:
            return False
        try:
            total = int(float(records))
        except (TypeError, ValueError):
            total = None
        return total is not None and total > len(rows)

    def _refresh_cookie_from_webhook(self) -> str:
        if not settings.jisilu_refresh_url:
            raise RuntimeError("missing JISILU_REFRESH_URL")

        headers: Dict[str, str] = {}
        if settings.jisilu_refresh_token:
            headers["Authorization"] = f"Bearer {settings.jisilu_refresh_token}"
        headers["Content-Type"] = "application/json"

        text = self.http.post_text(
            settings.jisilu_refresh_url,
            headers=headers,
            data=json.dumps(
                {
                    "source": "python",
                    "action": "refresh_cookie",
                    "site": "jisilu",
                }
            ),
        )
        payload = json.loads(text)
        cookie = str((payload or {}).get("cookie") or "").strip()
        if not cookie:
            raise RuntimeError("refresh webhook returned empty cookie")
        self._cookie = cookie
        self._apply_cookie()
        os.environ["JISILU_COOKIE"] = cookie
        self._cookie_expired_notified = False
        return cookie

    def _notify_cookie_expired(self) -> None:
        if self._cookie_expired_notified:
            return
        self._notifier.notify(
            "Jisilu Cookie 可能失效",
            "ETF 抓取检测到游客 20 条限制，已尝试刷新 cookie。若持续失败，请切换 JISILU_HEADLESS=0 并检查 webhook 登录。",
            level="WARNING",
        )
        self._cookie_expired_notified = True

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
        response = self.http.request("GET", JISILU_ETF_LIST_URL, params=params)
        raw_text = response.text
        payload = response.json()
        rows = payload.get("rows", []) or []
        records = payload.get("records", payload.get("total", payload.get("total_rows", "")))
        if self._detect_tourist_limit(raw_text, payload) or self._looks_like_tourist_rows(rows, records):
            self._notify_cookie_expired()
            raise RuntimeError("Jisilu tourist limited")
        return payload

    def fetch_etf_index_all(
        self,
        *,
        snapshot_date: str | None = None,
        rows_per_page: int = 500,
        max_pages: int = 20,
        min_unit_total_yi: float | int | str = 2,
        min_volume_wan: float | int | str = "",
        extra_query_string: str = "",
    ) -> Dict[str, Any]:
        refreshed = False
        while True:
            try:
                return self._fetch_etf_index_all_once(
                    snapshot_date=snapshot_date,
                    rows_per_page=rows_per_page,
                    max_pages=max_pages,
                    min_unit_total_yi=min_unit_total_yi,
                    min_volume_wan=min_volume_wan,
                    extra_query_string=extra_query_string,
                )
            except RuntimeError as exc:
                if refreshed or "Jisilu tourist limited" not in str(exc):
                    raise
                self._refresh_cookie_from_webhook()
                refreshed = True

    def _fetch_etf_index_all_once(
        self,
        *,
        snapshot_date: str | None = None,
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
            "snapshot_date": snapshot_date or today_ymd(),
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
        snapshot_date: str | None = None,
        rows_per_page: int = 500,
        max_pages: int = 20,
        min_unit_total_yi: float | int | str = 2,
        min_volume_wan: float | int | str = "",
        extra_query_string: str = "",
    ) -> FetchResult[Dict[str, Any]]:
        """统一返回 FetchResult，供 store/job 层复用。"""

        payload = self.fetch_etf_index_all(
            snapshot_date=snapshot_date,
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
