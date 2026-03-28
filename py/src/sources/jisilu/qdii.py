from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Literal

from src.core.models import JisiluQdiiRowSnapshot, JisiluQdiiSnapshot
from src.core.config import settings
from src.core.notify import build_notifier
from src.core.utils import now_text, today_ymd

from .._base import BaseSource, FetchResult

QdiiMarket = Literal["europe_america", "commodity", "asia"]

QDII_MARKET_CODE_MAP: dict[QdiiMarket, str] = {
    "europe_america": "E",
    "commodity": "C",
    "asia": "A",
}

JISILU_QDII_REFERER_URL = "https://www.jisilu.cn/data/qdii/#qdiie"
JISILU_QDII_LIST_URL = "https://www.jisilu.cn/data/qdii/qdii_list/{market_code}"


class JisiluQdiiSource(BaseSource):
    """集思录 QDII 来源。

    这里承接的是 QDII 页面族，而不是整个集思录站点。
    当前已知页面至少分成欧美市场、商品 QDII、亚洲市场三个视图，
    它们更像同一接口族下的不同 market 参数，而不是三个完全独立的来源。

    access kind:
    - `xhr_json`
    - 但仍依赖 cookie、referer 和登录态，维护方式更接近站内接口而不是开放 API。
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._cookie = settings.jisilu_cookie
        self._notifier = build_notifier(logging.getLogger(__name__))
        self._cookie_expired_notified = False
        self._apply_cookie()
        self.http.session.headers["User-Agent"] = settings.jisilu_user_agent
        self.http.session.headers["Referer"] = JISILU_QDII_REFERER_URL
        self.http.session.headers["X-Requested-With"] = "XMLHttpRequest"

    def _apply_cookie(self) -> None:
        if self._cookie:
            self.http.session.headers["Cookie"] = self._cookie
        elif "Cookie" in self.http.session.headers:
            del self.http.session.headers["Cookie"]

    @staticmethod
    def market_code(market: QdiiMarket) -> str:
        return QDII_MARKET_CODE_MAP[market]

    def build_list_url(self, market: QdiiMarket) -> str:
        return JISILU_QDII_LIST_URL.format(market_code=self.market_code(market))

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

    def _notify_cookie_expired(self, market: QdiiMarket) -> None:
        if self._cookie_expired_notified:
            return
        self._notifier.notify(
            "Jisilu Cookie 可能失效",
            f"QDII 抓取检测到游客 20 条限制，market={market}，已尝试刷新 cookie。",
            level="WARNING",
        )
        self._cookie_expired_notified = True

    def fetch_qdii_page(
        self,
        *,
        market: QdiiMarket,
        page: int = 1,
        rows_per_page: int = 50,
    ) -> Dict[str, Any]:
        params = {
            "___jsl": f"LST___t={int(time.time() * 1000)}",
            "rp": rows_per_page,
            "page": page,
        }
        response = self.http.request("GET", self.build_list_url(market), params=params)
        raw_text = response.text
        payload = response.json()
        rows = payload.get("rows", []) or []
        records = payload.get("records", payload.get("total", payload.get("total_rows", "")))
        if self._detect_tourist_limit(raw_text, payload) or self._looks_like_tourist_rows(rows, records):
            self._notify_cookie_expired(market)
            raise RuntimeError("Jisilu tourist limited")
        return payload

    def fetch_qdii_all(
        self,
        *,
        market: QdiiMarket,
        snapshot_date: str | None = None,
        rows_per_page: int = 50,
        max_pages: int = 20,
    ) -> JisiluQdiiSnapshot:
        refreshed = False
        while True:
            try:
                return self._fetch_qdii_all_once(
                    market=market,
                    snapshot_date=snapshot_date,
                    rows_per_page=rows_per_page,
                    max_pages=max_pages,
                )
            except RuntimeError as exc:
                if refreshed or "Jisilu tourist limited" not in str(exc):
                    raise
                self._refresh_cookie_from_webhook()
                refreshed = True

    def _fetch_qdii_all_once(
        self,
        *,
        market: QdiiMarket,
        snapshot_date: str | None = None,
        rows_per_page: int = 50,
        max_pages: int = 20,
    ) -> JisiluQdiiSnapshot:
        all_rows: List[Dict[str, Any]] = []
        records_total: Any = ""
        last_payload: Dict[str, Any] = {}
        seen_fund_ids: set[str] = set()
        for page in range(1, max_pages + 1):
            payload = self.fetch_qdii_page(
                market=market,
                page=page,
                rows_per_page=rows_per_page,
            )
            last_payload = payload
            rows = payload.get("rows", []) or []
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

        target_snapshot_date = self.require_text(snapshot_date or today_ymd(), field_name="snapshot_date")
        market_code = self.market_code(market)
        fetched_at = now_text()
        row_snapshots: List[JisiluQdiiRowSnapshot] = []
        for row in self.require_rows(all_rows):
            cell = row.get("cell", row) if isinstance(row, dict) else {}
            fund_id = self.require_text(str(cell.get("fund_id") or row.get("id") or ""), field_name="fund_id")
            row_snapshots.append(
                JisiluQdiiRowSnapshot(
                    market=market,
                    market_code=market_code,
                    fund_id=fund_id,
                    cell=dict(cell),
                    raw_row=dict(row),
                )
            )

        return JisiluQdiiSnapshot(
            snapshot_date=target_snapshot_date,
            fetched_at=fetched_at,
            source_url=self.build_list_url(market),
            market=market,
            market_code=market_code,
            records_total=records_total,
            rows=row_snapshots,
            meta={
                "last_payload": last_payload,
                "unique_rows": len(row_snapshots),
            },
        )

    def fetch_qdii_all_result(
        self,
        *,
        market: QdiiMarket,
        snapshot_date: str | None = None,
        rows_per_page: int = 50,
        max_pages: int = 20,
    ) -> FetchResult[JisiluQdiiSnapshot]:
        snapshot = self.fetch_qdii_all(
            market=market,
            snapshot_date=snapshot_date,
            rows_per_page=rows_per_page,
            max_pages=max_pages,
        )
        return FetchResult(
            payload=snapshot,
            source_url=self.build_list_url(market),
            meta=self.build_fetch_meta(
                provider="JISILU",
                biz_date=snapshot.snapshot_date,
                fetched_at=snapshot.fetched_at,
                params={
                    "market": market,
                    "market_code": self.market_code(market),
                    "rows_per_page": rows_per_page,
                    "max_pages": max_pages,
                },
                page_info={
                    "records_total": snapshot.records_total,
                    "unique_rows": len(snapshot.rows),
                },
                raw_sample=snapshot.meta.get("last_payload"),
                extra={
                    "market": snapshot.market,
                    "market_code": snapshot.market_code,
                },
            ),
        )
