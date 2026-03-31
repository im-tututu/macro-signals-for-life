from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List

from src.core.models import JisiluEtfRowSnapshot, JisiluEtfSnapshot
from src.core.config import persist_env_value, settings
from src.core.notify import build_notifier
from src.core.utils import now_text, today_ymd

from .._base import BaseSource, FetchResult

JISILU_ETF_LIST_URL = "https://www.jisilu.cn/data/etf/etf_list/"
JISILU_ETF_DETAIL_HISTS_URL = "https://www.jisilu.cn/data/etf/detail_hists/"
JISILU_ETF_DETAIL_URL = "https://www.jisilu.cn/data/etf/detail/{fund_id}"


class JisiluEtfSource(BaseSource):
    """集思录 ETF 来源。

    当前这个类只承接 ETF 列表入口。
    虽然都属于集思录站点，但 ETF、QDII、商品 ETF、可转债、分级等页面
    的筛选参数、鉴权要求和返回结构并不天然一致，后续更适合拆成按业务域划分
    的多个 source，而不是继续把整个站点做成一个大类。

    access kind:
    - `xhr_json`
    - 但它比普通开放 API 更脆弱，因为还依赖 cookie、referer 和站点登录态。
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._cookie = settings.jisilu_cookie
        self._notifier = build_notifier(logging.getLogger(__name__))
        self._cookie_expired_notified = False
        # 集思录接口对 cookie 很敏感，所以在来源初始化时就把会话头准备好。
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
        # cookie 刷新属于“访问来源所需的站点协商逻辑”，所以留在 source 层最合适。
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
        persist_env_value("JISILU_COOKIE", cookie)
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
        min_unit_total_yi: float | int | str = "",
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
        # “游客仅显示 20 条”是这个来源最常见的隐性失败，需要在来源层直接识别。
        if self._detect_tourist_limit(raw_text, payload) or self._looks_like_tourist_rows(rows, records):
            self._notify_cookie_expired()
            raise RuntimeError("Jisilu tourist limited")
        return payload

    def fetch_etf_detail_history_page(
        self,
        *,
        fund_id: str,
        page: int = 1,
        rows_per_page: int = 50,
    ) -> Dict[str, Any]:
        if not fund_id:
            raise ValueError("fund_id is required")
        headers = {
            "Referer": JISILU_ETF_DETAIL_URL.format(fund_id=fund_id),
            "X-Requested-With": "XMLHttpRequest",
        }
        data = {
            "is_search": 1,
            "fund_id": fund_id,
            "rp": rows_per_page,
            "page": page,
        }
        response = self.http.request("POST", JISILU_ETF_DETAIL_HISTS_URL, data=data, headers=headers)
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
        min_unit_total_yi: float | int | str = "",
        min_volume_wan: float | int | str = "",
        extra_query_string: str = "",
    ) -> JisiluEtfSnapshot:
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
        min_unit_total_yi: float | int | str = "",
        min_volume_wan: float | int | str = "",
        extra_query_string: str = "",
    ) -> JisiluEtfSnapshot:
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

        target_snapshot_date = self.require_text(snapshot_date or today_ymd(), field_name="snapshot_date")
        fetched_at = now_text()
        row_snapshots: List[JisiluEtfRowSnapshot] = []
        for row in self.require_rows(all_rows):
            cell = row.get("cell", row) if isinstance(row, dict) else {}
            fund_id = self.require_text(str(cell.get("fund_id") or row.get("id") or ""), field_name="fund_id")
            row_snapshots.append(JisiluEtfRowSnapshot(fund_id=fund_id, cell=dict(cell), raw_row=dict(row)))

        return JisiluEtfSnapshot(
            snapshot_date=target_snapshot_date,
            fetched_at=fetched_at,
            source_url=JISILU_ETF_LIST_URL,
            records_total=records_total,
            rows=row_snapshots,
            meta={
                "last_payload": last_payload,
                "unique_rows": len(row_snapshots),
            },
        )

    def fetch_etf_index_all_result(
        self,
        *,
        snapshot_date: str | None = None,
        rows_per_page: int = 500,
        max_pages: int = 20,
        min_unit_total_yi: float | int | str = "",
        min_volume_wan: float | int | str = "",
        extra_query_string: str = "",
    ) -> FetchResult[JisiluEtfSnapshot]:
        """统一返回 FetchResult，供 store/job 层复用。"""

        snapshot = self.fetch_etf_index_all(
            snapshot_date=snapshot_date,
            rows_per_page=rows_per_page,
            max_pages=max_pages,
            min_unit_total_yi=min_unit_total_yi,
            min_volume_wan=min_volume_wan,
            extra_query_string=extra_query_string,
        )
        return FetchResult(
            payload=snapshot,
            source_url=JISILU_ETF_LIST_URL,
            meta=self.build_fetch_meta(
                provider="JISILU",
                biz_date=snapshot.snapshot_date,
                fetched_at=snapshot.fetched_at,
                params={
                    "rows_per_page": rows_per_page,
                    "max_pages": max_pages,
                    "min_unit_total_yi": min_unit_total_yi,
                    "min_volume_wan": min_volume_wan,
                    "extra_query_string": extra_query_string,
                },
                page_info={
                    "records_total": snapshot.records_total,
                    "unique_rows": len(snapshot.rows),
                },
                raw_sample=snapshot.meta.get("last_payload"),
            ),
        )

    def fetch_etf_detail_history_result(
        self,
        *,
        fund_id: str,
        rows_per_page: int = 50,
        max_pages: int = 2,
    ) -> FetchResult[dict[str, Any]]:
        """抓取单只 ETF 的历史明细页。

        该接口更适合补历史，通常只保留近 50 条左右数据，字段比列表页少一些。
        """

        refreshed = False
        while True:
            try:
                payloads: list[dict[str, Any]] = []
                seen_dates: set[str] = set()
                for page in range(1, max_pages + 1):
                    payload = self.fetch_etf_detail_history_page(
                        fund_id=fund_id,
                        page=page,
                        rows_per_page=rows_per_page,
                    )
                    payloads.append(payload)
                    rows = payload.get("rows", []) or []
                    if not rows:
                        break
                    if len(rows) < rows_per_page:
                        break
                    # 历史页通常按日期倒序，若这一页没有新增日期也可以收手。
                    page_dates = {
                        self._extract_history_snapshot_date(row)
                        for row in rows
                        if self._extract_history_snapshot_date(row)
                    }
                    if page_dates and page_dates.issubset(seen_dates):
                        break
                    seen_dates.update(page_dates)
                return FetchResult(
                    payload={
                        "fund_id": fund_id,
                        "pages": payloads,
                    },
                    source_url=JISILU_ETF_DETAIL_URL.format(fund_id=fund_id),
                    meta=self.build_fetch_meta(
                        provider="JISILU",
                        biz_date="",
                        fetched_at=now_text(),
                        params={
                            "fund_id": fund_id,
                            "rows_per_page": rows_per_page,
                            "max_pages": max_pages,
                        },
                        page_info={
                            "pages": len(payloads),
                            "rows": sum(len((p.get("rows", []) or [])) for p in payloads),
                        },
                        raw_sample=payloads[0] if payloads else None,
                        extra={"mode": "detail_hists"},
                    ),
                )
            except RuntimeError as exc:
                if refreshed or "Jisilu tourist limited" not in str(exc):
                    raise
                self._refresh_cookie_from_webhook()
                refreshed = True

    @staticmethod
    def _extract_history_snapshot_date(row: Dict[str, Any]) -> str:
        cell = row.get("cell", row) if isinstance(row, dict) else {}
        for key in ("snapshot_date", "date", "trade_date", "price_dt", "nav_dt"):
            value = cell.get(key)
            if value not in (None, ""):
                return str(value).strip()
        return ""
