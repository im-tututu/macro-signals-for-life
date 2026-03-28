from __future__ import annotations

import json
from typing import Any

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - optional dependency guard
    BeautifulSoup = None

from src.core.models import JisiluTreasuryRowSnapshot, JisiluTreasurySnapshot
from src.core.utils import now_text, today_ymd
from src.core.config import settings

from ..base import BaseSource, FetchResult

JISILU_TREASURY_REFERER_URL = "https://www.jisilu.cn/data/bond/treasury/"
JISILU_TREASURY_ALL_URL = (
    "https://www.jisilu.cn/data/bond/treasury/"
    "?do_search=false&sort_column=&sort_order=&forall=0&treasury=0"
    "&from_year_left=0&from_repo=0&from_ytm=1&from_volume=10&from_market="
    "&to_year_left=50&to_repo=2&to_ytm=100&to_volume="
)

TREASURY_COLUMN_MAP = {
    "代码": "bond_id",
    "名称": "bond_nm",
    "现价": "price",
    "全价": "full_price",
    "涨幅": "increase_rt",
    "成交额(万元)": "volume_wan",
    "卖一": "ask_1",
    "买一": "bid_1",
    "距付息(天)": "days_to_coupon",
    "剩余年限": "years_left",
    "修正久期": "duration",
    "到期收益率": "ytm",
    "票息": "coupon_rt",
    "折算率": "repo_ratio",
    "回购利用率": "repo_usage_rt",
    "到期时间": "maturity_dt",
    "规模(亿)": "size_yi",
}
def _require_bs4() -> None:
    if BeautifulSoup is None:
        raise RuntimeError("缺少依赖 beautifulsoup4，请先执行 `pip install -r py/requirements.txt`。")


class JisiluTreasurySource(BaseSource):
    """集思录国债现券列表来源。

    这个页面与 ETF/QDII 不同，当前更接近：
    - `page_html`
    - 直接用带 query string 的页面 URL 返回完整 HTML 表格
    - 暂时不依赖额外 XHR 接口
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if settings.jisilu_cookie:
            self.http.session.headers["Cookie"] = settings.jisilu_cookie
        self.http.session.headers["User-Agent"] = settings.jisilu_user_agent
        self.http.session.headers["Referer"] = JISILU_TREASURY_REFERER_URL

    def fetch_html(self, url: str = JISILU_TREASURY_ALL_URL) -> str:
        return self.http.get_text(url)

    def parse_treasury_table(self, html: str) -> list[dict[str, Any]]:
        _require_bs4()
        soup = BeautifulSoup(html, "lxml")
        for table in soup.select("table"):
            header_cells = [cell.get_text(" ", strip=True) for cell in table.select("tr th")]
            if "代码" not in header_cells or "到期收益率" not in header_cells:
                continue

            column_names = header_cells
            rows: list[dict[str, Any]] = []
            for tr in table.select("tr"):
                value_cells = tr.select("td")
                if not value_cells:
                    continue
                values = [cell.get_text(" ", strip=True) for cell in value_cells]
                if len(values) != len(column_names):
                    continue
                normalized: dict[str, Any] = {}
                for key, value in zip(column_names, values):
                    target_key = TREASURY_COLUMN_MAP.get(str(key).strip(), str(key).strip())
                    normalized[target_key] = value
                rows.append(normalized)
            if rows:
                return rows
        return []

    def fetch_treasury_rows(self, url: str = JISILU_TREASURY_ALL_URL) -> list[dict[str, Any]]:
        html = self.fetch_html(url)
        rows = self.parse_treasury_table(html)
        if rows:
            return rows
        raise RuntimeError("Jisilu treasury table not found in HTML response")

    def fetch_treasury_result(
        self,
        url: str = JISILU_TREASURY_ALL_URL,
        *,
        snapshot_date: str | None = None,
    ) -> FetchResult[JisiluTreasurySnapshot]:
        rows = self.fetch_treasury_rows(url)
        target_snapshot_date = self.require_text(snapshot_date or today_ymd(), field_name="snapshot_date")
        fetched_at = now_text()
        row_snapshots = [
            JisiluTreasuryRowSnapshot(
                bond_id=self.require_text(str(row.get("bond_id") or ""), field_name="bond_id"),
                fields=dict(row),
            )
            for row in self.require_rows(rows)
        ]
        snapshot = JisiluTreasurySnapshot(
            snapshot_date=target_snapshot_date,
            fetched_at=fetched_at,
            source_url=url,
            rows=row_snapshots,
            meta={"raw_preview": rows[:3]},
        )
        return FetchResult(
            payload=snapshot,
            source_url=url,
            meta=self.build_fetch_meta(
                provider="JISILU",
                biz_date=snapshot.snapshot_date,
                fetched_at=snapshot.fetched_at,
                params={"url": url},
                page_info={"row_count": len(snapshot.rows)},
                raw_sample=json.dumps(rows[:3], ensure_ascii=False, default=str),
                extra={"category": "treasury"},
            ),
        )
