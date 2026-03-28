from __future__ import annotations

from typing import Any, Dict, List

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - optional dependency guard
    BeautifulSoup = None

from .base import BaseSource, FetchResult

NBS_HOME_URL = "https://www.stats.gov.cn/"


def _require_bs4() -> None:
    """只有真正解析 HTML 时才要求安装 bs4。"""

    if BeautifulSoup is None:
        raise RuntimeError("缺少依赖 beautifulsoup4，请先执行 `pip install -r py/requirements.txt`。")


class StatsGovSource(BaseSource):
    """国家统计局来源。

    当前主要服务于 life_asset 这条链路里的“房价/民生统计”语义。
    这和未来可能接入的工业、消费、固定资产投资等统计局数据是不同子域，
    所以后续也更适合按业务主题拆，而不是维持一个超大 StatsGovSource。

    当前 life_asset 只先使用占位快照，保证链路结构完整。
    """

    def fetch_html(self, url: str = NBS_HOME_URL) -> str:
        return self.http.get_text(url, headers={"User-Agent": "Mozilla/5.0"})

    def fetch_release_table(self, url: str, table_index: int = 0) -> List[List[str]]:
        html = self.fetch_html(url)
        _require_bs4()
        soup = BeautifulSoup(html, "lxml")
        tables = soup.select("table")
        if table_index >= len(tables):
            return []
        rows: List[List[str]] = []
        for tr in tables[table_index].select("tr"):
            rows.append([cell.get_text(" ", strip=True) for cell in tr.select("th,td")])
        return rows

    def fetch_placeholder_house_price_snapshot(self) -> Dict[str, Any]:
        return {
            "date": "",
            "house_price_tier1": None,
            "house_price_tier2": None,
            "house_price_nbs_70city": None,
            "source": NBS_HOME_URL,
            "implemented": False,
        }

    def fetch_placeholder_house_price_result(self) -> FetchResult[Dict[str, Any]]:
        """统一返回 FetchResult。"""

        return FetchResult(
            payload=self.fetch_placeholder_house_price_snapshot(),
            source_url=NBS_HOME_URL,
            meta={"provider": "STATS_GOV", "placeholder": True},
        )
