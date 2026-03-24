from __future__ import annotations

from typing import Any, Dict, List

from bs4 import BeautifulSoup

from .base import BaseSource

NBS_HOME_URL = "https://www.stats.gov.cn/"


class StatsGovSource(BaseSource):
    def fetch_html(self, url: str = NBS_HOME_URL) -> str:
        return self.http.get_text(url, headers={"User-Agent": "Mozilla/5.0"})

    def fetch_release_table(self, url: str, table_index: int = 0) -> List[List[str]]:
        html = self.fetch_html(url)
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
