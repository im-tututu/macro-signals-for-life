from __future__ import annotations

import csv
import io
from typing import Any, Dict, Iterable, List

from core.models import MoneyMarketSnapshot
from core.utils import now_text, norm_ymd, to_float, today_ymd

from .base import BaseSource

CHINAMONEY_PRR_MD_URL = "https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/prr-md.json"
CHINAMONEY_PRR_CHART_URL = "https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/prr-chrt.csv"
CHINAMONEY_IBLR_CHART_URL = "https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/iblr-chrt.csv"


class ChinaMoneySource(BaseSource):
    @staticmethod
    def _iter_objects(node: Any) -> Iterable[Dict[str, Any]]:
        if isinstance(node, dict):
            yield node
            for value in node.values():
                yield from ChinaMoneySource._iter_objects(value)
        elif isinstance(node, list):
            for item in node:
                yield from ChinaMoneySource._iter_objects(item)

    def fetch_money_market_snapshot(self) -> MoneyMarketSnapshot:
        data = self.http.get_json(CHINAMONEY_PRR_MD_URL)
        fields: Dict[str, Any] = {}
        data_block = data.get("data", {}) if isinstance(data, dict) else {}
        record_date = norm_ymd(data_block.get("showDateCN")) or today_ymd()

        records = data.get("records", []) if isinstance(data, dict) else []
        if isinstance(records, list) and records:
            for obj in records:
                code = str(obj.get("productCode") or obj.get("code") or obj.get("symbol") or "").strip()
                weighted = to_float(obj.get("weightedRate") or obj.get("rate") or obj.get("price") or obj.get("value"))
                latest = to_float(obj.get("latestRate"))
                avg_prd = to_float(obj.get("avgPrd"))
                if code and weighted is not None:
                    fields[code] = weighted
                    fields[f"{code}_weightedRate"] = weighted
                if code and latest is not None:
                    fields[f"{code}_latestRate"] = latest
                if code and avg_prd is not None:
                    fields[f"{code}_avgPrd"] = avg_prd
            return MoneyMarketSnapshot(date=record_date, source=CHINAMONEY_PRR_MD_URL, fields=fields, fetched_at=now_text())

        for obj in self._iter_objects(data):
            name = str(obj.get("name") or obj.get("label") or obj.get("termName") or obj.get("term") or "")
            code = str(obj.get("productCode") or obj.get("code") or obj.get("symbol") or obj.get("itemCode") or "")
            weighted = to_float(obj.get("weightedRate") or obj.get("latestRate") or obj.get("rate") or obj.get("price") or obj.get("value"))
            if obj.get("showDateCN"):
                record_date = norm_ymd(obj.get("showDateCN")) or record_date
            key = code or name
            if key and weighted is not None:
                fields[key] = weighted
            if "DR007" in key.upper() or name.upper() == "DR007":
                fields["DR007_weightedRate"] = weighted
            if "R007" in key.upper() or name.upper() == "R007":
                fields.setdefault("R007_weightedRate", weighted)
            if "R001" in key.upper() or name.upper() == "R001":
                fields.setdefault("R001_weightedRate", weighted)
        return MoneyMarketSnapshot(date=record_date, source=CHINAMONEY_PRR_MD_URL, fields=fields, fetched_at=now_text())

    def fetch_chart_csv(self, url: str) -> List[Dict[str, str]]:
        text = self.http.get_text(url)
        reader = csv.DictReader(io.StringIO(text))
        return list(reader)

    def fetch_prr_history_csv(self) -> List[Dict[str, str]]:
        return self.fetch_chart_csv(CHINAMONEY_PRR_CHART_URL)

    def fetch_iblr_history_csv(self) -> List[Dict[str, str]]:
        return self.fetch_chart_csv(CHINAMONEY_IBLR_CHART_URL)
