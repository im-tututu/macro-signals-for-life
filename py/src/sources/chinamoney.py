from __future__ import annotations

import csv
import io
from typing import Any, Dict, Iterable, List

from src.core.models import MoneyMarketSnapshot
from src.core.utils import now_text, norm_ymd, to_float, today_ymd

from ._base import BaseSource, FetchResult

CHINAMONEY_PRR_MD_URL = "https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/prr-md.json"
CHINAMONEY_PRR_CHART_URL = "https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/prr-chrt.csv"
CHINAMONEY_IBLR_CHART_URL = "https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/currency/iblr-chrt.csv"


class ChinaMoneySource(BaseSource):
    """ChinaMoney 资金面来源。

    这是国内货币市场原始快照的核心入口之一。

    当前文件聚焦“银行间资金面”这条来源语义，而不是把 ChinaMoney
    站内所有数据都揉成一个大类。这样后续如果接入存单、回购历史等别的入口，
    可以继续按业务主题拆分，而不是把所有东西塞进一个 source。

    access kind:
    - 最新快照主入口更接近 `api/json`
    - 历史图表入口属于 `file_download`
    - 所以这个来源的维护点会天然分成“接口字段稳定性”和“文件格式稳定性”两类

    这里不关心 SQLite 表结构，只返回领域对象和少量来源元信息。
    """

    BIZ_DATE_KEYS = ("showDateCN", "date", "showDate", "tradeDate")

    @staticmethod
    def _iter_objects(node: Any) -> Iterable[Dict[str, Any]]:
        if isinstance(node, dict):
            yield node
            for value in node.values():
                yield from ChinaMoneySource._iter_objects(value)
        elif isinstance(node, list):
            for item in node:
                yield from ChinaMoneySource._iter_objects(item)

    def _snapshot_from_payload(self, data: Any) -> MoneyMarketSnapshot:
        # ChinaMoney 的 JSON 结构偶尔会变形，所以这里先尽量走“显式字段”路径，
        # 再退回到递归扫描，优先保证日常抓取稳住。
        fields: Dict[str, Any] = {}
        data_block = data.get("data", {}) if isinstance(data, dict) else {}
        record_date = norm_ymd(data_block.get("showDateCN")) or today_ymd()
        fetched_at = now_text()

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
            if not fields:
                raise ValueError("ChinaMoney snapshot missing parsed fields")
            return MoneyMarketSnapshot(date=record_date, source=CHINAMONEY_PRR_MD_URL, fields=fields, fetched_at=fetched_at)

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
        if not fields:
            raise ValueError("ChinaMoney snapshot missing parsed fields")
        return MoneyMarketSnapshot(date=record_date, source=CHINAMONEY_PRR_MD_URL, fields=fields, fetched_at=fetched_at)

    def fetch_money_market_snapshot(self) -> MoneyMarketSnapshot:
        return self._snapshot_from_payload(self.http.get_json(CHINAMONEY_PRR_MD_URL))

    @classmethod
    def derive_business_date(cls, payload: Dict[str, Any]) -> str:
        data_block = payload.get("data", {}) if isinstance(payload, dict) else {}
        for key in cls.BIZ_DATE_KEYS:
            value = norm_ymd(data_block.get(key))
            if value:
                return value
        return today_ymd()

    def fetch_money_market(self) -> FetchResult[MoneyMarketSnapshot]:
        payload = self.http.get_json(CHINAMONEY_PRR_MD_URL)
        snapshot = self._snapshot_from_payload(payload)
        # 业务日期比“抓取日期”更重要，后续落表和比对都应尽量对齐业务日期。
        snapshot.date = self.require_text(self.derive_business_date(payload), field_name="biz_date")
        data_block = payload.get("data", {}) if isinstance(payload, dict) else {}
        if not snapshot.fields:
            raise ValueError("ChinaMoney snapshot fields are empty")
        return FetchResult(
            payload=snapshot,
            source_url=CHINAMONEY_PRR_MD_URL,
            meta=self.build_fetch_meta(
                provider="ChinaMoney",
                biz_date=snapshot.date,
                fetched_at=snapshot.fetched_at,
                params={"endpoint": "prr-md"},
                page_info={"field_count": len(snapshot.fields)},
                raw_sample=payload if isinstance(payload, dict) else None,
                extra={
                    "show_date_cn": str(data_block.get("showDateCN") or ""),
                },
            ),
        )

    def fetch_chart_csv(self, url: str) -> List[Dict[str, str]]:
        # 这里代表的是 file_download 型入口，维护时要更关注表头和编码变化。
        text = self.http.get_text(url)
        reader = csv.DictReader(io.StringIO(text))
        return list(reader)

    def fetch_prr_history_csv(self) -> List[Dict[str, str]]:
        return self.fetch_chart_csv(CHINAMONEY_PRR_CHART_URL)

    def fetch_iblr_history_csv(self) -> List[Dict[str, str]]:
        return self.fetch_chart_csv(CHINAMONEY_IBLR_CHART_URL)
