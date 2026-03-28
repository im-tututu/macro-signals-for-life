from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urlencode

from src.core.models import BondIndexSnapshot
from src.core.utils import now_text, to_float

from ..base import BaseSource, FetchResult

CHINABOND_INDEX_SINGLE_QUERY_URL = "https://yield.chinabond.com.cn/cbweb-mn/indices/singleIndexQueryResult"
CSI_BOND_FEATURE_URL = "https://www.csindex.com.cn/csindex-home/perf/get-bond-index-feature/{code}"
CNI_BOND_FEATURE_URL = "https://www.cnindex.com.cn/module/index-detail.html?act_menu=1&indexCode={code}"
TZ_SH = dt.timezone(dt.timedelta(hours=8))


class ChinaBondIndexSource(BaseSource):
    """中债指数特征来源。

    这是 ChinaBond 站内“指数特征”入口，不和收益率曲线 source 混放语义。

    当前 Python 侧先统一单指数抓取能力。
    后续如果接入指数成分、指数历史序列等，也更适合继续沿着 index 子域展开，
    而不是再回到一个笼统的 `chinabond.py` 里堆功能。

    access kind:
    - `xhr_json`
    - 风险点在于站内接口参数约定和前端字段变化，而不是 HTML 解析。
    """

    def fetch_chinabond_index_series(
        self,
        index_id: str,
        *,
        qxlxt: str = "00",
        ltcslx: str = "00",
        zslxt: Iterable[str] = ("PJSZFJQ", "PJSZFDQSYL", "PJSZFTX"),
        zslxt1: Iterable[str] = ("PJSZFJQ", "PJSZFDQSYL", "PJSZFTX"),
        lx: str = "1",
        locale: str = "zh_CN",
    ) -> Dict[str, Any]:
        params = {
            "indexid": index_id,
            "qxlxt": qxlxt,
            "ltcslx": ltcslx,
            "zslxt": ",".join(zslxt),
            "zslxt1": ",".join(zslxt1),
            "lx": lx,
            "locale": locale,
        }
        url = f"{CHINABOND_INDEX_SINGLE_QUERY_URL}?{urlencode(params)}"
        return self.http.post_json(
            url,
            headers={
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
            },
        )

    @staticmethod
    def _to_epoch_ms(raw_key: Any) -> Optional[int]:
        text = str(raw_key).strip()
        if not text:
            return None
        text = text.split("-", 1)[0]
        try:
            value = int(float(text))
        except Exception:  # noqa: BLE001
            return None
        if value < 10**11:
            return value * 1000
        return value

    @staticmethod
    def _epoch_ms_to_date(epoch_ms: int) -> str:
        return dt.datetime.fromtimestamp(epoch_ms / 1000.0, tz=dt.timezone.utc).astimezone(TZ_SH).strftime("%Y-%m-%d")

    @classmethod
    def latest_point(cls, series: Dict[str, Any] | None) -> Optional[Dict[str, Any]]:
        # 中债指数接口常把时间戳放在动态 key 里，因此先统一解析 key，
        # 再挑最新点，避免上层直接理解这种不稳定结构。
        if not isinstance(series, dict) or not series:
            return None
        candidates: list[tuple[int, str]] = []
        for key in series.keys():
            epoch_ms = cls._to_epoch_ms(key)
            if epoch_ms is not None:
                candidates.append((epoch_ms, str(key)))
        if not candidates:
            return None
        epoch_ms, raw_key = max(candidates, key=lambda x: x[0])
        return {"ts": epoch_ms, "date": cls._epoch_ms_to_date(epoch_ms), "value": to_float(series[raw_key])}

    def fetch_duration_snapshot(self, index_id: str) -> BondIndexSnapshot:
        # 这里选择“久期点”为主锚点，因为如果连久期都没有，这个指数快照通常不可用。
        payload = self.fetch_chinabond_index_series(index_id)
        duration = self.latest_point(payload.get("PJSZFJQ_00"))
        ytm = self.latest_point(payload.get("PJSZFDQSYL_00"))
        convexity = self.latest_point(payload.get("PJSZFTX_00"))
        if not duration:
            raise ValueError(f"No duration data for index_id={index_id}")
        return BondIndexSnapshot(
            date=duration["date"],
            index_id=index_id,
            duration=duration["value"],
            ytm=ytm["value"] if ytm else None,
            convexity=convexity["value"] if convexity else None,
            source=CHINABOND_INDEX_SINGLE_QUERY_URL,
            meta={"raw_keys": list(payload.keys())},
        )

    def fetch_duration_snapshot_result(
        self,
        index_id: str,
        *,
        index_name: str | None = None,
        index_code: str | None = None,
    ) -> FetchResult[BondIndexSnapshot]:
        """统一返回单指数抓取结果。"""

        snapshot = self.fetch_duration_snapshot(index_id)
        fetched_at = now_text()
        return FetchResult(
            payload=snapshot,
            source_url=CHINABOND_INDEX_SINGLE_QUERY_URL,
            meta=self.build_fetch_meta(
                provider="CHINABOND",
                biz_date=snapshot.date,
                fetched_at=fetched_at,
                params={
                    "index_id": index_id,
                    "index_name": index_name or index_id,
                    "index_code": index_code or index_id,
                },
                page_info={"raw_key_count": len(snapshot.meta.get("raw_keys", []))},
                raw_sample={
                    "raw_keys": snapshot.meta.get("raw_keys", []),
                },
                extra={
                    "index_name": index_name or index_id,
                    "index_code": index_code or index_id,
                },
            ),
        )

    def fetch_csindex_bond_feature(self, code: str) -> Dict[str, Any]:
        url = CSI_BOND_FEATURE_URL.format(code=code)
        data = self.http.get_json(url, headers={"Accept": "application/json, text/plain, */*"})
        return data.get("data", data)

    def fetch_cnindex_bond_feature(self, code: str) -> Dict[str, Any]:
        url = CNI_BOND_FEATURE_URL.format(code=code)
        text = self.http.get_text(url, headers={"Accept": "application/json, text/plain, */*"})
        try:
            import json

            parsed = json.loads(text)
            return parsed.get("data", parsed)
        except Exception:  # noqa: BLE001
            return {"raw": text}
