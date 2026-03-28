from __future__ import annotations

from typing import Any

from .base import BaseSource, FetchResult

SGE_HOME_URL = "https://www.sge.com.cn/"


class SgeSource(BaseSource):
    """上海黄金交易所来源。

    目前只是 life_asset 链路里的黄金价格占位来源。
    即使还没补真实抓取，也先独立成文件，避免以后把贵金属、现货、递延等
    完全不同入口继续混回别的 source 里。
    """

    def fetch_gold_snapshot(self) -> dict[str, Any]:
        return {"date": "", "gold_cny": None, "source": SGE_HOME_URL, "implemented": False}

    def fetch_gold_result(self) -> FetchResult[dict[str, Any]]:
        return FetchResult(payload=self.fetch_gold_snapshot(), source_url=SGE_HOME_URL, meta={"provider": "SGE", "placeholder": True})
