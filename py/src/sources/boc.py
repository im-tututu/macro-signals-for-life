from __future__ import annotations

from typing import Any

from .base import BaseSource, FetchResult

BOC_DEPOSIT_RATE_URL = "https://www.bankofchina.com/fimarkets/lilv/fd31/"


class BocSource(BaseSource):
    """中国银行存款利率来源。

    当前只承接 life_asset 所需的 1 年期存款利率占位来源。
    先把来源边界单独收口，后续如果补更多币种/期限，也可以继续沿着这个文件扩展。
    """

    def fetch_deposit_1y_snapshot(self) -> dict[str, Any]:
        return {"date": "", "deposit_1y": None, "source": BOC_DEPOSIT_RATE_URL, "implemented": False}

    def fetch_deposit_1y_result(self) -> FetchResult[dict[str, Any]]:
        return FetchResult(
            payload=self.fetch_deposit_1y_snapshot(),
            source_url=BOC_DEPOSIT_RATE_URL,
            meta={"provider": "BOC", "placeholder": True},
        )
