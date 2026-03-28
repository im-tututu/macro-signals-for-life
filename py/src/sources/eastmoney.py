from __future__ import annotations

from typing import Any

from .base import BaseSource, FetchResult

MONEY_FUND_PROXY_URL = "https://fundf10.eastmoney.com/jjjz_000198.html"


class EastMoneySource(BaseSource):
    """东方财富货基页面来源。

    当前只承接 life_asset 里的货基 7 日年化占位来源。
    先独立出来的价值在于：后面即使接入基金净值、ETF 行情、排行等别的东方财富页面，
    也不会再次回到“大杂烩 source”的状态。
    """

    def fetch_money_fund_7d_snapshot(self) -> dict[str, Any]:
        return {"date": "", "money_fund_7d": None, "source": MONEY_FUND_PROXY_URL, "implemented": False}

    def fetch_money_fund_7d_result(self) -> FetchResult[dict[str, Any]]:
        return FetchResult(
            payload=self.fetch_money_fund_7d_snapshot(),
            source_url=MONEY_FUND_PROXY_URL,
            meta={"provider": "MONEY_FUND", "placeholder": True},
        )
