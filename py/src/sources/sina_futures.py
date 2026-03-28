from __future__ import annotations

import re
from collections.abc import Iterable

from src.core.models import FuturesSnapshot
from src.core.utils import now_text, to_float, today_ymd

from .base import BaseSource, FetchResult

SINA_FUTURES_QUOTE_URL = "https://hq.sinajs.cn/list="


class SinaFuturesSource(BaseSource):
    """新浪国债期货报价来源。

    这里聚焦的是“国债期货最新报价”这一条能力，不和别的新浪财经页面能力混放。
    如果未来接入更多期货合约或盘中快照，也应沿着 futures 语义继续扩展。
    """

    @staticmethod
    def _normalize_price(value: float | None) -> float | None:
        if value is None:
            return None
        # 中金所国债期货主力/连续合约价格通常在百元附近，过小值多半是字段错位。
        if value < 50:
            return None
        return value

    def fetch_price(self, symbol: str | Iterable[str]) -> float | None:
        # 新浪有时要求不同前缀，这里在来源层把兼容尝试做掉，上层只关心“拿到价格”。
        candidates = [symbol] if isinstance(symbol, str) else list(symbol)
        expanded: list[str] = []
        for item in candidates:
            expanded.extend([item, f"nf_{item}"])
        for candidate in expanded:
            try:
                text = self.http.get_text(
                    SINA_FUTURES_QUOTE_URL + candidate,
                    headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn/"},
                )
            except Exception:  # noqa: BLE001
                continue
            m = re.search(r'="([^"]*)"', text)
            if not m:
                continue
            arr = m.group(1).split(",")
            if len(arr) > 3:
                value = to_float(arr[3])
                if value is not None and value > 0:
                    return value
            for item in arr:
                value = to_float(item)
                if value is not None and value > 0:
                    return value
        return None

    def fetch_bond_futures_snapshot(self) -> FuturesSnapshot:
        t0_last = self._normalize_price(self.fetch_price("T0"))
        tf0_last = self._normalize_price(self.fetch_price("TF0"))
        return FuturesSnapshot(
            date=today_ymd(),
            source="hq.sinajs.cn",
            values={"t0_last": t0_last, "tf0_last": tf0_last},
            fetched_at=now_text(),
        )

    def fetch_bond_futures_result(self) -> FetchResult[FuturesSnapshot]:
        return FetchResult(
            payload=self.fetch_bond_futures_snapshot(),
            source_url=SINA_FUTURES_QUOTE_URL,
            meta={"provider": "SINA_FUTURES"},
        )
