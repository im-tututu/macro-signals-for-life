from __future__ import annotations

from typing import Any

from src.core.models import AkshareBondUsSinaRow
from src.core.utils import norm_ymd, now_text, to_float

from .._base import BaseSource, FetchResult

SINA_BOND_GB_US_URL = "https://stock.finance.sina.com.cn/forex/globalbd/cn10yt.html"
AKSHARE_BOND_GB_US_SINA_SYMBOLS: tuple[str, ...] = (
    "美国1月期国债",
    "美国2月期国债",
    "美国3月期国债",
    "美国4月期国债",
    "美国6月期国债",
    "美国1年期国债",
    "美国2年期国债",
    "美国3年期国债",
    "美国5年期国债",
    "美国7年期国债",
    "美国10年期国债",
    "美国20年期国债",
    "美国30年期国债",
)


class AkshareBondGbUsSinaSource(BaseSource):
    """通过 akshare 抓取新浪美国国债收益率历史行情。"""

    @staticmethod
    def _pick_value(record: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in record and record[key] not in (None, ""):
                return record[key]
        return None

    def fetch_history(
        self,
        *,
        symbol: str = "美国10年期国债",
    ) -> list[AkshareBondUsSinaRow]:
        try:
            import akshare as ak
        except ImportError as exc:
            raise RuntimeError("akshare is required for akshare_bond_gb_us_sina") from exc

        df = ak.bond_gb_us_sina(symbol=symbol)
        rows: list[AkshareBondUsSinaRow] = []
        for raw in df.to_dict(orient="records"):
            trade_date = norm_ymd(
                self._pick_value(raw, "日期", "date", "Date")
            )
            if not trade_date:
                continue
            rows.append(
                AkshareBondUsSinaRow(
                    trade_date=trade_date,
                    symbol=symbol,
                    close=to_float(self._pick_value(raw, "收盘", "close", "Close")),
                    open=to_float(self._pick_value(raw, "开盘", "open", "Open")),
                    high=to_float(self._pick_value(raw, "高", "high", "High")),
                    low=to_float(self._pick_value(raw, "低", "low", "Low")),
                    change=to_float(self._pick_value(raw, "涨跌额", "涨跌", "change", "Change")),
                    pct_change=to_float(self._pick_value(raw, "涨跌幅", "pct_change", "Change Percent")),
                    volume=to_float(self._pick_value(raw, "成交量", "volume", "Volume")),
                    amount=to_float(self._pick_value(raw, "成交额", "amount", "Amount")),
                    meta={"raw": raw},
                )
            )
        return rows

    def fetch_histories(
        self,
        *,
        symbols: tuple[str, ...] = AKSHARE_BOND_GB_US_SINA_SYMBOLS,
    ) -> list[AkshareBondUsSinaRow]:
        rows: list[AkshareBondUsSinaRow] = []
        for symbol in symbols:
            rows.extend(self.fetch_history(symbol=symbol))
        return rows

    def fetch_history_result(
        self,
        *,
        symbol: str = "美国10年期国债",
    ) -> FetchResult[list[AkshareBondUsSinaRow]]:
        rows = self.fetch_history(symbol=symbol)
        return FetchResult(
            payload=rows,
            source_url=SINA_BOND_GB_US_URL,
            meta=self.build_fetch_meta(
                provider="SINA",
                fetched_at=now_text(),
                params={"symbol": symbol, "adapter": "AKSHARE"},
                page_info={"row_count": len(rows)},
                raw_sample=[row.meta.get("raw") for row in rows[:3]],
                extra={"adapter": "AKSHARE"},
            ),
        )

    def fetch_histories_result(
        self,
        *,
        symbols: tuple[str, ...] = AKSHARE_BOND_GB_US_SINA_SYMBOLS,
    ) -> FetchResult[list[AkshareBondUsSinaRow]]:
        rows = self.fetch_histories(symbols=symbols)
        return FetchResult(
            payload=rows,
            source_url=SINA_BOND_GB_US_URL,
            meta=self.build_fetch_meta(
                provider="SINA",
                fetched_at=now_text(),
                params={"symbols": list(symbols), "adapter": "AKSHARE"},
                page_info={"row_count": len(rows), "symbol_count": len(symbols)},
                raw_sample=[row.meta.get("raw") for row in rows[:3]],
                extra={"adapter": "AKSHARE"},
            ),
        )
