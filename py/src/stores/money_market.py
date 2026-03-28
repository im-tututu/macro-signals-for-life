from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.models import MoneyMarketSnapshot
from src.core.utils import to_float
from src.core.config import TABLE_RAW_MONEY_MARKET
from src.sources._base import FetchResult
from ._base import BaseSqliteStore, TableSpec


MONEY_MARKET_RATE_FIELDS = (
    "dr001_weighted_rate",
    "dr001_latest_rate",
    "dr001_avg_prd",
    "dr007_weighted_rate",
    "dr007_latest_rate",
    "dr007_avg_prd",
    "dr014_weighted_rate",
    "dr014_latest_rate",
    "dr014_avg_prd",
    "dr021_weighted_rate",
    "dr021_latest_rate",
    "dr021_avg_prd",
    "dr1m_weighted_rate",
    "dr1m_latest_rate",
    "dr1m_avg_prd",
)

MONEY_MARKET_SPEC = TableSpec(
    table_name=TABLE_RAW_MONEY_MARKET,
    key_fields=("date",),
    date_field="date",
    numeric_fields=MONEY_MARKET_RATE_FIELDS,
    integer_fields=("source_row_num",),
    text_fields=("show_date_cn", "source_url", "source_sheet"),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=(
        "date",
        "show_date_cn",
        "source_url",
        *MONEY_MARKET_RATE_FIELDS,
        "fetched_at",
        "source_sheet",
        "source_row_num",
        "migrated_at",
    ),
    default_order_by=("date",),
)


@dataclass
class MoneyMarketStore(BaseSqliteStore):
    """资金面原始表 store。

    职责分工：
    - source 负责抓取 ChinaMoney 并产出 MoneyMarketSnapshot
    - store 负责把 snapshot 映射成 raw_money_market 表行
    - job 负责调用 source + store 并执行增量 upsert
    """

    spec: TableSpec = MONEY_MARKET_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    @staticmethod
    def build_row_from_snapshot(
        snapshot: MoneyMarketSnapshot,
        *,
        show_date_cn: str = "",
        source_url: str | None = None,
    ) -> dict[str, Any]:
        """把领域对象转成 SQLite 表行。

        这是 store 层对“表结构”的唯一认知点。
        """
        fields = snapshot.fields
        row = {
            "date": snapshot.date,
            "show_date_cn": show_date_cn,
            "source_url": source_url or snapshot.source,
            "dr001_weighted_rate": to_float(fields.get("DR001_weightedRate")),
            "dr001_latest_rate": to_float(fields.get("DR001_latestRate")),
            "dr001_avg_prd": to_float(fields.get("DR001_avgPrd")),
            "dr007_weighted_rate": to_float(fields.get("DR007_weightedRate")),
            "dr007_latest_rate": to_float(fields.get("DR007_latestRate")),
            "dr007_avg_prd": to_float(fields.get("DR007_avgPrd")),
            "dr014_weighted_rate": to_float(fields.get("DR014_weightedRate")),
            "dr014_latest_rate": to_float(fields.get("DR014_latestRate")),
            "dr014_avg_prd": to_float(fields.get("DR014_avgPrd")),
            "dr021_weighted_rate": to_float(fields.get("DR021_weightedRate")),
            "dr021_latest_rate": to_float(fields.get("DR021_latestRate")),
            "dr021_avg_prd": to_float(fields.get("DR021_avgPrd")),
            "dr1m_weighted_rate": to_float(fields.get("DR1M_weightedRate")),
            "dr1m_latest_rate": to_float(fields.get("DR1M_latestRate")),
            "dr1m_avg_prd": to_float(fields.get("DR1M_avgPrd")),
            "fetched_at": snapshot.fetched_at,
        }
        if all(row.get(key) is None for key in ("dr001_weighted_rate", "dr007_weighted_rate", "dr014_weighted_rate")):
            raise ValueError("ChinaMoney snapshot missing critical weighted rates")
        return row

    @classmethod
    def build_row_from_fetch_result(cls, fetch_result: FetchResult[MoneyMarketSnapshot]) -> dict[str, Any]:
        """把 source 返回的抓取结果统一转成表行。"""
        return cls.build_row_from_snapshot(
            fetch_result.payload,
            show_date_cn=str(fetch_result.meta.get("show_date_cn") or ""),
            source_url=fetch_result.source_url,
        )

    def fetch_latest_rate(self, field_name: str) -> float | None:
        if field_name not in MONEY_MARKET_RATE_FIELDS:
            raise ValueError(f"invalid money market field: {field_name}")
        rows = self.fetch_recent(limit=1)
        if not rows:
            return None
        value = rows[0].get(field_name)
        return None if value is None else float(value)

    def fetch_spread_series(self, long_field: str, short_field: str, limit: int = 250) -> list[dict[str, Any]]:
        if long_field not in MONEY_MARKET_RATE_FIELDS or short_field not in MONEY_MARKET_RATE_FIELDS:
            raise ValueError("invalid money market spread field")
        sql = f"""
            SELECT date,
                   {long_field} AS long_value,
                   {short_field} AS short_value,
                   ({long_field} - {short_field}) AS spread
              FROM {self.spec.table_name}
          ORDER BY date DESC
             LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(sql, (limit,)).fetchall()
            return [dict(row) for row in rows]
