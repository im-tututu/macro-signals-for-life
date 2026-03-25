from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.models import PolicyRateEvent
from src.core.config import TABLE_RAW_POLICY_RATE
from src.sources.base import FetchResult
from .base import BaseSqliteStore, TableSpec


POLICY_RATE_SPEC = TableSpec(
    table_name=TABLE_RAW_POLICY_RATE,
    key_fields=("date", "type", "term"),
    date_field="date",
    numeric_fields=("rate", "amount"),
    integer_fields=("source_row_num",),
    text_fields=("type", "term", "source", "note", "source_sheet"),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=(
        "date",
        "type",
        "term",
        "rate",
        "amount",
        "source",
        "fetched_at",
        "note",
        "source_sheet",
        "source_row_num",
        "migrated_at",
    ),
    default_order_by=("date", "type", "term"),
)


@dataclass
class PolicyRateStore(BaseSqliteStore):
    """政策利率原始表 store。

    职责分工：
    - source 负责抓取和解析央行公告
    - store 负责把 PolicyRateEvent 映射成 raw_policy_rate 表行
    - job 负责定义抓取窗口与写入策略
    """

    spec: TableSpec = POLICY_RATE_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    @staticmethod
    def build_row_from_event(event: PolicyRateEvent) -> dict[str, Any]:
        """把单条政策事件转成表行。"""

        return {
            "date": event.date,
            "type": event.type,
            "term": event.term,
            "rate": event.rate,
            "amount": event.amount,
            "source": event.source,
            "fetched_at": event.fetched_at,
            "note": event.note,
        }

    @classmethod
    def build_rows_from_fetch_result(
        cls,
        fetch_result: FetchResult[list[PolicyRateEvent]],
    ) -> list[dict[str, Any]]:
        """把 source 返回的政策事件列表转成多条表行。"""

        return [cls.build_row_from_event(event) for event in fetch_result.payload if event.date and event.type and event.term]

    def fetch_latest_events(self, limit: int = 50, rate_type: str | None = None) -> list[dict[str, Any]]:
        if rate_type:
            return self.fetch_recent(type=rate_type, limit=limit)
        return self.fetch_recent(limit=limit)

    def fetch_series(self, rate_type: str, term: str, limit: int = 250) -> list[dict[str, Any]]:
        return self.fetch_recent(type=rate_type, term=term, limit=limit)
