from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.config import TABLE_RAW_POLICY_RATE
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
    spec: TableSpec = POLICY_RATE_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    def fetch_latest_events(self, limit: int = 50, rate_type: str | None = None) -> list[dict[str, Any]]:
        if rate_type:
            return self.fetch_recent(type=rate_type, limit=limit)
        return self.fetch_recent(limit=limit)

    def fetch_series(self, rate_type: str, term: str, limit: int = 250) -> list[dict[str, Any]]:
        return self.fetch_recent(type=rate_type, term=term, limit=limit)
