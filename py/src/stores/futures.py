from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.core.config import TABLE_RAW_FUTURES
from .base import BaseSqliteStore, TableSpec


FUTURES_SPEC = TableSpec(
    table_name=TABLE_RAW_FUTURES,
    key_fields=("date",),
    date_field="date",
    numeric_fields=("t0_last", "tf0_last"),
    integer_fields=("source_row_num",),
    text_fields=("source", "source_sheet"),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=("date", "t0_last", "tf0_last", "source", "fetched_at", "source_sheet", "source_row_num", "migrated_at"),
    default_order_by=("date",),
)


@dataclass
class FuturesStore(BaseSqliteStore):
    spec: TableSpec = FUTURES_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)
