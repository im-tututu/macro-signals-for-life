from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.core.config import TABLE_SIGNAL_MAIN
from .base import BaseSqliteStore, TableSpec


SIGNAL_MAIN_SPEC = TableSpec(
    table_name=TABLE_SIGNAL_MAIN,
    key_fields=("date", "signal_key"),
    date_field="date",
    numeric_fields=("score",),
    integer_fields=("source_row_num",),
    text_fields=("signal_key", "title", "value", "note", "source", "source_sheet"),
    datetime_fields=("fetched_at", "migrated_at"),
    default_order_by=("date", "signal_key"),
)


@dataclass
class SignalsStore(BaseSqliteStore):
    spec: TableSpec = SIGNAL_MAIN_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)
