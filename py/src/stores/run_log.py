from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.core.config import TABLE_RUN_LOG
from ._base import BaseSqliteStore, TableSpec


RUN_LOG_SPEC = TableSpec(
    table_name=TABLE_RUN_LOG,
    key_fields=("timestamp", "job_name"),
    date_field="timestamp",
    integer_fields=("source_row_num",),
    text_fields=("job_name", "status", "message", "detail", "source_sheet"),
    datetime_fields=("migrated_at",),
    default_order_by=("timestamp", "job_name"),
)


@dataclass
class RunLogStore(BaseSqliteStore):
    spec: TableSpec = RUN_LOG_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)
