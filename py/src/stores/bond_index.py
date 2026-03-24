from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.core.config import TABLE_RAW_BOND_INDEX
from .base import BaseSqliteStore, TableSpec


BOND_INDEX_NUMERIC_FIELDS = ("dm", "y", "cons_number", "d", "v")

BOND_INDEX_SPEC = TableSpec(
    table_name=TABLE_RAW_BOND_INDEX,
    key_fields=("trade_date", "index_name"),
    date_field="trade_date",
    numeric_fields=BOND_INDEX_NUMERIC_FIELDS,
    integer_fields=("source_row_num",),
    text_fields=(
        "index_name",
        "index_code",
        "provider",
        "type_lv1",
        "type_lv2",
        "type_lv3",
        "source_url",
        "data_date",
        "fetch_status",
        "raw_json",
        "error",
        "source_sheet",
    ),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=(
        "trade_date",
        "index_name",
        "index_code",
        "provider",
        "type_lv1",
        "type_lv2",
        "type_lv3",
        "source_url",
        "data_date",
        *BOND_INDEX_NUMERIC_FIELDS,
        "fetch_status",
        "raw_json",
        "fetched_at",
        "error",
        "source_sheet",
        "source_row_num",
        "migrated_at",
    ),
    default_order_by=("trade_date", "index_name"),
)


@dataclass
class BondIndexStore(BaseSqliteStore):
    spec: TableSpec = BOND_INDEX_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)
