from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.core.config import TABLE_RAW_JISILU_ETF
from .base import BaseSqliteStore, TableSpec


ETF_NUMERIC_FIELDS = (
    "price",
    "increase_rt",
    "volume_wan",
    "amount_yi",
    "unit_total_yi",
    "discount_rt",
    "fund_nav",
    "estimate_value",
    "creation_unit",
    "pe",
    "pb",
    "apply_fee",
    "redeem_fee",
    "records_total",
)

ETF_SPEC = TableSpec(
    table_name=TABLE_RAW_JISILU_ETF,
    key_fields=("snapshot_date", "fund_id"),
    date_field="snapshot_date",
    numeric_fields=ETF_NUMERIC_FIELDS,
    integer_fields=("source_row_num",),
    text_fields=(
        "fund_id",
        "fund_nm",
        "index_nm",
        "issuer_nm",
        "nav_dt",
        "last_time",
        "last_est_time",
        "is_qdii",
        "is_t0",
        "source_url",
        "source_sheet",
    ),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=(
        "snapshot_date",
        "fund_id",
        "fund_nm",
        "index_nm",
        "issuer_nm",
        *ETF_NUMERIC_FIELDS,
        "nav_dt",
        "last_time",
        "last_est_time",
        "is_qdii",
        "is_t0",
        "source_url",
        "fetched_at",
        "source_sheet",
        "source_row_num",
        "migrated_at",
    ),
    default_order_by=("snapshot_date", "fund_id"),
)


@dataclass
class EtfStore(BaseSqliteStore):
    spec: TableSpec = ETF_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)
