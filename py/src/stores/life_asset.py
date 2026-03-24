from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.core.config import TABLE_RAW_LIFE_ASSET
from .base import BaseSqliteStore, TableSpec


LIFE_ASSET_NUMERIC_FIELDS = (
    "mortgage_rate_est",
    "house_price_tier1",
    "house_price_tier2",
    "house_price_nbs_70city",
    "gold_cny",
    "money_fund_7d",
    "deposit_1y",
)

LIFE_ASSET_SPEC = TableSpec(
    table_name=TABLE_RAW_LIFE_ASSET,
    key_fields=("date",),
    date_field="date",
    numeric_fields=LIFE_ASSET_NUMERIC_FIELDS,
    integer_fields=("source_row_num",),
    text_fields=("source", "source_sheet"),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=("date", *LIFE_ASSET_NUMERIC_FIELDS, "source", "fetched_at", "source_sheet", "source_row_num", "migrated_at"),
    default_order_by=("date",),
)


@dataclass
class LifeAssetStore(BaseSqliteStore):
    spec: TableSpec = LIFE_ASSET_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)
