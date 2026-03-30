from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.config import TABLE_RAW_JISILU_TREASURY
from src.core.models import JisiluTreasuryRowSnapshot, JisiluTreasurySnapshot
from src.core.utils import norm_ymd, to_float
from src.sources._base import FetchResult
from ._base import BaseSqliteStore, TableSpec


TREASURY_NUMERIC_FIELDS = (
    "price",
    "full_price",
    "increase_rt",
    "volume_wan",
    "ask_1",
    "bid_1",
    "days_to_coupon",
    "years_left",
    "duration",
    "ytm",
    "coupon_rt",
    "repo_ratio",
    "repo_usage_rt",
    "size_yi",
)

TREASURY_SPEC = TableSpec(
    table_name=TABLE_RAW_JISILU_TREASURY,
    key_fields=("snapshot_date", "fetched_at", "bond_id"),
    date_field="snapshot_date",
    numeric_fields=TREASURY_NUMERIC_FIELDS,
    integer_fields=("source_row_num",),
    text_fields=(
        "bond_id",
        "bond_nm",
        "maturity_dt",
        "source_url",
        "source_sheet",
        "raw_json",
    ),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=(
        "snapshot_date",
        "bond_id",
        "bond_nm",
        *TREASURY_NUMERIC_FIELDS,
        "maturity_dt",
        "source_url",
        "source_sheet",
        "source_row_num",
        "raw_json",
        "fetched_at",
        "migrated_at",
    ),
    default_order_by=("snapshot_date", "fetched_at", "bond_id"),
)


@dataclass
class TreasuryStore(BaseSqliteStore):
    """集思录国债现券原始表 store。"""

    spec: TableSpec = TREASURY_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    @staticmethod
    def build_row_from_payload_row(
        snapshot_date: str,
        fetched_at: str,
        row: JisiluTreasuryRowSnapshot,
        *,
        source_url: str = "",
    ) -> dict[str, Any]:
        fields = row.fields
        return {
            "snapshot_date": snapshot_date,
            "fetched_at": fetched_at,
            "bond_id": row.bond_id,
            "bond_nm": str(fields.get("bond_nm") or ""),
            "price": to_float(fields.get("price")),
            "full_price": to_float(fields.get("full_price")),
            "increase_rt": to_float(fields.get("increase_rt")),
            "volume_wan": to_float(fields.get("volume_wan")),
            "ask_1": to_float(fields.get("ask_1")),
            "bid_1": to_float(fields.get("bid_1")),
            "days_to_coupon": to_float(fields.get("days_to_coupon")),
            "years_left": to_float(fields.get("years_left")),
            "duration": to_float(fields.get("duration")),
            "ytm": to_float(fields.get("ytm")),
            "coupon_rt": to_float(fields.get("coupon_rt")),
            "repo_ratio": to_float(fields.get("repo_ratio")),
            "repo_usage_rt": to_float(fields.get("repo_usage_rt")),
            "maturity_dt": norm_ymd(fields.get("maturity_dt")),
            "size_yi": to_float(fields.get("size_yi")),
            "source_url": source_url,
            "raw_json": json.dumps(fields, ensure_ascii=False, sort_keys=True, default=str),
        }

    @classmethod
    def build_rows_from_fetch_result(cls, fetch_result: FetchResult[JisiluTreasurySnapshot]) -> list[dict[str, Any]]:
        payload = fetch_result.payload
        snapshot_date = str(payload.snapshot_date or "")
        fetched_at = str(payload.fetched_at or "")
        source_url = str(payload.source_url or fetch_result.source_url or "")
        if not snapshot_date:
            raise ValueError("Treasury snapshot missing snapshot_date")
        if not fetched_at:
            raise ValueError("Treasury snapshot missing fetched_at")
        if not payload.rows:
            raise ValueError("Treasury snapshot rows are empty")
        return [
            cls.build_row_from_payload_row(
                snapshot_date,
                fetched_at,
                row,
                source_url=source_url,
            )
            for row in payload.rows
        ]
