from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.config import TABLE_RAW_JISILU_MONEY
from src.core.models import JisiluMoneyRowSnapshot, JisiluMoneySnapshot
from src.core.utils import to_float
from src.sources._base import FetchResult
from ._base import BaseSqliteStore, TableSpec


MONEY_NUMERIC_FIELDS = (
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
    "m_fee",
    "t_fee",
    "mt_fee",
    "apply_fee",
    "redeem_fee",
    "records_total",
)

MONEY_SPEC = TableSpec(
    table_name=TABLE_RAW_JISILU_MONEY,
    key_fields=("snapshot_date", "fetched_at", "fund_id"),
    date_field="snapshot_date",
    numeric_fields=MONEY_NUMERIC_FIELDS,
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
        "raw_json",
    ),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=(
        "snapshot_date",
        "fund_id",
        "fund_nm",
        "index_nm",
        "issuer_nm",
        *MONEY_NUMERIC_FIELDS,
        "nav_dt",
        "last_time",
        "last_est_time",
        "is_qdii",
        "is_t0",
        "source_url",
        "fetched_at",
        "source_sheet",
        "source_row_num",
        "raw_json",
        "migrated_at",
    ),
    default_order_by=("snapshot_date", "fetched_at", "fund_id"),
)


@dataclass
class MoneyEtfStore(BaseSqliteStore):
    """集思录货币 ETF 原始表 store。"""

    spec: TableSpec = MONEY_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    @staticmethod
    def build_row_from_payload_row(
        snapshot_date: str,
        fetched_at: str,
        row: JisiluMoneyRowSnapshot,
        *,
        records_total: Any = "",
        source_url: str = "",
    ) -> dict[str, Any]:
        cell = row.cell
        return {
            "snapshot_date": snapshot_date,
            "fetched_at": fetched_at,
            "fund_id": row.fund_id,
            "fund_nm": str(cell.get("fund_nm") or ""),
            "index_nm": str(cell.get("index_nm") or ""),
            "issuer_nm": str(cell.get("issuer_nm") or ""),
            "price": to_float(cell.get("price")),
            "increase_rt": to_float(cell.get("increase_rt")),
            "volume_wan": to_float(cell.get("volume")),
            "amount_yi": to_float(cell.get("amount")),
            "unit_total_yi": to_float(cell.get("unit_total")),
            "discount_rt": to_float(cell.get("discount_rt")),
            "fund_nav": to_float(cell.get("fund_nav")),
            "nav_dt": str(cell.get("nav_dt") or ""),
            "estimate_value": to_float(cell.get("estimate_value")),
            "creation_unit": to_float(cell.get("creation_unit")),
            "pe": to_float(cell.get("pe")),
            "pb": to_float(cell.get("pb")),
            "m_fee": to_float(cell.get("m_fee")),
            "t_fee": to_float(cell.get("t_fee")),
            "mt_fee": to_float(cell.get("mt_fee")),
            "last_time": str(cell.get("last_time") or ""),
            "last_est_time": str(cell.get("last_est_time") or ""),
            "is_qdii": str(cell.get("is_qdii") or ""),
            "is_t0": str(cell.get("is_t0") or ""),
            "apply_fee": to_float(cell.get("apply_fee")),
            "redeem_fee": to_float(cell.get("redeem_fee")),
            "records_total": to_float(records_total),
            "source_url": source_url,
            "raw_json": json.dumps(cell, ensure_ascii=False, sort_keys=True),
        }

    @classmethod
    def build_rows_from_fetch_result(cls, fetch_result: FetchResult[JisiluMoneySnapshot]) -> list[dict[str, Any]]:
        payload = fetch_result.payload
        snapshot_date = str(payload.snapshot_date or "")
        fetched_at = str(payload.fetched_at or "")
        rows = payload.rows or []
        records_total = payload.records_total
        source_url = str(payload.source_url or fetch_result.source_url or "")
        if not snapshot_date:
            raise ValueError("Money snapshot missing snapshot_date")
        if not fetched_at:
            raise ValueError("Money snapshot missing fetched_at")
        if not rows:
            raise ValueError("Money snapshot rows are empty")
        return [
            cls.build_row_from_payload_row(
                snapshot_date,
                fetched_at,
                row,
                records_total=records_total,
                source_url=source_url,
            )
            for row in rows
        ]
