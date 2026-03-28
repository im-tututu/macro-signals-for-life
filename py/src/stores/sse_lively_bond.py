from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.config import TABLE_RAW_SSE_LIVELY_BOND
from src.core.models import SseLivelyBondRowSnapshot, SseLivelyBondSnapshot
from src.core.utils import norm_ymd, to_float
from src.sources._base import FetchResult
from ._base import BaseSqliteStore, TableSpec


SSE_LIVELY_BOND_NUMERIC_FIELDS = (
    "open_price",
    "close_price",
    "change_ratio",
    "amplitude",
    "volume_hand",
    "amount_wanyuan",
    "ytm",
)

SSE_LIVELY_BOND_SPEC = TableSpec(
    table_name=TABLE_RAW_SSE_LIVELY_BOND,
    key_fields=("trade_date", "bond_id"),
    date_field="trade_date",
    numeric_fields=SSE_LIVELY_BOND_NUMERIC_FIELDS,
    integer_fields=("rank_num", "source_row_num"),
    text_fields=(
        "bond_id",
        "bond_nm",
        "bond_nm_full",
        "source_url",
        "source_sheet",
        "raw_json",
    ),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=(
        "trade_date",
        "bond_id",
        "bond_nm",
        "bond_nm_full",
        "rank_num",
        *SSE_LIVELY_BOND_NUMERIC_FIELDS,
        "source_url",
        "source_sheet",
        "source_row_num",
        "raw_json",
        "fetched_at",
        "migrated_at",
    ),
    default_order_by=("trade_date", "rank_num", "bond_id"),
)


@dataclass
class SseLivelyBondStore(BaseSqliteStore):
    """上交所活跃国债原始表 store。"""

    spec: TableSpec = SSE_LIVELY_BOND_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    @staticmethod
    def build_row_from_payload_row(
        fetched_at: str,
        row: SseLivelyBondRowSnapshot,
        *,
        source_url: str = "",
    ) -> dict[str, Any]:
        fields = row.fields
        return {
            "trade_date": row.trade_date,
            "fetched_at": fetched_at,
            "rank_num": int(float(fields["NUM"])) if str(fields.get("NUM") or "").strip() else None,
            "bond_id": row.bond_id,
            "bond_nm": str(fields.get("SEC_NAME") or ""),
            "bond_nm_full": str(fields.get("SECURITY_ABBR_FULL") or ""),
            "open_price": to_float(fields.get("OPEN_PRICE")),
            "close_price": to_float(fields.get("CLOSE_PRICE")),
            "change_ratio": to_float(fields.get("SUM_CHANGE_RATIO")),
            "amplitude": to_float(fields.get("QJZF")),
            "volume_hand": to_float(fields.get("SUM_TRADE_VOL")),
            "amount_wanyuan": to_float(fields.get("SUM_TRADE_AMT")),
            "ytm": to_float(fields.get("SUM_TO_RATE")),
            "source_url": source_url,
            "raw_json": json.dumps(fields, ensure_ascii=False, sort_keys=True, default=str),
        }

    @classmethod
    def build_rows_from_fetch_result(cls, fetch_result: FetchResult[SseLivelyBondSnapshot]) -> list[dict[str, Any]]:
        payload = fetch_result.payload
        fetched_at = str(payload.fetched_at or fetch_result.meta.get("fetched_at") or "")
        source_url = str(payload.source_url or fetch_result.source_url or "")
        rows = payload.rows or []
        if not fetched_at:
            raise ValueError("SSE lively bond snapshot missing fetched_at")
        if not rows:
            raise ValueError("SSE lively bond snapshot rows are empty")
        return [
            cls.build_row_from_payload_row(
                fetched_at,
                row,
                source_url=source_url,
            )
            for row in rows
        ]
