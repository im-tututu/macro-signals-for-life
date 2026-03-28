from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.config import TABLE_RAW_SSE_LIVELY_BOND
from src.core.utils import norm_ymd, to_float
from src.sources.base import FetchResult
from .base import BaseSqliteStore, TableSpec


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
        row: dict[str, Any],
        *,
        source_url: str = "",
    ) -> dict[str, Any]:
        return {
            "trade_date": norm_ymd(row.get("TRADE_DATE")),
            "fetched_at": fetched_at,
            "rank_num": int(float(row["NUM"])) if str(row.get("NUM") or "").strip() else None,
            "bond_id": str(row.get("SEC_CODE") or ""),
            "bond_nm": str(row.get("SEC_NAME") or ""),
            "bond_nm_full": str(row.get("SECURITY_ABBR_FULL") or ""),
            "open_price": to_float(row.get("OPEN_PRICE")),
            "close_price": to_float(row.get("CLOSE_PRICE")),
            "change_ratio": to_float(row.get("SUM_CHANGE_RATIO")),
            "amplitude": to_float(row.get("QJZF")),
            "volume_hand": to_float(row.get("SUM_TRADE_VOL")),
            "amount_wanyuan": to_float(row.get("SUM_TRADE_AMT")),
            "ytm": to_float(row.get("SUM_TO_RATE")),
            "source_url": source_url,
            "raw_json": json.dumps(row, ensure_ascii=False, sort_keys=True, default=str),
        }

    @classmethod
    def build_rows_from_fetch_result(cls, fetch_result: FetchResult[dict[str, Any]]) -> list[dict[str, Any]]:
        payload = fetch_result.payload
        fetched_at = str(payload.get("fetched_at") or fetch_result.meta.get("fetched_at") or "")
        source_url = str(fetch_result.source_url or "")
        rows = payload.get("rows", []) or []
        return [
            cls.build_row_from_payload_row(
                fetched_at,
                row,
                source_url=source_url,
            )
            for row in rows
        ]
