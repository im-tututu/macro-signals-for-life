from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Any

from src.core.models import BondIndexSnapshot
from src.core.utils import now_text
from src.core.config import (
    TABLE_RAW_CHINABOND_BOND_INDEX,
    TABLE_RAW_CNINDEX_BOND_INDEX,
    TABLE_RAW_CSINDEX_BOND_INDEX,
)
from src.sources._base import FetchResult
from ._base import BaseSqliteStore, TableSpec


BOND_INDEX_NUMERIC_FIELDS = ("dm", "y", "cons_number", "d", "v")


def _build_spec(table_name: str) -> TableSpec:
    return TableSpec(
        table_name=table_name,
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


CHINABOND_BOND_INDEX_SPEC = _build_spec(TABLE_RAW_CHINABOND_BOND_INDEX)
CSINDEX_BOND_INDEX_SPEC = _build_spec(TABLE_RAW_CSINDEX_BOND_INDEX)
CNINDEX_BOND_INDEX_SPEC = _build_spec(TABLE_RAW_CNINDEX_BOND_INDEX)


class BaseBondIndexStore(BaseSqliteStore):
    spec: TableSpec
    provider_name: str = ""

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    @classmethod
    def build_row_from_fetch_result(cls, fetch_result: FetchResult[BondIndexSnapshot]) -> dict[str, Any]:
        """把单指数抓取结果转成表行。"""

        snapshot = fetch_result.payload
        index_name = str(fetch_result.meta.get("index_name") or snapshot.index_id)
        index_code = str(fetch_result.meta.get("index_code") or snapshot.index_id)
        return {
            "trade_date": snapshot.date,
            "index_name": index_name,
            "index_code": index_code,
            "provider": cls.provider_name,
            "type_lv1": "",
            "type_lv2": "",
            "type_lv3": "",
            "source_url": fetch_result.source_url,
            "data_date": snapshot.date,
            "dm": snapshot.duration,
            "y": snapshot.ytm,
            "cons_number": snapshot.cons_number,
            "d": snapshot.modified_duration,
            "v": snapshot.convexity,
            "fetch_status": "OK",
            "raw_json": json.dumps(snapshot.meta, ensure_ascii=False, sort_keys=True, default=str),
            "fetched_at": now_text(),
            "error": "",
        }


class ChinabondBondIndexStore(BaseBondIndexStore):
    spec: TableSpec = CHINABOND_BOND_INDEX_SPEC
    provider_name: str = "CHINABOND"


class CsindexBondIndexStore(BaseBondIndexStore):
    spec: TableSpec = CSINDEX_BOND_INDEX_SPEC
    provider_name: str = "CSINDEX"


class CnindexBondIndexStore(BaseBondIndexStore):
    spec: TableSpec = CNINDEX_BOND_INDEX_SPEC
    provider_name: str = "CNINDEX"
