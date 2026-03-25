from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.models import FuturesSnapshot
from src.core.config import TABLE_RAW_FUTURES
from src.sources.base import FetchResult
from .base import BaseSqliteStore, TableSpec


FUTURES_SPEC = TableSpec(
    table_name=TABLE_RAW_FUTURES,
    key_fields=("date",),
    date_field="date",
    numeric_fields=("t0_last", "tf0_last"),
    integer_fields=("source_row_num",),
    text_fields=("source", "source_sheet"),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=("date", "t0_last", "tf0_last", "source", "fetched_at", "source_sheet", "source_row_num", "migrated_at"),
    default_order_by=("date",),
)


@dataclass
class FuturesStore(BaseSqliteStore):
    """国债期货原始表 store。

    职责分工：
    - source 负责抓取期货快照
    - store 负责把 FuturesSnapshot 映射成 raw_futures 表行
    - job 负责定义抓取时点与写入策略
    """

    spec: TableSpec = FUTURES_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    @staticmethod
    def build_row_from_snapshot(snapshot: FuturesSnapshot) -> dict[str, Any]:
        """把单条期货快照转成表行。"""

        return {
            "date": snapshot.date,
            "t0_last": snapshot.values.get("t0_last"),
            "tf0_last": snapshot.values.get("tf0_last"),
            "source": snapshot.source,
            "fetched_at": snapshot.fetched_at,
        }

    @classmethod
    def build_row_from_fetch_result(cls, fetch_result: FetchResult[FuturesSnapshot]) -> dict[str, Any]:
        """把 source 返回的抓取结果统一转成表行。"""

        return cls.build_row_from_snapshot(fetch_result.payload)
