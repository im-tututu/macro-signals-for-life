from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.core.config import TABLE_METRICS
from ._base import BaseSqliteStore, TableSpec


METRICS_SPEC = TableSpec(
    table_name=TABLE_METRICS,
    key_fields=("date", "metric_key"),
    date_field="date",
    numeric_fields=("metric_value", "score", "percentile"),
    integer_fields=("source_row_num",),
    text_fields=("metric_key", "metric_name", "metric_unit", "metric_group", "note", "source", "source_sheet"),
    datetime_fields=("fetched_at", "migrated_at"),
    default_order_by=("date", "metric_key"),
)


@dataclass
class MetricsStore(BaseSqliteStore):
    spec: TableSpec = METRICS_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        # metrics/signals 还没有在仓库当前默认迁移中固化；这里保留统一接口，
        # 但初始化是否成功取决于用户后续是否补充对应 migration。
        super().__init__(db_path=db_path, auto_init=auto_init)
