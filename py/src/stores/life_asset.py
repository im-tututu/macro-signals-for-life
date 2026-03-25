from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.config import TABLE_RAW_LIFE_ASSET
from src.core.utils import now_text, today_ymd, to_float
from src.sources.base import FetchResult
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
    """民生与资产价格原始表 store。

    当前先统一“占位但可落表”的最小结构：
    - 国家统计局房价占位
    - SGE / BOC / 货基占位
    - job 层可安全执行，不阻断主流程
    """

    spec: TableSpec = LIFE_ASSET_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    @classmethod
    def build_row_from_fetch_results(
        cls,
        house_price_result: FetchResult[dict[str, Any]],
        gold_result: FetchResult[dict[str, Any]],
        deposit_result: FetchResult[dict[str, Any]],
        money_fund_result: FetchResult[dict[str, Any]],
    ) -> dict[str, Any]:
        """把多个占位来源结果合并成单条 life_asset 表行。"""

        house = house_price_result.payload
        gold = gold_result.payload
        deposit = deposit_result.payload
        money_fund = money_fund_result.payload
        source_parts = [
            "placeholder: stats_gov",
            "sge",
            "boc",
            "money_fund",
        ]
        return {
            "date": str(house.get("date") or gold.get("date") or deposit.get("date") or money_fund.get("date") or today_ymd()),
            "mortgage_rate_est": None,
            "house_price_tier1": to_float(house.get("house_price_tier1")),
            "house_price_tier2": to_float(house.get("house_price_tier2")),
            "house_price_nbs_70city": to_float(house.get("house_price_nbs_70city")),
            "gold_cny": to_float(gold.get("gold_cny")),
            "money_fund_7d": to_float(money_fund.get("money_fund_7d")),
            "deposit_1y": to_float(deposit.get("deposit_1y")),
            "source": " | ".join(source_parts),
            "fetched_at": now_text(),
        }
