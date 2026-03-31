from __future__ import annotations

from dataclasses import dataclass

from src.datasets.raw_schema import (
    RAW_ALPHA_VANTAGE_DEF,
    RAW_CHINABOND_BOND_INDEX_DEF,
    RAW_CHINABOND_CURVE_DEF,
    RAW_CNINDEX_BOND_INDEX_DEF,
    RAW_CSINDEX_BOND_INDEX_DEF,
    RAW_FRED_DEF,
    RAW_FUTURES_DEF,
    RAW_JISILU_ETF_DEF,
    RAW_JISILU_GOLD_ETF_DEF,
    RAW_JISILU_MONEY_ETF_DEF,
    RAW_JISILU_QDII_DEF,
    RAW_JISILU_TREASURY_DEF,
    RAW_MONEY_MARKET_DEF,
    RAW_POLICY_RATE_DEF,
    RAW_SSE_LIVELY_BOND_DEF,
    RawSchemaDef,
)
from src.stores._base import TableSpec


@dataclass(frozen=True)
class RawDatasetSpec:
    dataset_id: str
    schema: RawSchemaDef

    @property
    def table_name(self) -> str:
        return self.schema.table_name

    @property
    def table_spec(self) -> TableSpec:
        return self.schema.table_spec

    @property
    def build_row(self):
        return self.schema.build_row

    @property
    def build_rows(self):
        return self.schema.build_rows


RAW_DATASET_REGISTRY: dict[str, RawDatasetSpec] = {
    spec.dataset_id: spec
    for spec in (
        RawDatasetSpec("fred", RAW_FRED_DEF),
        RawDatasetSpec("alpha_vantage", RAW_ALPHA_VANTAGE_DEF),
        RawDatasetSpec("futures", RAW_FUTURES_DEF),
        RawDatasetSpec("money_market", RAW_MONEY_MARKET_DEF),
        RawDatasetSpec("chinabond_curve", RAW_CHINABOND_CURVE_DEF),
        RawDatasetSpec("policy_rate", RAW_POLICY_RATE_DEF),
        RawDatasetSpec("sse_lively_bond", RAW_SSE_LIVELY_BOND_DEF),
        RawDatasetSpec("jisilu_treasury", RAW_JISILU_TREASURY_DEF),
        RawDatasetSpec("jisilu_gold_etf", RAW_JISILU_GOLD_ETF_DEF),
        RawDatasetSpec("jisilu_money_etf", RAW_JISILU_MONEY_ETF_DEF),
        RawDatasetSpec("jisilu_etf", RAW_JISILU_ETF_DEF),
        RawDatasetSpec("jisilu_qdii", RAW_JISILU_QDII_DEF),
        RawDatasetSpec("chinabond_bond_index", RAW_CHINABOND_BOND_INDEX_DEF),
        RawDatasetSpec("csindex_bond_index", RAW_CSINDEX_BOND_INDEX_DEF),
        RawDatasetSpec("cnindex_bond_index", RAW_CNINDEX_BOND_INDEX_DEF),
    )
}

RAW_TABLE_REGISTRY: dict[str, RawDatasetSpec] = {
    spec.table_name: spec
    for spec in RAW_DATASET_REGISTRY.values()
}


def get_raw_dataset_spec(dataset_id: str) -> RawDatasetSpec:
    try:
        return RAW_DATASET_REGISTRY[dataset_id]
    except KeyError as exc:
        raise ValueError(f"unknown raw dataset: {dataset_id}") from exc


def get_raw_spec_by_table_name(table_name: str) -> RawDatasetSpec:
    try:
        return RAW_TABLE_REGISTRY[table_name]
    except KeyError as exc:
        raise ValueError(f"unknown raw table: {table_name}") from exc
