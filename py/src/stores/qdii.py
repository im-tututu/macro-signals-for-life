from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.config import TABLE_RAW_JISILU_QDII
from src.core.models import JisiluQdiiRowSnapshot, JisiluQdiiSnapshot
from src.core.utils import strip_tags, to_float
from src.sources._base import FetchResult
from ._base import BaseSqliteStore, TableSpec


QDII_NUMERIC_FIELDS = (
    "price",
    "pre_close",
    "increase_rt",
    "volume_wan",
    "stock_volume_wan",
    "amount_yi",
    "amount_incr",
    "amount_increase_rt",
    "unit_total_yi",
    "discount_rt",
    "fund_nav",
    "iopv",
    "estimate_value",
    "ref_price",
    "ref_increase_rt",
    "est_val_increase_rt",
    "m_fee",
    "t_fee",
    "mt_fee",
    "nav_discount_rt",
    "iopv_discount_rt",
    "turnover_rt",
    "records_total",
)

QDII_SPEC = TableSpec(
    table_name=TABLE_RAW_JISILU_QDII,
    key_fields=("snapshot_date", "market", "fund_id"),
    date_field="snapshot_date",
    numeric_fields=QDII_NUMERIC_FIELDS,
    integer_fields=("source_row_num",),
    text_fields=(
        "market",
        "market_code",
        "fund_id",
        "fund_nm",
        "fund_nm_display",
        "qtype",
        "index_nm",
        "index_id",
        "issuer_nm",
        "price_dt",
        "nav_dt",
        "iopv_dt",
        "last_est_dt",
        "last_time",
        "last_est_time",
        "apply_fee",
        "apply_status",
        "apply_fee_tips",
        "redeem_fee",
        "redeem_status",
        "redeem_fee_tips",
        "money_cd",
        "asset_ratio",
        "lof_type",
        "t0",
        "eval_show",
        "notes",
        "has_iopv",
        "has_us_ref",
        "source_url",
        "source_sheet",
        "raw_json",
    ),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=(
        "snapshot_date",
        "market",
        "market_code",
        "fund_id",
        "fund_nm",
        "fund_nm_display",
        "qtype",
        "index_nm",
        "index_id",
        "issuer_nm",
        *QDII_NUMERIC_FIELDS,
        "price_dt",
        "nav_dt",
        "iopv_dt",
        "last_est_dt",
        "last_time",
        "last_est_time",
        "apply_fee",
        "apply_status",
        "apply_fee_tips",
        "redeem_fee",
        "redeem_status",
        "redeem_fee_tips",
        "money_cd",
        "asset_ratio",
        "lof_type",
        "t0",
        "eval_show",
        "notes",
        "has_iopv",
        "has_us_ref",
        "source_url",
        "fetched_at",
        "source_sheet",
        "source_row_num",
        "raw_json",
        "migrated_at",
    ),
    default_order_by=("snapshot_date", "market", "fund_id"),
)


@dataclass
class QdiiStore(BaseSqliteStore):
    """QDII 原始表 store。

    这是和 ETF 原始表并列的新表：
    - source 负责抓取集思录 QDII 分市场视图
    - store 负责把快照列表映射成 raw_jisilu_qdii 多行
    - raw 层保留 market 维度和 raw_json，方便后续细化字段映射
    """

    spec: TableSpec = QDII_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    @staticmethod
    def build_row_from_payload_row(
        snapshot_date: str,
        fetched_at: str,
        market: str,
        market_code: str,
        row: JisiluQdiiRowSnapshot,
        *,
        records_total: Any = "",
        source_url: str = "",
    ) -> dict[str, Any]:
        cell = row.cell
        fund_nm = str(cell.get("fund_nm") or cell.get("fund_name") or "")
        return {
            "snapshot_date": snapshot_date,
            "fetched_at": fetched_at,
            "market": market,
            "market_code": market_code,
            "fund_id": row.fund_id,
            "fund_nm": fund_nm,
            "fund_nm_display": strip_tags(str(cell.get("fund_nm_color") or fund_nm)),
            "qtype": str(cell.get("qtype") or ""),
            "index_nm": str(cell.get("index_nm") or cell.get("index_name") or cell.get("ref_nm") or ""),
            "index_id": str(cell.get("index_id") or ""),
            "issuer_nm": str(cell.get("issuer_nm") or cell.get("company_nm") or ""),
            "price": to_float(cell.get("price")),
            "pre_close": to_float(cell.get("pre_close")),
            "increase_rt": to_float(cell.get("increase_rt")),
            "volume_wan": to_float(cell.get("volume")),
            "stock_volume_wan": to_float(cell.get("stock_volume")),
            "amount_yi": to_float(cell.get("amount")),
            "amount_incr": to_float(cell.get("amount_incr")),
            "amount_increase_rt": to_float(cell.get("amount_increase_rt")),
            "unit_total_yi": to_float(cell.get("unit_total")),
            "discount_rt": to_float(cell.get("discount_rt")),
            "fund_nav": to_float(cell.get("fund_nav")),
            "iopv": to_float(cell.get("iopv")),
            "ref_price": to_float(cell.get("ref_price")),
            "ref_increase_rt": to_float(cell.get("ref_increase_rt")),
            "est_val_increase_rt": to_float(cell.get("est_val_increase_rt")),
            "m_fee": to_float(cell.get("m_fee")),
            "t_fee": to_float(cell.get("t_fee")),
            "mt_fee": to_float(cell.get("mt_fee")),
            "nav_discount_rt": to_float(cell.get("nav_discount_rt")),
            "iopv_discount_rt": to_float(cell.get("iopv_discount_rt")),
            "turnover_rt": to_float(cell.get("turnover_rt")),
            "price_dt": str(cell.get("price_dt") or ""),
            "nav_dt": str(cell.get("nav_dt") or ""),
            "iopv_dt": str(cell.get("iopv_dt") or ""),
            "estimate_value": to_float(cell.get("estimate_value")),
            "last_est_dt": str(cell.get("last_est_dt") or ""),
            "last_time": str(cell.get("last_time") or ""),
            "last_est_time": str(cell.get("last_est_time") or ""),
            "apply_fee": str(cell.get("apply_fee") or ""),
            "apply_status": str(cell.get("apply_status") or ""),
            "apply_fee_tips": str(cell.get("apply_fee_tips") or ""),
            "redeem_fee": str(cell.get("redeem_fee") or ""),
            "redeem_status": str(cell.get("redeem_status") or ""),
            "redeem_fee_tips": str(cell.get("redeem_fee_tips") or ""),
            "money_cd": str(cell.get("money_cd") or ""),
            "asset_ratio": str(cell.get("asset_ratio") or ""),
            "lof_type": str(cell.get("lof_type") or ""),
            "t0": str(cell.get("t0") or ""),
            "eval_show": str(cell.get("eval_show") or ""),
            "notes": str(cell.get("notes") or ""),
            "has_iopv": str(cell.get("has_iopv") or ""),
            "has_us_ref": str(cell.get("has_us_ref") or ""),
            "records_total": to_float(records_total),
            "source_url": source_url,
            "raw_json": json.dumps(cell, ensure_ascii=False, sort_keys=True),
        }

    @classmethod
    def build_rows_from_fetch_result(cls, fetch_result: FetchResult[JisiluQdiiSnapshot]) -> list[dict[str, Any]]:
        payload = fetch_result.payload
        snapshot_date = str(payload.snapshot_date or "")
        fetched_at = str(payload.fetched_at or "")
        market = str(payload.market or "")
        market_code = str(payload.market_code or "")
        rows = payload.rows or []
        records_total = payload.records_total
        source_url = str(payload.source_url or fetch_result.source_url or "")
        if not snapshot_date:
            raise ValueError("QDII snapshot missing snapshot_date")
        if not fetched_at:
            raise ValueError("QDII snapshot missing fetched_at")
        if not market or not market_code:
            raise ValueError("QDII snapshot missing market identity")
        if not rows:
            raise ValueError("QDII snapshot rows are empty")
        return [
            cls.build_row_from_payload_row(
                snapshot_date,
                fetched_at,
                market,
                market_code,
                row,
                records_total=records_total,
                source_url=source_url,
            )
            for row in rows
        ]
