from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.models import Observation
from src.core.utils import now_text, today_ymd
from src.core.config import TABLE_RAW_OVERSEAS_MACRO
from src.sources.base import FetchResult
from .base import BaseSqliteStore, TableSpec


OVERSEAS_NUMERIC_FIELDS = (
    "fed_upper",
    "fed_lower",
    "sofr",
    "ust_2y",
    "ust_10y",
    "us_real_10y",
    "usd_broad",
    "usd_cny",
    "gold",
    "wti",
    "brent",
    "copper",
    "vix",
    "spx",
    "nasdaq_100",
)

OVERSEAS_SPEC = TableSpec(
    table_name=TABLE_RAW_OVERSEAS_MACRO,
    key_fields=("date",),
    date_field="date",
    numeric_fields=OVERSEAS_NUMERIC_FIELDS,
    integer_fields=("source_row_num",),
    text_fields=("source", "source_sheet"),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=(
        "date",
        *OVERSEAS_NUMERIC_FIELDS,
        "source",
        "fetched_at",
        "source_sheet",
        "source_row_num",
        "migrated_at",
    ),
    default_order_by=("date",),
)


@dataclass
class OverseasStore(BaseSqliteStore):
    """海外宏观原始表 store。

    职责分工：
    - source 分别抓取 FRED / Alpha Vantage
    - store 负责把两边 observation 合并成一条 raw_overseas_macro 表行
    - job 负责定义抓取频率与写入策略
    """

    spec: TableSpec = OVERSEAS_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    @staticmethod
    def _pick_obs_date(obs: Observation | None) -> str:
        if obs is None:
            return ""
        return obs.date or ""

    @staticmethod
    def _pick_obs_value(obs: Observation | None) -> float | None:
        if obs is None:
            return None
        return obs.value

    @staticmethod
    def _build_source_note(
        fred_payload: dict[str, Observation],
        alpha_payload: dict[str, Observation | None],
    ) -> str:
        parts: list[str] = []
        if fred_payload:
            parts.append("FRED")
        if alpha_payload:
            parts.append("ALPHA_VANTAGE")
        return " | ".join(parts)

    @classmethod
    def build_row_from_fetch_results(
        cls,
        fred_result: FetchResult[dict[str, Observation]],
        alpha_result: FetchResult[dict[str, Observation | None]],
    ) -> dict[str, Any]:
        """把两个来源的抓取结果合并成单条海外宏观表行。"""

        fred_payload = fred_result.payload
        alpha_payload = alpha_result.payload
        row_date = (
            cls._pick_obs_date(fred_payload.get("spx"))
            or cls._pick_obs_date(fred_payload.get("nasdaq_100"))
            or cls._pick_obs_date(fred_payload.get("ust_10y"))
            or cls._pick_obs_date(fred_payload.get("sofr"))
            or today_ymd()
        )
        return {
            "date": row_date,
            "fed_upper": cls._pick_obs_value(fred_payload.get("fed_upper")),
            "fed_lower": cls._pick_obs_value(fred_payload.get("fed_lower")),
            "sofr": cls._pick_obs_value(fred_payload.get("sofr")),
            "ust_2y": cls._pick_obs_value(fred_payload.get("ust_2y")),
            "ust_10y": cls._pick_obs_value(fred_payload.get("ust_10y")),
            "us_real_10y": cls._pick_obs_value(fred_payload.get("us_real_10y")),
            "usd_broad": cls._pick_obs_value(fred_payload.get("usd_broad")),
            "usd_cny": cls._pick_obs_value(fred_payload.get("usd_cny")),
            "gold": cls._pick_obs_value(alpha_payload.get("gold")),
            "wti": cls._pick_obs_value(alpha_payload.get("wti")),
            "brent": cls._pick_obs_value(alpha_payload.get("brent")),
            "copper": cls._pick_obs_value(alpha_payload.get("copper")),
            "vix": cls._pick_obs_value(fred_payload.get("vix")),
            "spx": cls._pick_obs_value(fred_payload.get("spx")),
            "nasdaq_100": cls._pick_obs_value(fred_payload.get("nasdaq_100")),
            "source": cls._build_source_note(fred_payload, alpha_payload),
            "fetched_at": now_text(),
        }

    def fetch_series(self, field_name: str, limit: int = 250) -> list[dict[str, Any]]:
        if field_name not in OVERSEAS_NUMERIC_FIELDS:
            raise ValueError(f"invalid overseas field: {field_name}")
        sql = f"SELECT date, {field_name} AS value FROM {self.spec.table_name} ORDER BY date DESC LIMIT ?"
        with self._connect() as conn:
            rows = conn.execute(sql, (limit,)).fetchall()
            return [dict(row) for row in rows]

    def fetch_spread_series(self, long_field: str, short_field: str, limit: int = 250) -> list[dict[str, Any]]:
        if long_field not in OVERSEAS_NUMERIC_FIELDS or short_field not in OVERSEAS_NUMERIC_FIELDS:
            raise ValueError("invalid overseas spread field")
        sql = f"""
            SELECT date,
                   {long_field} AS long_value,
                   {short_field} AS short_value,
                   ({long_field} - {short_field}) AS spread
              FROM {self.spec.table_name}
          ORDER BY date DESC
             LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(sql, (limit,)).fetchall()
            return [dict(row) for row in rows]
