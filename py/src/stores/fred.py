from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.config import TABLE_RAW_FRED
from src.core.models import Observation
from src.sources._base import FetchResult

from ._base import BaseSqliteStore, TableSpec


FRED_NUMERIC_FIELDS = (
    "fed_upper",
    "fed_lower",
    "sofr",
    "ust_2y",
    "ust_10y",
    "us_real_10y",
    "usd_broad",
    "usd_cny",
    "vix",
    "spx",
    "nasdaq_100",
)

FRED_SPEC = TableSpec(
    table_name=TABLE_RAW_FRED,
    key_fields=("date",),
    date_field="date",
    numeric_fields=FRED_NUMERIC_FIELDS,
    integer_fields=("source_row_num",),
    text_fields=("source", "source_sheet"),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=(
        "date",
        *FRED_NUMERIC_FIELDS,
        "source",
        "fetched_at",
        "source_sheet",
        "source_row_num",
        "migrated_at",
    ),
    default_order_by=("date",),
)


@dataclass
class FredStore(BaseSqliteStore):
    spec: TableSpec = FRED_SPEC

    def __init__(self, db_path: Path | None = None, *, auto_init: bool = True) -> None:
        super().__init__(db_path=db_path, auto_init=auto_init)

    @staticmethod
    def _pick_obs_date(obs: Observation | None) -> str:
        if obs is None:
            return ""
        return str(obs.date or "")

    @staticmethod
    def _pick_obs_value(obs: Observation | None) -> float | None:
        if obs is None:
            return None
        return obs.value

    @classmethod
    def build_row_from_fetch_result(cls, fetch_result: FetchResult[dict[str, Observation]]) -> dict[str, Any]:
        payload = fetch_result.payload
        fetched_at = str(fetch_result.meta.get("fetched_at") or "")
        row_date = (
            cls._pick_obs_date(payload.get("spx"))
            or cls._pick_obs_date(payload.get("nasdaq_100"))
            or cls._pick_obs_date(payload.get("ust_10y"))
            or cls._pick_obs_date(payload.get("sofr"))
        )
        if not row_date:
            raise ValueError("FRED payload missing usable date")
        if not fetched_at:
            raise ValueError("FRED fetch result missing fetched_at")
        return {
            "date": row_date,
            "fed_upper": cls._pick_obs_value(payload.get("fed_upper")),
            "fed_lower": cls._pick_obs_value(payload.get("fed_lower")),
            "sofr": cls._pick_obs_value(payload.get("sofr")),
            "ust_2y": cls._pick_obs_value(payload.get("ust_2y")),
            "ust_10y": cls._pick_obs_value(payload.get("ust_10y")),
            "us_real_10y": cls._pick_obs_value(payload.get("us_real_10y")),
            "usd_broad": cls._pick_obs_value(payload.get("usd_broad")),
            "usd_cny": cls._pick_obs_value(payload.get("usd_cny")),
            "vix": cls._pick_obs_value(payload.get("vix")),
            "spx": cls._pick_obs_value(payload.get("spx")),
            "nasdaq_100": cls._pick_obs_value(payload.get("nasdaq_100")),
            "source": "FRED",
            "fetched_at": fetched_at,
        }

