from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.config import TABLE_RAW_ALPHA_VANTAGE
from src.core.models import Observation
from src.sources._base import FetchResult

from ._base import BaseSqliteStore, TableSpec


ALPHA_VANTAGE_NUMERIC_FIELDS = (
    "gold",
    "wti",
    "brent",
    "copper",
)

ALPHA_VANTAGE_SPEC = TableSpec(
    table_name=TABLE_RAW_ALPHA_VANTAGE,
    key_fields=("date",),
    date_field="date",
    numeric_fields=ALPHA_VANTAGE_NUMERIC_FIELDS,
    integer_fields=("source_row_num",),
    text_fields=("source", "source_sheet"),
    datetime_fields=("fetched_at", "migrated_at"),
    compare_fields=(
        "date",
        *ALPHA_VANTAGE_NUMERIC_FIELDS,
        "source",
        "fetched_at",
        "source_sheet",
        "source_row_num",
        "migrated_at",
    ),
    default_order_by=("date",),
)


@dataclass
class AlphaVantageStore(BaseSqliteStore):
    spec: TableSpec = ALPHA_VANTAGE_SPEC

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
    def build_row_from_fetch_result(cls, fetch_result: FetchResult[dict[str, Observation | None]]) -> dict[str, Any]:
        payload = fetch_result.payload
        fetched_at = str(fetch_result.meta.get("fetched_at") or "")
        row_date = (
            cls._pick_obs_date(payload.get("gold"))
            or cls._pick_obs_date(payload.get("wti"))
            or cls._pick_obs_date(payload.get("brent"))
            or cls._pick_obs_date(payload.get("copper"))
        )
        if not row_date:
            raise ValueError("Alpha Vantage payload missing usable date")
        if not fetched_at:
            raise ValueError("Alpha Vantage fetch result missing fetched_at")
        return {
            "date": row_date,
            "gold": cls._pick_obs_value(payload.get("gold")),
            "wti": cls._pick_obs_value(payload.get("wti")),
            "brent": cls._pick_obs_value(payload.get("brent")),
            "copper": cls._pick_obs_value(payload.get("copper")),
            "source": "ALPHA_VANTAGE",
            "fetched_at": fetched_at,
        }

