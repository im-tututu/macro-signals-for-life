#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.core.config import default_sql_paths
from src.core.db import connect, run_sql_files
from src.core.utils import now_text
from src.metrics import upsert_metric_daily_rows, upsert_metric_registry_rows


@dataclass(frozen=True)
class CurveMetricSpec:
    code: str
    name_cn: str
    curve: str
    tenor_col: str
    category: str = "利率水平"
    unit: str = "0-1（显示为%）"
    importance: str = "观察"
    primary_use: str = "基础曲线指标"
    note: str = "由 raw_bond_curve 同步到 metric_daily"


CURVE_METRICS: tuple[CurveMetricSpec, ...] = (
    CurveMetricSpec(code="local_gov_1y", name_cn="1年期地方债到期收益率", curve="地方债", tenor_col="y_1"),
    CurveMetricSpec(code="local_gov_5y", name_cn="5年期地方债到期收益率", curve="地方债", tenor_col="y_5"),
    CurveMetricSpec(code="local_gov_10y", name_cn="10年期地方债到期收益率", curve="地方债", tenor_col="y_10"),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="将 raw_bond_curve 中的曲线点位同步到 metric_registry / metric_daily。")
    parser.add_argument("--db", dest="db_path", type=Path, default=None, help="可选 SQLite 路径覆盖。")
    parser.add_argument(
        "--codes",
        nargs="*",
        default=None,
        help="可选，只同步指定指标 code；默认同步预置的全部曲线指标。",
    )
    parser.add_argument("--dry-run", action="store_true", help="仅演练，不提交事务。")
    return parser


def selected_specs(codes: list[str] | None) -> list[CurveMetricSpec]:
    if not codes:
        return list(CURVE_METRICS)
    allowed = set(codes)
    return [spec for spec in CURVE_METRICS if spec.code in allowed]


def build_registry_rows(specs: list[CurveMetricSpec]) -> list[dict[str, object]]:
    return [
        {
            "code": spec.code,
            "name_cn": spec.name_cn,
            "name_en": spec.code,
            "category": spec.category,
            "unit": spec.unit,
            "importance": spec.importance,
            "interpret_up": "",
            "interpret_down": "",
            "primary_use": spec.primary_use,
            "note": spec.note,
            "calc_type": "precomputed",
            "depends_on_json": None,
            "schedule_group": "manual",
            "is_active": 1,
        }
        for spec in specs
    ]


def build_daily_rows(conn, specs: list[CurveMetricSpec]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    fetched_at = now_text()
    for spec in specs:
        rows = conn.execute(
            f"""
            SELECT date, {spec.tenor_col} AS value
            FROM raw_bond_curve
            WHERE curve = ?
              AND {spec.tenor_col} IS NOT NULL
            ORDER BY date ASC
            """,
            (spec.curve,),
        ).fetchall()
        for row in rows:
            try:
                value = float(row["value"]) / 100.0
            except (TypeError, ValueError):
                continue
            out.append(
                {
                    "date": str(row["date"] or ""),
                    "code": spec.code,
                    "value": value,
                    "source": f"raw_bond_curve:{spec.curve}:{spec.tenor_col}",
                    "fetched_at": fetched_at,
                }
            )
    return out


def main() -> None:
    args = build_parser().parse_args()
    specs = selected_specs(args.codes)
    if not specs:
        raise ValueError("没有匹配到要同步的曲线指标 code。")

    with connect(args.db_path) as conn:
        run_sql_files(conn, default_sql_paths())
        conn.commit()
        daily_rows = build_daily_rows(conn, specs)

    registry_rows = build_registry_rows(specs)
    registry_written = upsert_metric_registry_rows(
        registry_rows,
        db_path=str(args.db_path) if args.db_path else None,
        dry_run=args.dry_run,
    )
    daily_written = upsert_metric_daily_rows(
        daily_rows,
        db_path=str(args.db_path) if args.db_path else None,
        dry_run=args.dry_run,
    )

    print(
        json.dumps(
            {
                "curve_metrics": [spec.code for spec in specs],
                "registry_rows": len(registry_rows),
                "registry_written": registry_written,
                "daily_rows": len(daily_rows),
                "daily_written": daily_written,
                "dry_run": args.dry_run,
                "sample_daily_rows": daily_rows[:5],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
