#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.core.trading_calendar import DEFAULT_TRADING_DAYS_CSV, sync_trading_days_csv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同步项目内交易日文件 trading_days.csv。")
    parser.add_argument("--source-csv", type=Path, default=None, help="可选外部交易日源文件。")
    parser.add_argument("--target-csv", type=Path, default=DEFAULT_TRADING_DAYS_CSV, help="目标 trading_days.csv 路径。")
    parser.add_argument("--dry-run", action="store_true", help="仅演练，不写入目标文件。")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = sync_trading_days_csv(
        source_path=args.source_csv,
        target_path=args.target_csv,
        dry_run=args.dry_run,
    )
    print(
        json.dumps(
            {
                "target_path": str(result.target_path),
                "source_path": str(result.source_path) if result.source_path else None,
                "coverage_start": result.coverage_start,
                "coverage_end": result.coverage_end,
                "row_count": result.row_count,
                "changed": result.changed,
                "created": result.created,
                "dry_run": result.dry_run,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
