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

from src.jobs.ingest import fetch_latest_money_market


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取 ChinaMoney 资金面快照并写入 raw_money_market。")
    parser.add_argument("--db", dest="db_path", type=Path, default=None, help="可选 SQLite 路径覆盖。")
    parser.add_argument("--dry-run", action="store_true", help="仅演练，不提交数据库事务。")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stats = fetch_latest_money_market(dry_run=args.dry_run, db_path=args.db_path)
    print(json.dumps(stats.__dict__, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
