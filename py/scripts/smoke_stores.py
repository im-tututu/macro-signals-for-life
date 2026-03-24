from __future__ import annotations

import json
from pathlib import Path

from src.jobs.manual import upsert_batch


def main() -> None:
    db_path = Path("py/data/smoke.sqlite")
    rows = [
        {
            "date": "2026-03-20",
            "curve": "国债",
            "y_1": 1.20,
            "y_10": 1.85,
            "source_sheet": "smoke",
        },
        {
            "date": "2026-03-21",
            "curve": "国债",
            "y_1": 1.18,
            "y_10": 1.80,
            "source_sheet": "smoke",
        },
    ]
    stats = upsert_batch("raw_bond_curve", rows, dry_run=False, db_path=db_path)
    print(json.dumps(stats.__dict__, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
