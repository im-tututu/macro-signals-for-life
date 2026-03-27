from __future__ import annotations

import json
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.jobs.manual import upsert_batch


def main() -> None:
    db_path = Path("runtime/db/smoke.sqlite")
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
