from __future__ import annotations

import argparse
import json
import sys
from pprint import pprint
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.core.config import PY_ROOT as BASE_DIR, settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", choices=["chinabond", "chinabond_index", "chinamoney", "pbc", "fred", "alpha", "futures", "jisilu", "env"])
    parser.add_argument("--date", default="2026-03-20")
    parser.add_argument("--index-id", default="8a8b2ca0332abed20134ea76d8885831")
    parser.add_argument("--pages", type=int, default=2)
    args = parser.parse_args()

    if args.source == "chinabond":
        from src.sources.chinabond import ChinaBondSource

        rows = ChinaBondSource().fetch_daily_wide(args.date)
        pprint([ChinaBondSource.flatten_curve(item) for item in rows])
    elif args.source == "chinabond_index":
        from src.sources.chinabond_index import ChinaBondIndexSource

        pprint(ChinaBondIndexSource().fetch_duration_snapshot(args.index_id))
    elif args.source == "chinamoney":
        from src.sources.chinamoney import ChinaMoneySource

        pprint(ChinaMoneySource().fetch_money_market_snapshot())
    elif args.source == "pbc":
        from src.sources.pbc import PbcSource

        pprint(PbcSource().fetch_recent_omo_events(5))
    elif args.source == "fred":
        from src.sources.fred import FredSource

        pprint(FredSource().fetch_overseas_macro())
    elif args.source == "alpha":
        from src.sources.external_misc import ExternalMiscSource

        src = ExternalMiscSource()
        payload = {}
        for field, spec in src.__class__.__mro__[0].__dict__.get('__annotations__', {}).items():
            pass
        pprint(src.fetch_overseas_macro_from_alpha_vantage())
    elif args.source == "futures":
        from src.sources.external_misc import ExternalMiscSource

        pprint(ExternalMiscSource().fetch_bond_futures_snapshot())
    elif args.source == "jisilu":
        from src.sources.jisilu import JisiluSource

        payload = JisiluSource().fetch_etf_index_all(max_pages=args.pages)
        print(json.dumps({"records": payload["records"], "row_count": len(payload["rows"])}, ensure_ascii=False, indent=2))
    elif args.source == "env":
        print(json.dumps({
            "cwd": str(BASE_DIR),
            "fred_api_key_len": len(settings.fred_api_key),
            "alpha_vantage_api_key_len": len(settings.alpha_vantage_api_key),
            "jisilu_cookie_len": len(settings.jisilu_cookie),
        }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
