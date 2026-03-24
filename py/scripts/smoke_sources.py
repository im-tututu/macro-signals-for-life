from __future__ import annotations

import argparse
import json
from pprint import pprint

from core.config import BASE_DIR, settings
from sources.chinabond import ChinaBondSource
from sources.chinabond_index import ChinaBondIndexSource
from sources.chinamoney import ChinaMoneySource
from sources.external_misc import ExternalMiscSource
from sources.fred import FredSource
from sources.jisilu import JisiluSource
from sources.pbc import PbcSource


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", choices=["chinabond", "chinabond_index", "chinamoney", "pbc", "fred", "alpha", "futures", "jisilu", "env"])
    parser.add_argument("--date", default="2026-03-20")
    parser.add_argument("--index-id", default="8a8b2ca0332abed20134ea76d8885831")
    parser.add_argument("--pages", type=int, default=2)
    args = parser.parse_args()

    if args.source == "chinabond":
        rows = ChinaBondSource().fetch_daily_wide(args.date)
        pprint([ChinaBondSource.flatten_curve(item) for item in rows])
    elif args.source == "chinabond_index":
        pprint(ChinaBondIndexSource().fetch_duration_snapshot(args.index_id))
    elif args.source == "chinamoney":
        pprint(ChinaMoneySource().fetch_money_market_snapshot())
    elif args.source == "pbc":
        pprint(PbcSource().fetch_recent_omo_events(5))
    elif args.source == "fred":
        pprint(FredSource().fetch_overseas_macro())
    elif args.source == "alpha":
        src = ExternalMiscSource()
        payload = {}
        for field, spec in src.__class__.__mro__[0].__dict__.get('__annotations__', {}).items():
            pass
        pprint(src.fetch_overseas_macro_from_alpha_vantage())
    elif args.source == "futures":
        pprint(ExternalMiscSource().fetch_bond_futures_snapshot())
    elif args.source == "jisilu":
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
