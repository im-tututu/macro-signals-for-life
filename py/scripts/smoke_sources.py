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
    parser.add_argument("source", choices=["chinabond", "chinabond_index", "chinamoney", "pbc", "fred", "alpha", "futures", "jisilu", "qdii", "treasury", "sse_lively_bond", "env"])
    parser.add_argument("--date", default="2026-03-20")
    parser.add_argument("--index-id", default="8a8b2ca0332abed20134ea76d8885831")
    parser.add_argument("--pages", type=int, default=2)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--market", choices=["all", "europe_america", "commodity", "asia"], default="all")
    parser.add_argument("--sample-limit", type=int, default=3)
    args = parser.parse_args()

    if args.source == "chinabond":
        from src.sources.chinabond import ChinaBondSource

        rows = ChinaBondSource().fetch_daily_wide(args.date)
        pprint([ChinaBondSource.flatten_curve(item) for item in rows])
    elif args.source == "chinabond_index":
        from src.sources.chinabond import ChinaBondIndexSource

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
        from src.sources.alpha_vantage import AlphaVantageSource

        pprint(AlphaVantageSource().fetch_overseas_macro())
    elif args.source == "futures":
        from src.sources.sina_futures import SinaFuturesSource

        pprint(SinaFuturesSource().fetch_bond_futures_snapshot())
    elif args.source == "jisilu":
        from src.sources.jisilu import JisiluEtfSource

        payload = JisiluEtfSource().fetch_etf_index_all(max_pages=args.pages)
        print(json.dumps({"records": payload["records"], "row_count": len(payload["rows"])}, ensure_ascii=False, indent=2))
    elif args.source == "qdii":
        from src.sources.jisilu import JisiluQdiiSource
        from src.stores.qdii import QdiiStore

        source = JisiluQdiiSource()
        markets = ["europe_america", "commodity", "asia"] if args.market == "all" else [args.market]
        summaries: list[dict[str, object]] = []
        samples: dict[str, list[dict[str, object]]] = {}
        for market in markets:
            result = source.fetch_qdii_all_result(market=market, max_pages=args.pages)
            payload = result.payload
            mapped_rows = QdiiStore.build_rows_from_fetch_result(result)
            summaries.append(
                {
                    "market": payload["market"],
                    "market_code": payload["market_code"],
                    "records": payload["records"],
                    "row_count": len(payload["rows"]),
                    "mapped_row_count": len(mapped_rows),
                    "snapshot_date": payload["snapshot_date"],
                }
            )
            samples[market] = [
                {
                    "fund_id": row.get("fund_id"),
                    "fund_nm_display": row.get("fund_nm_display"),
                    "index_nm": row.get("index_nm"),
                    "price": row.get("price"),
                    "fund_nav": row.get("fund_nav"),
                    "iopv": row.get("iopv"),
                    "nav_discount_rt": row.get("nav_discount_rt"),
                    "iopv_discount_rt": row.get("iopv_discount_rt"),
                    "money_cd": row.get("money_cd"),
                    "apply_status": row.get("apply_status"),
                    "redeem_status": row.get("redeem_status"),
                }
                for row in mapped_rows[: args.sample_limit]
            ]
        print(json.dumps({"summaries": summaries, "samples": samples}, ensure_ascii=False, indent=2))
    elif args.source == "treasury":
        from src.sources.jisilu import JisiluTreasurySource

        rows = JisiluTreasurySource().fetch_treasury_rows()
        print(
            json.dumps(
                {
                    "row_count": len(rows),
                    "samples": rows[: args.sample_limit],
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )
    elif args.source == "sse_lively_bond":
        from src.sources.sse import SseLivelyBondSource
        from src.stores.sse_lively_bond import SseLivelyBondStore

        source = SseLivelyBondSource()
        payload = source.fetch_lively_bond_all(page_size=args.page_size, max_pages=args.pages)
        fetch_result = source.fetch_lively_bond_all_result(page_size=args.page_size, max_pages=args.pages)
        mapped_rows = SseLivelyBondStore.build_rows_from_fetch_result(fetch_result)
        print(
            json.dumps(
                {
                    "snapshot_date": payload["snapshot_date"],
                    "row_count": payload["row_count"],
                    "total_count": payload["total_count"],
                    "samples": payload["rows"][: args.sample_limit],
                    "mapped_samples": mapped_rows[: args.sample_limit],
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )
    elif args.source == "env":
        print(json.dumps({
            "cwd": str(BASE_DIR),
            "fred_api_key_len": len(settings.fred_api_key),
            "alpha_vantage_api_key_len": len(settings.alpha_vantage_api_key),
            "jisilu_cookie_len": len(settings.jisilu_cookie),
        }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
