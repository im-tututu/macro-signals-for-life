#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.core.config import settings
from src.sources.jisilu import JisiluEtfSource, JisiluQdiiEtfSource


DETAIL_CONFIG: dict[str, dict[str, str]] = {
    "etf": {
        "detail_url": "https://www.jisilu.cn/data/etf/detail/{fund_id}",
        "hist_url": "https://www.jisilu.cn/data/etf/detail_hists/",
        "referer": "https://www.jisilu.cn/data/etf/detail/{fund_id}",
    },
    "qdii": {
        "detail_url": "https://www.jisilu.cn/data/qdii/detail/{fund_id}",
        "hist_url": "https://www.jisilu.cn/data/qdii/detail_hists/",
        "referer": "https://www.jisilu.cn/data/qdii/detail/{fund_id}",
    },
    "gold": {
        "detail_url": "https://www.jisilu.cn/data/etf/detail/{fund_id}",
        "hist_url": "https://www.jisilu.cn/data/etf/detail_hists/",
        "referer": "https://www.jisilu.cn/data/etf/detail/{fund_id}",
    },
}

QDII_MARKET_CODE_MAP = {
    "europe_america": "E",
    "commodity": "C",
    "asia": "A",
}


def _extract_date(row: dict[str, Any]) -> str:
    cell = row.get("cell", row) if isinstance(row, dict) else {}
    for key in ("snapshot_date", "date", "trade_date", "price_dt", "nav_dt"):
        value = cell.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def main() -> None:
    parser = argparse.ArgumentParser(description="测试集思录 ETF/QDII/黄金 detail_hists 接口。")
    parser.add_argument("fund_id", help="基金代码，例如 159330")
    parser.add_argument("--kind", choices=["etf", "qdii", "gold"], default="etf", help="详情页类型。")
    parser.add_argument("--pages", type=int, default=1, help="最多抓多少页。")
    parser.add_argument("--page-size", type=int, default=50, help="每页条数。")
    parser.add_argument("--sleep", type=float, default=0.2, help="每页之间的基础休眠秒数。")
    parser.add_argument("--market", choices=["europe_america", "commodity", "asia"], default=None, help="QDII 必填的 market。")
    parser.add_argument("--dry-run", action="store_true", help="只抓取并打印，不做额外处理。")
    parser.add_argument("--pretty", action="store_true", help="输出更详细的明细。")
    args = parser.parse_args()

    cfg = DETAIL_CONFIG[args.kind]
    source = JisiluQdiiEtfSource() if args.kind == "qdii" else JisiluEtfSource()
    source.http.session.headers["User-Agent"] = settings.jisilu_user_agent
    source.http.session.headers["X-Requested-With"] = "XMLHttpRequest"
    source.http.session.headers["Referer"] = cfg["referer"].format(fund_id=args.fund_id)
    if args.kind == "qdii" and not args.market:
        raise ValueError("qdii kind requires --market")

    all_rows: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    pages_fetched = 0
    for page in range(1, args.pages + 1):
        pages_fetched = page
        if args.kind == "qdii":
            payload = source.fetch_qdii_detail_history_page(
                fund_id=args.fund_id,
                market=args.market,
                page=page,
                rows_per_page=args.page_size,
            )
        else:
            payload = source.http.request(
                "POST",
                cfg["hist_url"],
                headers={
                    "Referer": cfg["referer"].format(fund_id=args.fund_id),
                    "X-Requested-With": "XMLHttpRequest",
                },
                data={
                    "is_search": 1,
                    "fund_id": args.fund_id,
                    "rp": args.page_size,
                    "page": page,
                },
            ).json()

        rows = payload.get("rows", []) or []
        dates = {_extract_date(row) for row in rows if _extract_date(row)}
        new_dates = dates - seen_dates
        seen_dates.update(dates)
        all_rows.extend(rows)
        print(
            json.dumps(
                {
                    "kind": args.kind,
                    "market": args.market or "",
                    "page": page,
                    "rows": len(rows),
                    "new_dates": sorted(new_dates),
                    "records": payload.get("records", payload.get("total", payload.get("total_rows", ""))),
                },
                ensure_ascii=False,
            )
        )
        if not rows or len(rows) < args.page_size:
            break
        if not new_dates:
            break
        if page < args.pages:
            time.sleep(args.sleep)

    dates = sorted({d for d in (_extract_date(row) for row in all_rows) if d})
    print(
        json.dumps(
                {
                    "kind": args.kind,
                    "fund_id": args.fund_id,
                    "market": args.market or "",
                    "page_count": pages_fetched,
                    "row_count": len(all_rows),
                    "date_count": len(dates),
                    "oldest_date": dates[0] if dates else "",
                    "newest_date": dates[-1] if dates else "",
                    "sample_dates": dates[:10],
                },
            ensure_ascii=False,
            indent=2,
        )
    )
    if args.dry_run:
        return
    if args.pretty:
        print(json.dumps(all_rows[:20], ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
