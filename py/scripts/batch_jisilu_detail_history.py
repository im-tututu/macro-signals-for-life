#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.core.config import AppConfig, settings
from src.jobs import fetch_etf_detail_history
from src.sources.jisilu import JisiluEtfSource, JisiluQdiiEtfSource
from src.stores.etf import EtfStore
from src.stores.gold_etf import GoldEtfStore
from src.stores.qdii_etf import QdiiEtfStore


CHECKPOINT_VERSION = 1
QDII_MARKET_CODE_MAP = {
    "europe_america": "E",
    "commodity": "C",
    "asia": "A",
}


@dataclass
class Checkpoint:
    version: int = CHECKPOINT_VERSION
    kind: str = ""
    fund_ids: list[str] = field(default_factory=list)
    done: list[str] = field(default_factory=list)
    failed: list[dict[str, str]] = field(default_factory=list)
    last_updated: str = ""


def load_checkpoint(path: Path) -> Checkpoint:
    if not path.exists():
        return Checkpoint()
    data = json.loads(path.read_text(encoding="utf-8"))
    return Checkpoint(
        version=int(data.get("version", CHECKPOINT_VERSION)),
        kind=str(data.get("kind", "")),
        fund_ids=[str(x) for x in data.get("fund_ids", [])],
        done=[str(x) for x in data.get("done", [])],
        failed=[{"fund_id": str(item.get("fund_id", "")), "error": str(item.get("error", ""))} for item in data.get("failed", [])],
        last_updated=str(data.get("last_updated", "")),
    )


def save_checkpoint(path: Path, checkpoint: Checkpoint) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = asdict(checkpoint)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    db_uri = f"file:{db_path.resolve().as_posix()}?mode=ro"
    conn = sqlite3.connect(db_uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_latest_fund_rows(
    db_path: Path,
    kind: str,
    snapshot_date: str | None = None,
) -> list[dict[str, str]]:
    with connect_readonly(db_path) as conn:
        if kind == "etf":
            date = snapshot_date or conn.execute("SELECT MAX(snapshot_date) FROM raw_jisilu_etf").fetchone()[0]
            rows = conn.execute("SELECT fund_id FROM raw_jisilu_etf WHERE snapshot_date = ? ORDER BY fund_id", (date,)).fetchall()
            return [{"fund_id": str(row[0]), "market": ""} for row in rows]
        elif kind == "gold":
            date = snapshot_date or conn.execute("SELECT MAX(snapshot_date) FROM raw_jisilu_gold").fetchone()[0]
            rows = conn.execute("SELECT fund_id FROM raw_jisilu_gold WHERE snapshot_date = ? ORDER BY fund_id", (date,)).fetchall()
            return [{"fund_id": str(row[0]), "market": ""} for row in rows]
        elif kind == "qdii":
            date = snapshot_date or conn.execute("SELECT MAX(snapshot_date) FROM raw_jisilu_qdii").fetchone()[0]
            rows = conn.execute(
                "SELECT DISTINCT fund_id, market FROM raw_jisilu_qdii WHERE snapshot_date = ? ORDER BY market, fund_id",
                (date,),
            ).fetchall()
            return [{"fund_id": str(row[0]), "market": str(row[1] or "")} for row in rows]
        else:
            raise ValueError(f"unsupported kind: {kind}")


def read_fund_ids_from_file(path: Path) -> list[str]:
    items: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        items.append(text)
    return items


def build_store_and_runner(kind: str) -> tuple[Any, Callable[..., Any], str]:
    if kind in {"etf", "gold"}:
        store = EtfStore() if kind == "etf" else GoldEtfStore()
        runner = fetch_etf_detail_history
        table_name = store.spec.table_name
        return store, runner, table_name
    if kind == "qdii":
        return QdiiEtfStore(), None, QdiiEtfStore().spec.table_name
    raise ValueError(f"unsupported kind: {kind}")


def _extract_date(row: dict[str, Any]) -> str:
    cell = row.get("cell", row) if isinstance(row, dict) else {}
    for key in ("snapshot_date", "date", "trade_date", "price_dt", "nav_dt"):
        value = cell.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def fetch_detail_rows(
    kind: str,
    fund_id: str,
    *,
    pages: int,
    page_size: int,
) -> list[dict[str, Any]]:
    if kind in {"etf", "gold"}:
        source = JisiluEtfSource()
        detail_url = f"https://www.jisilu.cn/data/etf/detail/{fund_id}"
        hist_url = "https://www.jisilu.cn/data/etf/detail_hists/"
    elif kind == "qdii":
        source = JisiluQdiiEtfSource()
        detail_url = f"https://www.jisilu.cn/data/qdii/detail/{fund_id}"
    else:
        raise ValueError(f"unsupported kind: {kind}")

    source.http.session.headers["User-Agent"] = settings.jisilu_user_agent
    source.http.session.headers["X-Requested-With"] = "XMLHttpRequest"
    source.http.session.headers["Referer"] = detail_url

    rows: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    for page in range(1, pages + 1):
        if kind == "qdii":
            payload = source.fetch_qdii_detail_history_page(
                fund_id=fund_id,
                page=page,
                rows_per_page=page_size,
            )
        else:
            data = {
                "is_search": 1,
                "fund_id": fund_id,
                "rp": page_size,
                "page": page,
            }
            payload = source.http.request(
                "POST",
                hist_url,
                headers={
                    "Referer": detail_url,
                    "X-Requested-With": "XMLHttpRequest",
                },
                data=data,
            ).json()
        page_rows = payload.get("rows", []) or []
        page_dates = {_extract_date(row) for row in page_rows if _extract_date(row)}
        new_dates = page_dates - seen_dates
        rows.extend(page_rows)
        seen_dates.update(page_dates)
        print(
            json.dumps(
                {
                    "fund_id": fund_id,
                    "page": page,
                    "rows": len(page_rows),
                    "new_dates": sorted(new_dates),
                },
                ensure_ascii=False,
            )
        )
        if not page_rows or len(page_rows) < page_size or not new_dates:
            break
        time.sleep(0.2)
    return rows


def resolve_qdii_market(
    fund_id: str,
    *,
    fund_market_map: dict[str, str],
    fallback_market: str | None,
) -> str:
    market = fund_market_map.get(fund_id) or (fallback_market or "")
    if not market:
        raise ValueError(f"qdii market not found for fund_id={fund_id}")
    return market


def map_rows(kind: str, fund_id: str, rows: list[dict[str, Any]], fetched_at: str, source_url: str) -> list[dict[str, Any]]:
    if kind == "etf":
        store = EtfStore()
        return [
            store.build_row_from_history_cell(
                snapshot_date=_extract_date(row),
                fetched_at=fetched_at,
                fund_id=fund_id,
                cell=dict(row.get("cell", row) if isinstance(row, dict) else {}),
                source_url=source_url,
            )
            for row in rows
            if _extract_date(row)
        ]
    if kind == "gold":
        store = GoldEtfStore()
        return [
            store.build_row_from_history_cell(
                snapshot_date=_extract_date(row),
                fetched_at=fetched_at,
                fund_id=fund_id,
                cell=dict(row.get("cell", row) if isinstance(row, dict) else {}),
                source_url=source_url,
            )
            for row in rows
            if _extract_date(row)
        ]
    if kind == "qdii":
        store = QdiiEtfStore()
        return [
            store.build_row_from_history_cell(
                snapshot_date=_extract_date(row),
                fetched_at=fetched_at,
                market=str((row.get("cell", row) if isinstance(row, dict) else {}).get("market") or ""),
                market_code=str((row.get("cell", row) if isinstance(row, dict) else {}).get("market_code") or ""),
                fund_id=fund_id,
                cell=dict(row.get("cell", row) if isinstance(row, dict) else {}),
                source_url=source_url,
            )
            for row in rows
            if _extract_date(row)
        ]
    raise ValueError(f"unsupported kind: {kind}")


def main() -> None:
    cfg = AppConfig.load()
    parser = argparse.ArgumentParser(description="批量补集思录 ETF/QDII/黄金 detail 历史。")
    parser.add_argument("kind", choices=["etf", "gold", "qdii"], help="要补的来源类型。")
    parser.add_argument("--db", dest="db_path", type=Path, default=cfg.db_path, help="SQLite 路径。")
    parser.add_argument("--fund-id", action="append", default=[], help="手工指定 fund_id，可重复传入。")
    parser.add_argument("--fund-id-file", type=Path, default=None, help="每行一个 fund_id 的文本文件。")
    parser.add_argument("--from-db", action="store_true", help="从数据库最新快照自动取 fund_id 列表。")
    parser.add_argument("--snapshot-date", default=None, help="从指定 snapshot_date 取 fund_id。")
    parser.add_argument("--pages", type=int, default=1, help="每只基金最多抓多少页。")
    parser.add_argument("--page-size", type=int, default=50, help="每页条数。")
    parser.add_argument("--base-sleep", type=float, default=0.25, help="每只基金成功后基础休眠。")
    parser.add_argument("--jitter", type=float, default=0.5, help="每只基金成功后随机抖动。")
    parser.add_argument("--burst", type=int, default=25, help="每抓多少只额外停顿一次。")
    parser.add_argument("--burst-sleep", type=float, default=4.0, help="burst 休息秒数。")
    parser.add_argument("--market", choices=["europe_america", "commodity", "asia"], default=None, help="QDII 的 market，既用于取 fund_id，也用于请求历史页。")
    parser.add_argument("--checkpoint", type=Path, default=Path("runtime/checkpoints/jisilu_detail_history.json"), help="checkpoint 文件。")
    parser.add_argument("--resume", action="store_true", help="从 checkpoint 恢复。")
    parser.add_argument("--dry-run", action="store_true", help="只打印不写库。")
    parser.add_argument("--limit", type=int, default=None, help="只处理前 N 只，便于测试。")
    args = parser.parse_args()

    if args.resume and args.checkpoint.exists():
        checkpoint = load_checkpoint(args.checkpoint)
        if checkpoint.kind and checkpoint.kind != args.kind:
            raise ValueError(f"checkpoint kind mismatch: {checkpoint.kind} vs {args.kind}")
    else:
        checkpoint = Checkpoint(kind=args.kind)

    fund_ids = list(args.fund_id)
    fund_market_map: dict[str, str] = {}
    if args.fund_id_file:
        fund_ids.extend(read_fund_ids_from_file(args.fund_id_file))
    if args.from_db:
        fund_rows = fetch_latest_fund_rows(args.db_path, args.kind, args.snapshot_date)
        fund_ids.extend([row["fund_id"] for row in fund_rows])
        if args.kind == "qdii":
            fund_market_map = {row["fund_id"]: row["market"] for row in fund_rows}
    if checkpoint.fund_ids and args.resume:
        fund_ids = checkpoint.fund_ids
    fund_ids = list(dict.fromkeys([fid.strip() for fid in fund_ids if fid.strip()]))
    if args.limit is not None:
        fund_ids = fund_ids[: args.limit]
    if not fund_ids:
        raise ValueError("no fund_ids provided")
    if args.kind == "qdii" and not args.from_db and not args.market:
        raise ValueError("qdii kind requires --market when fund_id source does not include market")

    done = set(checkpoint.done if args.resume else [])
    failed: list[dict[str, str]] = list(checkpoint.failed if args.resume else [])
    store = {"etf": EtfStore(db_path=args.db_path), "gold": GoldEtfStore(db_path=args.db_path), "qdii": QdiiEtfStore(db_path=args.db_path)}[args.kind]
    fetched = 0
    for idx, fund_id in enumerate(fund_ids, start=1):
        if fund_id in done:
            print(json.dumps({"fund_id": fund_id, "status": "skip_done"}, ensure_ascii=False))
            continue
        try:
            row_market = ""
            if args.kind == "qdii":
                row_market = resolve_qdii_market(
                    fund_id,
                    fund_market_map=fund_market_map,
                    fallback_market=args.market,
                )
            rows = fetch_detail_rows(args.kind, fund_id, pages=args.pages, page_size=args.page_size)
            fetched_at = str(time.strftime("%Y-%m-%dT%H:%M:%S"))
            source_url = f"https://www.jisilu.cn/data/{'qdii' if args.kind == 'qdii' else 'etf'}/detail/{fund_id}"
            mapped = map_rows(args.kind, fund_id, rows, fetched_at, source_url)
            if args.kind == "qdii":
                for row in mapped:
                    row["market"] = row_market
                    row["market_code"] = QDII_MARKET_CODE_MAP[row_market]
            if not args.dry_run and mapped:
                store.upsert_many(mapped)
            done.add(fund_id)
            fetched += 1
            checkpoint.fund_ids = fund_ids
            checkpoint.done = sorted(done)
            checkpoint.failed = failed
            checkpoint.last_updated = fetched_at
            if fetched % max(1, args.burst) == 0:
                time.sleep(args.burst_sleep)
            else:
                time.sleep(random.uniform(0, args.jitter) + args.base_sleep)
            print(json.dumps({"fund_id": fund_id, "status": "ok", "rows": len(mapped)}, ensure_ascii=False))
            if fetched % max(1, args.burst) == 0 or idx == len(fund_ids):
                save_checkpoint(args.checkpoint, checkpoint)
        except Exception as exc:  # noqa: BLE001
            failed.append({"fund_id": fund_id, "error": str(exc)})
            checkpoint.fund_ids = fund_ids
            checkpoint.done = sorted(done)
            checkpoint.failed = failed
            checkpoint.last_updated = str(time.strftime("%Y-%m-%dT%H:%M:%S"))
            save_checkpoint(args.checkpoint, checkpoint)
            print(json.dumps({"fund_id": fund_id, "status": "error", "error": str(exc)}, ensure_ascii=False))
            time.sleep(max(2.0, args.burst_sleep))

    save_checkpoint(args.checkpoint, checkpoint)
    print(
        json.dumps(
            {
                "kind": args.kind,
                "total": len(fund_ids),
                "done": len(done),
                "failed": len(failed),
                "checkpoint": str(args.checkpoint),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
