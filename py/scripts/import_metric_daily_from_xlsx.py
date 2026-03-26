#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.core.config import default_sql_paths
from src.core.db import connect, run_sql_files
from src.metrics import upsert_metric_daily_rows

MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
NS = {"x": MAIN_NS}
EPOCH_1900 = date(1899, 12, 30)


def _excel_serial_to_ymd(serial: float) -> str:
    days = int(serial)
    return (EPOCH_1900 + timedelta(days=days)).isoformat()


def _parse_date_text(value: str) -> str | None:
    text = value.strip()
    if not text:
        return None
    try:
        if text.replace(".", "", 1).isdigit():
            return _excel_serial_to_ymd(float(text))
        return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def _col_ref_to_index(cell_ref: str) -> int:
    col = "".join(ch for ch in cell_ref if ch.isalpha()).upper()
    if not col:
        return 0
    out = 0
    for ch in col:
        out = out * 26 + (ord(ch) - 64)
    return out


def _load_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    out: list[str] = []
    for si in root.findall("x:si", NS):
        texts = [t.text or "" for t in si.findall(".//x:t", NS)]
        out.append("".join(texts))
    return out


def _sheet_name_to_target(zf: zipfile.ZipFile, sheet_name: str) -> str:
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    rel_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))

    rel_map: dict[str, str] = {}
    for node in rel_root.findall(f"{{{PKG_REL_NS}}}Relationship"):
        rel_id = node.attrib.get("Id", "")
        target = node.attrib.get("Target", "")
        rel_map[rel_id] = target

    for sheet in workbook.findall("x:sheets/x:sheet", NS):
        name = sheet.attrib.get("name", "")
        rel_id = sheet.attrib.get(f"{{{REL_NS}}}id", "")
        if name == sheet_name and rel_id in rel_map:
            target = rel_map[rel_id].lstrip("/")
            if not target.startswith("xl/"):
                target = f"xl/{target}"
            return target
    raise ValueError(f"sheet not found: {sheet_name}")


def _read_sheet_rows(
    xlsx_path: Path,
    *,
    sheet_name: str,
    source: str,
    fetched_at: str,
) -> list[dict[str, Any]]:
    with zipfile.ZipFile(xlsx_path) as zf:
        shared = _load_shared_strings(zf)
        sheet_target = _sheet_name_to_target(zf, sheet_name)
        root = ET.fromstring(zf.read(sheet_target))
        rows = root.findall("x:sheetData/x:row", NS)

        if not rows:
            return []

        header_by_col: dict[int, str] = {}
        out_rows: list[dict[str, Any]] = []

        for idx, row in enumerate(rows):
            cells = row.findall("x:c", NS)
            if not cells:
                continue

            row_map: dict[int, str] = {}
            for cell in cells:
                cell_ref = cell.attrib.get("r", "")
                col_idx = _col_ref_to_index(cell_ref)
                if col_idx <= 0:
                    continue
                cell_type = cell.attrib.get("t")
                value_node = cell.find("x:v", NS)
                if value_node is None:
                    continue
                raw = value_node.text or ""
                if cell_type == "s":
                    if raw.isdigit():
                        shared_idx = int(raw)
                        if 0 <= shared_idx < len(shared):
                            row_map[col_idx] = shared[shared_idx]
                    continue
                row_map[col_idx] = raw

            if not row_map:
                continue

            if idx == 0:
                for col_idx, header in row_map.items():
                    header_clean = header.strip()
                    if col_idx >= 2 and header_clean:
                        header_by_col[col_idx] = header_clean
                continue

            date_text = _parse_date_text(row_map.get(1, ""))
            if not date_text:
                continue

            for col_idx, code in header_by_col.items():
                raw_value = row_map.get(col_idx)
                if raw_value is None:
                    continue
                try:
                    value = float(raw_value)
                except (TypeError, ValueError):
                    continue
                out_rows.append(
                    {
                        "date": date_text,
                        "code": code,
                        "value": value,
                        "source": source,
                        "fetched_at": fetched_at,
                    }
                )
        return out_rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="从 xlsx 的指标 sheet 导入 metric_daily 历史数据。")
    parser.add_argument("--xlsx", required=True, type=Path, help="xlsx 文件路径。")
    parser.add_argument("--sheet", default="指标", help="sheet 名，默认“指标”。")
    parser.add_argument("--db", dest="db_path", type=Path, default=None, help="可选 SQLite 路径覆盖。")
    parser.add_argument("--source", default="xlsx:指标", help="写入 metric_daily 的 source。")
    parser.add_argument("--dry-run", action="store_true", help="仅演练，不提交事务。")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    xlsx_path = args.xlsx.expanduser().resolve()
    if not xlsx_path.exists():
        raise FileNotFoundError(f"xlsx not found: {xlsx_path}")

    with connect(args.db_path) as conn:
        run_sql_files(conn, default_sql_paths())
        conn.commit()

    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = _read_sheet_rows(
        xlsx_path,
        sheet_name=args.sheet,
        source=args.source,
        fetched_at=fetched_at,
    )
    written = upsert_metric_daily_rows(
        rows,
        db_path=str(args.db_path) if args.db_path else None,
        dry_run=args.dry_run,
    )

    print(
        json.dumps(
            {
                "xlsx": str(xlsx_path),
                "sheet": args.sheet,
                "rows_parsed": len(rows),
                "rows_written": written,
                "dry_run": args.dry_run,
                "sample": rows[:5],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
