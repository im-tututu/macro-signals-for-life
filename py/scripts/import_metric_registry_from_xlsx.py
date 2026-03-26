#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.core.config import default_sql_paths
from src.core.db import connect, run_sql_files
from src.metrics import upsert_metric_registry_rows

MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
NS = {"x": MAIN_NS}


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


def _read_registry_rows(
    xlsx_path: Path,
    *,
    sheet_name: str,
    calc_type: str,
    schedule_group: str,
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
                col_idx = _col_ref_to_index(cell.attrib.get("r", ""))
                if col_idx <= 0:
                    continue
                value_node = cell.find("x:v", NS)
                if value_node is None:
                    continue
                raw = value_node.text or ""
                if cell.attrib.get("t") == "s":
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
                    header_by_col[col_idx] = header.strip()
                continue

            col_to_val = {header_by_col.get(c, ""): v for c, v in row_map.items()}
            code = str(col_to_val.get("code", "")).strip()
            if not code:
                continue
            out_rows.append(
                {
                    "code": code,
                    "name_cn": str(col_to_val.get("中文名称", "")).strip() or code,
                    "name_en": str(col_to_val.get("English common name", "")).strip() or code,
                    "category": str(col_to_val.get("分类", "")).strip() or "未分类",
                    "unit": str(col_to_val.get("单位", "")).strip() or "原值",
                    "importance": str(col_to_val.get("重要性", "")).strip() or "观察",
                    "interpret_up": str(col_to_val.get("偏高/上行解读", "")).strip(),
                    "interpret_down": str(col_to_val.get("偏低/下行解读", "")).strip(),
                    "primary_use": str(col_to_val.get("主要用途", "")).strip(),
                    "note": str(col_to_val.get("备注", "")).strip(),
                    "calc_type": calc_type,
                    "schedule_group": schedule_group,
                    "is_active": 1,
                }
            )
        return out_rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="从 xlsx 的指标说明 sheet 导入 metric_registry。")
    parser.add_argument("--xlsx", required=True, type=Path, help="xlsx 文件路径。")
    parser.add_argument("--sheet", default="指标说明", help="sheet 名，默认“指标说明”。")
    parser.add_argument("--db", dest="db_path", type=Path, default=None, help="可选 SQLite 路径覆盖。")
    parser.add_argument("--calc-type", default="precomputed", help="导入时统一 calc_type。")
    parser.add_argument("--schedule-group", default="manual", help="导入时统一 schedule_group。")
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

    rows = _read_registry_rows(
        xlsx_path,
        sheet_name=args.sheet,
        calc_type=args.calc_type,
        schedule_group=args.schedule_group,
    )
    written = upsert_metric_registry_rows(
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
                "calc_type": args.calc_type,
                "schedule_group": args.schedule_group,
                "dry_run": args.dry_run,
                "imported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "sample": rows[:5],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
