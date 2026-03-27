#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

try:
    from src.core.config import AppConfig, getenv_first, load_local_env  # type: ignore
except Exception:
    try:
        from core.config import AppConfig, getenv_first, load_local_env  # type: ignore
    except Exception:
        CURRENT_DIR = Path(__file__).resolve().parent
        PY_ROOT = CURRENT_DIR.parent
        if str(PY_ROOT) not in sys.path:
            sys.path.insert(0, str(PY_ROOT))
        if str(CURRENT_DIR) not in sys.path:
            sys.path.insert(0, str(CURRENT_DIR))
        from src.core.config import AppConfig, getenv_first, load_local_env  # type: ignore


def build_parser(cfg: AppConfig) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="将 metric_snapshot 最新日期的数据写入指定 Google Spreadsheet；若同名工作表已存在则覆盖，否则新增。"
    )
    parser.add_argument(
        "--db",
        default=str(cfg.db_path),
        help="SQLite 数据库路径。默认读取 .env 中的 DB_PATH，或使用 runtime/db/app.sqlite。",
    )
    parser.add_argument(
        "--creds",
        default=str(cfg.google_credentials_path) if cfg.google_credentials_path else None,
        help="可选的 Google service account JSON 路径覆盖值。默认从 .env 中读取。",
    )
    parser.add_argument(
        "--spreadsheet-id",
        default=(
            getenv_first("GOOGLE_SPREADSHEET_ID_SNAPSHOT")
            or cfg.spreadsheet_id
            or getenv_first("GOOGLE_SPREADSHEET_ID")
        ),
        help="目标 Spreadsheet ID。默认优先读取 .env 中的 GOOGLE_SPREADSHEET_ID_SNAPSHOT，其次 GOOGLE_SPREADSHEET_ID。",
    )
    parser.add_argument(
        "--as-of-date",
        default=None,
        help="可选快照日期，格式 YYYY-MM-DD；省略时自动取 metric_snapshot 中最新日期。",
    )
    parser.add_argument(
        "--worksheet",
        default=None,
        help="可选工作表名覆盖值；默认使用快照日期。",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="可选的 .env 文件路径；默认会自动探测。",
    )
    return parser


def fetch_latest_as_of_date(conn: sqlite3.Connection) -> str:
    row = conn.execute("SELECT MAX(as_of_date) FROM metric_snapshot").fetchone()
    value = "" if row is None else str(row[0] or "").strip()
    if not value:
        raise RuntimeError("metric_snapshot 表中没有可用快照日期。")
    return value


def has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row[1] or "") == column_name for row in rows)


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    # 导出脚本只读访问数据库，尽量避免与更新任务争抢写锁。
    db_uri = f"file:{db_path.resolve().as_posix()}?mode=ro"
    return sqlite3.connect(db_uri, uri=True)


def fetch_snapshot_rows(conn: sqlite3.Connection, as_of_date: str) -> list[list[object]]:
    order_by = "s.code"
    cur = conn.execute(
        f"""
        SELECT
            COALESCE(r.name_cn, s.code) AS name_cn,
            s.code,
            COALESCE(r.category, '') AS category,
            COALESCE(r.unit, '') AS unit,
            s.latest_date,
            s.latest_value,
            s.change_5d,
            CASE WHEN COALESCE(s.sample_count, 0) >= 20 THEN s.change_20d ELSE NULL END AS change_20d,
            CASE WHEN COALESCE(s.sample_count, 0) >= 20 THEN s.deviation_ma20 ELSE NULL END AS deviation_ma20,
            CASE WHEN COALESCE(s.sample_count, 0) >= 250 THEN s.percentile_250 ELSE NULL END AS percentile_250,
            CASE WHEN COALESCE(s.sample_count, 0) >= 750 THEN s.percentile_3y ELSE NULL END AS percentile_3y,
            s.percentile_all,
            CASE WHEN COALESCE(s.sample_count, 0) >= 250 THEN s.zscore_1y ELSE NULL END AS zscore_1y,
            CASE WHEN COALESCE(s.sample_count, 0) >= 250 THEN s.distance_median_1y_bp ELSE NULL END AS distance_median_1y_display,
            s.sample_count,
            CASE
                WHEN COALESCE(s.sample_count, 0) < 5 THEN '严重不足(<5)'
                WHEN COALESCE(s.sample_count, 0) < 20 THEN '偏少(<20)'
                WHEN COALESCE(s.sample_count, 0) < 250 THEN '一年不足(<250，不展示250日/1年统计)'
                WHEN COALESCE(s.sample_count, 0) < 750 THEN '三年不足(<750，不展示3年分位)'
                ELSE ''
            END AS history_flag
        FROM metric_snapshot s
        LEFT JOIN metric_registry r
          ON r.code = s.code
        WHERE s.as_of_date = ?
        ORDER BY {order_by}
        """,
        (as_of_date,),
    )
    headers = [d[0] for d in cur.description]
    headers.extend(["evidence_codes", "evidence_status"])
    rows = cur.fetchall()
    snapshot_code_set = {str(row[1] or "") for row in rows}
    synthesized = synthesize_support_rows(conn, as_of_date, snapshot_code_set)
    all_rows = list(rows) + synthesized
    full_code_set = {str(row[1] or "") for row in all_rows}
    out = [headers]
    normalized = [normalize_snapshot_row(headers, list(row), snapshot_code_set=full_code_set) for row in all_rows]
    out.extend(normalized)
    return out


def normalize_snapshot_row(
    headers: list[str],
    row: list[object],
    *,
    snapshot_code_set: set[str],
) -> list[object]:
    index = {name: idx for idx, name in enumerate(headers)}
    code = str(row[index["code"]] or "")
    name_cn = str(row[index["name_cn"]] or "")
    is_percentile_metric = ("_prank" in code) or ("历史分位" in name_cn)
    if not is_percentile_metric:
        evidence_codes, evidence_status = infer_evidence(code, snapshot_code_set)
        return row + [evidence_codes, evidence_status]

    for name in ("change_5d", "change_20d", "deviation_ma20", "distance_median_1y_display"):
        idx = index.get(name)
        if idx is None:
            continue
        value = row[idx]
        if value is None or value == "":
            continue
        row[idx] = float(value) / 10000.0
    evidence_codes, evidence_status = infer_evidence(code, snapshot_code_set)
    return row + [evidence_codes, evidence_status]


def infer_evidence(code: str, snapshot_code_set: set[str]) -> tuple[str, str]:
    dependencies: list[str] = []
    is_derived_metric = False

    if code.endswith("_prank250"):
        is_derived_metric = True
        base_code = code[: -len("_prank250")]
        dependencies.append(base_code)
    elif code.endswith(("_ma20", "_ma60", "_ma120")):
        is_derived_metric = True
        base_code = code.rsplit("_", 1)[0]
        dependencies.append(base_code)

    spread_code = ""
    if code.startswith("spread_") and code.endswith("_bp"):
        is_derived_metric = True
        spread_code = code
    elif code.startswith("spread_") and code.endswith("_bp_prank250"):
        is_derived_metric = True
        spread_code = code[: -len("_prank250")]

    if spread_code:
        body = spread_code[len("spread_") : -len("_bp")]
        parts = body.split("_")
        for i in range(1, len(parts)):
            left = "_".join(parts[:i])
            right = "_".join(parts[i:])
            if left in snapshot_code_set and right in snapshot_code_set:
                dependencies.extend([left, right])
                break

    dependencies = list(dict.fromkeys(dep for dep in dependencies if dep))
    if not dependencies:
        if is_derived_metric:
            return "", "基础指标未完整纳入 snapshot"
        return "", ""

    missing = [dep for dep in dependencies if dep not in snapshot_code_set]
    if missing:
        return ", ".join(dependencies), f"缺少基础指标: {', '.join(missing)}"
    return ", ".join(dependencies), "可由基础指标自证"


def synthesize_support_rows(
    conn: sqlite3.Connection,
    as_of_date: str,
    snapshot_code_set: set[str],
) -> list[tuple[object, ...]]:
    needed_codes: set[str] = set()
    for code in snapshot_code_set:
        evidence_codes, evidence_status = infer_evidence(code, snapshot_code_set)
        if not evidence_status.startswith("缺少基础指标"):
            continue
        for dep in [item.strip() for item in evidence_codes.split(",") if item.strip()]:
            if dep not in snapshot_code_set:
                needed_codes.add(dep)

    out: list[tuple[object, ...]] = []
    for code in sorted(needed_codes):
        synthesized = synthesize_curve_metric_row(conn, as_of_date, code)
        if synthesized is not None:
            out.append(synthesized)
    return out


def synthesize_curve_metric_row(
    conn: sqlite3.Connection,
    as_of_date: str,
    code: str,
) -> tuple[object, ...] | None:
    curve_mapping = {
        "local_gov_1y": ("地方债", "y_1", "1年期地方债到期收益率"),
        "local_gov_5y": ("地方债", "y_5", "5年期地方债到期收益率"),
        "local_gov_10y": ("地方债", "y_10", "10年期地方债到期收益率"),
    }
    if code not in curve_mapping:
        return None

    curve_name, tenor_col, default_name = curve_mapping[code]
    row = conn.execute(
        f"""
        SELECT date, {tenor_col}
        FROM raw_bond_curve
        WHERE curve = ?
          AND date <= ?
          AND {tenor_col} IS NOT NULL
        ORDER BY date DESC
        LIMIT 1
        """,
        (curve_name, as_of_date),
    ).fetchone()
    if row is None or row[1] is None:
        return None

    latest_date = str(row[0] or "")
    latest_value = float(row[1]) / 100.0
    return (
        default_name,
        code,
        "利率水平",
        "0-1（显示为%）",
        latest_date,
        latest_value,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        1,
        "导出补充(来自raw_bond_curve)",
    )


def log(message: str) -> None:
    print(message, flush=True)


def export_metric_snapshot_to_sheet(
    *,
    db: str | Path,
    creds: str | None = None,
    spreadsheet_id: str | None = None,
    as_of_date: str | None = None,
    worksheet: str | None = None,
    env_file: str | None = None,
) -> dict[str, object]:
    db_path = Path(db).expanduser()
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    log(f"[INFO] 使用数据库: {db_path}")
    log("[INFO] 正在读取 metric_snapshot 最新快照日期...")

    log("[INFO] 正在以只读方式打开数据库...")
    conn = connect_readonly(db_path)
    try:
        resolved_as_of_date = as_of_date or fetch_latest_as_of_date(conn)
        resolved_worksheet_title = worksheet or resolved_as_of_date
        log(f"[INFO] 目标快照日期: {resolved_as_of_date}")
        log("[INFO] 正在查询快照数据...")
        rows = fetch_snapshot_rows(conn, resolved_as_of_date)
    finally:
        conn.close()

    data_row_count = max(len(rows) - 1, 0)
    log(f"[INFO] 已读取 {data_row_count} 行，目标工作表: {resolved_worksheet_title}")
    log(f"[INFO] 目标 spreadsheet_id: {spreadsheet_id}")
    log(f"[INFO] 目标 credentials: {creds}")
    log("[INFO] 正在加载 Google Sheets 模块...")
    from src.outputs.sheets import GoogleSheetsWriter  # type: ignore

    log("[INFO] Google Sheets 模块加载完成")
    log("[INFO] 正在准备 Google Sheets 认证...")
    writer = GoogleSheetsWriter(
        credentials_path=creds,
        spreadsheet_id=spreadsheet_id,
        env_file=env_file,
    )
    log("[INFO] Google Sheets 认证完成")
    log(f"[INFO] 已连接 spreadsheet: {writer.spreadsheet_id}")
    log(f"[INFO] 正在写入 worksheet: {resolved_worksheet_title}")
    writer.replace_all(resolved_worksheet_title, rows)
    log("[INFO] 数据写入完成")
    log("[INFO] 正在应用表头 / 筛选 / 数字格式 / 条件格式...")
    writer.format_metric_snapshot_sheet(resolved_worksheet_title, rows)
    log("[INFO] 样式设置完成")
    log("[INFO] 写入完成")

    log(
        f"已导出 metric_snapshot {data_row_count} 行，"
        f"as_of_date={resolved_as_of_date!r}，"
        f"写入 spreadsheet={writer.spreadsheet_id!r} 的 worksheet={resolved_worksheet_title!r}"
    )
    return {
        "db_path": str(db_path),
        "as_of_date": resolved_as_of_date,
        "worksheet_title": resolved_worksheet_title,
        "spreadsheet_id": writer.spreadsheet_id,
        "data_row_count": data_row_count,
    }


def main() -> None:
    log("[INFO] 脚本启动")
    log("[INFO] 正在加载本地环境配置...")
    load_local_env()
    cfg = AppConfig.load()
    parser = build_parser(cfg)
    args = parser.parse_args()
    if args.env_file:
        log(f"[INFO] 重新加载 env 文件: {args.env_file}")
        load_local_env(env_file=args.env_file)

    export_metric_snapshot_to_sheet(
        db=args.db,
        creds=args.creds,
        spreadsheet_id=args.spreadsheet_id,
        as_of_date=args.as_of_date,
        worksheet=args.worksheet,
        env_file=args.env_file,
    )


if __name__ == "__main__":
    main()
