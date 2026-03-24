from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final


# -----------------------------
# Paths
# -----------------------------
# py/src/core/config.py -> parents:
# 0 core, 1 src, 2 py, 3 repo root
PY_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[3]
SRC_ROOT: Final[Path] = PY_ROOT / "src"
DATA_DIR: Final[Path] = PY_ROOT / "data"
SQL_DIR: Final[Path] = PY_ROOT / "sql"
SCRIPT_DIR: Final[Path] = PY_ROOT / "scripts"
TEST_DIR: Final[Path] = PY_ROOT / "tests"

LOG_DIR: Final[Path] = DATA_DIR / "logs"
EXPORT_DIR: Final[Path] = DATA_DIR / "exports"
SNAPSHOT_DIR: Final[Path] = DATA_DIR / "snapshots"
IMPORT_DIR: Final[Path] = DATA_DIR / "imports"

DEFAULT_DB_PATH: Final[Path] = DATA_DIR / "db.sqlite"
DEFAULT_INIT_SQL_PATH: Final[Path] = SQL_DIR / "0001_init_import_first.sql"
DEFAULT_WORKBOOK_NAME: Final[str] = "宏观观察.xlsx"
WORKBOOK_CANDIDATES: Final[list[Path]] = [
    REPO_ROOT / DEFAULT_WORKBOOK_NAME,
    PY_ROOT / DEFAULT_WORKBOOK_NAME,
    IMPORT_DIR / DEFAULT_WORKBOOK_NAME,
]


# -----------------------------
# Environment variable names
# -----------------------------
ENV_DB_PATH: Final[str] = "MSFL_DB_PATH"
ENV_INIT_SQL_PATH: Final[str] = "MSFL_INIT_SQL_PATH"
ENV_WORKBOOK_PATH: Final[str] = "MSFL_WORKBOOK_PATH"
ENV_GSHEET_SPREADSHEET_ID: Final[str] = "MSFL_GSHEET_SPREADSHEET_ID"


# -----------------------------
# Sheet names (keep aligned with current GAS workbook)
# -----------------------------
SHEET_RUN_LOG: Final[str] = "运行日志"
SHEET_BOND_CURVE_RAW: Final[str] = "原始_收益率曲线"
SHEET_OVERSEAS_MACRO_RAW: Final[str] = "原始_海外宏观"
SHEET_POLICY_RATE_RAW: Final[str] = "原始_政策利率"
SHEET_MONEY_MARKET_RAW: Final[str] = "原始_资金面"
SHEET_MONEY_MARKET_LEGACY: Final[str] = "原始_货币"
SHEET_FUTURES_RAW: Final[str] = "原始_国债期货"
SHEET_LIFE_ASSET_RAW: Final[str] = "原始_民生与资产价格"
SHEET_JISILU_ETF_RAW: Final[str] = "原始_指数ETF"
SHEET_BOND_INDEX_RAW: Final[str] = "原始_债券指数特征"
SHEET_BOND_INDEX_LIST: Final[str] = "配置_债券指数清单"
SHEET_JISILU_BOND_INDEX_CANDIDATE: Final[str] = "映射_Jisilu债券指数候选"
SHEET_METRICS: Final[str] = "指标"
SHEET_METRIC_DICTIONARY: Final[str] = "指标说明"
SHEET_SIGNAL_MAIN: Final[str] = "信号-主要"
SHEET_SIGNAL_DETAIL: Final[str] = "信号-明细"
SHEET_SIGNAL_REVIEW: Final[str] = "信号-复盘"

RAW_SHEET_NAMES: Final[list[str]] = [
    SHEET_RUN_LOG,
    SHEET_BOND_CURVE_RAW,
    SHEET_OVERSEAS_MACRO_RAW,
    SHEET_POLICY_RATE_RAW,
    SHEET_MONEY_MARKET_RAW,
    SHEET_MONEY_MARKET_LEGACY,
    SHEET_FUTURES_RAW,
    SHEET_LIFE_ASSET_RAW,
    SHEET_JISILU_ETF_RAW,
    SHEET_BOND_INDEX_RAW,
    SHEET_BOND_INDEX_LIST,
    SHEET_JISILU_BOND_INDEX_CANDIDATE,
]


# -----------------------------
# Table names
# -----------------------------
TABLE_INGEST_RUN: Final[str] = "ingest_run"
TABLE_INGEST_ERROR: Final[str] = "ingest_error"
TABLE_RUN_LOG: Final[str] = "run_log"
TABLE_CFG_BOND_INDEX_LIST: Final[str] = "cfg_bond_index_list"
TABLE_MAP_JISILU_BOND_INDEX_CANDIDATE: Final[str] = "map_jisilu_bond_index_candidate"
TABLE_RAW_BOND_CURVE: Final[str] = "raw_bond_curve"
TABLE_RAW_OVERSEAS_MACRO: Final[str] = "raw_overseas_macro"
TABLE_RAW_POLICY_RATE: Final[str] = "raw_policy_rate"
TABLE_RAW_MONEY_MARKET: Final[str] = "raw_money_market"
TABLE_RAW_FUTURES: Final[str] = "raw_futures"
TABLE_RAW_LIFE_ASSET: Final[str] = "raw_life_asset"
TABLE_RAW_JISILU_ETF: Final[str] = "raw_jisilu_etf"
TABLE_RAW_BOND_INDEX: Final[str] = "raw_bond_index"

RAW_TABLES: Final[list[str]] = [
    TABLE_RUN_LOG,
    TABLE_CFG_BOND_INDEX_LIST,
    TABLE_MAP_JISILU_BOND_INDEX_CANDIDATE,
    TABLE_RAW_BOND_CURVE,
    TABLE_RAW_OVERSEAS_MACRO,
    TABLE_RAW_POLICY_RATE,
    TABLE_RAW_MONEY_MARKET,
    TABLE_RAW_FUTURES,
    TABLE_RAW_LIFE_ASSET,
    TABLE_RAW_JISILU_ETF,
    TABLE_RAW_BOND_INDEX,
]


# -----------------------------
# Bond curve terms / columns
# -----------------------------
CURVE_TERMS: Final[list[str]] = [
    "0", "0.08", "0.17", "0.25", "0.5", "0.75",
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "15", "20", "30", "40", "50",
]
CURVE_VALUE_COLUMNS: Final[list[str]] = [
    "y_0", "y_0_08", "y_0_17", "y_0_25", "y_0_5", "y_0_75",
    "y_1", "y_2", "y_3", "y_4", "y_5", "y_6", "y_7", "y_8", "y_9", "y_10", "y_15", "y_20", "y_30", "y_40", "y_50",
]


# -----------------------------
# Import-time sheet/header quirks
# -----------------------------
HEADER_SCAN_MAX_ROWS: Final[int] = 10
BOND_INDEX_LIST_HEADER_MIN_HITS: Final[int] = 4

# Some legacy sheets contain duplicate or trailing blank columns.
LEGACY_SHEET_ALLOWANCES: Final[dict[str, list[str]]] = {
    SHEET_POLICY_RATE_RAW: ["extra_1", "extra_2"],
    SHEET_LIFE_ASSET_RAW: ["deposit_1y_dup", "source_dup", "fetched_at_dup"],
}


# -----------------------------
# Simple runtime config
# -----------------------------
@dataclass(frozen=True)
class AppConfig:
    db_path: Path
    init_sql_path: Path
    workbook_path: Path | None
    spreadsheet_id: str | None = None

    @classmethod
    def load(cls) -> "AppConfig":
        db_path = _env_path(ENV_DB_PATH) or DEFAULT_DB_PATH
        init_sql_path = _env_path(ENV_INIT_SQL_PATH) or DEFAULT_INIT_SQL_PATH
        workbook_path = _env_path(ENV_WORKBOOK_PATH) or find_default_workbook()
        spreadsheet_id = _env_text(ENV_GSHEET_SPREADSHEET_ID)
        return cls(
            db_path=db_path,
            init_sql_path=init_sql_path,
            workbook_path=workbook_path,
            spreadsheet_id=spreadsheet_id,
        )


def _env_text(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value or None


def _env_path(name: str) -> Path | None:
    value = _env_text(name)
    if not value:
        return None
    return Path(value).expanduser().resolve()


def find_default_workbook() -> Path | None:
    for path in WORKBOOK_CANDIDATES:
        if path.exists():
            return path.resolve()
    return None


def ensure_runtime_dirs() -> None:
    for path in [DATA_DIR, LOG_DIR, EXPORT_DIR, SNAPSHOT_DIR, IMPORT_DIR, SQL_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def as_dict(config: AppConfig | None = None) -> dict[str, str | None]:
    cfg = config or AppConfig.load()
    return {
        "repo_root": str(REPO_ROOT),
        "py_root": str(PY_ROOT),
        "data_dir": str(DATA_DIR),
        "db_path": str(cfg.db_path),
        "init_sql_path": str(cfg.init_sql_path),
        "workbook_path": str(cfg.workbook_path) if cfg.workbook_path else None,
        "spreadsheet_id": cfg.spreadsheet_id,
    }
