from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final

# -----------------------------
# 路径
# -----------------------------
# py/src/core/config.py 的父级层级：
# 0 core, 1 src, 2 py, 3 仓库根目录
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
DEFAULT_INIT_SQL_PATH: Final[Path] = SQL_DIR / "0001_init.sql"
DEFAULT_WORKBOOK_NAME: Final[str] = "宏观观察.xlsx"
WORKBOOK_CANDIDATES: Final[list[Path]] = [
    REPO_ROOT / DEFAULT_WORKBOOK_NAME,
    PY_ROOT / DEFAULT_WORKBOOK_NAME,
    IMPORT_DIR / DEFAULT_WORKBOOK_NAME,
]

# -----------------------------
# 环境变量名
# -----------------------------
ENV_DB_PATH_KEYS: Final[tuple[str, ...]] = ("DB_PATH", "MSFL_DB_PATH", "SQLITE_DB_PATH")
ENV_INIT_SQL_PATH_KEYS: Final[tuple[str, ...]] = ("INIT_SQL_PATH", "MSFL_INIT_SQL_PATH")
ENV_WORKBOOK_PATH_KEYS: Final[tuple[str, ...]] = ("WORKBOOK_PATH", "MSFL_WORKBOOK_PATH")
ENV_GSHEET_SPREADSHEET_ID_KEYS: Final[tuple[str, ...]] = ("GOOGLE_SPREADSHEET_ID", "MSFL_GSHEET_SPREADSHEET_ID")
ENV_GSHEET_CREDENTIALS_PATH_KEYS: Final[tuple[str, ...]] = ("GOOGLE_APPLICATION_CREDENTIALS",)
ENV_WORKSHEET_PREFIX_KEYS: Final[tuple[str, ...]] = ("GOOGLE_WORKSHEET_PREFIX",)
ENV_EXPORT_LIMIT_PER_TABLE_KEYS: Final[tuple[str, ...]] = ("EXPORT_LIMIT_PER_TABLE",)
ENV_EXPORT_INCLUDE_EMPTY_KEYS: Final[tuple[str, ...]] = ("EXPORT_INCLUDE_EMPTY",)
ENV_LOG_LEVEL_KEYS: Final[tuple[str, ...]] = ("LOG_LEVEL", "MSFL_LOG_LEVEL")
ENV_BARK_URL_KEYS: Final[tuple[str, ...]] = ("BARK_BASE_URL", "BARK_URL", "MSFL_BARK_URL")
ENV_BARK_GROUP_KEYS: Final[tuple[str, ...]] = ("BARK_GROUP", "MSFL_BARK_GROUP")
ENV_BARK_DEVICE_KEY_KEYS: Final[tuple[str, ...]] = ("BARK_DEVICE_KEY", "MSFL_BARK_DEVICE_KEY")

# -----------------------------
# Sheet 名称（需与当前 GAS 工作簿保持一致）
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
# 数据表名
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
TABLE_METRICS: Final[str] = "metrics"
TABLE_SIGNAL_MAIN: Final[str] = "signal_main"
TABLE_SIGNAL_DETAIL: Final[str] = "signal_detail"
TABLE_SIGNAL_REVIEW: Final[str] = "signal_review"

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
# 债券收益率曲线期限 / 列名
# -----------------------------
CURVE_TERMS: Final[list[str]] = [
    "0",
    "0.08",
    "0.17",
    "0.25",
    "0.5",
    "0.75",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "10",
    "15",
    "20",
    "30",
    "40",
    "50",
]
CURVE_VALUE_COLUMNS: Final[list[str]] = [
    "y_0",
    "y_0_08",
    "y_0_17",
    "y_0_25",
    "y_0_5",
    "y_0_75",
    "y_1",
    "y_2",
    "y_3",
    "y_4",
    "y_5",
    "y_6",
    "y_7",
    "y_8",
    "y_9",
    "y_10",
    "y_15",
    "y_20",
    "y_30",
    "y_40",
    "y_50",
]

# -----------------------------
# 导入时的 sheet / 表头兼容规则
# -----------------------------
HEADER_SCAN_MAX_ROWS: Final[int] = 10
BOND_INDEX_LIST_HEADER_MIN_HITS: Final[int] = 4
LEGACY_SHEET_ALLOWANCES: Final[dict[str, list[str]]] = {
    SHEET_POLICY_RATE_RAW: ["extra_1", "extra_2"],
    SHEET_LIFE_ASSET_RAW: ["deposit_1y_dup", "source_dup", "fetched_at_dup"],
}


@dataclass(frozen=True)
class AppConfig:
    db_path: Path
    init_sql_path: Path
    workbook_path: Path | None
    spreadsheet_id: str | None = None
    google_credentials_path: Path | None = None
    worksheet_prefix: str = ""
    export_limit_per_table: int | None = None
    export_include_empty: bool = False
    log_level: str = "INFO"
    bark_url: str | None = None
    bark_group: str | None = None
    bark_device_key: str | None = None

    @classmethod
    def load(cls) -> "AppConfig":
        load_local_env()
        db_path = _env_path(*ENV_DB_PATH_KEYS) or DEFAULT_DB_PATH
        init_sql_path = _env_path(*ENV_INIT_SQL_PATH_KEYS) or DEFAULT_INIT_SQL_PATH
        workbook_path = _env_path(*ENV_WORKBOOK_PATH_KEYS) or find_default_workbook()
        spreadsheet_id = _env_text(*ENV_GSHEET_SPREADSHEET_ID_KEYS)
        google_credentials_path = _env_path(*ENV_GSHEET_CREDENTIALS_PATH_KEYS)
        worksheet_prefix = _env_text(*ENV_WORKSHEET_PREFIX_KEYS) or ""
        export_limit_per_table = _env_int(*ENV_EXPORT_LIMIT_PER_TABLE_KEYS)
        export_include_empty = _env_bool(*ENV_EXPORT_INCLUDE_EMPTY_KEYS, default=False)
        log_level = (_env_text(*ENV_LOG_LEVEL_KEYS) or "INFO").upper()
        bark_url = _env_text(*ENV_BARK_URL_KEYS)
        bark_group = _env_text(*ENV_BARK_GROUP_KEYS)
        bark_device_key = _env_text(*ENV_BARK_DEVICE_KEY_KEYS)
        return cls(
            db_path=db_path,
            init_sql_path=init_sql_path,
            workbook_path=workbook_path,
            spreadsheet_id=spreadsheet_id,
            google_credentials_path=google_credentials_path,
            worksheet_prefix=worksheet_prefix,
            export_limit_per_table=export_limit_per_table,
            export_include_empty=export_include_empty,
            log_level=log_level,
            bark_url=bark_url,
            bark_group=bark_group,
            bark_device_key=bark_device_key,
        )


def _candidate_env_files() -> list[Path]:
    candidates = [
        REPO_ROOT / ".env",
        PY_ROOT / ".env",
        Path.cwd().resolve() / ".env",
    ]
    seen: set[str] = set()
    ordered: list[Path] = []
    for candidate in candidates:
        text = str(candidate)
        if text not in seen:
            seen.add(text)
            ordered.append(candidate)
    return ordered


def load_env_file(path: Path, *, override: bool = False) -> dict[str, str]:
    loaded: dict[str, str] = {}
    if not path.exists():
        return loaded

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        loaded[key] = value
        if override or key not in os.environ:
            os.environ[key] = value
    return loaded


def load_local_env(*, override: bool = False, env_file: str | Path | None = None) -> Path | None:
    if env_file:
        path = Path(env_file).expanduser().resolve()
        if path.exists():
            load_env_file(path, override=override)
            return path
        return None

    for candidate in _candidate_env_files():
        if candidate.exists():
            load_env_file(candidate, override=override)
            return candidate
    return None


def getenv_first(*keys: str) -> str | None:
    load_local_env()
    for key in keys:
        value = os.environ.get(key, "").strip()
        if value:
            return value
    return None


def _env_text(*names: str) -> str | None:
    return getenv_first(*names)


def _env_path(*names: str) -> Path | None:
    value = _env_text(*names)
    if not value:
        return None
    return Path(value).expanduser().resolve()


def _env_bool(*names: str, default: bool = False) -> bool:
    value = _env_text(*names)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(*names: str) -> int | None:
    value = _env_text(*names)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def find_default_workbook() -> Path | None:
    for path in WORKBOOK_CANDIDATES:
        if path.exists():
            return path.resolve()
    return None


def ensure_runtime_dirs() -> None:
    for path in [DATA_DIR, LOG_DIR, EXPORT_DIR, SNAPSHOT_DIR, IMPORT_DIR, SQL_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def default_sql_paths() -> list[Path]:
    paths = sorted(SQL_DIR.glob("000*.sql"))
    if not paths and DEFAULT_INIT_SQL_PATH.exists():
        paths = [DEFAULT_INIT_SQL_PATH]
    return paths


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
        "google_credentials_path": str(cfg.google_credentials_path) if cfg.google_credentials_path else None,
        "worksheet_prefix": cfg.worksheet_prefix,
        "export_limit_per_table": str(cfg.export_limit_per_table) if cfg.export_limit_per_table is not None else None,
        "export_include_empty": str(cfg.export_include_empty),
        "log_level": cfg.log_level,
        "bark_url": cfg.bark_url,
        "bark_group": cfg.bark_group,
    }
