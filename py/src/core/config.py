from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from src.core.models import CurveSpec

# -----------------------------
# 路径
# -----------------------------
# py/src/core/config.py 的父级层级：
# 0 core, 1 src, 2 py, 3 仓库根目录
PY_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[3]
SRC_ROOT: Final[Path] = PY_ROOT / "src"
RUNTIME_DIR: Final[Path] = REPO_ROOT / "runtime"
LOCAL_DIR: Final[Path] = REPO_ROOT / "tools" / "local"
SQL_DIR: Final[Path] = PY_ROOT / "sql"
SCRIPT_DIR: Final[Path] = PY_ROOT / "scripts"
TEST_DIR: Final[Path] = PY_ROOT / "tests"
LOG_DIR: Final[Path] = RUNTIME_DIR / "logs"
EXPORT_DIR: Final[Path] = RUNTIME_DIR / "exports"
SNAPSHOT_DIR: Final[Path] = RUNTIME_DIR / "snapshots"
TMP_DIR: Final[Path] = RUNTIME_DIR / "tmp"
DB_DIR: Final[Path] = RUNTIME_DIR / "db"
DEFAULT_DB_PATH: Final[Path] = DB_DIR / "app.sqlite"
DEFAULT_INIT_SQL_PATH: Final[Path] = SQL_DIR / "0001_init.sql"
DEFAULT_WORKBOOK_NAME: Final[str] = "宏观观察.xlsx"
WORKBOOK_CANDIDATES: Final[list[Path]] = [
    REPO_ROOT / DEFAULT_WORKBOOK_NAME,
    LOCAL_DIR / DEFAULT_WORKBOOK_NAME,
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
SHEET_FRED_RAW: Final[str] = "原始_FRED"
SHEET_ALPHA_VANTAGE_RAW: Final[str] = "原始_AlphaVantage"
SHEET_POLICY_RATE_RAW: Final[str] = "原始_政策利率"
SHEET_MONEY_MARKET_RAW: Final[str] = "原始_资金面"
SHEET_MONEY_MARKET_LEGACY: Final[str] = "原始_货币"
SHEET_FUTURES_RAW: Final[str] = "原始_国债期货"
SHEET_JISILU_ETF_RAW: Final[str] = "原始_指数ETF"
SHEET_JISILU_QDII_RAW: Final[str] = "原始_QDII"
SHEET_JISILU_TREASURY_RAW: Final[str] = "原始_国债现券"
SHEET_SSE_LIVELY_BOND_RAW: Final[str] = "原始_上交所活跃国债"
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
    SHEET_FRED_RAW,
    SHEET_ALPHA_VANTAGE_RAW,
    SHEET_POLICY_RATE_RAW,
    SHEET_MONEY_MARKET_RAW,
    SHEET_MONEY_MARKET_LEGACY,
    SHEET_FUTURES_RAW,
    SHEET_JISILU_ETF_RAW,
    SHEET_JISILU_QDII_RAW,
    SHEET_JISILU_TREASURY_RAW,
    SHEET_SSE_LIVELY_BOND_RAW,
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
TABLE_RAW_FRED: Final[str] = "raw_fred"
TABLE_RAW_ALPHA_VANTAGE: Final[str] = "raw_alpha_vantage"
TABLE_RAW_POLICY_RATE: Final[str] = "raw_policy_rate"
TABLE_RAW_MONEY_MARKET: Final[str] = "raw_money_market"
TABLE_RAW_FUTURES: Final[str] = "raw_futures"
TABLE_RAW_JISILU_ETF: Final[str] = "raw_jisilu_etf"
TABLE_RAW_JISILU_QDII: Final[str] = "raw_jisilu_qdii"
TABLE_RAW_JISILU_TREASURY: Final[str] = "raw_jisilu_treasury"
TABLE_RAW_SSE_LIVELY_BOND: Final[str] = "raw_sse_lively_bond"
TABLE_RAW_CHINABOND_BOND_INDEX: Final[str] = "raw_chinabond_bond_index"
TABLE_RAW_CSINDEX_BOND_INDEX: Final[str] = "raw_csindex_bond_index"
TABLE_RAW_CNINDEX_BOND_INDEX: Final[str] = "raw_cnindex_bond_index"
TABLE_METRICS: Final[str] = "metrics"
TABLE_SIGNAL_MAIN: Final[str] = "signal_main"
TABLE_SIGNAL_DETAIL: Final[str] = "signal_detail"
TABLE_SIGNAL_REVIEW: Final[str] = "signal_review"

RAW_TABLES: Final[list[str]] = [
    TABLE_RUN_LOG,
    TABLE_CFG_BOND_INDEX_LIST,
    TABLE_MAP_JISILU_BOND_INDEX_CANDIDATE,
    TABLE_RAW_BOND_CURVE,
    TABLE_RAW_FRED,
    TABLE_RAW_ALPHA_VANTAGE,
    TABLE_RAW_POLICY_RATE,
    TABLE_RAW_MONEY_MARKET,
    TABLE_RAW_FUTURES,
    TABLE_RAW_JISILU_ETF,
    TABLE_RAW_JISILU_QDII,
    TABLE_RAW_JISILU_TREASURY,
    TABLE_RAW_SSE_LIVELY_BOND,
    TABLE_RAW_CHINABOND_BOND_INDEX,
    TABLE_RAW_CSINDEX_BOND_INDEX,
    TABLE_RAW_CNINDEX_BOND_INDEX,
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
TERMS: Final[list[float]] = [float(term) for term in CURVE_TERMS]
CURVES: Final[list[CurveSpec]] = [
    CurveSpec(name="国债", id="2c9081e50a2f9606010a3068cae70001", tier="main", aliases=["国债收益率曲线", "Government Bond"]),
    CurveSpec(name="国开债", id="8a8b2ca037a7ca910137bfaa94fa5057", tier="main", aliases=["国开债收益率曲线", "政策性金融债", "Policy Bank"]),
    CurveSpec(name="AAA信用", id="2c9081e50a2f9606010a309f4af50111", tier="main", aliases=["企业债收益率曲线(AAA)", "企业债AAA", "Enterprise Bond AAA"]),
    CurveSpec(name="AA+信用", id="2c908188138b62cd01139a2ee6b51e25", tier="main", aliases=["企业债收益率曲线(AA+)", "企业债收益率曲线(AA＋)", "企业债AA+", "Enterprise Bond AA+"]),
    CurveSpec(name="AAA+中票", id="2c9081e9257ddf2a012590efdded1d35", tier="main", aliases=["中短期票据收益率曲线(AAA+)", "中票AAA+", "CP&Note AAA+"]),
    CurveSpec(name="AAA中票", id="2c9081880fa9d507010fb8505b393fe7", tier="main", aliases=["中短期票据收益率曲线(AAA)", "中票AAA", "CP&Note AAA"]),
    CurveSpec(name="AAA存单", id="8308218D1D030E0DE0540010E03EE6DA", tier="main", aliases=["同业存单收益率曲线(AAA)", "存单AAA", "NCD AAA", "Negotiable CD AAA"]),
    CurveSpec(name="AAA城投", id="2c9081e91b55cc84011be3c53b710598", tier="extended", aliases=["城投债收益率曲线(AAA)", "城投AAA", "LGFV AAA"]),
    CurveSpec(name="AAA银行债", id="2c9081e9259b766a0125be8b5115149f", tier="extended", aliases=["商业银行普通债收益率曲线(AAA)", "商业银行债收益率曲线(AAA)", "银行债AAA", "Financial Bond of Commercial Bank AAA"]),
    CurveSpec(name="地方债", id="998183ff8c00f640018c32d4721a0d16", tier="short_history", fetch_separately=True, aliases=["地方政府债收益率曲线", "地方政府债", "Local Government"]),
]
OVERSEAS_MACRO_FRED_SERIES: Final[dict[str, str]] = {
    "fed_upper": "DFEDTARU",
    "fed_lower": "DFEDTARL",
    "sofr": "SOFR",
    "ust_2y": "DGS2",
    "ust_10y": "DGS10",
    "us_real_10y": "DFII10",
    "usd_broad": "DTWEXBGS",
    "usd_cny": "DEXCHUS",
    "vix": "VIXCLS",
    "spx": "SP500",
    "nasdaq_100": "NASDAQ100",
}
OVERSEAS_MACRO_ALPHA_SERIES: Final[dict[str, dict[str, str]]] = {
    "gold": {"fn": "GOLD_SILVER_HISTORY", "symbol": "GOLD", "interval": "daily"},
    "wti": {"fn": "WTI", "interval": "daily"},
    "brent": {"fn": "BRENT", "interval": "daily"},
    "copper": {"fn": "COPPER", "interval": "monthly"},
}

# -----------------------------
# 导入时的 sheet / 表头兼容规则
# -----------------------------
HEADER_SCAN_MAX_ROWS: Final[int] = 10
BOND_INDEX_LIST_HEADER_MIN_HITS: Final[int] = 4
LEGACY_SHEET_ALLOWANCES: Final[dict[str, list[str]]] = {
    SHEET_POLICY_RATE_RAW: ["extra_1", "extra_2"],
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
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (REPO_ROOT / path).resolve()


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
    for path in [RUNTIME_DIR, LOCAL_DIR, DB_DIR, LOG_DIR, EXPORT_DIR, SNAPSHOT_DIR, TMP_DIR, SQL_DIR]:
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
        "runtime_dir": str(RUNTIME_DIR),
        "local_dir": str(LOCAL_DIR),
        "db_dir": str(DB_DIR),
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


@dataclass(frozen=True)
class RuntimeSettings:
    http_timeout_seconds: float = 20.0
    http_retry_count: int = 2
    http_retry_sleep_seconds: float = 1.0
    fred_api_key: str = ""
    alpha_vantage_api_key: str = ""
    jisilu_cookie: str = ""
    jisilu_user_agent: str = "Mozilla/5.0"
    jisilu_refresh_url: str = ""
    jisilu_refresh_token: str = ""

    @classmethod
    def load(cls) -> "RuntimeSettings":
        load_local_env()
        return cls(
            http_timeout_seconds=float(_env_text("HTTP_TIMEOUT_SECONDS", "MSFL_HTTP_TIMEOUT_SECONDS") or 20.0),
            http_retry_count=int(_env_text("HTTP_RETRY_COUNT", "MSFL_HTTP_RETRY_COUNT") or 2),
            http_retry_sleep_seconds=float(_env_text("HTTP_RETRY_SLEEP_SECONDS", "MSFL_HTTP_RETRY_SLEEP_SECONDS") or 1.0),
            fred_api_key=_env_text("FRED_API_KEY", "MSFL_FRED_API_KEY") or "",
            alpha_vantage_api_key=_env_text("ALPHA_VANTAGE_API_KEY", "MSFL_ALPHA_VANTAGE_API_KEY") or "",
            jisilu_cookie=_env_text("JISILU_COOKIE", "MSFL_JISILU_COOKIE") or "",
            jisilu_user_agent=_env_text("JISILU_USER_AGENT", "MSFL_JISILU_USER_AGENT") or "Mozilla/5.0",
            jisilu_refresh_url=_env_text("JISILU_REFRESH_URL", "MSFL_JISILU_REFRESH_URL") or "",
            jisilu_refresh_token=_env_text("JISILU_REFRESH_TOKEN", "MSFL_JISILU_REFRESH_TOKEN") or "",
        )


settings = RuntimeSettings.load()
