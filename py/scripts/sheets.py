from __future__ import annotations

from pathlib import Path
from typing import Sequence

import gspread
from google.oauth2.service_account import Credentials

try:
    from src.core.config import AppConfig, getenv_first, load_local_env  # type: ignore
except Exception:
    import sys

    CURRENT_DIR = Path(__file__).resolve().parent
    PY_ROOT = CURRENT_DIR.parent
    SRC_DIR = PY_ROOT / "src"
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    from core.config import AppConfig, getenv_first, load_local_env  # type: ignore


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class GoogleSheetsWriter:
    """基于 gspread 的轻量封装，用于整表替换工作表内容。

    默认配置来自 AppConfig / 本地 .env。
    """

    def __init__(
        self,
        credentials_path: str | None = None,
        spreadsheet_id: str | None = None,
        *,
        env_file: str | Path | None = None,
    ) -> None:
        load_local_env(env_file=env_file)
        cfg = AppConfig.load()

        resolved_credentials = credentials_path or (
            str(cfg.google_credentials_path) if cfg.google_credentials_path else None
        )
        resolved_spreadsheet_id = spreadsheet_id or cfg.spreadsheet_id or getenv_first("GOOGLE_SPREADSHEET_ID")

        if not resolved_credentials:
            raise ValueError(
                "未提供 Google 凭证路径。请在 .env / 环境变量中设置 "
                "GOOGLE_APPLICATION_CREDENTIALS，或显式传入 credentials_path。"
            )
        if not resolved_spreadsheet_id:
            raise ValueError(
                "未提供 Google Spreadsheet ID。请在 .env / 环境变量中设置 "
                "GOOGLE_SPREADSHEET_ID，或显式传入 spreadsheet_id。"
            )

        creds = Credentials.from_service_account_file(resolved_credentials, scopes=SCOPES)
        self._gc = gspread.authorize(creds)
        self._spreadsheet = self._gc.open_by_key(resolved_spreadsheet_id)
        self._spreadsheet_id = resolved_spreadsheet_id
        self._credentials_path = resolved_credentials

    @property
    def spreadsheet_id(self) -> str:
        return self._spreadsheet_id

    @property
    def credentials_path(self) -> str:
        return self._credentials_path

    def ensure_worksheet(
        self,
        title: str,
        rows: int = 1000,
        cols: int = 26,
    ) -> gspread.Worksheet:
        try:
            return self._spreadsheet.worksheet(title)
        except gspread.WorksheetNotFound:
            return self._spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)

    def replace_all(
        self,
        worksheet_title: str,
        rows: Sequence[Sequence[object]],
        *,
        freeze_header: bool = True,
        resize: bool = True,
    ) -> None:
        values = [[self._to_cell(v) for v in row] for row in rows]
        max_rows = max(len(values), 1000)
        max_cols = max((len(r) for r in values), default=1)

        ws = self.ensure_worksheet(worksheet_title, rows=max_rows, cols=max_cols)
        ws.clear()
        if resize:
            ws.resize(rows=max_rows, cols=max_cols)
        if values:
            ws.update("A1", values, value_input_option="RAW")
        if freeze_header and values:
            ws.freeze(rows=1)

    @staticmethod
    def _to_cell(value: object) -> object:
        if value is None:
            return ""
        return value
