from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

import gspread
from google.oauth2.service_account import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _candidate_env_files() -> list[Path]:
    cwd = Path.cwd().resolve()
    here = Path(__file__).resolve()

    candidates: list[Path] = []
    # Current working directory and its parents.
    for p in [cwd, *cwd.parents]:
        candidates.append(p / ".env")

    # Script/module location and a few parents.
    # Typical layouts:
    #   repo/py/src/outputs/sheets.py
    #   repo/py/scripts/*.py
    for p in [here.parent, *here.parents]:
        candidates.append(p / ".env")

    # De-duplicate while preserving order.
    seen: set[str] = set()
    ordered: list[Path] = []
    for c in candidates:
        s = str(c)
        if s not in seen:
            seen.add(s)
            ordered.append(c)
    return ordered


def load_env_file(path: Path, *, override: bool = False) -> dict[str, str]:
    loaded: dict[str, str] = {}
    if not path.exists():
        return loaded

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        # Remove optional surrounding quotes.
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
    for key in keys:
        value = os.environ.get(key)
        if value:
            return value
    return None


@dataclass
class GoogleSheetsConfig:
    credentials_path: str
    spreadsheet_id: str


class GoogleSheetsWriter:
    """Small wrapper around gspread for replacing worksheet content.

    Defaults can come from environment variables:
    - GOOGLE_APPLICATION_CREDENTIALS
    - GOOGLE_SPREADSHEET_ID

    A local .env file is auto-loaded if present.
    """

    def __init__(
        self,
        credentials_path: str | None = None,
        spreadsheet_id: str | None = None,
        *,
        env_file: str | Path | None = None,
    ) -> None:
        load_local_env(env_file=env_file)

        resolved_credentials = credentials_path or getenv_first("GOOGLE_APPLICATION_CREDENTIALS")
        resolved_spreadsheet_id = spreadsheet_id or getenv_first("GOOGLE_SPREADSHEET_ID")

        if not resolved_credentials:
            raise ValueError(
                "Google credentials path not provided. Set GOOGLE_APPLICATION_CREDENTIALS "
                "in .env/environment or pass credentials_path explicitly."
            )
        if not resolved_spreadsheet_id:
            raise ValueError(
                "Google spreadsheet id not provided. Set GOOGLE_SPREADSHEET_ID "
                "in .env/environment or pass spreadsheet_id explicitly."
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
