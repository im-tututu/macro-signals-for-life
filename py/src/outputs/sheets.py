from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

_INSTALL_HINT = (
    "缺少 Google Sheets 导出依赖。请先在当前 Python 环境安装 "
    "`py/requirements.txt`，例如运行：`python3 -m pip install -r py/requirements.txt`。"
)

try:
    import gspread
except ModuleNotFoundError as exc:
    if exc.name != "gspread":
        raise
    raise ModuleNotFoundError(_INSTALL_HINT) from exc

try:
    from google.oauth2.service_account import Credentials
except ModuleNotFoundError as exc:
    if exc.name not in {"google", "google.oauth2", "google.oauth2.service_account"}:
        raise
    raise ModuleNotFoundError(_INSTALL_HINT) from exc

from src.core.config import AppConfig, REPO_ROOT, getenv_first, load_local_env


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

        credentials_file = Path(resolved_credentials).expanduser()
        if not credentials_file.is_absolute():
            repo_relative = (REPO_ROOT / credentials_file).resolve()
            if repo_relative.exists():
                credentials_file = repo_relative
            else:
                credentials_file = (Path.cwd() / credentials_file).resolve()
        resolved_credentials = str(credentials_file)

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
    ) -> gspread.Worksheet:
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
        return ws

    def format_metric_snapshot_sheet(
        self,
        worksheet_title: str,
        rows: Sequence[Sequence[object]],
    ) -> None:
        if not rows:
            return

        ws = self._spreadsheet.worksheet(worksheet_title)
        headers = [str(v or "") for v in rows[0]]
        data_rows = [list(row) for row in rows[1:]]
        row_count = max(len(rows), 1)
        col_count = max((len(r) for r in rows), default=1)
        sheet_id = ws.id

        requests: list[dict[str, Any]] = []
        requests.extend(self._delete_all_conditional_rules_requests(sheet_id))
        requests.append(
            {
                "repeatCell": {
                    "range": self._grid_range(sheet_id, start_row=0, end_row=1, start_col=0, end_col=col_count),
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 0.90, "green": 0.94, "blue": 0.98},
                            "textFormat": {"bold": True, "foregroundColor": {"red": 0.12, "green": 0.24, "blue": 0.36}},
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat)",
                }
            }
        )
        requests.append(
            {
                "setBasicFilter": {
                    "filter": {
                        "range": self._grid_range(sheet_id, start_row=0, end_row=row_count, start_col=0, end_col=col_count)
                    }
                }
            }
        )
        requests.append(
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": col_count,
                    }
                }
            }
        )

        header_index = {name: idx for idx, name in enumerate(headers)}

        for name in ("sample_count",):
            idx = header_index.get(name)
            if idx is not None:
                requests.append(self._number_format_request(sheet_id, idx, 1, row_count, "NUMBER", "0"))

        for name in ("percentile_250", "percentile_3y", "percentile_all"):
            idx = header_index.get(name)
            if idx is not None:
                requests.append(self._number_format_request(sheet_id, idx, 1, row_count, "PERCENT", "0.0%"))

        zscore_idx = header_index.get("zscore_1y")
        if zscore_idx is not None:
            requests.append(self._number_format_request(sheet_id, zscore_idx, 1, row_count, "NUMBER", "0.00"))

        metric_cols = [
            "latest_value",
            "change_5d",
            "change_20d",
            "deviation_ma20",
            "distance_median_1y_display",
        ]
        unit_idx = header_index.get("unit")
        code_idx = header_index.get("code")
        name_idx = header_index.get("name_cn")
        if unit_idx is not None:
            for row_offset, row in enumerate(data_rows, start=1):
                unit = str(row[unit_idx] or "")
                code = str(row[code_idx] or "") if code_idx is not None and code_idx < len(row) else ""
                name_cn = str(row[name_idx] or "") if name_idx is not None and name_idx < len(row) else ""
                for col_name in metric_cols:
                    col_idx = header_index.get(col_name)
                    if col_idx is None:
                        continue
                    number_format = self._metric_number_format(unit, col_name, code=code, name_cn=name_cn)
                    if number_format is None:
                        continue
                    requests.append(
                        self._single_cell_number_format_request(
                            sheet_id,
                            row_offset,
                            col_idx,
                            number_format["type"],
                            number_format["pattern"],
                        )
                    )

        signed_cols = [
            "change_5d",
            "change_20d",
            "deviation_ma20",
            "distance_median_1y_display",
            "zscore_1y",
        ]
        for col_name in signed_cols:
            col_idx = header_index.get(col_name)
            if col_idx is None:
                continue
            requests.extend(self._signed_value_rules(sheet_id, col_idx, row_count))

        for col_name in ("percentile_250", "percentile_3y", "percentile_all"):
            col_idx = header_index.get(col_name)
            if col_idx is None:
                continue
            requests.extend(self._percentile_extreme_rules(sheet_id, col_idx, row_count))

        if zscore_idx is not None:
            requests.extend(self._absolute_threshold_rules(sheet_id, zscore_idx, row_count, 2.0))

        history_flag_idx = header_index.get("history_flag")
        sample_count_idx = header_index.get("sample_count")
        evidence_status_idx = header_index.get("evidence_status")
        if history_flag_idx is not None:
            requests.extend(self._history_flag_rules(sheet_id, history_flag_idx, row_count))
        if sample_count_idx is not None:
            requests.extend(self._sample_count_row_rules(sheet_id, sample_count_idx, col_count, row_count))
        if evidence_status_idx is not None:
            requests.extend(self._evidence_status_rules(sheet_id, evidence_status_idx, row_count))

        if requests:
            self._spreadsheet.batch_update({"requests": requests})

    def _delete_all_conditional_rules_requests(self, sheet_id: int) -> list[dict[str, Any]]:
        metadata = self._spreadsheet.fetch_sheet_metadata()
        for sheet in metadata.get("sheets", []):
            properties = sheet.get("properties", {})
            if int(properties.get("sheetId", -1)) != int(sheet_id):
                continue
            count = len(sheet.get("conditionalFormats", []))
            return [
                {
                    "deleteConditionalFormatRule": {
                        "sheetId": sheet_id,
                        "index": idx,
                    }
                }
                for idx in range(count - 1, -1, -1)
            ]
        return []

    @staticmethod
    def _grid_range(
        sheet_id: int,
        *,
        start_row: int | None = None,
        end_row: int | None = None,
        start_col: int | None = None,
        end_col: int | None = None,
    ) -> dict[str, int]:
        out: dict[str, int] = {"sheetId": sheet_id}
        if start_row is not None:
            out["startRowIndex"] = start_row
        if end_row is not None:
            out["endRowIndex"] = end_row
        if start_col is not None:
            out["startColumnIndex"] = start_col
        if end_col is not None:
            out["endColumnIndex"] = end_col
        return out

    def _number_format_request(
        self,
        sheet_id: int,
        col_idx: int,
        start_row: int,
        end_row: int,
        fmt_type: str,
        pattern: str,
    ) -> dict[str, Any]:
        return {
            "repeatCell": {
                "range": self._grid_range(
                    sheet_id,
                    start_row=start_row,
                    end_row=end_row,
                    start_col=col_idx,
                    end_col=col_idx + 1,
                ),
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {
                            "type": fmt_type,
                            "pattern": pattern,
                        }
                    }
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        }

    def _single_cell_number_format_request(
        self,
        sheet_id: int,
        row_idx: int,
        col_idx: int,
        fmt_type: str,
        pattern: str,
    ) -> dict[str, Any]:
        return self._number_format_request(sheet_id, col_idx, row_idx, row_idx + 1, fmt_type, pattern)

    @staticmethod
    def _metric_number_format(
        unit: str,
        column_name: str,
        *,
        code: str = "",
        name_cn: str = "",
    ) -> dict[str, str] | None:
        normalized = unit.strip().lower()
        is_percentile_metric = ("_prank" in code) or ("历史分位" in name_cn)

        if is_percentile_metric:
            if column_name in {"latest_value", "change_5d", "change_20d", "deviation_ma20", "distance_median_1y_display"}:
                return {"type": "PERCENT", "pattern": "0.0%"}

        if column_name == "distance_median_1y_display":
            return {"type": "NUMBER", "pattern": '0.0" bp"'}
        if "0-1" in normalized or "%" in normalized or "％" in normalized:
            if column_name in {"latest_value", "prev_value"}:
                return {"type": "PERCENT", "pattern": "0.00%"}
            if column_name in {"change_5d", "change_20d", "deviation_ma20"}:
                return {"type": "NUMBER", "pattern": '0.0" bp"'}
        if normalized == "bp":
            return {"type": "NUMBER", "pattern": '0.0" bp"'}
        if column_name in {"latest_value", "prev_value", "change_5d", "change_20d", "deviation_ma20"}:
            return {"type": "NUMBER", "pattern": "0.00"}
        return None

    def _signed_value_rules(self, sheet_id: int, col_idx: int, row_count: int) -> list[dict[str, Any]]:
        target_range = self._grid_range(
            sheet_id,
            start_row=1,
            end_row=row_count,
            start_col=col_idx,
            end_col=col_idx + 1,
        )
        positive = {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [target_range],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                        "format": {"textFormat": {"foregroundColor": {"red": 0.11, "green": 0.53, "blue": 0.28}}},
                    },
                },
                "index": 0,
            }
        }
        negative = {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [target_range],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                        "format": {"textFormat": {"foregroundColor": {"red": 0.76, "green": 0.18, "blue": 0.16}}},
                    },
                },
                "index": 0,
            }
        }
        return [positive, negative]

    def _percentile_extreme_rules(self, sheet_id: int, col_idx: int, row_count: int) -> list[dict[str, Any]]:
        target_range = self._grid_range(
            sheet_id,
            start_row=1,
            end_row=row_count,
            start_col=col_idx,
            end_col=col_idx + 1,
        )
        high = {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [target_range],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_GREATER_THAN_EQ", "values": [{"userEnteredValue": "0.9"}]},
                        "format": {
                            "backgroundColor": {"red": 0.98, "green": 0.89, "blue": 0.89},
                            "textFormat": {"foregroundColor": {"red": 0.65, "green": 0.00, "blue": 0.00}, "bold": True},
                        },
                    },
                },
                "index": 0,
            }
        }
        low = {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [target_range],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_LESS_THAN_EQ", "values": [{"userEnteredValue": "0.1"}]},
                        "format": {
                            "backgroundColor": {"red": 0.98, "green": 0.89, "blue": 0.89},
                            "textFormat": {"foregroundColor": {"red": 0.65, "green": 0.00, "blue": 0.00}, "bold": True},
                        },
                    },
                },
                "index": 0,
            }
        }
        return [high, low]

    def _absolute_threshold_rules(
        self,
        sheet_id: int,
        col_idx: int,
        row_count: int,
        threshold: float,
    ) -> list[dict[str, Any]]:
        col_letter = self._column_letter(col_idx + 1)
        formula = f'=ABS(${col_letter}2)>={threshold}'
        return [
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [
                            self._grid_range(
                                sheet_id,
                                start_row=1,
                                end_row=row_count,
                                start_col=col_idx,
                                end_col=col_idx + 1,
                            )
                        ],
                        "booleanRule": {
                            "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": formula}]},
                            "format": {
                                "backgroundColor": {"red": 0.98, "green": 0.89, "blue": 0.89},
                                "textFormat": {"foregroundColor": {"red": 0.65, "green": 0.00, "blue": 0.00}, "bold": True},
                            },
                        },
                    },
                    "index": 0,
                }
            }
        ]

    def _history_flag_rules(self, sheet_id: int, col_idx: int, row_count: int) -> list[dict[str, Any]]:
        target_range = self._grid_range(
            sheet_id,
            start_row=1,
            end_row=row_count,
            start_col=col_idx,
            end_col=col_idx + 1,
        )
        return [
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [target_range],
                        "booleanRule": {
                            "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "严重不足"}]},
                            "format": {
                                "backgroundColor": {"red": 0.98, "green": 0.84, "blue": 0.84},
                                "textFormat": {"foregroundColor": {"red": 0.65, "green": 0.00, "blue": 0.00}, "bold": True},
                            },
                        },
                    },
                    "index": 0,
                }
            },
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [target_range],
                        "booleanRule": {
                            "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "偏少"}]},
                            "format": {
                                "backgroundColor": {"red": 1.0, "green": 0.95, "blue": 0.80},
                                "textFormat": {"foregroundColor": {"red": 0.55, "green": 0.32, "blue": 0.00}, "bold": True},
                            },
                        },
                    },
                    "index": 0,
                }
            },
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [target_range],
                        "booleanRule": {
                            "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "一年不足"}]},
                            "format": {
                                "backgroundColor": {"red": 0.94, "green": 0.94, "blue": 0.94},
                                "textFormat": {"foregroundColor": {"red": 0.35, "green": 0.35, "blue": 0.35}, "italic": True},
                            },
                        },
                    },
                    "index": 0,
                }
            },
        ]

    def _sample_count_row_rules(
        self,
        sheet_id: int,
        sample_count_idx: int,
        col_count: int,
        row_count: int,
    ) -> list[dict[str, Any]]:
        target_range = self._grid_range(
            sheet_id,
            start_row=1,
            end_row=row_count,
            start_col=0,
            end_col=col_count,
        )
        col_letter = self._column_letter(sample_count_idx + 1)
        return [
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [target_range],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [{"userEnteredValue": f"=${col_letter}2<5"}],
                            },
                            "format": {
                                "backgroundColor": {"red": 0.99, "green": 0.92, "blue": 0.92},
                            },
                        },
                    },
                    "index": 0,
                }
            },
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [target_range],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [{"userEnteredValue": f"=AND(${col_letter}2>=5,${col_letter}2<20)"}],
                            },
                            "format": {
                                "backgroundColor": {"red": 1.0, "green": 0.97, "blue": 0.90},
                            },
                        },
                    },
                    "index": 0,
                }
            },
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [target_range],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [{"userEnteredValue": f"=AND(${col_letter}2>=20,${col_letter}2<250)"}],
                            },
                            "format": {
                                "backgroundColor": {"red": 0.96, "green": 0.96, "blue": 0.96},
                            },
                        },
                    },
                    "index": 0,
                }
            },
        ]

    def _evidence_status_rules(self, sheet_id: int, col_idx: int, row_count: int) -> list[dict[str, Any]]:
        target_range = self._grid_range(
            sheet_id,
            start_row=1,
            end_row=row_count,
            start_col=col_idx,
            end_col=col_idx + 1,
        )
        return [
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [target_range],
                        "booleanRule": {
                            "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "缺少基础指标"}]},
                            "format": {
                                "backgroundColor": {"red": 0.98, "green": 0.89, "blue": 0.89},
                                "textFormat": {"foregroundColor": {"red": 0.65, "green": 0.00, "blue": 0.00}, "bold": True},
                            },
                        },
                    },
                    "index": 0,
                }
            },
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [target_range],
                        "booleanRule": {
                            "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "可由基础指标自证"}]},
                            "format": {
                                "backgroundColor": {"red": 0.90, "green": 0.96, "blue": 0.90},
                                "textFormat": {"foregroundColor": {"red": 0.11, "green": 0.53, "blue": 0.28}},
                            },
                        },
                    },
                    "index": 0,
                }
            },
        ]

    @staticmethod
    def _column_letter(col_number: int) -> str:
        result = []
        current = col_number
        while current > 0:
            current, remainder = divmod(current - 1, 26)
            result.append(chr(65 + remainder))
        return "".join(reversed(result))

    @staticmethod
    def _to_cell(value: object) -> object:
        if value is None:
            return ""
        return value
