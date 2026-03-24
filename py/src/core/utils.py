from __future__ import annotations

import datetime as dt
import json
import re
from html import unescape
from typing import Any, Iterable, Optional

from dateutil import parser as date_parser

TZ_SH = dt.timezone(dt.timedelta(hours=8))


def now_shanghai() -> dt.datetime:
    return dt.datetime.now(tz=TZ_SH)


def today_ymd() -> str:
    return now_shanghai().strftime("%Y-%m-%d")


def now_text() -> str:
    return now_shanghai().strftime("%Y-%m-%d %H:%M:%S")


def norm_ymd(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, dt.datetime):
        return value.astimezone(TZ_SH).strftime("%Y-%m-%d")
    if isinstance(value, dt.date):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if not text:
        return ""
    text = text.replace("年", "-").replace("月", "-").replace("日", "")
    text = text.replace("/", "-").replace(".", "-")
    try:
        return date_parser.parse(text).date().strftime("%Y-%m-%d")
    except Exception:
        m = re.search(r"(20\d{2})[-](\d{1,2})[-](\d{1,2})", text)
        if not m:
            return text
        y, mo, day = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(day):02d}"


def strip_tags(text: str) -> str:
    return re.sub(r"<[^>]+>", "", unescape(text or "")).strip()


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text in {"--", "-", ".", "null", "None"}:
        return None
    text = text.replace(",", "")
    text = text.replace("%", "")
    text = text.replace("bp", "")
    text = text.replace("BP", "")
    try:
        return float(text)
    except ValueError:
        return None


def coalesce(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def safe_json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def first_not_none(items: Iterable[Any]) -> Any:
    for item in items:
        if item is not None:
            return item
    return None
