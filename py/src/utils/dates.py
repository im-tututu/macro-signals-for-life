"""Date helpers."""

from datetime import datetime


def to_ymd(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    return str(value)
