"""Numeric helpers."""

def safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
