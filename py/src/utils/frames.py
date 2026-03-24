"""DataFrame helpers."""

def normalize_columns(columns):
    return [str(c).strip().lower() for c in columns]
