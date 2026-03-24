"""Text helpers."""

def clean_text(value: str) -> str:
    return " ".join(str(value).split())
