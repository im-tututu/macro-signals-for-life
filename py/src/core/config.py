"""Central configuration for the Python project."""

from pathlib import Path
import os


BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
SQLITE_PATH = Path(os.getenv("SQLITE_PATH", DATA_DIR / "db.sqlite"))
TIMEZONE = os.getenv("TZ", "Asia/Shanghai")
