"""Initialize SQLite schema."""

from pathlib import Path
import sqlite3


def main() -> None:
    db_path = Path("py/data/db.sqlite")
    sql_dir = Path("py/sql")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        for sql_file in ["0001_init.sql", "0002_indexes.sql"]:
            conn.executescript((sql_dir / sql_file).read_text(encoding="utf-8"))
        conn.commit()
        print(f"Initialized database at {db_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
