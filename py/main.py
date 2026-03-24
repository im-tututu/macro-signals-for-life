from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.jobs.common import get_store
from src.jobs.manual import clean_table, review_table


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Macro Signals for Life - SQLite store utilities")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_review = sub.add_parser("review", help="Review one table")
    p_review.add_argument("store_name")
    p_review.add_argument("--db", dest="db_path", type=Path, default=None)

    p_clean = sub.add_parser("clean", help="Clean one table")
    p_clean.add_argument("store_name")
    p_clean.add_argument("--db", dest="db_path", type=Path, default=None)
    p_clean.add_argument("--apply", action="store_true", help="Apply changes. Default is dry-run.")
    p_clean.add_argument("--delete-invalid", action="store_true")

    p_init = sub.add_parser("init-db", help="Initialize sqlite schema")
    p_init.add_argument("--db", dest="db_path", type=Path, default=None)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.cmd == "review":
        report = review_table(args.store_name, db_path=args.db_path)
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return

    if args.cmd == "clean":
        stats = clean_table(
            args.store_name,
            dry_run=not args.apply,
            delete_invalid_rows=args.delete_invalid,
            db_path=args.db_path,
        )
        print(json.dumps(stats.__dict__, ensure_ascii=False, indent=2, default=str))
        return

    if args.cmd == "init-db":
        store = get_store("raw_bond_curve", db_path=args.db_path)
        store.initialize()
        print("initialized", store.db_path)
        return

    parser.error(f"unknown cmd: {args.cmd}")


if __name__ == "__main__":
    main()
