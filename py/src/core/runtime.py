from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig, LOG_DIR, TABLE_INGEST_ERROR, TABLE_INGEST_RUN, TABLE_RUN_LOG, ensure_runtime_dirs
from .db import connect, run_sql_files
from .notify import Notifier, build_notifier


UTC = timezone.utc


@dataclass
class WriteStats:
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0
    reviewed: int = 0
    deleted: int = 0
    changed: int = 0
    incoming: int = 0

    def merge(self, other: "WriteStats") -> None:
        for field_name in self.__dataclass_fields__:
            setattr(self, field_name, getattr(self, field_name) + getattr(other, field_name))

    def as_message(self) -> str:
        return (
            f"incoming={self.incoming} inserted={self.inserted} updated={self.updated} "
            f"skipped={self.skipped} failed={self.failed} deleted={self.deleted} changed={self.changed}"
        )


@dataclass
class RunContext:
    job_name: str
    source_type: str
    dry_run: bool = False
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    started_at: str = field(default_factory=lambda: utc_now_text())
    logger: logging.Logger | None = None
    notifier: Notifier | None = None
    db_path: Path | None = None
    _finished: bool = field(default=False, init=False, repr=False)

    @classmethod
    def create(
        cls,
        job_name: str,
        source_type: str,
        *,
        dry_run: bool = False,
        db_path: Path | None = None,
    ) -> "RunContext":
        ensure_runtime_dirs()
        cfg = AppConfig.load()
        logger = build_logger(job_name, level=cfg.log_level)
        notifier = build_notifier(logger)
        ctx = cls(
            job_name=job_name,
            source_type=source_type,
            dry_run=dry_run,
            logger=logger,
            notifier=notifier,
            db_path=db_path or cfg.db_path,
        )
        ctx._record_start()
        return ctx

    def finish(self, status: str, stats: WriteStats, message: str, *, detail: str | None = None) -> None:
        if self._finished:
            return
        finished_at = utc_now_text()
        conn = connect(self.db_path)
        try:
            conn.execute(
                f"""
                UPDATE {TABLE_INGEST_RUN}
                   SET finished_at = ?,
                       status = ?,
                       rows_written = ?,
                       rows_read = ?,
                       message = ?
                 WHERE run_id = ?
                """,
                (finished_at, status, stats.changed, stats.incoming, message, self.run_id),
            )
            conn.execute(
                f"""
                INSERT OR REPLACE INTO {TABLE_RUN_LOG}
                (timestamp, job_name, status, message, detail, source_sheet, source_row_num, migrated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    finished_at,
                    self.job_name,
                    status,
                    message,
                    detail,
                    self.source_type,
                    None,
                    finished_at,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        if self.logger:
            self.logger.info("[%s] %s", status, message)
            if detail:
                self.logger.info(detail)
        disable_success_bark = os.getenv("MSFL_DISABLE_SUCCESS_BARK", "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        should_notify = (not self.dry_run or status != "success") and not (
            status == "success" and disable_success_bark
        )
        # dry-run 成功时不发外部通知，避免本地演练反复触发 Bark。
        if self.notifier and should_notify:
            title = f"{self.job_name} {'DRY-RUN' if self.dry_run else status.upper()}"
            self.notifier.notify(title, message, level="INFO" if status == "success" else "WARNING")
        self._finished = True

    def record_error(
        self,
        error_type: str,
        error_message: str,
        *,
        source_name: str | None = None,
        object_name: str | None = None,
        row_payload: dict[str, Any] | None = None,
    ) -> None:
        conn = connect(self.db_path)
        try:
            conn.execute(
                f"""
                INSERT INTO {TABLE_INGEST_ERROR}
                (run_id, source_name, object_name, error_type, error_message, row_payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.run_id,
                    source_name,
                    object_name,
                    error_type,
                    error_message,
                    json.dumps(row_payload, ensure_ascii=False) if row_payload is not None else None,
                    utc_now_text(),
                ),
            )
            conn.commit()
        finally:
            conn.close()
        if self.logger:
            self.logger.error("[%s] %s", error_type, error_message)

    def _record_start(self) -> None:
        conn = connect(self.db_path)
        try:
            cfg = AppConfig.load()
            sql_paths = [cfg.init_sql_path] + [p for p in cfg.init_sql_path.parent.glob("0002*.sql") if p.exists()]
            run_sql_files(conn, sql_paths)
            conn.execute(
                f"""
                INSERT OR REPLACE INTO {TABLE_INGEST_RUN}
                (run_id, job_name, source_type, started_at, finished_at, status, rows_read, rows_written, message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.run_id,
                    self.job_name,
                    self.source_type,
                    self.started_at,
                    None,
                    "running",
                    0,
                    0,
                    "dry-run" if self.dry_run else "started",
                ),
            )
            conn.commit()
        finally:
            conn.close()
        if self.logger:
            self.logger.info("开始任务: %s source=%s dry_run=%s", self.job_name, self.source_type, self.dry_run)


def utc_now_text() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S")


def build_logger(job_name: str, *, level: str = "INFO") -> logging.Logger:
    ensure_runtime_dirs()
    logger = logging.getLogger(f"msfl.{job_name}")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False
    if logger.handlers:
        return logger
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    file_handler = logging.FileHandler(LOG_DIR / f"{job_name}.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger
