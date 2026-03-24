from __future__ import annotations

import json
import logging
import os
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Protocol

from .config import AppConfig


class Notifier(Protocol):
    def notify(self, title: str, body: str, *, level: str = "INFO") -> None: ...


@dataclass
class ConsoleNotifier:
    logger: logging.Logger

    def notify(self, title: str, body: str, *, level: str = "INFO") -> None:
        text = f"[{level}] {title}: {body}"
        getattr(self.logger, level.lower(), self.logger.info)(text)


@dataclass
class BarkNotifier:
    base_url: str
    group: str | None = None
    device_key: str | None = None
    logger: logging.Logger | None = None

    def notify(self, title: str, body: str, *, level: str = "INFO") -> None:
        payload = {
            "title": title,
            "body": body,
            "group": self.group or "macro-signals",
            "level": level.lower(),
        }
        url = self._build_url(payload)
        req = urllib.request.Request(url=url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                _ = resp.read()
            if self.logger:
                self.logger.info("Bark 通知已发送: %s", title)
        except Exception as exc:  # pragma: no cover - network failures are acceptable in tests
            if self.logger:
                self.logger.warning("Bark 通知失败: %s", exc)

    def _build_url(self, payload: dict[str, str]) -> str:
        if self.device_key and "/" not in self.base_url.rstrip("/"):
            base = f"{self.base_url.rstrip('/')}/{self.device_key}"
        else:
            base = self.base_url.rstrip("/")
        title = urllib.parse.quote(payload["title"])
        body = urllib.parse.quote(payload["body"])
        query = urllib.parse.urlencode({k: v for k, v in payload.items() if k not in {"title", "body"} and v})
        return f"{base}/{title}/{body}?{query}" if query else f"{base}/{title}/{body}"


@dataclass
class MultiNotifier:
    notifiers: list[Notifier]

    def notify(self, title: str, body: str, *, level: str = "INFO") -> None:
        for notifier in self.notifiers:
            notifier.notify(title, body, level=level)



def build_notifier(logger: logging.Logger) -> Notifier:
    cfg = AppConfig.load()
    notifiers: list[Notifier] = [ConsoleNotifier(logger=logger)]
    bark_url = cfg.bark_url or os.getenv("BARK_URL", "").strip()
    if bark_url:
        notifiers.append(
            BarkNotifier(
                base_url=bark_url,
                group=cfg.bark_group,
                device_key=cfg.bark_device_key,
                logger=logger,
            )
        )
    return MultiNotifier(notifiers=notifiers)
