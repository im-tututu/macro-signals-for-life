from __future__ import annotations

from core.http import HttpClient


class BaseSource:
    def __init__(self, http: HttpClient | None = None) -> None:
        self.http = http or HttpClient()
