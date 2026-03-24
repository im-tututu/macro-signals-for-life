from __future__ import annotations

import time
from typing import Any, Dict, Optional

import requests

from .config import settings


class HttpClient:
    def __init__(self, timeout: Optional[float] = None, default_headers: Optional[Dict[str, str]] = None) -> None:
        self.timeout = timeout or settings.http_timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": default_headers.get("User-Agent") if default_headers else "Mozilla/5.0",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            }
        )
        if default_headers:
            self.session.headers.update(default_headers)

    def request(self, method: str, url: str, *, retries: Optional[int] = None, retry_sleep: Optional[float] = None, **kwargs: Any) -> requests.Response:
        retries = settings.http_retry_count if retries is None else retries
        retry_sleep = settings.http_retry_sleep_seconds if retry_sleep is None else retry_sleep
        last_exc: Exception | None = None
        for attempt in range(retries + 1):
            try:
                resp = self.session.request(method=method, url=url, timeout=self.timeout, **kwargs)
                resp.raise_for_status()
                return resp
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt >= retries:
                    raise
                time.sleep(retry_sleep)
        raise RuntimeError(f"HTTP request failed without exception: {url}") from last_exc

    def get_text(self, url: str, **kwargs: Any) -> str:
        resp = self.request("GET", url, **kwargs)
        if not resp.encoding or resp.encoding.lower() in {"iso-8859-1", "ascii"}:
            resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text

    def get_json(self, url: str, **kwargs: Any) -> Any:
        return self.request("GET", url, **kwargs).json()

    def post_text(self, url: str, **kwargs: Any) -> str:
        resp = self.request("POST", url, **kwargs)
        if not resp.encoding or resp.encoding.lower() in {"iso-8859-1", "ascii"}:
            resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text

    def post_json(self, url: str, **kwargs: Any) -> Any:
        return self.request("POST", url, **kwargs).json()
