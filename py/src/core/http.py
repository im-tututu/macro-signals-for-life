from __future__ import annotations

import time
from dataclasses import dataclass
import json
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from typing import Any, Dict, Optional

try:
    import requests
except Exception:  # noqa: BLE001
    requests = None

from .config import settings


@dataclass
class _FallbackResponse:
    body: bytes
    status_code: int
    headers: dict[str, str]
    encoding: str | None = None

    @property
    def apparent_encoding(self) -> str:
        return self.encoding or "utf-8"

    @property
    def text(self) -> str:
        return self.body.decode(self.encoding or "utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.text)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FallbackSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}

    def request(self, method: str, url: str, timeout: float, **kwargs: Any) -> _FallbackResponse:
        params = kwargs.pop("params", None)
        data = kwargs.pop("data", None)
        headers = dict(self.headers)
        headers.update(kwargs.pop("headers", {}) or {})
        if params:
            query = urlencode(params, doseq=True)
            url = f"{url}{'&' if '?' in url else '?'}{query}"
        payload: bytes | None = None
        if data is not None:
            if isinstance(data, dict):
                payload = urlencode(data, doseq=True).encode("utf-8")
                headers.setdefault("Content-Type", "application/x-www-form-urlencoded")
            elif isinstance(data, bytes):
                payload = data
            else:
                payload = str(data).encode("utf-8")
        req = Request(url=url, data=payload, headers=headers, method=method.upper())
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            resp_headers = {key: value for key, value in resp.headers.items()}
            content_type = resp_headers.get("Content-Type", "")
            encoding = "utf-8"
            marker = "charset="
            if marker in content_type:
                encoding = content_type.split(marker, 1)[1].split(";", 1)[0].strip()
            return _FallbackResponse(body=body, status_code=getattr(resp, "status", 200), headers=resp_headers, encoding=encoding)


class HttpClient:
    def __init__(self, timeout: Optional[float] = None, default_headers: Optional[Dict[str, str]] = None) -> None:
        self.timeout = timeout or settings.http_timeout_seconds
        self.session = requests.Session() if requests is not None else _FallbackSession()
        self.session.headers.update(
            {
                "User-Agent": default_headers.get("User-Agent") if default_headers else "Mozilla/5.0",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            }
        )
        if default_headers:
            self.session.headers.update(default_headers)

    def request(self, method: str, url: str, *, retries: Optional[int] = None, retry_sleep: Optional[float] = None, **kwargs: Any) -> Any:
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
