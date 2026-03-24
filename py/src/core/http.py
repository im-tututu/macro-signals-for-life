"""HTTP client/session helpers."""

import requests


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "macro-signals-for-life/0.1",
    })
    return session
