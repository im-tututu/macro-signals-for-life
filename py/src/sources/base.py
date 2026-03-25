from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from src.core.http import HttpClient

T = TypeVar("T")


@dataclass(slots=True)
class FetchResult(Generic[T]):
    """来源层统一返回结构。

    约定：
    - payload: 抓取并解析后的主对象
    - source_url: 本次抓取对应的来源 URL
    - meta: 供 store 做表结构映射时使用的补充元信息
    """

    payload: T
    source_url: str
    meta: dict[str, Any] = field(default_factory=dict)


class BaseSource:
    """外部来源抓取基类。

    source 层只负责：
    - 发起请求
    - 解析外部响应
    - 产出领域对象 / FetchResult
    """

    def __init__(self, http: HttpClient | None = None) -> None:
        self.http = http or HttpClient()
