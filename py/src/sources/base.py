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

    这层刻意保持得很薄，核心目的是统一“来源层”的职责边界：
    - source 只关心外部站点/API 的访问与解析；
    - store 才关心 SQLite 表结构；
    - job 决定什么时候抓、抓哪些参数、如何写入。

    这样拆开后，同一个来源既可以给 daily job 用，也可以给 smoke/debug
    脚本或未来的 backfill 逻辑复用，而不用把数据库细节带进来。

    读 source 代码时，建议先分清它属于哪种访问机制：
    - `api`: 标准开放 API，参数和返回结构通常最稳定，例如 FRED。
    - `xhr_json`: 网页背后的异步 JSON 接口，常依赖 headers/cookie/referer，
      例如中债指数、集思录 ETF。
    - `xhr_html`: 通过 XHR/POST 拿 HTML 片段，再自行解析，例如中债收益率曲线。
    - `page_html`: 直接抓网页正文/表格再解析，例如 PBC、统计局。
    - `file_download`: 直接下载 CSV/文件再解析，例如 ChinaMoney 历史图表 CSV。

    这个分类不是为了形式统一，而是为了提醒维护者：
    不同 access kind 的脆弱点、重试策略、鉴权方式和排错手段都不一样。

    source 层只负责：
    - 发起请求
    - 解析外部响应
    - 产出领域对象 / FetchResult
    """

    def __init__(self, http: HttpClient | None = None) -> None:
        # 允许注入共享 HttpClient，方便统一 cookie / headers / 重试策略，也便于测试替换。
        self.http = http or HttpClient()
