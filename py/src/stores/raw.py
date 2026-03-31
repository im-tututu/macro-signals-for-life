from __future__ import annotations

from pathlib import Path

from ._base import BaseSqliteStore, TableSpec


class RawStore(BaseSqliteStore):
    """通用 raw 表 store。

    只负责按给定 TableSpec 落库，不承载数据集专属的 row builder 逻辑。
    """

    def __init__(
        self,
        spec: TableSpec,
        db_path: Path | None = None,
        *,
        auto_init: bool = True,
    ) -> None:
        self.spec = spec
        super().__init__(db_path=db_path, auto_init=auto_init)
