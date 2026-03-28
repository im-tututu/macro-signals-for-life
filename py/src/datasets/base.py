from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


# 这组 Literal 不是为了“类型好看”，而是把调度层真正关心的更新语义显式化：
# 同样叫“更新”，不同数据集可能对应“抓今天”“抓最近可用交易日”“抓一批最新事件”等完全不同的动作。
LatestMode = Literal[
    "today",
    "trading_day",
    "latest_available_with_lookback",
    "latest_event_batch",
    "none",
]

BackfillMode = Literal[
    "date_range",
    "trading_day_range",
    "recent_n",
    "entity_date_matrix",
    "none",
]

UpdateMode = Literal[
    # 同一业务主键存在时覆盖，适合按 date / code 去重后的时序表。
    "upsert",
    # 保留每次快照，适合“某天导出当下全量状态”的快照表。
    "append_snapshot",
    # 先替换某个分区再写入，适合按日/按月重算的分区型结果表。
    "replace_partition",
]


@dataclass(frozen=True)
class DatasetSpec:
    """数据集元信息。

    这层配置的职责是把“数据源的业务特性”翻译成“调度器可执行的策略”。
    上层 job 不需要知道每个源站的细节，只要读取 DatasetSpec，就能决定：
    1. latest 任务该如何取数；
    2. backfill 应按自然日还是交易日展开；
    3. 落库时应该 upsert、追加快照，还是整段替换。
    """

    # 统一的数据集标识，用来串联 CLI、job 调度、注册表查找。
    dataset_id: str
    # 外部来源名；通常对应抓取模块、供应商或接口族。
    source_name: str
    # 原始/目标落表名；调度层最后要把数据写到这里。
    table_name: str
    # 用于判定“同一条记录”的业务主键集合。
    key_fields: tuple[str, ...]
    # 该数据集的主时间字段；没有稳定日期轴时可为 None。
    date_field: str | None
    # 入库策略，决定重复数据如何处理。
    update_mode: UpdateMode
    # 是否支持 latest 增量更新。
    supports_latest: bool
    # latest 更新语义，告诉执行器“最新”到底怎么定义。
    latest_mode: LatestMode
    # 是否支持历史回填。
    supports_backfill: bool
    # 回填语义，告诉执行器应该怎么展开历史任务。
    backfill_mode: BackfillMode
    # 金融市场类数据经常受节假日/停牌影响，不能把自然日当成有效观测日。
    trading_day_sensitive: bool = False
    # 即使用户给的是自然日窗口，也优先转成交易日窗口，避免无效抓取。
    prefer_trading_day_window: bool = False
    # 留给维护者的业务背景备注，帮助解释“为什么这样配”。
    notes: tuple[str, ...] = ()
