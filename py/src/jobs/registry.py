from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from src.datasets.base import DatasetSpec
from src.datasets.registry import get_dataset_spec


ExecutionKey = Literal[
    "simple_latest",
    "bond_curve",
    "chinabond_bond_index_batch",
    "csindex_bond_index_batch",
    "cnindex_bond_index_batch",
    "policy_rate_recent",
    "etf_snapshot",
    "jisilu_gold_snapshot",
    "jisilu_money_snapshot",
    "qdii_snapshot",
    "treasury_snapshot",
    "sse_lively_bond_snapshot",
    "akshare_bond_gb_us_sina",
    "akshare_bond_zh_us_rate",
    "trading_days_update",
]


@dataclass(frozen=True)
class JobScheduleAdvice:
    timezone: str
    window: str
    cron_hint: str
    rationale: str


@dataclass(frozen=True)
class DailyJobSpec:
    """日常 job 的调度侧元信息。

    它和 DatasetSpec 的分工刻意不同：
    - DatasetSpec 描述数据集本身的更新/回填语义；
    - DailyJobSpec 描述任务入口、调度窗口、区域归属等编排信息。

    当某个 job 直接对应一个 dataset 时，尽量只记录 dataset_id，
    由这里统一回读 source_name / target_table，避免两边重复维护。
    """

    job_name: str
    source_type: str
    execution_key: ExecutionKey
    cadence: str
    region: str
    suggested_schedule: JobScheduleAdvice
    dataset_id: str | None = None
    source_name_override: str | None = None
    target_table_override: str | None = None
    notes: tuple[str, ...] = ()

    @property
    def dataset_spec(self) -> DatasetSpec | None:
        if self.dataset_id is None:
            return None
        return get_dataset_spec(self.dataset_id)

    @property
    def source_name(self) -> str:
        if self.source_name_override:
            return self.source_name_override
        if self.dataset_spec is not None:
            return self.dataset_spec.source_name
        raise ValueError(f"daily job {self.job_name} is missing source_name metadata")

    @property
    def target_table(self) -> str:
        if self.target_table_override:
            return self.target_table_override
        if self.dataset_spec is not None:
            return self.dataset_spec.table_name
        raise ValueError(f"daily job {self.job_name} is missing target_table metadata")


CN_NIGHT_SCHEDULE = JobScheduleAdvice(
    timezone="Asia/Shanghai",
    window="22:10-22:40",
    cron_hint="10 22 * * 1-5",
    rationale="国内收盘后且晚间大部分日频数据已稳定，适合统一做中国本土来源抓取。",
)

US_MORNING_SCHEDULE = JobScheduleAdvice(
    timezone="Asia/Shanghai",
    window="07:05-07:35",
    cron_hint="5 7 * * 2-6",
    rationale="美股与美国宏观日频数据通常在北京时间清晨更稳定，适合统一抓海外来源。",
)

ANYTIME_SCHEDULE = JobScheduleAdvice(
    timezone="Asia/Shanghai",
    window="按需触发",
    cron_hint="manual",
    rationale="通常用于补采、回补、映射刷新或人工触发任务。",
)


DAILY_JOB_REGISTRY: dict[str, DailyJobSpec] = {
    "money_market": DailyJobSpec(
        job_name="money_market",
        source_type="chinamoney_prr_md",
        execution_key="simple_latest",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        dataset_id="money_market",
        notes=("银行间资金面属于国内日频数据，建议与其他国内源一起晚间统一调度。",),
    ),
    "bond_curve": DailyJobSpec(
        job_name="bond_curve",
        source_type="chinabond_yield_curve",
        execution_key="bond_curve",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        dataset_id="chinabond_curve",
        notes=("默认会寻找最近可用交易日；若用于回补历史，可改走 backfill。",),
    ),
    "chinabond_index": DailyJobSpec(
        job_name="chinabond_index",
        source_type="chinabond_index_single",
        execution_key="chinabond_bond_index_batch",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        dataset_id="chinabond_bond_index",
        notes=("默认按配置表里的中债名单批量抓取；也支持显式传 index_id。",),
    ),
    "csindex_bond_index": DailyJobSpec(
        job_name="csindex_bond_index",
        source_type="csindex_bond_feature",
        execution_key="csindex_bond_index_batch",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        dataset_id="csindex_bond_index",
        notes=("默认按配置表里的中证债券指数名单批量抓取；也支持显式传 index_id。",),
    ),
    "cnindex_bond_index": DailyJobSpec(
        job_name="cnindex_bond_index",
        source_type="cnindex_bond_feature",
        execution_key="cnindex_bond_index_batch",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        dataset_id="cnindex_bond_index",
        notes=("默认按配置表里的国证债券指数名单批量抓取；也支持显式传 index_id。",),
    ),
    "policy_rate": DailyJobSpec(
        job_name="policy_rate",
        source_type="pbc_policy_rate_latest",
        execution_key="simple_latest",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        dataset_id="policy_rate",
        notes=("适合抓取最新一组政策利率事件。",),
    ),
    "policy_rate_recent": DailyJobSpec(
        job_name="policy_rate_recent",
        source_type="pbc_policy_rate",
        execution_key="policy_rate_recent",
        cadence="backfill_or_daily",
        region="CN",
        suggested_schedule=ANYTIME_SCHEDULE,
        dataset_id="policy_rate",
        notes=("更偏补采任务，通常不建议与日常 latest 任务同时高频运行。",),
    ),
    "futures": DailyJobSpec(
        job_name="futures",
        source_type="sina_futures",
        execution_key="simple_latest",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        dataset_id="futures",
        notes=("国债期货建议在晚间统一抓一次；若要提高时效性，可额外加收盘后补抓。",),
    ),
    "etf": DailyJobSpec(
        job_name="etf",
        source_type="jisilu_etf",
        execution_key="etf_snapshot",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        dataset_id="etf",
        notes=(
            "按交易日快照累积。",
            "job 内已做 pre-check：同一 snapshot_date 已抓过则跳过。",
        ),
    ),
    "jisilu_gold": DailyJobSpec(
        job_name="jisilu_gold",
        source_type="jisilu_gold",
        execution_key="simple_latest",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        source_name_override="Jisilu Gold",
        target_table_override="raw_jisilu_gold",
        notes=("按交易日抓集思录黄金列表快照。",),
    ),
    "jisilu_money": DailyJobSpec(
        job_name="jisilu_money",
        source_type="jisilu_money",
        execution_key="simple_latest",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        source_name_override="Jisilu Money",
        target_table_override="raw_jisilu_money",
        notes=("按交易日抓集思录货币列表快照。",),
    ),
    "qdii": DailyJobSpec(
        job_name="qdii",
        source_type="jisilu_qdii",
        execution_key="qdii_snapshot",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        dataset_id="qdii",
        notes=(
            "按交易日抓集思录 QDII 分市场快照。",
            "当前默认一次抓欧美、商品、亚洲三个 market 视图。",
        ),
    ),
    "treasury": DailyJobSpec(
        job_name="treasury",
        source_type="jisilu_treasury",
        execution_key="treasury_snapshot",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        dataset_id="treasury",
        notes=(
            "按交易日抓集思录国债现券全量表格快照。",
            "当前默认使用 do_search=false 的全量列表页面。",
        ),
    ),
    "sse_lively_bond": DailyJobSpec(
        job_name="sse_lively_bond",
        source_type="sse_lively_bond",
        execution_key="sse_lively_bond_snapshot",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        dataset_id="sse_lively_bond",
        notes=(
            "按最近交易日抓上交所活跃国债榜单。",
            "榜单数据条数通常不大，更偏监控/校验辅助来源。",
        ),
    ),
    "fred": DailyJobSpec(
        job_name="fred",
        source_type="fred",
        execution_key="simple_latest",
        cadence="daily",
        region="US",
        suggested_schedule=US_MORNING_SCHEDULE,
        dataset_id="fred",
        notes=("建议在北京时间早晨执行，避开美国日频序列尚未稳定的窗口。",),
    ),
    "alpha_vantage": DailyJobSpec(
        job_name="alpha_vantage",
        source_type="alpha_vantage",
        execution_key="simple_latest",
        cadence="daily",
        region="US",
        suggested_schedule=US_MORNING_SCHEDULE,
        dataset_id="alpha_vantage",
        notes=("建议在北京时间早晨执行，避开商品与市场日频序列尚未稳定的窗口。",),
    ),
    "akshare_bond_gb_us_sina": DailyJobSpec(
        job_name="akshare_bond_gb_us_sina",
        source_type="akshare_bond_gb_us_sina",
        execution_key="simple_latest",
        cadence="daily",
        region="US",
        suggested_schedule=US_MORNING_SCHEDULE,
        dataset_id="akshare_bond_gb_us_sina",
        notes=("通过 akshare 抓新浪美国国债收益率历史行情，默认 symbol 为美国10年期国债。",),
    ),
    "akshare_bond_zh_us_rate": DailyJobSpec(
        job_name="akshare_bond_zh_us_rate",
        source_type="akshare_bond_zh_us_rate",
        execution_key="simple_latest",
        cadence="manual_or_periodic",
        region="GLOBAL",
        suggested_schedule=ANYTIME_SCHEDULE,
        dataset_id="akshare_bond_zh_us_rate",
        notes=("通过 akshare 抓中美国债收益率与 GDP 年增率对比序列，使用 start_date 控制历史窗口。",),
    ),
    "trading_days_update": DailyJobSpec(
        job_name="trading_days_update",
        source_type="trading_days_csv_sync",
        execution_key="trading_days_update",
        cadence="manual_or_periodic",
        region="CN",
        suggested_schedule=ANYTIME_SCHEDULE,
        source_name_override="Trading Calendar CSV",
        target_table_override="runtime/trading_days.csv",
        notes=("更适合人工维护或周级同步，不必每天跑。",),
    ),
}


def get_daily_job_spec(job_name: str) -> DailyJobSpec:
    try:
        return DAILY_JOB_REGISTRY[job_name]
    except KeyError as exc:
        raise ValueError(f"unknown daily job: {job_name}") from exc
