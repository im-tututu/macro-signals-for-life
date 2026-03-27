from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class JobScheduleAdvice:
    timezone: str
    window: str
    cron_hint: str
    rationale: str


@dataclass(frozen=True)
class DailyJobSpec:
    job_name: str
    source_type: str
    source_name: str
    target_table: str
    cadence: str
    region: str
    suggested_schedule: JobScheduleAdvice
    notes: tuple[str, ...] = ()


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
        source_name="ChinaMoney",
        target_table="raw_money_market",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        notes=("银行间资金面属于国内日频数据，建议与其他国内源一起晚间统一调度。",),
    ),
    "bond_curve": DailyJobSpec(
        job_name="bond_curve",
        source_type="chinabond_yield_curve",
        source_name="ChinaBond",
        target_table="raw_bond_curve",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        notes=("默认会寻找最近可用交易日；若用于回补历史，可改走 backfill。",),
    ),
    "bond_index": DailyJobSpec(
        job_name="bond_index",
        source_type="chinabond_index_single",
        source_name="ChinaBond Index",
        target_table="raw_bond_index",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        notes=("需要显式提供 index_id，更适合作为按名单批量调度，而不是单条裸 cron。",),
    ),
    "policy_rate": DailyJobSpec(
        job_name="policy_rate",
        source_type="pbc_policy_rate_latest",
        source_name="PBC",
        target_table="raw_policy_rate",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        notes=("适合抓取最新一组政策利率事件。",),
    ),
    "policy_rate_recent": DailyJobSpec(
        job_name="policy_rate_recent",
        source_type="pbc_policy_rate",
        source_name="PBC",
        target_table="raw_policy_rate",
        cadence="backfill_or_daily",
        region="CN",
        suggested_schedule=ANYTIME_SCHEDULE,
        notes=("更偏补采任务，通常不建议与日常 latest 任务同时高频运行。",),
    ),
    "futures": DailyJobSpec(
        job_name="futures",
        source_type="sina_futures",
        source_name="Sina Futures",
        target_table="raw_futures",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        notes=("国债期货建议在晚间统一抓一次；若要提高时效性，可额外加收盘后补抓。",),
    ),
    "etf": DailyJobSpec(
        job_name="etf",
        source_type="jisilu_etf",
        source_name="Jisilu",
        target_table="raw_jisilu_etf",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        notes=(
            "按交易日快照累积。",
            "job 内已做 pre-check：同一 snapshot_date 已抓过则跳过。",
        ),
    ),
    "life_asset": DailyJobSpec(
        job_name="life_asset",
        source_type="life_asset_placeholder",
        source_name="StatsGov/SGE/BOC Placeholder",
        target_table="raw_life_asset",
        cadence="daily",
        region="CN",
        suggested_schedule=CN_NIGHT_SCHEDULE,
        notes=("当前仍以占位链路为主，后续来源补齐后继续沿用国内晚间窗口。",),
    ),
    "overseas_macro": DailyJobSpec(
        job_name="overseas_macro",
        source_type="fred_alpha_vantage",
        source_name="FRED + Alpha Vantage",
        target_table="raw_overseas_macro",
        cadence="daily",
        region="US",
        suggested_schedule=US_MORNING_SCHEDULE,
        notes=("建议在北京时间早晨执行，避开美国日频序列尚未稳定的窗口。",),
    ),
    "trading_days_update": DailyJobSpec(
        job_name="trading_days_update",
        source_type="trading_days_csv_sync",
        source_name="Trading Calendar CSV",
        target_table="runtime/trading_days.csv",
        cadence="manual_or_periodic",
        region="CN",
        suggested_schedule=ANYTIME_SCHEDULE,
        notes=("更适合人工维护或周级同步，不必每天跑。",),
    ),
}


def get_daily_job_spec(job_name: str) -> DailyJobSpec:
    try:
        return DAILY_JOB_REGISTRY[job_name]
    except KeyError as exc:
        raise ValueError(f"unknown daily job: {job_name}") from exc
