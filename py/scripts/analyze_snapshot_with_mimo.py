# py/scripts/analyze_snapshot_with_mimo.py
import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

import requests

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.core.config import AppConfig, load_local_env

load_local_env()
APP_CONFIG = AppConfig.load()

DB = os.getenv("DB_PATH", str(APP_CONFIG.db_path))
AS_OF_DATE = os.getenv("AS_OF_DATE", "").strip()
MAX_METRICS = int(os.getenv("ANALYZE_MAX_METRICS", "120"))
TEMPERATURE = float(os.getenv("ANALYZE_TEMPERATURE", "0.2"))
TIMEOUT_SECONDS = int(os.getenv("ANALYZE_TIMEOUT_SECONDS", "60"))


@dataclass(frozen=True)
class LlmConfig:
    provider: str
    api_key: str
    base_url: str
    model: str


def _clean_base_url(url: str) -> str:
    return url.strip().rstrip("/")


def _resolve_llm_config() -> LlmConfig:
    provider = os.getenv("LLM_PROVIDER", "mimo").strip().lower()

    if provider == "deepseek":
        api_key = os.getenv("LLM_API_KEY", "").strip() or os.getenv("DEEPSEEK_API_KEY", "").strip()
        base_url = _clean_base_url(os.getenv("LLM_BASE_URL", "").strip() or os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"))
        model = os.getenv("LLM_MODEL", "").strip() or os.getenv("DEEPSEEK_MODEL", "deepseek-chat").strip()
    else:
        # 默认走 MiMo，也兼容以后自定义 OpenAI 兼容网关
        api_key = os.getenv("LLM_API_KEY", "").strip() or os.getenv("MIMO_API_KEY", "").strip()
        base_url = _clean_base_url(os.getenv("LLM_BASE_URL", "").strip() or os.getenv("MIMO_BASE_URL", "https://api.xiaomimimo.com/v1"))
        model = os.getenv("LLM_MODEL", "").strip() or os.getenv("MIMO_MODEL", "mimo-v2-pro").strip()

    if not api_key:
        raise RuntimeError(f"缺少 API key：provider={provider}，请在 .env 中配置 LLM_API_KEY 或 provider 专属 key。")
    if not base_url:
        raise RuntimeError("缺少 base_url，请在 .env 中配置 LLM_BASE_URL。")
    if not model:
        raise RuntimeError("缺少 model，请在 .env 中配置 LLM_MODEL。")
    return LlmConfig(provider=provider, api_key=api_key, base_url=base_url, model=model)


cfg = _resolve_llm_config()

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
if not AS_OF_DATE:
    latest_row = conn.execute(
        """
        SELECT as_of_date
        FROM metric_snapshot
        WHERE as_of_date IS NOT NULL
          AND as_of_date <> ''
        ORDER BY as_of_date DESC
        LIMIT 1
        """
    ).fetchone()
    if latest_row is None or not latest_row["as_of_date"]:
        raise RuntimeError("缺少 AS_OF_DATE，且 metric_snapshot 表中没有可用快照日期。")
    AS_OF_DATE = str(latest_row["as_of_date"])
rows = conn.execute("""
SELECT s.code, r.name_cn, r.category, r.importance, s.latest_value, s.change_5d, s.change_20d,
       s.percentile_250, s.zscore_1y, s.latest_date
FROM metric_snapshot s
JOIN metric_registry r ON r.code = s.code
WHERE s.as_of_date = ?
ORDER BY r.importance DESC, s.code ASC
""", (AS_OF_DATE,)).fetchall()
conn.close()

data = [dict(r) for r in rows]
prompt = f"""请基于以下指标快照做宏观解读，纯粹基于数字解读，不要联网搜索。
你是量化与大类资产配置分析师，必须先识别异常与不常规读数，再给配置建议。

as_of_date={AS_OF_DATE}
data={json.dumps(data[:MAX_METRICS], ensure_ascii=False)}

请按以下结构输出（中文）：
1) 总体宏观判断（未来3-6个月）
- 给出“增长/通胀/流动性/风险偏好”四象限判断（偏强/中性/偏弱）
- 给出核心结论与关键驱动（3-5条）

2) 异常与重点指标清单（必须单独分析）
- 先筛出“值得关注或不太常规”的指标（建议10-20个）：
  - 优先条件：|zscore_1y|>=1、percentile_250>=0.9 或 <=0.1、20日变化绝对值显著、与同类分化明显
- 对每个入选指标给出：
  - 当前读数与历史位置（分位/zscore）
  - 这意味着什么（宏观含义）
  - 对未来3-6个月资产价格的潜在影响方向

3) 资产配置建议（给明确比例，合计100%）
- 仅基于本次数字，不要泛泛表述，给“基准情景”配置，并附“偏风险/偏防守”微调建议
- 债券部分必须细化：
  - 按久期：短久期(0-3Y)、中久期(3-7Y)、长久期(7Y+)
  - 按工具：债券指数基金（考虑约20bp及以上管理费） vs 国债现券（无管理费）
  - 给出两者取舍逻辑（流动性/费率/交易便利/跟踪误差）
- 股票部分必须细化：
  - 大盘价值、红利低波、质量成长、宽基增强（分别给比例与触发条件）
- 商品部分必须细化：
  - 黄金ETF、能源类ETF、工业金属ETF（分别给比例与用途）
- 现金与货基：
  - 明确是否需要提高现金类资产比例，并给出建议区间

4) 风险与反例
- 给出至少3条“可能使上述判断失效”的风险情景
- 每条风险给一个可跟踪的高频指标作为验证锚

5) 最终给出一版可执行组合（百分比表）
- 输出表头：资产大类 | 子类 | 比例 | 调整触发条件
- 比例保留到整数或0.5%，总和=100%
"""

payload = {
    "model": cfg.model,
    "messages": [{"role": "user", "content": prompt}],
    "temperature": TEMPERATURE,
}
payload_chars = len(json.dumps(payload, ensure_ascii=False))
print(
    json.dumps(
        {
            "provider": cfg.provider,
            "base_url": cfg.base_url,
            "model": cfg.model,
            "as_of_date": AS_OF_DATE,
            "rows_loaded": len(data),
            "rows_sent": min(len(data), MAX_METRICS),
            "request_body_chars": payload_chars,
        },
        ensure_ascii=False,
    )
)

resp = requests.post(
    f"{cfg.base_url}/chat/completions",
    headers={"Authorization": f"Bearer {cfg.api_key}", "Content-Type": "application/json"},
    json=payload,
    timeout=TIMEOUT_SECONDS,
)
resp.raise_for_status()
print(resp.json()["choices"][0]["message"]["content"])
