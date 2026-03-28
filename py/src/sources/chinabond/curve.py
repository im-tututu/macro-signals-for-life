from __future__ import annotations

import random
import re
import time
from typing import Dict, Iterable, List, Optional

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - optional dependency guard
    BeautifulSoup = None

from src.core.config import CURVES, TERMS, settings
from src.core.models import CurveBlock, CurveSnapshot, CurveSpec
from src.core.utils import now_text, strip_tags

from ..base import BaseSource, FetchResult

CHINABOND_YC_DETAIL_URL = "https://yield.chinabond.com.cn/cbweb-mn/yc/ycDetail"
CHINABOND_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Referer": "https://yield.chinabond.com.cn/",
    "Origin": "https://yield.chinabond.com.cn",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}


def _require_bs4() -> None:
    """需要解析 HTML 时再检查可选依赖，避免 import 阶段直接失败。"""

    if BeautifulSoup is None:
        raise RuntimeError("缺少依赖 beautifulsoup4，请先执行 `pip install -r py/requirements.txt`。")


class ChinaBondSource(BaseSource):
    """中债收益率曲线来源。

    这里专门承接“中债收益率曲线”这一条来源语义。
    虽然都属于 ChinaBond 站点，但曲线、指数、估值等接口的请求参数、
    响应结构和业务主键差异都很大，所以不应该继续收拢成一个超大 source。

    access kind:
    - `xhr_html`
    - 虽然也是异步请求，但返回的是 HTML 片段，不是干净 JSON；
      维护重点通常在 headers、页面结构和别名匹配，而不是 schema 校验。

    source 层只负责：
    - 调用中债接口
    - 解析 block
    - 产出曲线快照列表
    """

    def fetch_curve_html(self, work_time: str, yc_def_ids: Iterable[str]) -> str:
        # 中债这个接口稳定性一般，且偶发返回过短 HTML，所以这里保留来源级重试，
        # 避免把“站点脆弱性”泄漏到 job 层。
        payload = {
            "ycDefIds": ",".join(yc_def_ids),
            "zblx": "txy",
            "workTime": work_time,
            "dxbj": "0",
            "qxlx": "0",
            "yqqxN": "N",
            "yqqxK": "K",
            "wrjxCBFlag": "0",
            "locale": "zh_CN",
        }
        last_exc: Exception | None = None
        attempts = max(settings.http_retry_count, 2) + 1
        for attempt in range(1, attempts + 1):
            try:
                text = self.http.post_text(
                    CHINABOND_YC_DETAIL_URL,
                    data=payload,
                    headers=CHINABOND_HEADERS,
                    retries=0,
                )
                if not text or len(text) < 500:
                    raise RuntimeError(f"中债响应内容异常，长度过短: {len(text) if text is not None else 0}")
                return text
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt >= attempts:
                    break
                sleep_seconds = min(8.0, 1.2 * attempt) + random.uniform(0.2, 0.8)
                time.sleep(sleep_seconds)
        raise RuntimeError(f"抓取中债曲线失败 date={work_time} err={last_exc}") from last_exc

    @staticmethod
    def build_curve_title_key(text: str) -> str:
        # 页面标题并不是稳定主键，这里做的是“弱规范化”，目标是尽量把页面文案
        # 对齐到内部 curve registry 能识别的别名体系。
        key = strip_tags(text or "")
        replacements = [
            (r"\s+", ""),
            (r"[（(]", ""),
            (r"[）)]", ""),
            (r"＋", "+"),
            (r"中债", ""),
            (r"收益率曲线", ""),
            (r"yield\s*curve", ""),
            (r"curve", ""),
            (r"chinabond", ""),
            (r"governmentbond", "国债"),
            (r"policybank", "国开债"),
            (r"enterprisebond", "企业债"),
            (r"commercialbank", "银行"),
            (r"financialbondof", ""),
            (r"financialbond", "银行债"),
            (r"ordinarybond", "普通债"),
            (r"negotiablecd", "存单"),
            (r"ncd", "存单"),
            (r"localgovernment", "地方债"),
            (r"地方政府债", "地方债"),
            (r"中短期票据", "中票"),
            (r"中期票据", "中票"),
            (r"商业银行普通债", "银行债"),
            (r"商业银行债", "银行债"),
            (r"同业存单", "存单"),
            (r"城投债", "城投"),
            (r"lgfv", "城投"),
            (r"[._-]", ""),
        ]
        for pattern, repl in replacements:
            key = re.sub(pattern, repl, key, flags=re.I)
        return key.lower()

    @staticmethod
    def parse_table_list_to_map(table_html: str) -> Dict[float, float]:
        _require_bs4()
        soup = BeautifulSoup(table_html, "lxml")
        out: Dict[float, float] = {}
        for row in soup.select("tr"):
            cells = [strip_tags(td.get_text(" ", strip=True)) for td in row.select("td")]
            if len(cells) < 2:
                continue
            term_text = cells[0].replace("y", "")
            try:
                term = float(term_text)
                value = float(cells[1])
            except ValueError:
                continue
            out[term] = value
        return out

    def parse_curve_blocks(self, html: str) -> List[CurveBlock]:
        blocks: List[CurveBlock] = []
        pair_re = re.compile(
            r'<table[^>]*class="t1"[\s\S]*?<span>\s*([^<]+?)\s*</span>[\s\S]*?</table>\s*<table[^>]*class="tablelist"[\s\S]*?</table>',
            re.I,
        )
        table_re = re.compile(r'<table[^>]*class="tablelist"[\s\S]*?</table>', re.I)
        for match in pair_re.finditer(html):
            title = strip_tags(match.group(1))
            block_text = match.group(0)
            table_match = table_re.search(block_text)
            if not table_match:
                continue
            points = self.parse_table_list_to_map(table_match.group(0))
            blocks.append(CurveBlock(title=title, title_key=self.build_curve_title_key(title), points=points))
        return blocks

    def resolve_curve_block(
        self,
        curve: CurveSpec,
        blocks: List[CurveBlock],
        used: set[int],
        request_index: int,
    ) -> Optional[CurveBlock]:
        alias_keys = [self.build_curve_title_key(curve.name)] + [self.build_curve_title_key(a) for a in curve.aliases]
        for idx, block in enumerate(blocks):
            if idx in used:
                continue
            for alias_key in alias_keys:
                if not alias_key:
                    continue
                if block.title_key == alias_key or alias_key in block.title_key or block.title_key in alias_key:
                    used.add(idx)
                    return block
        if 0 <= request_index < len(blocks) and request_index not in used:
            used.add(request_index)
            return blocks[request_index]
        return None

    def fetch_curve_separately(self, date: str, curve: CurveSpec) -> Optional[CurveBlock]:
        time.sleep(random.uniform(0.15, 0.35))
        html = self.fetch_curve_html(date, [curve.id])
        blocks = self.parse_curve_blocks(html)
        if not blocks:
            return None
        if len(blocks) == 1:
            return blocks[0]
        return self.resolve_curve_block(curve, blocks, set(), 0) or blocks[0]

    def fetch_daily_wide(self, date: str, curves: Optional[List[CurveSpec]] = None) -> List[CurveSnapshot]:
        curves = curves or CURVES
        # 大多数曲线可以批量抓，少数必须单独抓；这是来源层知识，不应散落到 job 层。
        batch_curves = [c for c in curves if not c.fetch_separately]

        batch_blocks: List[CurveBlock] = []
        if batch_curves:
            html = self.fetch_curve_html(date, [c.id for c in batch_curves])
            batch_blocks = self.parse_curve_blocks(html)

        used: set[int] = set()
        out: List[CurveSnapshot] = []
        for curve in curves:
            if curve.fetch_separately:
                matched = self.fetch_curve_separately(date, curve)
            else:
                request_index = next((i for i, c in enumerate(batch_curves) if c.name == curve.name), -1)
                matched = self.resolve_curve_block(curve, batch_blocks, used, request_index)
            if not matched or not matched.points:
                continue
            out.append(
                CurveSnapshot(
                    date=date,
                    curve_name=curve.name,
                    curve_id=curve.id,
                    tier=curve.tier,
                    source_title=matched.title,
                    points=matched.points,
                )
            )
        return out

    def fetch_daily_wide_result(self, date: str, curves: Optional[List[CurveSpec]] = None) -> FetchResult[List[CurveSnapshot]]:
        """统一返回 FetchResult，便于 job 层复用通用编排逻辑。"""

        payload = self.fetch_daily_wide(date, curves=curves)
        self.require_rows(payload, field_name="curve_snapshots")
        fetched_at = now_text()
        return FetchResult(
            payload=payload,
            source_url=CHINABOND_YC_DETAIL_URL,
            meta=self.build_fetch_meta(
                provider="CHINABOND",
                biz_date=date,
                fetched_at=fetched_at,
                params={
                    "date": date,
                    "curve_count": len(curves) if curves is not None else None,
                },
                page_info={"row_count": len(payload)},
            ),
        )

    @staticmethod
    def flatten_curve(snapshot: CurveSnapshot) -> Dict[str, object]:
        row: Dict[str, object] = {
            "date": snapshot.date,
            "curve": snapshot.curve_name,
            "curve_id": snapshot.curve_id,
            "curve_tier": snapshot.tier,
            "source_title": snapshot.source_title,
        }
        for term in TERMS:
            row[f"Y_{term}"] = snapshot.points.get(term)
        return row
