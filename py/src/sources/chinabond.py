from __future__ import annotations

import re
from typing import Dict, Iterable, List, Optional

from bs4 import BeautifulSoup

from core.config import CURVES, TERMS
from core.models import CurveBlock, CurveSnapshot, CurveSpec
from core.utils import strip_tags

from .base import BaseSource

CHINABOND_YC_DETAIL_URL = "https://yield.chinabond.com.cn/cbweb-mn/yc/ycDetail"


class ChinaBondSource(BaseSource):
    def fetch_curve_html(self, work_time: str, yc_def_ids: Iterable[str]) -> str:
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
        return self.http.post_text(CHINABOND_YC_DETAIL_URL, data=payload, headers={"User-Agent": "Mozilla/5.0"})

    @staticmethod
    def build_curve_title_key(text: str) -> str:
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
        html = self.fetch_curve_html(date, [curve.id])
        blocks = self.parse_curve_blocks(html)
        if not blocks:
            return None
        if len(blocks) == 1:
            return blocks[0]
        return self.resolve_curve_block(curve, blocks, set(), 0) or blocks[0]

    def fetch_daily_wide(self, date: str, curves: Optional[List[CurveSpec]] = None) -> List[CurveSnapshot]:
        curves = curves or CURVES
        batch_curves = [c for c in curves if not c.fetch_separately]
        single_curves = [c for c in curves if c.fetch_separately]

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
