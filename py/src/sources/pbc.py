from __future__ import annotations

import re
from dataclasses import asdict
from typing import Iterable, List, Optional
from urllib.parse import urljoin

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - optional dependency guard
    BeautifulSoup = None

from src.core.models import PolicyRateEvent
from src.core.utils import now_text, norm_ymd, to_float

from .base import BaseSource, FetchResult

PBC_OMO_LIST_URL = "https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125431/125475/index.html"
PBC_MLF_LIST_URL = "https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125437/125446/125873/index.html"
PBC_LPR_LIST_URL = "https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125440/3876551/index.html"
PBC_OMO_ARTICLE_HINT = "公开市场业务交易公告"


def _require_bs4() -> None:
    """需要解析 HTML 时再检查可选依赖，避免 import 阶段直接失败。"""

    if BeautifulSoup is None:
        raise RuntimeError("缺少依赖 beautifulsoup4，请先执行 `pip install -r py/requirements.txt`。")


class PbcSource(BaseSource):
    """央行政策利率来源。

    这里聚焦的是“政策利率事件”这一类来源语义，而不是人民银行网站的全部内容。
    OMO、MLF、LPR 虽然入口不同，但最终都汇聚成同一类 `PolicyRateEvent`，
    所以当前放在同一个 source 里是合理的。

    当前 Python 侧先收口已有能力：
    - 公开市场操作 OMO 事件
    - MLF / LPR 的正文解析能力已在文件内，但列表入口后续再补齐

    access kind:
    - `page_html`
    - 维护难点主要在网页结构漂移、正文清洗和正则抽取，而不是接口鉴权。
    """

    def fetch_html(self, url: str) -> str:
        return self.http.get_text(url, headers={"User-Agent": "Mozilla/5.0"})

    def extract_links(self, list_url: str, keyword: str = "") -> List[dict]:
        # PBC 页面结构不稳定，所以既走 DOM 选择器，也保留正则兜底。
        html = self.fetch_html(list_url)
        _require_bs4()
        soup = BeautifulSoup(html, "lxml")
        out: List[dict] = []
        for a in soup.select("a[href]"):
            title = a.get_text(" ", strip=True)
            href = a.get("href", "")
            if not href or not title:
                continue
            if keyword and keyword not in title:
                continue
            out.append({"title": title, "url": urljoin(list_url, href)})
        if not out and keyword:
            patt = r'<a[^>]+href="([^"]+)"[^>]*>([^<]*' + re.escape(keyword) + r'[^<]*)</a>'
            for m in re.finditer(patt, html, flags=re.I):
                href, title = m.group(1), m.group(2).strip()
                out.append({"title": title, "url": urljoin(list_url, href)})
        seen = set()
        unique = []
        for item in out:
            key = (item["title"], item["url"])
            if key in seen:
                continue
            seen.add(key)
            unique.append(item)
        return unique

    @staticmethod
    def _sort_events_desc(events: Iterable[PolicyRateEvent]) -> List[PolicyRateEvent]:
        return sorted(
            events,
            key=lambda item: (item.date or "", item.type or "", item.term or ""),
            reverse=True,
        )

    @staticmethod
    def _normalize_text(text: str) -> str:
        text = text.replace("\u3000", " ").replace("&nbsp;", " ")
        text = text.replace("％", "%").replace("﹪", "%")
        text = text.replace("／", "/").replace("－", "-")
        text = re.sub(r"\s+", " ", text)
        # PBC 表格正文里常把小数拆成 "1. 40 %"，这里先归一回标准写法。
        text = re.sub(r"(\d)\s*\.\s*(\d)", r"\1.\2", text)
        return text.strip()

    @staticmethod
    def _extract_article_text(html: str) -> str:
        _require_bs4()
        soup = BeautifulSoup(html, "lxml")
        selectors = [
            "#zoom",
            ".zoom",
            ".TRS_Editor",
            ".content",
            ".article",
            ".article-content",
            ".mainContent",
            "#articleContent",
            "td.content",
        ]
        for sel in selectors:
            node = soup.select_one(sel)
            if node:
                return PbcSource._normalize_text(node.get_text(" ", strip=True))

        # fallback: keep only the meaningful central region when available
        body = soup.get_text("\n", strip=True)
        body = body.replace("\r", "")
        # Common strong anchors in PBC pages.
        start_patterns = [r"公开市场业务交易公告", r"贷款市场报价利率", r"中期借贷便利"]
        end_patterns = [r"打印本页", r"关闭窗口", r"法律声明", r"中国人民银行办公厅"]
        start = 0
        for sp in start_patterns:
            m = re.search(sp, body)
            if m:
                start = m.start()
                break
        sliced = body[start:]
        for ep in end_patterns:
            m = re.search(ep, sliced)
            if m:
                sliced = sliced[:m.start()]
                break
        return PbcSource._normalize_text(sliced)

    @staticmethod
    def _detect_type(text: str, title: str, url: str = "") -> str:
        title_first = f"{title} {url}"
        if "公开市场业务交易公告" in title_first or "逆回购" in title_first:
            return "OMO"
        if "中期借贷便利" in title_first or re.search(r"\bMLF\b", title_first, flags=re.I):
            return "MLF"
        if "贷款市场报价利率" in title_first or re.search(r"\bLPR\b", title_first, flags=re.I):
            return "LPR"

        merged = f"{title} {text}"
        if "公开市场业务交易公告" in merged or "逆回购" in merged:
            return "OMO"
        if "中期借贷便利" in merged or re.search(r"\bMLF\b", merged, flags=re.I):
            return "MLF"
        if "贷款市场报价利率" in merged or re.search(r"\bLPR\b", merged, flags=re.I):
            return "LPR"
        return "UNKNOWN"

    @staticmethod
    def _extract_amount(text: str) -> Optional[float]:
        # For OMO/MLF announcements the first "xx亿元" in the正文 usually就是操作量。
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*亿元", text)
        return to_float(m.group(1)) if m else None

    @staticmethod
    def _normalize_rate_value(value: Optional[float]) -> Optional[float]:
        """把政策利率统一到百分比口径。

        例如正则偶发地从 `1.40%` 里截出 `40`，这里兜底修正回 `1.40`。
        当前政策利率通常远小于 10%，因此大于 10 的值都按百分位回退两位。
        """

        if value is None:
            return None
        if value > 10:
            return round(value / 100, 4)
        return value

    @staticmethod
    def _clean_term(term: str) -> str:
        term = re.sub(r"\s+", "", term)
        return (
            term.replace("天期", "D")
            .replace("天", "D")
            .replace("个月期", "M")
            .replace("个月", "M")
            .replace("年期", "Y")
            .replace("年", "Y")
        )

    @staticmethod
    def _is_valid_event_date(value: str) -> bool:
        return bool(re.fullmatch(r"20\d{2}-\d{2}-\d{2}", str(value or "").strip()))

    @classmethod
    def _extract_event_date(cls, title: str, text: str, url: str) -> str:
        """优先返回真正可用的业务日期。"""

        for candidate in (norm_ymd(title), norm_ymd(text)):
            if cls._is_valid_event_date(candidate):
                return candidate

        url_match = re.search(r"(20\d{2})(\d{2})(\d{2})", url or "")
        if url_match:
            year, month, day = url_match.groups()
            return f"{year}-{month}-{day}"
        return ""

    def _parse_omo(self, text: str, event_date: str, amount: Optional[float], url: str, fetched_at: str, title: str) -> List[PolicyRateEvent]:
        # OMO 文案既可能是叙述句，也可能是表格转文本，所以这里保留多套模式并行提取。
        out: List[PolicyRateEvent] = []

        patterns = [
            # narrative sentence: 开展了175亿元7天期逆回购操作... 1.40%
            r"([0-9]+(?:\.[0-9]+)?)\s*亿元\s*(7\s*天|14\s*天|28\s*天)期?逆回购.*?(?<![\d.])([0-9]+(?:\.[0-9]+)?)(?![\d.])\s*[%％]",
            # table/body text: 7 天 1.40% 175 亿元
            r"(7\s*天|14\s*天|28\s*天)\s*.*?(?<![\d.])([0-9]+(?:\.[0-9]+)?)(?![\d.])\s*[%％]\s*.*?([0-9]+(?:\.[0-9]+)?)\s*亿元",
            # looser fallback
            r"(7\s*天|14\s*天|28\s*天).*?(?<![\d.])([0-9]+(?:\.[0-9]+)?)(?![\d.])\s*[%％]",
        ]

        for patt in patterns:
            for tup in re.findall(patt, text, flags=re.I):
                if len(tup) == 3 and "天" in tup[1]:
                    amt, term, rate = tup
                    amt_v = to_float(amt)
                elif len(tup) == 3:
                    term, rate, amt = tup
                    amt_v = to_float(amt)
                else:
                    term, rate = tup
                    amt_v = amount
                out.append(
                    PolicyRateEvent(
                        event_date,
                        "OMO",
                        self._clean_term(term),
                        self._normalize_rate_value(to_float(rate)),
                        amt_v or amount,
                        url,
                        fetched_at,
                        title,
                    )
                )
            if out:
                return self._dedupe(out)
        return []

    def _parse_mlf(self, text: str, event_date: str, amount: Optional[float], url: str, fetched_at: str, title: str) -> List[PolicyRateEvent]:
        out: List[PolicyRateEvent] = []
        patterns = [
            r"(1\s*年|6\s*个月|3\s*个月).*?(?<![\d.])([0-9]+(?:\.[0-9]+)?)(?![\d.])\s*[%％]",
        ]
        for patt in patterns:
            for term, rate in re.findall(patt, text, flags=re.I):
                out.append(
                    PolicyRateEvent(
                        event_date,
                        "MLF",
                        self._clean_term(term),
                        self._normalize_rate_value(to_float(rate)),
                        amount,
                        url,
                        fetched_at,
                        title,
                    )
                )
        return self._dedupe(out)

    def _parse_lpr(self, text: str, event_date: str, url: str, fetched_at: str, title: str) -> List[PolicyRateEvent]:
        out: List[PolicyRateEvent] = []
        patterns = [
            r"(1\s*年期|5\s*年期)\s*LPR\s*(?:为)?\s*(?<![\d.])([0-9]+(?:\.[0-9]+)?)(?![\d.])\s*[%％]",
            r"(1\s*年期|5\s*年期).*?(?<![\d.])([0-9]+(?:\.[0-9]+)?)(?![\d.])\s*[%％]",
        ]
        for patt in patterns:
            for term, rate in re.findall(patt, text, flags=re.I):
                out.append(
                    PolicyRateEvent(
                        event_date,
                        "LPR",
                        self._clean_term(term),
                        self._normalize_rate_value(to_float(rate)),
                        None,
                        url,
                        fetched_at,
                        title,
                    )
                )
            if out:
                return self._dedupe(out)
        return []

    def parse_policy_events_from_html(self, html: str, url: str, title: str = "") -> List[PolicyRateEvent]:
        text = self._extract_article_text(html)
        event_type = self._detect_type(text, title, url)
        event_date = self._extract_event_date(title, text, url)
        amount = self._extract_amount(text)
        fetched_at = now_text()

        if event_type == "OMO":
            return self._parse_omo(text, event_date, amount, url, fetched_at, title)
        if event_type == "MLF":
            return self._parse_mlf(text, event_date, amount, url, fetched_at, title)
        if event_type == "LPR":
            return self._parse_lpr(text, event_date, url, fetched_at, title)
        return []

    @staticmethod
    def _dedupe(events: Iterable[PolicyRateEvent]) -> List[PolicyRateEvent]:
        seen = set()
        out = []
        for event in events:
            key = (event.date, event.type, event.term, event.rate, event.amount)
            if key in seen:
                continue
            seen.add(key)
            out.append(event)
        return out

    @classmethod
    def _pick_latest_event_group(cls, events: Iterable[PolicyRateEvent], event_type: str) -> List[PolicyRateEvent]:
        """选出某一类政策事件的“最新一组”。

        口径：
        - 先取该类型里最新的业务日期
        - 再保留这一天的全部事件
        - 例如 LPR 会保留同一公告里的 1Y / 5Y 两条
        - MLF / OMO 若同日有多期限，也一并保留
        """

        filtered = [event for event in cls._sort_events_desc(cls._dedupe(events)) if event.type == event_type and event.date]
        if not filtered:
            return []
        latest_date = filtered[0].date
        latest_group = [event for event in filtered if event.date == latest_date]
        return cls._sort_events_desc(cls._dedupe(latest_group))

    def fetch_events_from_url(self, url: str, title: str = "") -> List[PolicyRateEvent]:
        html = self.fetch_html(url)
        return self.parse_policy_events_from_html(html, url, title)

    def fetch_recent_omo_events(self, limit: int = 20) -> List[PolicyRateEvent]:
        links = self.extract_links(PBC_OMO_LIST_URL, keyword=PBC_OMO_ARTICLE_HINT)[:limit]
        events: List[PolicyRateEvent] = []
        for item in links:
            events.extend(self.fetch_events_from_url(item["url"], item["title"]))
        return self._sort_events_desc(self._dedupe(events))

    def fetch_recent_mlf_events(self, limit: int = 20) -> List[PolicyRateEvent]:
        links = self.extract_links(PBC_MLF_LIST_URL)
        picked = []
        for item in links:
            title = str(item.get("title") or "")
            url = str(item.get("url") or "")
            if "中期借贷便利开展情况" not in title:
                continue
            if "/125873/" not in url or url.endswith("/125873/index.html"):
                continue
            picked.append(item)
            if len(picked) >= limit:
                break

        events: List[PolicyRateEvent] = []
        for item in picked:
            events.extend(self.fetch_events_from_url(item["url"], item["title"]))
        only_mlf = [event for event in events if event.type == "MLF"]
        return self._sort_events_desc(self._dedupe(only_mlf))[:limit]

    def fetch_recent_lpr_events(self, limit: int = 20) -> List[PolicyRateEvent]:
        links = self.extract_links(PBC_LPR_LIST_URL)
        picked = []
        for item in links:
            title = str(item.get("title") or "")
            url = str(item.get("url") or "")
            if "贷款市场报价利率（LPR）公告" not in title and "贷款市场报价利率(LPR)公告" not in title:
                continue
            if "/3876551/" not in url or url.endswith("/3876551/index.html"):
                continue
            picked.append(item)
            if len(picked) >= limit:
                break

        events: List[PolicyRateEvent] = []
        for item in picked:
            events.extend(self.fetch_events_from_url(item["url"], item["title"]))
        only_lpr = [event for event in events if event.type == "LPR"]
        return self._sort_events_desc(self._dedupe(only_lpr))[: limit * 2]

    def fetch_latest_policy_rate_events(self) -> List[PolicyRateEvent]:
        """抓取最新一组政策事件。

        返回口径：
        - OMO: 最新业务日的整组事件
        - MLF: 最新业务日的整组事件
        - LPR: 最新公告中的全部期限事件
        """

        events: List[PolicyRateEvent] = []
        events.extend(self._pick_latest_event_group(self.fetch_recent_omo_events(limit=5), "OMO"))
        events.extend(self._pick_latest_event_group(self.fetch_recent_mlf_events(limit=5), "MLF"))
        events.extend(self._pick_latest_event_group(self.fetch_recent_lpr_events(limit=5), "LPR"))
        return self._sort_events_desc(self._dedupe(events))

    def fetch_recent_policy_rate_events_result(self, limit: int = 20) -> FetchResult[List[PolicyRateEvent]]:
        """统一返回 FetchResult。

        返回近期政策事件列表：
        - OMO
        - MLF
        - LPR
        """

        events = []
        events.extend(self.fetch_recent_omo_events(limit=limit))
        events.extend(self.fetch_recent_mlf_events(limit=limit))
        events.extend(self.fetch_recent_lpr_events(limit=limit))
        payload = self._sort_events_desc(self._dedupe(events))
        self.require_rows(payload, field_name="policy_rate_events")
        return FetchResult(
            payload=payload,
            source_url=PBC_OMO_LIST_URL,
            meta=self.build_fetch_meta(
                provider="PBC",
                biz_date=str(payload[0].date or ""),
                fetched_at=str(payload[0].fetched_at or ""),
                params={"event_scope": "recent_all", "limit_per_type": limit},
                page_info={"event_count": len(payload)},
                raw_sample=[asdict(item) for item in payload[:3]],
                extra={"event_scope": "recent_all"},
            ),
        )

    def fetch_latest_policy_rate_events_result(self) -> FetchResult[List[PolicyRateEvent]]:
        """统一返回最新政策事件结果。"""

        payload = self.fetch_latest_policy_rate_events()
        self.require_rows(payload, field_name="policy_rate_events")
        return FetchResult(
            payload=payload,
            source_url=PBC_OMO_LIST_URL,
            meta=self.build_fetch_meta(
                provider="PBC",
                biz_date=str(payload[0].date or ""),
                fetched_at=str(payload[0].fetched_at or ""),
                params={"event_scope": "latest"},
                page_info={"event_count": len(payload)},
                raw_sample=[asdict(item) for item in payload[:3]],
                extra={"event_scope": "latest"},
            ),
        )
