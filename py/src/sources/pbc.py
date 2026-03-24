from __future__ import annotations

import re
from typing import Iterable, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from core.models import PolicyRateEvent
from core.utils import now_text, norm_ymd, to_float

from .base import BaseSource

PBC_OMO_LIST_URL = "https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125431/125475/index.html"
PBC_OMO_ARTICLE_HINT = "公开市场业务交易公告"


class PbcSource(BaseSource):
    def fetch_html(self, url: str) -> str:
        return self.http.get_text(url, headers={"User-Agent": "Mozilla/5.0"})

    def extract_links(self, list_url: str, keyword: str = "") -> List[dict]:
        html = self.fetch_html(list_url)
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
    def _normalize_text(text: str) -> str:
        text = text.replace("\u3000", " ").replace("&nbsp;", " ")
        text = text.replace("％", "%").replace("﹪", "%")
        text = text.replace("／", "/").replace("－", "-")
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    @staticmethod
    def _extract_article_text(html: str) -> str:
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

    def _parse_omo(self, text: str, event_date: str, amount: Optional[float], url: str, fetched_at: str, title: str) -> List[PolicyRateEvent]:
        out: List[PolicyRateEvent] = []

        patterns = [
            # narrative sentence: 开展了175亿元7天期逆回购操作... 1.40%
            r"([0-9]+(?:\.[0-9]+)?)\s*亿元\s*(7\s*天|14\s*天|28\s*天)期?逆回购.*?([0-9]+(?:\.[0-9]+)?)\s*[%％]",
            # table/body text: 7 天 1.40% 175 亿元
            r"(7\s*天|14\s*天|28\s*天)\s*.*?([0-9]+(?:\.[0-9]+)?)\s*[%％]\s*.*?([0-9]+(?:\.[0-9]+)?)\s*亿元",
            # looser fallback
            r"(7\s*天|14\s*天|28\s*天).*?([0-9]+(?:\.[0-9]+)?)\s*[%％]",
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
                out.append(PolicyRateEvent(event_date, "OMO", self._clean_term(term), to_float(rate), amt_v or amount, url, fetched_at, title))
            if out:
                return self._dedupe(out)
        return []

    def _parse_mlf(self, text: str, event_date: str, amount: Optional[float], url: str, fetched_at: str, title: str) -> List[PolicyRateEvent]:
        out: List[PolicyRateEvent] = []
        patterns = [
            r"(1\s*年|6\s*个月|3\s*个月).*?([0-9]+(?:\.[0-9]+)?)\s*[%％]",
        ]
        for patt in patterns:
            for term, rate in re.findall(patt, text, flags=re.I):
                out.append(PolicyRateEvent(event_date, "MLF", self._clean_term(term), to_float(rate), amount, url, fetched_at, title))
        return self._dedupe(out)

    def _parse_lpr(self, text: str, event_date: str, url: str, fetched_at: str, title: str) -> List[PolicyRateEvent]:
        out: List[PolicyRateEvent] = []
        patterns = [
            r"(1\s*年期|5\s*年期)\s*LPR\s*(?:为)?\s*([0-9]+(?:\.[0-9]+)?)\s*[%％]",
            r"(1\s*年期|5\s*年期).*?([0-9]+(?:\.[0-9]+)?)\s*[%％]",
        ]
        for patt in patterns:
            for term, rate in re.findall(patt, text, flags=re.I):
                out.append(PolicyRateEvent(event_date, "LPR", self._clean_term(term), to_float(rate), None, url, fetched_at, title))
            if out:
                return self._dedupe(out)
        return []

    def parse_policy_events_from_html(self, html: str, url: str, title: str = "") -> List[PolicyRateEvent]:
        text = self._extract_article_text(html)
        event_type = self._detect_type(text, title, url)
        event_date = norm_ymd(title) or norm_ymd(text) or ""
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

    def fetch_events_from_url(self, url: str, title: str = "") -> List[PolicyRateEvent]:
        html = self.fetch_html(url)
        return self.parse_policy_events_from_html(html, url, title)

    def fetch_recent_omo_events(self, limit: int = 20) -> List[PolicyRateEvent]:
        links = self.extract_links(PBC_OMO_LIST_URL, keyword=PBC_OMO_ARTICLE_HINT)[:limit]
        events: List[PolicyRateEvent] = []
        for item in links:
            events.extend(self.fetch_events_from_url(item["url"], item["title"]))
        return self._dedupe(events)
