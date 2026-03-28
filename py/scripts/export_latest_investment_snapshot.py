#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Iterable

CURRENT_DIR = Path(__file__).resolve().parent
PY_ROOT = CURRENT_DIR.parent
if str(PY_ROOT) not in sys.path:
    sys.path.insert(0, str(PY_ROOT))

from src.core.config import AppConfig, EXPORT_DIR


def build_parser() -> argparse.ArgumentParser:
    cfg = AppConfig.load()
    parser = argparse.ArgumentParser(description="导出最新交易日投资工具快照。")
    parser.add_argument("--db", dest="db_path", type=Path, default=cfg.db_path, help="SQLite 路径。")
    parser.add_argument(
        "--output",
        type=Path,
        default=EXPORT_DIR / "latest_investment_snapshot.csv",
        help="导出 CSV 路径。",
    )
    return parser


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    db_uri = f"file:{db_path.resolve().as_posix()}?mode=ro"
    conn = sqlite3.connect(db_uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_scalar(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> Any:
    row = conn.execute(sql, tuple(params)).fetchone()
    if row is None:
        return None
    return row[0]


def fetch_rows(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    return conn.execute(sql, tuple(params)).fetchall()


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("%", "").replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def normalize_name(value: str) -> str:
    text = (value or "").strip().lower()
    for token in ("指数", "etf", "lof", "联接", "基金", " ", "-", "_"):
        text = text.replace(token, "")
    return text


def duration_risk_level(duration: float | None) -> str:
    if duration is None:
        return ""
    if duration < 1:
        return "低"
    if duration < 3:
        return "中低"
    if duration < 5:
        return "中"
    if duration < 7:
        return "中高"
    return "高"


def benchmark_fee_pct(category: str) -> float | None:
    # 本地 ETF 原始表目前没有稳定的管理费字段，因此这里采用“类别代表性 ETF 费率基准”。
    mapping = {
        "利率债": 0.15,
        "政金债": 0.15,
        "地方债": 0.20,
        "信用债": 0.30,
        "转债": 0.50,
    }
    return mapping.get(category)


def classify_bond_bucket(name: str) -> str:
    text = name.replace(" ", "")
    if any(token in text for token in ("转债", "可转债")):
        return "转债"
    if any(token in text for token in ("国债", "国开", "政策性金融债", "政金债")):
        if any(token in text for token in ("国开", "政策性金融债", "政金债")):
            return "政金债"
        return "利率债"
    if any(token in text for token in ("地方债", "地债", "地方政府债")):
        return "地方债"
    if any(token in text for token in ("信用债", "公司债", "企业债", "中票", "短融", "城投", "银行债", "科创债")):
        return "信用债"
    return "债券"


def classify_general_asset(name: str) -> tuple[str, str]:
    text = name.replace(" ", "")
    if any(token in text for token in ("黄金", "金", "gold")) and "金融" not in text:
        return ("商品", "黄金")
    if any(token in text.lower() for token in ("wti", "brent", "crude")) or any(token in text for token in ("原油", "石油", "能源")):
        return ("商品", "石油")
    if any(token in text for token in ("豆粕", "大豆", "农产品", "商品")):
        return ("商品", "农产品")
    if any(token in text for token in ("铜", "有色", "工业金属")):
        return ("商品", "工业金属")
    if any(token in text for token in ("国债", "地债", "政金债", "公司债", "信用债", "转债", "可转债", "中票", "短融", "城投", "债")):
        return ("债券", classify_bond_bucket(text))
    return ("股票", "股票指数")


def infer_region(name: str, currency: str, market: str = "") -> str:
    text = name.replace(" ", "")
    if market == "commodity":
        return "全球"
    if any(token in text for token in ("美国", "纳指", "纳斯达克", "标普", "道琼斯", "美股", "REIT")):
        return "美国"
    if any(token in text for token in ("德国", "法国", "欧洲", "欧元区", "英国")):
        return "欧洲"
    if any(token in text for token in ("日本", "日经")):
        return "日本"
    if any(token in text for token in ("印度",)):
        return "印度"
    if any(token in text for token in ("香港", "恒生")):
        return "中国香港"
    if any(token in text for token in ("中国", "中证", "上证", "深证", "沪深", "科创", "创业板", "国证")):
        return "中国"
    if currency in {"USD", "EUR", "JPY", "HKD"}:
        return "海外"
    return "中国"


def risk_return_note(asset_type: str, category: str, duration: float | None, ytm: float | None, currency: str) -> str:
    if asset_type == "债券指数":
        parts: list[str] = [category]
        if duration is not None:
            parts.append(f"久期{duration:.2f}年")
            parts.append(f"久期风险{duration_risk_level(duration)}")
        if ytm is not None:
            parts.append(f"到期收益率{ytm:.2f}%")
        return "，".join(parts)
    if asset_type == "国债现券":
        parts = ["利率债"]
        if duration is not None:
            parts.append(f"久期风险{duration_risk_level(duration)}")
        if ytm is not None:
            parts.append(f"到期收益率{ytm:.2f}%")
        return "，".join(parts)
    if asset_type == "商品":
        return f"{category}，波动通常高于利率债，底层货币{currency or '未识别'}"
    return f"{category}，权益风险主导，底层货币{currency or 'CNY'}"


def build_etf_match_map(conn: sqlite3.Connection, anchor_date: str) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}

    def add_match(index_name: str, fund_id: str) -> None:
        key = normalize_name(index_name)
        if not key:
            return
        mapping.setdefault(key, set()).add(fund_id)

    etf_date = fetch_scalar(conn, "SELECT MAX(snapshot_date) FROM raw_jisilu_etf WHERE snapshot_date <= ?", (anchor_date,))
    if etf_date:
        rows = conn.execute(
            """
            SELECT fund_id, fund_nm, index_nm
            FROM raw_jisilu_etf
            WHERE snapshot_date = ?
            """,
            (etf_date,),
        ).fetchall()
        for row in rows:
            fund_id = str(row["fund_id"] or "")
            add_match(str(row["index_nm"] or ""), fund_id)

    qdii_date = fetch_scalar(conn, "SELECT MAX(snapshot_date) FROM raw_jisilu_qdii WHERE snapshot_date <= ?", (anchor_date,))
    if qdii_date:
        rows = conn.execute(
            """
            SELECT fund_id, display_name, index_nm
            FROM vw_qdii_snapshot_enriched
            WHERE snapshot_date = ?
            """,
            (qdii_date,),
        ).fetchall()
        for row in rows:
            fund_id = str(row["fund_id"] or "")
            add_match(str(row["index_nm"] or ""), fund_id)

    return mapping


def infer_available_etf_count(index_name: str, etf_match_map: dict[str, set[str]]) -> int:
    key = normalize_name(index_name)
    if not key:
        return 0
    return len(etf_match_map.get(key, set()))


def _prefer_larger_scale(left: dict[str, Any] | None, right: dict[str, Any]) -> dict[str, Any]:
    if left is None:
        return right
    left_scale = safe_float(left.get("scale_yi"))
    right_scale = safe_float(right.get("scale_yi"))
    if left_scale is None:
        return right if right_scale is not None else left
    if right_scale is None:
        return left
    return right if right_scale > left_scale else left


def build_representative_product_map(conn: sqlite3.Connection, anchor_date: str) -> dict[str, dict[str, Any]]:
    product_map: dict[str, dict[str, Any]] = {}

    etf_date = fetch_scalar(conn, "SELECT MAX(snapshot_date) FROM raw_jisilu_etf WHERE snapshot_date <= ?", (anchor_date,))
    if etf_date:
        etf_cols = table_columns(conn, "raw_jisilu_etf")
        fee_select = "NULL AS m_fee, NULL AS t_fee, NULL AS mt_fee"
        if {"m_fee", "t_fee", "mt_fee"}.issubset(etf_cols):
            fee_select = "m_fee, t_fee, mt_fee"
        for row in fetch_rows(
            conn,
            f"""
            SELECT snapshot_date, fund_id, fund_nm, index_nm, issuer_nm, unit_total_yi,
                   {fee_select}, apply_fee, redeem_fee, price, discount_rt
            FROM raw_jisilu_etf
            WHERE snapshot_date = ?
            """,
            (etf_date,),
        ):
            fee_pct = safe_float(row["mt_fee"])
            if fee_pct is None:
                m_fee = safe_float(row["m_fee"])
                t_fee = safe_float(row["t_fee"])
                fee_pct = (m_fee or 0.0) + (t_fee or 0.0) if m_fee is not None or t_fee is not None else None
            index_name = str(row["index_nm"] or "").strip()
            if not index_name:
                continue
            product = {
                "product_type": "ETF",
                "fund_id": str(row["fund_id"] or ""),
                "fund_name": str(row["fund_nm"] or ""),
                "issuer_name": str(row["issuer_nm"] or ""),
                "scale_yi": safe_float(row["unit_total_yi"]),
                "fee_pct": fee_pct,
                "apply_fee": safe_float(row["apply_fee"]),
                "redeem_fee": safe_float(row["redeem_fee"]),
                "price_or_nav": safe_float(row["price"]),
                "premium_discount_pct": safe_float(row["discount_rt"]),
            }
            key = normalize_name(index_name)
            product_map[key] = _prefer_larger_scale(product_map.get(key), product)

    qdii_date = fetch_scalar(conn, "SELECT MAX(snapshot_date) FROM raw_jisilu_qdii WHERE snapshot_date <= ?", (anchor_date,))
    if qdii_date:
        for row in fetch_rows(
            conn,
            """
            SELECT q.snapshot_date, q.fund_id,
                   COALESCE(NULLIF(q.fund_nm_display, ''), q.fund_nm) AS display_name,
                   q.index_nm, q.issuer_nm, q.unit_total_yi, q.m_fee, q.t_fee, q.mt_fee, q.price,
                   COALESCE(q.iopv_discount_rt, q.nav_discount_rt, q.discount_rt) AS premium_discount_rt
            FROM raw_jisilu_qdii AS q
            WHERE q.snapshot_date = ?
            """,
            (qdii_date,),
        ):
            index_name = str(row["index_nm"] or "").strip()
            if not index_name:
                continue
            fee_pct = safe_float(row["mt_fee"])
            if fee_pct is None:
                m_fee = safe_float(row["m_fee"])
                t_fee = safe_float(row["t_fee"])
                fee_pct = (m_fee or 0.0) + (t_fee or 0.0) if m_fee is not None or t_fee is not None else None
            product = {
                "product_type": "QDII",
                "fund_id": str(row["fund_id"] or ""),
                "fund_name": str(row["display_name"] or ""),
                "issuer_name": str(row["issuer_nm"] or ""),
                "scale_yi": safe_float(row["unit_total_yi"]),
                "fee_pct": fee_pct,
                "apply_fee": None,
                "redeem_fee": None,
                "price_or_nav": safe_float(row["price"]),
                "premium_discount_pct": safe_float(row["premium_discount_rt"]),
            }
            key = normalize_name(index_name)
            product_map[key] = _prefer_larger_scale(product_map.get(key), product)

    return product_map


def representative_product_for(name: str, product_map: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    key = normalize_name(name)
    if not key:
        return None
    product = product_map.get(key)
    if product is not None:
        return product
    return None


def lookup_bond_index_feature(conn: sqlite3.Connection, anchor_date: str, index_name: str) -> dict[str, Any] | None:
    key = normalize_name(index_name)
    if not key:
        return None
    candidates: list[dict[str, Any]] = []
    for provider, table_name in [
        ("中债", "raw_chinabond_bond_index"),
        ("中证", "raw_csindex_bond_index"),
        ("国证", "raw_cnindex_bond_index"),
    ]:
        latest_date = fetch_scalar(conn, f"SELECT MAX(trade_date) FROM {table_name} WHERE trade_date <= ?", (anchor_date,))
        if not latest_date:
            continue
        rows = fetch_rows(
            conn,
            f"""
            SELECT trade_date, index_name, index_code, dm, y, d, v, total_market_value
            FROM {table_name}
            WHERE trade_date = ?
            """,
            (latest_date,),
        )
        for row in rows:
            candidate_name = str(row["index_name"] or "")
            candidate_key = normalize_name(candidate_name)
            if not candidate_key:
                continue
            score = -1
            if candidate_key == key:
                score = 100
            elif key in candidate_key or candidate_key in key:
                score = min(len(candidate_key), len(key))
            if score < 0:
                continue
            candidates.append(
                {
                    "score": score,
                    "provider": provider,
                    "table_name": table_name,
                    "trade_date": str(row["trade_date"] or ""),
                    "index_name": candidate_name,
                    "index_code": str(row["index_code"] or ""),
                    "duration": safe_float(row["dm"]) or safe_float(row["d"]),
                    "ytm": safe_float(row["y"]),
                }
            )
    if not candidates:
        return None
    candidates.sort(key=lambda item: (int(item["score"]), str(item["trade_date"])), reverse=True)
    return candidates[0]


def make_row(
    *,
    date: str,
    name: str,
    asset_bucket: str,
    asset_type: str,
    provider: str,
    source_table: str,
    code: str = "",
    category_l1: str = "",
    category_l2: str = "",
    invest_region: str = "",
    underlying_currency: str = "",
    duration_years: float | None = None,
    ytm_pct: float | None = None,
    representative_product: dict[str, Any] | None = None,
    representative_fee_pct_override: float | None = None,
    available_etf_count: int | None = None,
    risk_return_note_text: str = "",
) -> dict[str, Any]:
    rep_fee = safe_float(representative_product.get("fee_pct")) if representative_product else None
    if rep_fee is None:
        rep_fee = representative_fee_pct_override
    return {
        "date": date,
        "name": name,
        "asset_bucket": asset_bucket,
        "asset_type": asset_type,
        "provider": provider,
        "source_table": source_table,
        "code": code,
        "category_l1": category_l1,
        "category_l2": category_l2,
        "invest_region": invest_region,
        "underlying_currency": underlying_currency,
        "duration_years": duration_years,
        "duration_risk_level": duration_risk_level(duration_years),
        "ytm_pct": ytm_pct,
        "available_etf_count": available_etf_count,
        "representative_fund_type": "" if not representative_product else str(representative_product.get("product_type") or ""),
        "representative_fund_name": "" if not representative_product else str(representative_product.get("fund_name") or ""),
        "representative_fund_code": "" if not representative_product else str(representative_product.get("fund_id") or ""),
        "representative_fund_issuer": "" if not representative_product else str(representative_product.get("issuer_name") or ""),
        "representative_fund_scale_yi": None if not representative_product else safe_float(representative_product.get("scale_yi")),
        "representative_fee_pct": rep_fee,
        "net_yield_after_fee_pct": (ytm_pct - rep_fee) if ytm_pct is not None and rep_fee is not None else None,
        "apply_fee_pct": None if not representative_product else safe_float(representative_product.get("apply_fee")),
        "redeem_fee_pct": None if not representative_product else safe_float(representative_product.get("redeem_fee")),
        "price_or_nav": None if not representative_product else safe_float(representative_product.get("price_or_nav")),
        "premium_discount_pct": None if not representative_product else safe_float(representative_product.get("premium_discount_pct")),
        "risk_return_note": risk_return_note_text,
    }


def build_bond_index_rows(
    conn: sqlite3.Connection,
    anchor_date: str,
    etf_match_map: dict[str, set[str]],
    product_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    return []


def build_csindex_equity_rows(
    conn: sqlite3.Connection,
    anchor_date: str,
    etf_match_map: dict[str, set[str]],
    product_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    return []


def build_investment_underlying_rows(
    conn: sqlite3.Connection,
    anchor_date: str,
    etf_match_map: dict[str, set[str]],
    product_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    etf_date = fetch_scalar(conn, "SELECT MAX(snapshot_date) FROM raw_jisilu_etf WHERE snapshot_date <= ?", (anchor_date,))
    if etf_date:
        for row in fetch_rows(
            conn,
            """
            SELECT DISTINCT snapshot_date, index_nm
            FROM raw_jisilu_etf
            WHERE snapshot_date = ? AND COALESCE(index_nm, '') <> ''
            ORDER BY index_nm
            """,
            (etf_date,),
        ):
            name = str(row["index_nm"] or "")
            key = f"cn:{normalize_name(name)}"
            if not name or key in seen:
                continue
            seen.add(key)
            asset_family, category = classify_general_asset(name)
            rep = representative_product_for(name, product_map)
            feature = lookup_bond_index_feature(conn, anchor_date, name) if asset_family == "债券" else None
            duration = None if feature is None else safe_float(feature.get("duration"))
            ytm = None if feature is None else safe_float(feature.get("ytm"))
            provider = "集思录" if feature is None else f"集思录+{feature['provider']}"
            source_table = "raw_jisilu_etf" if feature is None else f"raw_jisilu_etf+{feature['table_name']}"
            code = "" if feature is None else str(feature.get("index_code") or "")
            note = risk_return_note("债券指数" if asset_family == "债券" else ("商品" if asset_family == "商品" else "股票指数"), category, duration, ytm, "CNY")
            out.append(
                make_row(
                    date=str(feature["trade_date"] if feature is not None else row["snapshot_date"] or ""),
                    name=name,
                    asset_bucket="投资标的物",
                    asset_type="商品标的" if asset_family == "商品" else ("债券指数" if asset_family == "债券" else "股票标的"),
                    provider=provider,
                    source_table=source_table,
                    code=code,
                    category_l1=asset_family,
                    category_l2=category,
                    invest_region=infer_region(name, "CNY"),
                    underlying_currency="CNY",
                    duration_years=duration,
                    ytm_pct=ytm,
                    representative_product=rep,
                    representative_fee_pct_override=benchmark_fee_pct(category) if asset_family == "债券" else None,
                    available_etf_count=infer_available_etf_count(name, etf_match_map),
                    risk_return_note_text=note,
                )
            )
    qdii_date = fetch_scalar(conn, "SELECT MAX(snapshot_date) FROM raw_jisilu_qdii WHERE snapshot_date <= ?", (anchor_date,))
    if qdii_date:
        for row in fetch_rows(
            conn,
            """
            SELECT DISTINCT snapshot_date, market, index_nm, money_cd
            FROM vw_qdii_snapshot_enriched
            WHERE snapshot_date = ? AND COALESCE(index_nm, '') <> ''
            ORDER BY market, index_nm
            """,
            (qdii_date,),
        ):
            name = str(row["index_nm"] or "")
            key = f"qdii:{normalize_name(name)}"
            if not name or key in seen:
                continue
            seen.add(key)
            asset_family, category = classify_general_asset(name)
            currency = str(row["money_cd"] or "")
            rep = representative_product_for(name, product_map)
            out.append(
                make_row(
                    date=str(row["snapshot_date"] or ""),
                    name=name,
                    asset_bucket="投资标的物",
                    asset_type="商品标的" if asset_family == "商品" else "海外标的",
                    provider="集思录QDII",
                    source_table="vw_qdii_snapshot_enriched",
                    category_l1=asset_family,
                    category_l2=category,
                    invest_region=infer_region(name, currency, str(row["market"] or "")),
                underlying_currency=currency,
                representative_product=rep,
                representative_fee_pct_override=safe_float(rep.get("fee_pct")) if rep else None,
                available_etf_count=infer_available_etf_count(name, etf_match_map),
                risk_return_note_text=risk_return_note("商品" if asset_family == "商品" else "股票指数", category, None, None, currency),
            )
            )
    return out


def build_treasury_rows(conn: sqlite3.Connection, anchor_date: str) -> list[dict[str, Any]]:
    latest_date = fetch_scalar(conn, "SELECT MAX(snapshot_date) FROM raw_jisilu_treasury WHERE snapshot_date <= ?", (anchor_date,))
    if not latest_date:
        return []
    out: list[dict[str, Any]] = []
    rows = conn.execute(
        """
        SELECT snapshot_date, bond_id, bond_nm, price, full_price, years_left, duration, ytm, size_yi
        FROM raw_jisilu_treasury
        WHERE snapshot_date = ?
        ORDER BY duration DESC, ytm DESC, bond_id
        """,
        (latest_date,),
    ).fetchall()
    lively_ids = {
        str(row["bond_id"] or "")
        for row in fetch_rows(
            conn,
            "SELECT bond_id FROM raw_sse_lively_bond WHERE trade_date = (SELECT MAX(trade_date) FROM raw_sse_lively_bond WHERE trade_date <= ?)",
            (anchor_date,),
        )
    }
    for row in rows:
        if str(row["bond_id"] or "") in lively_ids:
            continue
        duration = safe_float(row["duration"]) or safe_float(row["years_left"])
        ytm = safe_float(row["ytm"])
        fee_pct = 0.0
        out.append(
            make_row(
                date=str(row["snapshot_date"] or ""),
                name=str(row["bond_nm"] or ""),
                asset_bucket="国债",
                asset_type="国债现券",
                provider="集思录",
                source_table="raw_jisilu_treasury",
                code=str(row["bond_id"] or ""),
                category_l1="债券",
                category_l2="利率债",
                invest_region="中国",
                underlying_currency="CNY",
                duration_years=duration,
                ytm_pct=ytm,
                representative_product={"fee_pct": fee_pct} if fee_pct is not None else None,
                representative_fee_pct_override=fee_pct,
                risk_return_note_text=risk_return_note("国债现券", "利率债", duration, ytm, "CNY") + f"，费率{fee_pct:.2f}%",
            )
        )
    return out


def build_treasury_merged_rows(conn: sqlite3.Connection, anchor_date: str) -> list[dict[str, Any]]:
    treasury_date = fetch_scalar(conn, "SELECT MAX(snapshot_date) FROM raw_jisilu_treasury WHERE snapshot_date <= ?", (anchor_date,))
    if not treasury_date:
        return []
    lively_date = fetch_scalar(conn, "SELECT MAX(trade_date) FROM raw_sse_lively_bond WHERE trade_date <= ?", (anchor_date,))
    lively_map = {}
    if lively_date:
        lively_map = {
            str(row["bond_id"] or ""): row
            for row in fetch_rows(
                conn,
                """
                SELECT trade_date, bond_id, bond_nm, close_price
                FROM raw_sse_lively_bond
                WHERE trade_date = ?
                """,
                (lively_date,),
            )
        }

    out: list[dict[str, Any]] = []
    rows = fetch_rows(
        conn,
        """
        SELECT snapshot_date, bond_id, bond_nm, price, full_price, years_left, duration, ytm, size_yi
        FROM raw_jisilu_treasury
        WHERE snapshot_date = ?
        ORDER BY duration DESC, ytm DESC, bond_id
        """,
        (treasury_date,),
    )
    for row in rows:
        code = str(row["bond_id"] or "")
        lively = lively_map.get(code)
        duration = safe_float(row["duration"]) or safe_float(row["years_left"])
        ytm = safe_float(row["ytm"])
        fee_pct = 0.0
        is_lively = lively is not None
        note = risk_return_note("国债现券", "利率债", duration, ytm, "CNY")
        if is_lively:
            note += "，上交所活跃券"
        note += f"，费率{fee_pct:.2f}%"
        out.append(
            make_row(
                date=str(row["snapshot_date"] or ""),
                name=str(row["bond_nm"] or (lively["bond_nm"] if lively is not None else "") or ""),
                asset_bucket="国债",
                asset_type="活跃国债" if is_lively else "国债现券",
                provider="上交所+集思录" if is_lively else "集思录",
                source_table="raw_jisilu_treasury+raw_sse_lively_bond" if is_lively else "raw_jisilu_treasury",
                code=code,
                category_l1="债券",
                category_l2="利率债",
                invest_region="中国",
                underlying_currency="CNY",
                duration_years=duration,
                ytm_pct=ytm,
                representative_product={"fee_pct": fee_pct, "price_or_nav": safe_float(row["full_price"]) or safe_float(lively["close_price"]) if lively is not None else safe_float(row["full_price"])},
                representative_fee_pct_override=fee_pct,
                risk_return_note_text=note,
            )
        )
    return out


def build_lively_bond_rows(conn: sqlite3.Connection, anchor_date: str) -> list[dict[str, Any]]:
    latest_date = fetch_scalar(conn, "SELECT MAX(trade_date) FROM raw_sse_lively_bond WHERE trade_date <= ?", (anchor_date,))
    if not latest_date:
        return []
    out: list[dict[str, Any]] = []
    rows = conn.execute(
        """
        SELECT trade_date, bond_id, bond_nm, close_price, ytm
        FROM raw_sse_lively_bond
        WHERE trade_date = ?
        ORDER BY ytm DESC, bond_id
        """,
        (latest_date,),
    ).fetchall()
    treasury_map = {
        str(row["bond_id"]): row
        for row in conn.execute(
            """
            SELECT snapshot_date, bond_id, bond_nm, price, full_price, duration, years_left, ytm, size_yi
            FROM raw_jisilu_treasury
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM raw_jisilu_treasury WHERE snapshot_date <= ?)
            """,
            (anchor_date,),
        ).fetchall()
    }
    for row in rows:
        ref = treasury_map.get(str(row["bond_id"] or ""))
        duration = safe_float(ref["duration"]) if ref is not None else None
        if duration is None and ref is not None:
            duration = safe_float(ref["years_left"])
        ytm = safe_float(ref["ytm"]) if ref is not None else None
        fee_pct = 0.0
        out.append(
            make_row(
                date=str(row["trade_date"] or ""),
                name=str(ref["bond_nm"] if ref is not None and ref["bond_nm"] else row["bond_nm"] or ""),
                asset_bucket="国债",
                asset_type="活跃国债",
                provider="上交所",
                source_table="raw_sse_lively_bond+raw_jisilu_treasury",
                code=str(row["bond_id"] or ""),
                category_l1="债券",
                category_l2="利率债",
                invest_region="中国",
                underlying_currency="CNY",
                duration_years=duration,
                ytm_pct=ytm,
                representative_product={"fee_pct": fee_pct, "price_or_nav": safe_float(ref["full_price"]) if ref is not None else safe_float(row["close_price"])} if fee_pct is not None else None,
                risk_return_note_text=risk_return_note("国债现券", "利率债", duration, ytm, "CNY") + f"，上交所活跃券，费率{fee_pct:.2f}%",
            )
        )
    return out


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "date",
        "name",
        "asset_bucket",
        "asset_type",
        "provider",
        "source_table",
        "code",
        "available_etf_count",
        "representative_fund_type",
        "representative_fund_name",
        "representative_fund_code",
        "representative_fund_issuer",
        "representative_fund_scale_yi",
        "representative_fee_pct",
        "apply_fee_pct",
        "redeem_fee_pct",
        "category_l1",
        "category_l2",
        "invest_region",
        "underlying_currency",
        "duration_years",
        "duration_risk_level",
        "ytm_pct",
        "net_yield_after_fee_pct",
        "price_or_nav",
        "premium_discount_pct",
        "risk_return_note",
    ]
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    args = build_parser().parse_args()
    with connect_readonly(args.db_path) as conn:
        anchor_date = max(
            str(fetch_scalar(conn, "SELECT MAX(snapshot_date) FROM raw_jisilu_etf") or ""),
            str(fetch_scalar(conn, "SELECT MAX(snapshot_date) FROM raw_jisilu_qdii") or ""),
            str(fetch_scalar(conn, "SELECT MAX(snapshot_date) FROM raw_jisilu_treasury") or ""),
            str(fetch_scalar(conn, "SELECT MAX(trade_date) FROM raw_sse_lively_bond") or ""),
            str(fetch_scalar(conn, "SELECT MAX(date) FROM raw_bond_curve") or ""),
        )
        etf_match_map = build_etf_match_map(conn, anchor_date)
        product_map = build_representative_product_map(conn, anchor_date)
        rows = []
        rows.extend(build_bond_index_rows(conn, anchor_date, etf_match_map, product_map))
        rows.extend(build_csindex_equity_rows(conn, anchor_date, etf_match_map, product_map))
        rows.extend(build_investment_underlying_rows(conn, anchor_date, etf_match_map, product_map))
        rows.extend(build_treasury_merged_rows(conn, anchor_date))
    rows.sort(key=lambda item: (str(item["asset_bucket"]), str(item["asset_type"]), str(item["category_l2"]), str(item["name"])))
    write_csv(args.output, rows)
    summary = {
        "anchor_trade_date": anchor_date,
        "output": str(args.output),
        "row_count": len(rows),
        "asset_type_counts": {},
    }
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row["asset_type"])
        counts[key] = counts.get(key, 0) + 1
    summary["asset_type_counts"] = counts
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
