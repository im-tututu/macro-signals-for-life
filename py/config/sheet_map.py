from __future__ import annotations

# 按当前 GAS raw sheet 命名来写。
# 目标：
# 1) 先把历史 Google Sheets 原样迁到 SQLite
# 2) 尽量兼容现有宽表 schema
# 3) 后续若要改成长表，再做 0002/0003 migration

SHEET_MAPPINGS = {
    # =========================
    # 原始_收益率曲线
    # GAS 现状：date, curve, Y_*
    # =========================
    "原始_收益率曲线": {
        "target_table": "raw_bond_curve",
        "primary_key": ["date", "curve"],
        "columns": {
            "date": "date",
            "curve": "curve",
            "Y_0": "y_0",
            "Y_0.08": "y_0_08",
            "Y_0.17": "y_0_17",
            "Y_0.25": "y_0_25",
            "Y_0.5": "y_0_5",
            "Y_0.75": "y_0_75",
            "Y_1": "y_1",
            "Y_2": "y_2",
            "Y_3": "y_3",
            "Y_4": "y_4",
            "Y_5": "y_5",
            "Y_6": "y_6",
            "Y_7": "y_7",
            "Y_8": "y_8",
            "Y_9": "y_9",
            "Y_10": "y_10",
            "Y_15": "y_15",
            "Y_20": "y_20",
            "Y_30": "y_30",
            "Y_40": "y_40",
            "Y_50": "y_50",
        },
        "required": ["date", "curve"],
        "date_columns": ["date"],
        "datetime_columns": [],
        "numeric_columns": [
            "y_0", "y_0_08", "y_0_17", "y_0_25", "y_0_5", "y_0_75",
            "y_1", "y_2", "y_3", "y_4", "y_5", "y_6", "y_7", "y_8",
            "y_9", "y_10", "y_15", "y_20", "y_30", "y_40", "y_50",
        ],
        "default_values": {},
    },

    # =========================
    # 原始_债券指数特征
    # 20_raw_bond_index.js
    # =========================
    "原始_债券指数特征": {
        "target_table": "raw_bond_index",
        "primary_key": ["trade_date", "index_code"],
        "columns": {
            "trade_date": "trade_date",
            "index_name": "index_name",
            "index_code": "index_code",
            "provider": "provider",
            "type_lv1": "type_lv1",
            "type_lv2": "type_lv2",
            "type_lv3": "type_lv3",
            "source_url": "source_url",
            "data_date": "data_date",
            "dm": "dm",
            "y": "y",
            "cons_number": "cons_number",
            "d": "d",
            "v": "v",
            "fetch_status": "fetch_status",
            "raw_json": "raw_json",
            "fetched_at": "fetched_at",
            "error": "error",
            # 兼容一些你表里可能混有的中文列
            "指数名称": "index_name",
            "指数": "index_name",
            "指数代码": "index_code",
            "代码": "index_code",
            "指数发行公司": "provider",
            "类型": "type_lv1",
            "类型-二级": "type_lv2",
            "类型-三级": "type_lv3",
            "来源链接": "source_url",
        },
        "required": ["trade_date", "index_code"],
        "date_columns": ["trade_date", "data_date"],
        "datetime_columns": ["fetched_at"],
        "numeric_columns": ["dm", "y", "cons_number", "d", "v"],
        "default_values": {
            "fetch_status": "OK",
        },
    },

    # =========================
    # 原始_政策利率
    # RAW_POLICY_RATE_HEADERS
    # =========================
    "原始_政策利率": {
        "target_table": "raw_policy_rate",
        "primary_key": ["date", "type", "term"],
        "columns": {
            "date": "date",
            "type": "type",
            "term": "term",
            "rate": "rate",
            "amount": "amount",
            "source": "source",
            "fetched_at": "fetched_at",
            "note": "note",
        },
        "required": ["date", "type", "term"],
        "date_columns": ["date"],
        "datetime_columns": ["fetched_at"],
        "numeric_columns": ["rate", "amount"],
        "default_values": {},
    },

    # =========================
    # 原始_资金面
    # MM_HEADERS
    # =========================
    "原始_资金面": {
        "target_table": "raw_money_market",
        "primary_key": ["date"],
        "columns": {
            "date": "date",
            "showDateCN": "show_date_cn",
            "source_url": "source_url",

            "DR001_weightedRate": "dr001_weighted_rate",
            "DR001_latestRate": "dr001_latest_rate",
            "DR001_avgPrd": "dr001_avg_prd",

            "DR007_weightedRate": "dr007_weighted_rate",
            "DR007_latestRate": "dr007_latest_rate",
            "DR007_avgPrd": "dr007_avg_prd",

            "DR014_weightedRate": "dr014_weighted_rate",
            "DR014_latestRate": "dr014_latest_rate",
            "DR014_avgPrd": "dr014_avg_prd",

            "DR021_weightedRate": "dr021_weighted_rate",
            "DR021_latestRate": "dr021_latest_rate",
            "DR021_avgPrd": "dr021_avg_prd",

            "DR1M_weightedRate": "dr1m_weighted_rate",
            "DR1M_latestRate": "dr1m_latest_rate",
            "DR1M_avgPrd": "dr1m_avg_prd",

            "fetched_at": "fetched_at",
        },
        "required": ["date"],
        "date_columns": ["date"],
        "datetime_columns": ["fetched_at"],
        "numeric_columns": [
            "dr001_weighted_rate", "dr001_latest_rate", "dr001_avg_prd",
            "dr007_weighted_rate", "dr007_latest_rate", "dr007_avg_prd",
            "dr014_weighted_rate", "dr014_latest_rate", "dr014_avg_prd",
            "dr021_weighted_rate", "dr021_latest_rate", "dr021_avg_prd",
            "dr1m_weighted_rate", "dr1m_latest_rate", "dr1m_avg_prd",
        ],
        "default_values": {},
    },

    # =========================
    # 原始_国债期货
    # ensureFuturesHeader_()
    # =========================
    "原始_国债期货": {
        "target_table": "raw_futures",
        "primary_key": ["date"],
        "columns": {
            "date": "date",
            "T0_last": "t0_last",
            "TF0_last": "tf0_last",
            "source": "source",
            "fetched_at": "fetched_at",
        },
        "required": ["date"],
        "date_columns": ["date"],
        "datetime_columns": ["fetched_at"],
        "numeric_columns": ["t0_last", "tf0_last"],
        "default_values": {},
    },

    # =========================
    # 原始_海外宏观
    # OVERSEAS_MACRO_HEADERS
    # =========================
    "原始_海外宏观": {
        "target_table": "raw_macro",
        "primary_key": ["date"],
        "columns": {
            "date": "date",
            "fed_upper": "fed_upper",
            "fed_lower": "fed_lower",
            "sofr": "sofr",
            "ust_2y": "ust_2y",
            "ust_10y": "ust_10y",
            "us_real_10y": "us_real_10y",
            "usd_broad": "usd_broad",
            "usd_cny": "usd_cny",
            "gold": "gold",
            "wti": "wti",
            "brent": "brent",
            "copper": "copper",
            "vix": "vix",
            "spx": "spx",
            "nasdaq_100": "nasdaq_100",
            "source": "source",
            "fetched_at": "fetched_at",
        },
        "required": ["date"],
        "date_columns": ["date"],
        "datetime_columns": ["fetched_at"],
        "numeric_columns": [
            "fed_upper", "fed_lower", "sofr", "ust_2y", "ust_10y",
            "us_real_10y", "usd_broad", "usd_cny", "gold", "wti",
            "brent", "copper", "vix", "spx", "nasdaq_100",
        ],
        "default_values": {},
    },

    # =========================
    # 原始_民生与资产价格
    # LIFE_ASSET_HEADERS
    # =========================
    "原始_民生与资产价格": {
        "target_table": "raw_life_asset",
        "primary_key": ["date"],
        "columns": {
            "date": "date",
            "mortgage_rate_est": "mortgage_rate_est",
            "house_price_tier1": "house_price_tier1",
            "house_price_tier2": "house_price_tier2",
            "house_price_nbs_70city": "house_price_nbs_70city",
            "gold_cny": "gold_cny",
            "money_fund_7d": "money_fund_7d",
            "deposit_1y": "deposit_1y",
            "source": "source",
            "fetched_at": "fetched_at",
        },
        "required": ["date"],
        "date_columns": ["date"],
        "datetime_columns": ["fetched_at"],
        "numeric_columns": [
            "mortgage_rate_est",
            "house_price_tier1",
            "house_price_tier2",
            "house_price_nbs_70city",
            "gold_cny",
            "money_fund_7d",
            "deposit_1y",
        ],
        "default_values": {},
    },

    # =========================
    # 原始_指数ETF
    # ETF_INDEX_RAW_HEADERS
    # =========================
    "原始_指数ETF": {
        "target_table": "raw_jisilu_etf",
        "primary_key": ["snapshot_date", "fund_id"],
        "columns": {
            "snapshot_date": "snapshot_date",
            "fetched_at": "fetched_at",
            "fund_id": "fund_id",
            "fund_nm": "fund_nm",
            "index_nm": "index_nm",
            "issuer_nm": "issuer_nm",
            "price": "price",
            "increase_rt": "increase_rt",
            "volume_wan": "volume_wan",
            "amount_yi": "amount_yi",
            "unit_total_yi": "unit_total_yi",
            "discount_rt": "discount_rt",
            "fund_nav": "fund_nav",
            "nav_dt": "nav_dt",
            "estimate_value": "estimate_value",
            "creation_unit": "creation_unit",
            "pe": "pe",
            "pb": "pb",
            "last_time": "last_time",
            "last_est_time": "last_est_time",
            "is_qdii": "is_qdii",
            "is_t0": "is_t0",
            "apply_fee": "apply_fee",
            "redeem_fee": "redeem_fee",
            "records_total": "records_total",
            "source_url": "source_url",
        },
        "required": ["snapshot_date", "fund_id"],
        "date_columns": ["snapshot_date", "nav_dt"],
        "datetime_columns": ["fetched_at"],
        "numeric_columns": [
            "price", "increase_rt", "volume_wan", "amount_yi",
            "unit_total_yi", "discount_rt", "fund_nav", "estimate_value",
            "creation_unit", "pe", "pb", "apply_fee", "redeem_fee",
            "records_total",
        ],
        "default_values": {},
    },
}


HEADER_ALIASES = {
    # 通用
    "日期": "date",
    "数据日期": "date",
    "抓取时间": "fetched_at",
    "来源": "source",
    "来源链接": "source_url",

    # 债券指数特征常见中文列
    "指数": "index_name",
    "指数名称": "index_name",
    "指数代码": "index_code",
    "代码": "index_code",
    "指数发行公司": "provider",
    "类型": "type_lv1",
    "类型-二级": "type_lv2",
    "类型-三级": "type_lv3",

    # ETF 常见兼容
    "基金代码": "fund_id",
    "基金名称": "fund_nm",
    "指数名称": "index_nm",
    "发行人": "issuer_nm",
}


def get_sheet_mapping(sheet_name: str) -> dict:
    return SHEET_MAPPINGS[sheet_name]