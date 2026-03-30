BEGIN;

CREATE TABLE IF NOT EXISTS cfg_invest_bucket (
    bucket_id TEXT PRIMARY KEY,
    bucket_name_cn TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    issuer_type TEXT NOT NULL DEFAULT 'na',
    duration_band TEXT NOT NULL DEFAULT 'na',
    region_tag TEXT NOT NULL DEFAULT 'global',
    style_tag TEXT NOT NULL DEFAULT 'na',
    is_active INTEGER NOT NULL DEFAULT 1,
    note TEXT
);

CREATE TABLE IF NOT EXISTS map_invest_exposure_bucket (
    exposure_key TEXT PRIMARY KEY,
    exposure_name TEXT NOT NULL,
    bucket_id TEXT NOT NULL,
    match_method TEXT NOT NULL DEFAULT 'manual',
    match_confidence REAL,
    note TEXT,
    updated_at TEXT,
    FOREIGN KEY (bucket_id) REFERENCES cfg_invest_bucket(bucket_id)
);

INSERT INTO cfg_invest_bucket (
    bucket_id,
    bucket_name_cn,
    asset_class,
    issuer_type,
    duration_band,
    region_tag,
    style_tag,
    is_active,
    note
) VALUES
    ('cash_cn_ultrashort', '现金及超短债', 'cash', 'na', 'ultrashort', 'cn', 'na', 1, NULL),
    ('rates_cn_short', '中国利率债-短久期', 'rates', 'treasury', 'short', 'cn', 'na', 1, NULL),
    ('rates_cn_intermediate', '中国利率债-中久期', 'rates', 'treasury', 'intermediate', 'cn', 'na', 1, NULL),
    ('rates_cn_long', '中国利率债-长久期', 'rates', 'treasury', 'long', 'cn', 'na', 1, NULL),
    ('rates_cn_ultra_long', '中国利率债-超长久期', 'rates', 'treasury', 'ultra_long', 'cn', 'na', 1, NULL),
    ('credit_cn', '中国信用类债券', 'credit', 'corporate', 'na', 'cn', 'na', 1, NULL),
    ('convertibles_cn', '中国可转债', 'convertibles', 'convertible', 'na', 'cn', 'na', 1, NULL),
    ('cmdty_gold', '黄金', 'commodity', 'na', 'na', 'global', 'commodity', 1, NULL),
    ('cmdty_energy', '能源', 'commodity', 'na', 'na', 'global', 'commodity', 1, NULL),
    ('cmdty_industrial_metals', '工业金属', 'commodity', 'na', 'na', 'global', 'commodity', 1, NULL),
    ('cmdty_agriculture', '农产品', 'commodity', 'na', 'na', 'global', 'commodity', 1, NULL),
    ('eq_cn_core', '中国股票-核心', 'equity', 'na', 'na', 'cn', 'broad_market', 1, NULL),
    ('eq_cn_smid_cap', '中国股票-中小盘', 'equity', 'na', 'na', 'cn', 'broad_market', 1, NULL),
    ('eq_cn_dividend', '中国股票-红利', 'equity', 'na', 'na', 'cn', 'dividend', 1, NULL),
    ('eq_cn_thematic', '中国股票-主题', 'equity', 'na', 'na', 'cn', 'growth', 1, NULL),
    ('eq_offshore_china_broad_market', '离岸中国股票-宽基', 'equity', 'na', 'na', 'offshore_china', 'broad_market', 1, NULL),
    ('eq_offshore_china_growth', '离岸中国股票-成长', 'equity', 'na', 'na', 'offshore_china', 'growth', 1, NULL),
    ('eq_us_broad_market', '美国股票-宽基', 'equity', 'na', 'na', 'us', 'broad_market', 1, NULL),
    ('eq_us_growth', '美国股票-成长', 'equity', 'na', 'na', 'us', 'growth', 1, NULL),
    ('eq_dm_ex_us', '发达市场股票（除美国）', 'equity', 'na', 'na', 'dm_ex_us', 'broad_market', 1, NULL),
    ('eq_em', '新兴市场股票', 'equity', 'na', 'na', 'em', 'broad_market', 1, NULL)
ON CONFLICT(bucket_id) DO UPDATE SET
    bucket_name_cn = excluded.bucket_name_cn,
    asset_class = excluded.asset_class,
    issuer_type = excluded.issuer_type,
    duration_band = excluded.duration_band,
    region_tag = excluded.region_tag,
    style_tag = excluded.style_tag,
    is_active = excluded.is_active,
    note = excluded.note;

INSERT INTO map_invest_exposure_bucket (
    exposure_key,
    exposure_name,
    bucket_id,
    match_method,
    match_confidence,
    note,
    updated_at
) VALUES
    ('cn_treasury_bond|short', '中国国债', 'rates_cn_short', 'system_seed', 1.0, '上交所活跃国债现券按久期映射', datetime('now')),
    ('cn_treasury_bond|intermediate', '中国国债', 'rates_cn_intermediate', 'system_seed', 1.0, '上交所活跃国债现券按久期映射', datetime('now')),
    ('cn_treasury_bond|long', '中国国债', 'rates_cn_long', 'system_seed', 1.0, '上交所活跃国债现券按久期映射', datetime('now')),
    ('cn_treasury_bond|ultra_long', '中国国债', 'rates_cn_ultra_long', 'system_seed', 1.0, '上交所活跃国债现券按久期映射', datetime('now')),

    ('沪深300', '沪深300', 'eq_cn_core', 'system_seed', 0.95, '中国股票核心宽基', datetime('now')),
    ('上证50', '上证50', 'eq_cn_core', 'system_seed', 0.95, '中国股票核心宽基', datetime('now')),
    ('中证a50', '中证A50', 'eq_cn_core', 'system_seed', 0.95, '中国股票核心宽基', datetime('now')),
    ('上证180', '上证180', 'eq_cn_core', 'system_seed', 0.90, '中国股票核心宽基', datetime('now')),
    ('中证100', '中证100', 'eq_cn_core', 'system_seed', 0.90, '中国股票核心宽基', datetime('now')),

    ('中证500', '中证500', 'eq_cn_smid_cap', 'system_seed', 0.95, '中国股票中小盘宽基', datetime('now')),
    ('中证1000', '中证1000', 'eq_cn_smid_cap', 'system_seed', 0.95, '中国股票中小盘宽基', datetime('now')),
    ('国证2000', '国证2000', 'eq_cn_smid_cap', 'system_seed', 0.95, '中国股票中小盘宽基', datetime('now')),
    ('中证2000', '中证2000', 'eq_cn_smid_cap', 'system_seed', 0.95, '中国股票中小盘宽基', datetime('now')),

    ('中证红利', '中证红利', 'eq_cn_dividend', 'system_seed', 0.95, '中国股票红利', datetime('now')),
    ('红利低波', '红利低波', 'eq_cn_dividend', 'system_seed', 0.95, '中国股票红利', datetime('now')),
    ('红利价值', '红利价值', 'eq_cn_dividend', 'system_seed', 0.90, '中国股票红利', datetime('now')),

    ('创业板', '创业板', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('创业板指', '创业板指', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('科创50', '科创50', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('科创创业50', '科创创业50', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('中证科技', '中证科技', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票主题', datetime('now')),
    ('中证人工智能', '中证人工智能', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票主题', datetime('now')),
    ('中证新能源', '中证新能源', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票主题', datetime('now')),
    ('中证半导体', '中证半导体', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('国证芯片', '国证芯片', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),

    ('恒生中国企业', '恒生中国企业', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基', datetime('now')),
    ('恒生国企', '恒生国企', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基', datetime('now')),
    ('恒生中国央企', '恒生中国央企', 'eq_offshore_china_broad_market', 'system_seed', 0.85, '离岸中国宽基', datetime('now')),
    ('mscichina', 'MSCI China', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基', datetime('now')),
    ('富时中国50', '富时中国50', 'eq_offshore_china_broad_market', 'system_seed', 0.90, '离岸中国宽基', datetime('now')),

    ('恒生科技', '恒生科技', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('恒生互联网科技业', '恒生互联网科技业', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('中概互联网', '中概互联网', 'eq_offshore_china_growth', 'system_seed', 0.90, '离岸中国成长', datetime('now')),
    ('中国互联网', '中国互联网', 'eq_offshore_china_growth', 'system_seed', 0.90, '离岸中国成长', datetime('now')),

    ('标普500', '标普500', 'eq_us_broad_market', 'system_seed', 0.95, '美国股票宽基', datetime('now')),
    ('标普500500', '标普500', 'eq_us_broad_market', 'system_seed', 0.95, '兼容标准化后重复数字形式', datetime('now')),
    ('道琼斯工业平均', '道琼斯工业平均', 'eq_us_broad_market', 'system_seed', 0.90, '美国股票宽基', datetime('now')),
    ('msciusa', 'MSCI USA', 'eq_us_broad_market', 'system_seed', 0.95, '美国股票宽基', datetime('now')),
    ('罗素3000', '罗素3000', 'eq_us_broad_market', 'system_seed', 0.90, '美国股票宽基', datetime('now')),

    ('纳斯达克100', '纳斯达克100', 'eq_us_growth', 'system_seed', 0.95, '美国股票成长', datetime('now')),
    ('纳指100', '纳指100', 'eq_us_growth', 'system_seed', 0.95, '美国股票成长', datetime('now')),
    ('nasdaq100', 'NASDAQ 100', 'eq_us_growth', 'system_seed', 0.95, '美国股票成长', datetime('now')),
    ('纳斯达克', '纳斯达克', 'eq_us_growth', 'system_seed', 0.85, '美国股票成长', datetime('now')),
    ('美国科技', '美国科技', 'eq_us_growth', 'system_seed', 0.90, '美国股票成长', datetime('now')),

    ('mscieafe', 'MSCI EAFE', 'eq_dm_ex_us', 'system_seed', 0.95, '发达市场除美国', datetime('now')),
    ('msciworldexusa', 'MSCI World ex USA', 'eq_dm_ex_us', 'system_seed', 0.95, '发达市场除美国', datetime('now')),
    ('欧洲', '欧洲', 'eq_dm_ex_us', 'system_seed', 0.75, '发达市场除美国', datetime('now')),
    ('德国dax', '德国DAX', 'eq_dm_ex_us', 'system_seed', 0.80, '发达市场除美国', datetime('now')),
    ('日经225', '日经225', 'eq_dm_ex_us', 'system_seed', 0.90, '发达市场除美国', datetime('now')),

    ('msciem', 'MSCI EM', 'eq_em', 'system_seed', 0.95, '新兴市场股票', datetime('now')),
    ('msciemergingmarkets', 'MSCI Emerging Markets', 'eq_em', 'system_seed', 0.95, '新兴市场股票', datetime('now')),
    ('印度', '印度', 'eq_em', 'system_seed', 0.85, '新兴市场股票', datetime('now')),
    ('东南亚', '东南亚', 'eq_em', 'system_seed', 0.80, '新兴市场股票', datetime('now')),
    ('拉美', '拉美', 'eq_em', 'system_seed', 0.80, '新兴市场股票', datetime('now')),

    ('黄金', '黄金', 'cmdty_gold', 'system_seed', 0.95, '黄金商品', datetime('now')),
    ('gold', 'Gold', 'cmdty_gold', 'system_seed', 0.95, '黄金商品', datetime('now')),
    ('伦敦金', '伦敦金', 'cmdty_gold', 'system_seed', 0.90, '黄金商品', datetime('now')),

    ('原油', '原油', 'cmdty_energy', 'system_seed', 0.95, '能源商品', datetime('now')),
    ('wti', 'WTI', 'cmdty_energy', 'system_seed', 0.95, '能源商品', datetime('now')),
    ('brent', 'Brent', 'cmdty_energy', 'system_seed', 0.95, '能源商品', datetime('now')),
    ('石油', '石油', 'cmdty_energy', 'system_seed', 0.90, '能源商品', datetime('now')),
    ('能源', '能源', 'cmdty_energy', 'system_seed', 0.80, '能源商品', datetime('now')),

    ('工业金属', '工业金属', 'cmdty_industrial_metals', 'system_seed', 0.95, '工业金属商品', datetime('now')),
    ('铜', '铜', 'cmdty_industrial_metals', 'system_seed', 0.90, '工业金属商品', datetime('now')),
    ('有色金属', '有色金属', 'cmdty_industrial_metals', 'system_seed', 0.85, '工业金属商品', datetime('now')),

    ('农产品', '农产品', 'cmdty_agriculture', 'system_seed', 0.95, '农产品商品', datetime('now')),
    ('大豆', '大豆', 'cmdty_agriculture', 'system_seed', 0.90, '农产品商品', datetime('now')),
    ('豆粕', '豆粕', 'cmdty_agriculture', 'system_seed', 0.90, '农产品商品', datetime('now')),

    ('可转债', '可转债', 'convertibles_cn', 'system_seed', 0.95, '中国可转债', datetime('now')),
    ('转债', '转债', 'convertibles_cn', 'system_seed', 0.90, '中国可转债', datetime('now')),
    ('中证转债', '中证转债', 'convertibles_cn', 'system_seed', 0.95, '中国可转债', datetime('now')),

    ('信用债', '信用债', 'credit_cn', 'system_seed', 0.95, '中国信用债', datetime('now')),
    ('公司债', '公司债', 'credit_cn', 'system_seed', 0.90, '中国信用债', datetime('now')),
    ('城投债', '城投债', 'credit_cn', 'system_seed', 0.90, '中国信用债', datetime('now')),
    ('中高等级信用债', '中高等级信用债', 'credit_cn', 'system_seed', 0.90, '中国信用债', datetime('now')),

    ('中证a500', '中证A500', 'eq_cn_core', 'system_seed', 0.95, '中国股票核心宽基', datetime('now')),
    ('中证50', '中证50', 'eq_cn_core', 'system_seed', 0.95, '中国股票核心宽基', datetime('now')),
    ('中国a50互联互通', '中国A50互联互通', 'eq_cn_core', 'system_seed', 0.95, '中国股票核心宽基', datetime('now')),
    ('中证a100', '中证A100', 'eq_cn_core', 'system_seed', 0.90, '中国股票核心宽基', datetime('now')),
    ('上证', '上证指数', 'eq_cn_core', 'system_seed', 0.85, '中国股票核心宽基', datetime('now')),
    ('中证800', '中证800', 'eq_cn_core', 'system_seed', 0.85, '中国股票核心宽基', datetime('now')),

    ('a500红利低波', 'A500红利低波', 'eq_cn_dividend', 'system_seed', 0.90, '中国股票红利', datetime('now')),
    ('中证红利质量', '中证红利质量', 'eq_cn_dividend', 'system_seed', 0.90, '中国股票红利', datetime('now')),
    ('红利低波100', '红利低波100', 'eq_cn_dividend', 'system_seed', 0.90, '中国股票红利', datetime('now')),
    ('中证现金流', '中证现金流', 'eq_cn_dividend', 'system_seed', 0.90, '中国股票红利', datetime('now')),
    ('自由现金流', '自由现金流', 'eq_cn_dividend', 'system_seed', 0.90, '中国股票红利', datetime('now')),
    ('800现金流', '800现金流', 'eq_cn_dividend', 'system_seed', 0.90, '中国股票红利', datetime('now')),
    ('300现金流', '300现金流', 'eq_cn_dividend', 'system_seed', 0.90, '中国股票红利', datetime('now')),

    ('科创综指', '科创综指', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('科创100', '科创100', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('科创200', '科创200', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('科创ai', '科创AI', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('科创创业ai', '科创创业AI', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('科创芯片', '科创芯片', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('科创芯片设计', '科创芯片设计', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('科创半导体材料设备', '科创半导体材料设备', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('科创生物', '科创生物', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('光伏产业', '光伏产业', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('机器人', '机器人', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('机器人产业', '机器人产业', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('创业板50', '创业板50', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('创业板人工智能', '创业板人工智能', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('创业软件', '创业软件', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('创新能源', '创新能源', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('cs电池', 'CS电池', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('新能电池', '新能电池', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('cs新能车', 'CS新能车', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('云计算', '云计算', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('shs云计算', 'SHS云计算', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('芯片产业', '芯片产业', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('半导体材料设备', '半导体材料设备', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('中证半导', '中证半导', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('金融科技', '金融科技', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('cs人工智', 'CS人工智', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('中证军工', '中证军工', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('国证航天', '国证航天', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('卫星产业', '卫星产业', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('通用航空', '通用航空', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('消费电子', '消费电子', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('中证软件', '中证软件', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('软件', '软件指数', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('cs计算机', 'CS计算机', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('中证数据', '中证数据', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('中证传媒', '中证传媒', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票主题', datetime('now')),
    ('中证影视', '中证影视', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票主题', datetime('now')),
    ('动漫游戏', '动漫游戏', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票主题', datetime('now')),
    ('电力', '电力指数', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票主题', datetime('now')),
    ('绿色电力', '绿色电力', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('恒生a股电网设备', '恒生A股电网设备', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票主题', datetime('now')),
    ('seee碳中和', 'SEEE碳中和', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票主题', datetime('now')),
    ('内地低碳', '内地低碳', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票主题', datetime('now')),
    ('央企创新', '央企创新', 'eq_cn_thematic', 'system_seed', 0.80, '中国股票主题', datetime('now')),
    ('细分化工', '细分化工', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票主题', datetime('now')),
    ('中证新能', '中证新能', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('5g通信', '5G通信', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('cs智汽车', 'CS智汽车', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('中证机床', '中证机床', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('医疗器械', '医疗器械', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('cs创新药', 'CS创新药', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('shs创新药', 'SHS创新药', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('中证医疗', '中证医疗', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('中证中药', '中证中药', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票主题', datetime('now')),
    ('中证农业', '中证农业', 'eq_cn_thematic', 'system_seed', 0.75, '中国股票主题', datetime('now')),
    ('中证畜牧', '中证畜牧', 'eq_cn_thematic', 'system_seed', 0.75, '中国股票主题', datetime('now')),
    ('中证消费', '中证消费', 'eq_cn_thematic', 'system_seed', 0.75, '中国股票主题', datetime('now')),
    ('细分食品', '细分食品', 'eq_cn_thematic', 'system_seed', 0.75, '中国股票主题', datetime('now')),
    ('300成长', '300成长', 'eq_cn_thematic', 'system_seed', 0.80, '中国股票主题', datetime('now')),
    ('500质量', '500质量', 'eq_cn_thematic', 'system_seed', 0.80, '中国股票主题', datetime('now')),
    ('a股国际通', 'A股国际通', 'eq_cn_core', 'system_seed', 0.80, '中国股票核心宽基', datetime('now')),

    ('港股通科技', '港股通科技', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('港股通互联网', '港股通互联网', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('国证港股通科技', '国证港股通科技', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('恒生港股通科技', '恒生港股通科技', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('港股通医药c', '港股通医药C', 'eq_offshore_china_growth', 'system_seed', 0.90, '离岸中国成长', datetime('now')),
    ('国证港股通创新药', '国证港股通创新药', 'eq_offshore_china_growth', 'system_seed', 0.90, '离岸中国成长', datetime('now')),
    ('港股通医疗主题', '港股通医疗主题', 'eq_offshore_china_growth', 'system_seed', 0.90, '离岸中国成长', datetime('now')),
    ('中证港股通互联网', '中证港股通互联网', 'eq_offshore_china_growth', 'system_seed', 0.90, '离岸中国成长', datetime('now')),
    ('恒生生物科技', '恒生生物科技', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('恒生医疗保健', '恒生医疗保健', 'eq_offshore_china_growth', 'system_seed', 0.90, '离岸中国成长', datetime('now')),
    ('恒生互联', '恒生互联', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('港股通创新药', '港股通创新药指数', 'eq_offshore_china_growth', 'system_seed', 0.90, '离岸中国成长', datetime('now')),
    ('中证海外中国互联网', '中证海外中国互联网指数', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('中证海外中国互联网30', '中证海外中国互联网30指数', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('shs互联网', 'SHS互联网', 'eq_offshore_china_growth', 'system_seed', 0.85, '离岸中国成长', datetime('now')),

    ('港股通高股息', '港股通高股息', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基/红利', datetime('now')),
    ('恒生', '恒生指数', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基', datetime('now')),
    ('恒指港股通', '恒指港股通', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基', datetime('now')),
    ('港股通50', '港股通50', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基', datetime('now')),
    ('港股通低波红利', '港股通低波红利', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基/红利', datetime('now')),
    ('中证港股通央企红利', '中证港股通央企红利', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基/红利', datetime('now')),
    ('国证港股通消费', '国证港股通消费', 'eq_offshore_china_broad_market', 'system_seed', 0.85, '离岸中国宽基', datetime('now')),
    ('中证港股通消费', '中证港股通消费', 'eq_offshore_china_broad_market', 'system_seed', 0.85, '离岸中国宽基', datetime('now')),
    ('恒生消费', '恒生消费', 'eq_offshore_china_broad_market', 'system_seed', 0.85, '离岸中国宽基', datetime('now')),
    ('恒生港股通高股息低波动', '恒生港股通高股息低波动', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基/红利', datetime('now')),
    ('中证国新港股通央企红利', '中证国新港股通央企红利', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基/红利', datetime('now')),
    ('内地国有', '内地国有', 'eq_offshore_china_broad_market', 'system_seed', 0.80, '离岸中国宽基', datetime('now')),

    ('ssh黄金股票', 'SSH黄金股票', 'cmdty_gold', 'system_seed', 0.85, '黄金股 proxy 先归黄金', datetime('now')),
    ('伦敦金价格', '伦敦金价格', 'cmdty_gold', 'system_seed', 0.95, '黄金商品', datetime('now')),
    ('国证油气', '国证油气', 'cmdty_energy', 'system_seed', 0.95, '能源商品', datetime('now')),
    ('工业有色', '工业有色', 'cmdty_industrial_metals', 'system_seed', 0.95, '工业金属商品', datetime('now')),
    ('有色矿业', '有色矿业', 'cmdty_industrial_metals', 'system_seed', 0.95, '工业金属商品', datetime('now')),
    ('cs稀金属', 'CS稀金属', 'cmdty_industrial_metals', 'system_seed', 0.95, '工业金属商品', datetime('now')),
    ('稀土产业', '稀土产业', 'cmdty_industrial_metals', 'system_seed', 0.90, '工业金属商品', datetime('now')),
    ('中证有色', '中证有色', 'cmdty_industrial_metals', 'system_seed', 0.90, '工业金属商品', datetime('now')),

    ('aaa科创债', 'AAA科创债', 'credit_cn', 'system_seed', 0.95, '中国信用债', datetime('now')),
    ('沪aaa科创债', '沪AAA科创债', 'credit_cn', 'system_seed', 0.95, '中国信用债', datetime('now')),
    ('沪做市信用债', '沪做市信用债', 'credit_cn', 'system_seed', 0.95, '中国信用债', datetime('now')),
    ('深做市信用债', '深做市信用债', 'credit_cn', 'system_seed', 0.95, '中国信用债', datetime('now')),
    ('深aaa科创债', '深AAA科创债', 'credit_cn', 'system_seed', 0.95, '中国信用债', datetime('now')),
    ('上证城投债', '上证城投债指数', 'credit_cn', 'system_seed', 0.95, '中国信用债', datetime('now')),
    ('中债0-3年国开行债券', '中债0-3年国开行债券', 'rates_cn_short', 'system_seed', 0.95, '中国短久期利率债', datetime('now')),
    ('上证5年期国债', '上证5年期国债', 'rates_cn_intermediate', 'system_seed', 0.95, '中国中久期利率债', datetime('now')),
    ('上证10年期国债', '上证10年期国债', 'rates_cn_long', 'system_seed', 0.95, '中国长久期利率债', datetime('now')),
    ('上证5年期地债', '上证5年期地债', 'rates_cn_intermediate', 'system_seed', 0.90, '中国中久期利率债', datetime('now')),
    ('上证10年地债', '上证10年地债', 'rates_cn_long', 'system_seed', 0.90, '中国长久期利率债', datetime('now')),

    ('证券公司', '证券公司', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票行业暴露先归主题动作层', datetime('now')),
    ('中证银行', '中证银行', 'eq_cn_dividend', 'system_seed', 0.90, '中国银行高股息偏红利', datetime('now')),
    ('家用电器', '家用电器', 'eq_cn_thematic', 'system_seed', 0.80, '中国股票行业暴露', datetime('now')),
    ('工程机械主题', '工程机械主题', 'eq_cn_thematic', 'system_seed', 0.80, '中国股票行业暴露', datetime('now')),
    ('房地产', '房地产', 'eq_cn_dividend', 'system_seed', 0.75, '中国股票高股息价值风格', datetime('now')),
    ('新能源', '新能源', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('新能源电池', '新能源电池', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('新能源车', '新能源车', 'eq_cn_thematic', 'system_seed', 0.95, '中国股票主题', datetime('now')),
    ('中证旅游', '中证旅游', 'eq_cn_thematic', 'system_seed', 0.75, '中国股票行业暴露', datetime('now')),
    ('结构调整', '结构调整', 'eq_cn_dividend', 'system_seed', 0.80, '央企结构调整先归红利/价值动作层', datetime('now')),
    ('国企一带一路', '国企一带一路', 'eq_cn_core', 'system_seed', 0.80, '中国股票核心央企主题', datetime('now')),
    ('深证100', '深证100', 'eq_cn_core', 'system_seed', 0.90, '中国股票核心宽基', datetime('now')),
    ('深证成指', '深证成指', 'eq_cn_core', 'system_seed', 0.90, '中国股票核心宽基', datetime('now')),
    ('科创价格', '科创价格', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票主题', datetime('now')),
    ('科创成长', '科创成长', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('科创新能', '科创新能', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('科创新药', '科创新药', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('科技龙头', '科技龙头', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('长江保护', '长江保护', 'eq_cn_thematic', 'system_seed', 0.70, '中国股票主题暴露', datetime('now')),
    ('180金融', '180金融', 'eq_cn_dividend', 'system_seed', 0.75, '中国金融风格偏红利', datetime('now')),
    ('300非银', '300非银', 'eq_cn_thematic', 'system_seed', 0.75, '中国金融行业动作层', datetime('now')),
    ('800银行', '800银行', 'eq_cn_dividend', 'system_seed', 0.85, '中国银行高股息偏红利', datetime('now')),
    ('800非银', '800非银', 'eq_cn_thematic', 'system_seed', 0.75, '中国金融行业动作层', datetime('now')),
    ('上国红利', '上国红利', 'eq_cn_dividend', 'system_seed', 0.90, '中国股票红利', datetime('now')),
    ('上海国企', '上海国企', 'eq_cn_core', 'system_seed', 0.75, '中国股票核心国企主题', datetime('now')),
    ('上证380', '上证380', 'eq_cn_smid_cap', 'system_seed', 0.85, '中国股票中盘宽基', datetime('now')),
    ('上证580', '上证580', 'eq_cn_smid_cap', 'system_seed', 0.80, '中国股票中盘宽基', datetime('now')),
    ('上证中盘', '上证中盘', 'eq_cn_smid_cap', 'system_seed', 0.85, '中国股票中盘宽基', datetime('now')),
    ('180治理', '180治理', 'eq_cn_core', 'system_seed', 0.70, '中国股票核心宽基', datetime('now')),
    ('300质量', '300质量', 'eq_cn_dividend', 'system_seed', 0.75, '中国股票质量/价值动作层', datetime('now')),
    ('300红利lv', '300红利LV', 'eq_cn_dividend', 'system_seed', 0.90, '中国股票红利', datetime('now')),
    ('500现金流', '500现金流', 'eq_cn_dividend', 'system_seed', 0.90, '中国股票红利', datetime('now')),
    ('500信息', '500信息', 'eq_cn_thematic', 'system_seed', 0.75, '中国股票主题', datetime('now')),
    ('800地产', '800地产', 'eq_cn_dividend', 'system_seed', 0.75, '中国股票价值风格', datetime('now')),
    ('800汽车', '800汽车', 'eq_cn_thematic', 'system_seed', 0.80, '中国股票主题', datetime('now')),
    ('cs消费50', 'CS消费50', 'eq_cn_core', 'system_seed', 0.75, '中国股票核心消费宽基', datetime('now')),
    ('cs食品饮', 'CS食品饮', 'eq_cn_thematic', 'system_seed', 0.75, '中国股票行业暴露', datetime('now')),
    ('300医药', '300医药', 'eq_cn_thematic', 'system_seed', 0.80, '中国股票主题', datetime('now')),
    ('cs医药创新', 'CS医药创新', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('cs生医', 'CS生医', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('生物医药', '生物医药', 'eq_cn_thematic', 'system_seed', 0.90, '中国股票主题', datetime('now')),
    ('cs电子', 'CS电子', 'eq_cn_thematic', 'system_seed', 0.85, '中国股票主题', datetime('now')),
    ('石化产业', '石化产业', 'eq_cn_thematic', 'system_seed', 0.75, '中国股票行业暴露', datetime('now')),

    ('中证港股通汽车产业', '中证港股通汽车产业', 'eq_offshore_china_growth', 'system_seed', 0.90, '离岸中国成长', datetime('now')),
    ('恒生港股通中国科技', '恒生港股通中国科技', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('恒生港股通创新药精选', '恒生港股通创新药精选', 'eq_offshore_china_growth', 'system_seed', 0.90, '离岸中国成长', datetime('now')),
    ('恒生港股通新经济', '恒生港股通新经济', 'eq_offshore_china_growth', 'system_seed', 0.90, '离岸中国成长', datetime('now')),
    ('恒生汽车', '恒生汽车', 'eq_offshore_china_growth', 'system_seed', 0.85, '离岸中国成长', datetime('now')),
    ('港股科技', '港股科技', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('香港证券', '香港证券', 'eq_offshore_china_growth', 'system_seed', 0.75, '离岸中国金融行业动作层', datetime('now')),
    ('港股通科技30', '港股通科技30', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),

    ('恒生中国30', '恒生中国30', 'eq_offshore_china_broad_market', 'system_seed', 0.90, '离岸中国宽基', datetime('now')),
    ('恒生港股通50', '恒生港股通50', 'eq_offshore_china_broad_market', 'system_seed', 0.90, '离岸中国宽基', datetime('now')),
    ('恒生港股通央企红利', '恒生港股通央企红利', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基/红利', datetime('now')),
    ('恒生高股息', '恒生高股息', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基/红利', datetime('now')),
    ('h300金融服务', 'H300金融服务', 'eq_offshore_china_broad_market', 'system_seed', 0.75, '离岸中国宽基/金融', datetime('now')),
    ('hk银行', 'HK银行', 'eq_offshore_china_broad_market', 'system_seed', 0.80, '离岸中国宽基/金融', datetime('now')),
    ('hk高股息', 'HK高股息', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基/红利', datetime('now')),

    ('富时沙特阿拉伯', '富时沙特阿拉伯', 'eq_em', 'system_seed', 0.95, '新兴市场股票', datetime('now')),
    ('巴西ibovespa', '巴西IBOVESPA', 'eq_em', 'system_seed', 0.95, '新兴市场股票', datetime('now')),
    ('美国50', '美国50', 'eq_us_broad_market', 'system_seed', 0.90, '美国股票宽基', datetime('now')),
    ('标普生物科技精选行业', '标普生物科技精选行业指数', 'eq_us_growth', 'system_seed', 0.90, '美国股票成长', datetime('now')),

    ('标普石油天然气勘探及生产', '标普石油天然气勘探及生产', 'cmdty_energy', 'system_seed', 0.95, '能源商品', datetime('now')),
    ('油气产业', '油气产业', 'cmdty_energy', 'system_seed', 0.95, '能源商品', datetime('now')),
    ('油气资源', '油气资源', 'cmdty_energy', 'system_seed', 0.95, '能源商品', datetime('now')),
    ('wti原油价格', 'WTI原油价格', 'cmdty_energy', 'system_seed', 0.95, '能源商品', datetime('now')),
    ('60%wti+40%brent原油价格', '60%WTI+40%BRENT原油价格', 'cmdty_energy', 'system_seed', 0.95, '能源商品', datetime('now')),
    ('上期有色金属', '上期有色金属', 'cmdty_industrial_metals', 'system_seed', 0.90, '工业金属商品', datetime('now')),
    ('细分有色', '细分有色', 'cmdty_industrial_metals', 'system_seed', 0.90, '工业金属商品', datetime('now')),
    ('上证商品', '上证商品', 'cmdty_agriculture', 'system_seed', 0.60, '综合商品暂归农产品，后续可再细化', datetime('now')),
    ('50%伦敦金现货+50%全球金矿股', '50%伦敦金现货+50%全球金矿股指数', 'cmdty_gold', 'system_seed', 0.80, '黄金 proxy', datetime('now')),

    ('shs高股息', 'SHS高股息', 'eq_cn_dividend', 'system_seed', 0.85, '中国股票红利', datetime('now')),
    ('东证', '东证指数', 'eq_cn_core', 'system_seed', 0.70, '中国股票核心宽基', datetime('now')),

    ('中华港股通精选100', '中华港股通精选100', 'eq_offshore_china_broad_market', 'system_seed', 0.90, '离岸中国宽基', datetime('now')),
    ('中国互联网50', '中国互联网50', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('中证全球中国互联网', '中证全球中国互联网指数', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('中证全球中国教育主题', '中证全球中国教育主题指数', 'eq_offshore_china_growth', 'system_seed', 0.85, '离岸中国成长', datetime('now')),
    ('中证港股通信息c', '中证港股通信息C', 'eq_offshore_china_growth', 'system_seed', 0.90, '离岸中国成长', datetime('now')),
    ('国证港股通互联网', '国证港股通互联网', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('恒生创新药', '恒生创新药指数', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('恒生医疗', '恒生医疗', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('恒生沪深港创新药精选50', '恒生沪深港创新药精选50', 'eq_offshore_china_growth', 'system_seed', 0.90, '离岸中国成长', datetime('now')),
    ('恒生港股通创新药', '恒生港股通创新药', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('港股创新药', '港股创新药', 'eq_offshore_china_growth', 'system_seed', 0.95, '离岸中国成长', datetime('now')),
    ('港股通', '港股通', 'eq_offshore_china_broad_market', 'system_seed', 0.80, '离岸中国宽基', datetime('now')),
    ('港股通中国100', '港股通中国100', 'eq_offshore_china_broad_market', 'system_seed', 0.90, '离岸中国宽基', datetime('now')),
    ('港股通内地金融', '港股通内地金融', 'eq_offshore_china_broad_market', 'system_seed', 0.80, '离岸中国宽基/金融', datetime('now')),
    ('港股通红利低波', '港股通红利低波', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基/红利', datetime('now')),
    ('港股通非银', '港股通非银', 'eq_offshore_china_growth', 'system_seed', 0.80, '离岸中国金融成长', datetime('now')),
    ('恒生红利', '恒生红利', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基/红利', datetime('now')),
    ('恒生综指', '恒生综指', 'eq_offshore_china_broad_market', 'system_seed', 0.95, '离岸中国宽基', datetime('now')),
    ('恒生综合中型股', '恒生综合中型股', 'eq_offshore_china_broad_market', 'system_seed', 0.85, '离岸中国宽基', datetime('now')),
    ('恒生小型股', '恒生小型股', 'eq_offshore_china_broad_market', 'system_seed', 0.80, '离岸中国宽基', datetime('now')),
    ('香港中小', '香港中小', 'eq_offshore_china_broad_market', 'system_seed', 0.75, '离岸中国宽基', datetime('now')),
    ('标普中国价值', '标普中国价值', 'eq_offshore_china_broad_market', 'system_seed', 0.80, '离岸中国宽基/价值', datetime('now')),
    ('标普中国新经济行业', '标普中国新经济行业指数', 'eq_offshore_china_growth', 'system_seed', 0.85, '离岸中国成长', datetime('now')),

    ('中韩半导体', '中韩半导体', 'eq_dm_ex_us', 'system_seed', 0.75, '发达市场除美/亚洲科技', datetime('now')),
    ('全球芯片', '全球芯片', 'eq_dm_ex_us', 'system_seed', 0.75, '海外科技主题', datetime('now')),
    ('富时亚太低碳精选', '富时亚太低碳精选', 'eq_dm_ex_us', 'system_seed', 0.80, '发达市场除美', datetime('now')),
    ('法国cac40', '法国CAC40', 'eq_dm_ex_us', 'system_seed', 0.95, '发达市场除美', datetime('now')),
    ('海外科技', '海外科技', 'eq_dm_ex_us', 'system_seed', 0.75, '海外科技主题', datetime('now')),

    ('印度etp', '印度ETP指数', 'eq_em', 'system_seed', 0.95, '新兴市场股票', datetime('now')),
    ('新交所新兴亚洲精选50', '新交所新兴亚洲精选50指数', 'eq_em', 'system_seed', 0.95, '新兴市场股票', datetime('now')),
    ('新交所泛东南亚科技', '新交所泛东南亚科技指数', 'eq_em', 'system_seed', 0.90, '新兴市场股票', datetime('now')),

    ('标普500信息科技', '标普500信息科技指数', 'eq_us_growth', 'system_seed', 0.95, '美国股票成长', datetime('now')),
    ('标普500医疗保健等权重', '标普500医疗保健等权重指数', 'eq_us_growth', 'system_seed', 0.90, '美国股票成长', datetime('now')),
    ('标普500消费精选', '标普500消费精选指数', 'eq_us_growth', 'system_seed', 0.85, '美国股票成长', datetime('now')),
    ('标普可选消费品精选版块', '标普可选消费品精选版块指数', 'eq_us_growth', 'system_seed', 0.85, '美国股票成长', datetime('now')),
    ('纳斯达克生物科技', '纳斯达克生物科技', 'eq_us_growth', 'system_seed', 0.95, '美国股票成长', datetime('now')),
    ('纳斯达克科技市值加权', '纳斯达克科技市值加权', 'eq_us_growth', 'system_seed', 0.95, '美国股票成长', datetime('now')),
    ('道琼斯美国精选reit', '道琼斯美国精选REIT指数', 'eq_us_broad_market', 'system_seed', 0.80, '美国股票宽基/REIT', datetime('now')),
    ('美国债券综合', '美国债券综合指数', 'eq_us_broad_market', 'system_seed', 0.60, '暂挂美国宽基，后续可单独扩 bucket', datetime('now')),

    ('中证cme中国商品消费', '中证CME中国商品消费指数', 'cmdty_agriculture', 'system_seed', 0.70, '商品消费篮子暂归农产品', datetime('now')),
    ('国泰大宗商品配置', '国泰大宗商品配置指数', 'cmdty_agriculture', 'system_seed', 0.65, '综合商品暂归农产品', datetime('now')),
    ('标普全球石油', '标普全球石油指数', 'cmdty_energy', 'system_seed', 0.95, '能源商品', datetime('now')),
    ('标普石油天然气上游股票', '标普石油天然气上游股票指数', 'cmdty_energy', 'system_seed', 0.95, '能源商品', datetime('now')),
    ('标普能源行业', '标普能源行业指数', 'cmdty_energy', 'system_seed', 0.95, '能源商品', datetime('now')),
    ('标普高盛原油商品', '标普高盛原油商品指数', 'cmdty_energy', 'system_seed', 0.95, '能源商品', datetime('now')),
    ('道琼斯美国石油开发与生产', '道琼斯美国石油开发与生产指数', 'cmdty_energy', 'system_seed', 0.95, '能源商品', datetime('now')),
    ('标普高盛商品总', '标普高盛商品总指数', 'cmdty_agriculture', 'system_seed', 0.60, '综合商品暂归农产品', datetime('now'))
ON CONFLICT(exposure_key) DO UPDATE SET
    exposure_name = excluded.exposure_name,
    bucket_id = excluded.bucket_id,
    match_method = excluded.match_method,
    match_confidence = excluded.match_confidence,
    note = excluded.note,
    updated_at = excluded.updated_at;

UPDATE map_invest_exposure_bucket
SET bucket_id = 'eq_cn_core'
WHERE bucket_id = 'eq_cn_large_cap';

UPDATE map_invest_exposure_bucket
SET bucket_id = 'eq_cn_thematic'
WHERE bucket_id = 'eq_cn_growth';

DELETE FROM cfg_invest_bucket
WHERE bucket_id IN ('eq_cn_large_cap', 'eq_cn_growth');

DROP VIEW IF EXISTS vw_invest_instrument_latest;
CREATE VIEW vw_invest_instrument_latest AS
WITH latest_etf_date AS (
    SELECT MAX(COALESCE(NULLIF(TRIM(nav_dt), ''), snapshot_date)) AS nav_dt
    FROM raw_jisilu_etf
    WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
),
latest_etf_batch AS (
    SELECT snapshot_date, MAX(fetched_at) AS fetched_at
    FROM raw_jisilu_etf
    WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
      AND COALESCE(NULLIF(TRIM(nav_dt), ''), snapshot_date) = (SELECT nav_dt FROM latest_etf_date)
    GROUP BY snapshot_date
),
latest_qdii_date AS (
    SELECT MAX(COALESCE(NULLIF(TRIM(nav_dt), ''), snapshot_date)) AS nav_dt
    FROM raw_jisilu_qdii
    WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
),
latest_qdii_batch AS (
    SELECT snapshot_date, market, MAX(fetched_at) AS fetched_at
    FROM raw_jisilu_qdii
    WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
      AND COALESCE(NULLIF(TRIM(nav_dt), ''), snapshot_date) = (SELECT nav_dt FROM latest_qdii_date)
    GROUP BY snapshot_date, market
),
latest_lively_bond AS (
    SELECT MAX(trade_date) AS trade_date
    FROM raw_sse_lively_bond
),
latest_treasury_date AS (
    SELECT MAX(snapshot_date) AS snapshot_date
    FROM raw_jisilu_treasury
),
latest_treasury_batch AS (
    SELECT snapshot_date, MAX(fetched_at) AS fetched_at
    FROM raw_jisilu_treasury
    WHERE snapshot_date = (SELECT snapshot_date FROM latest_treasury_date)
    GROUP BY snapshot_date
),
lively_bond_enriched AS (
    SELECT
        'lively_bond_' || s.bond_id AS instrument_id,
        'treasury_bond' AS instrument_type,
        s.bond_id AS instrument_code,
        COALESCE(NULLIF(t.bond_nm, ''), s.bond_nm) AS instrument_name,
        NULL AS issuer_name,
        'raw_sse_lively_bond+raw_jisilu_treasury' AS source_table,
        s.trade_date AS snapshot_date,
        s.trade_date AS nav_dt,
        COALESCE(t.full_price, s.close_price) AS price,
        s.amount_wanyuan / 10000.0 AS amount_yi,
        s.volume_hand AS volume_wan,
        t.size_yi AS unit_total_yi,
        NULL AS premium_discount_rt,
        NULL AS apply_status,
        NULL AS redeem_status,
        COALESCE(t.duration, t.years_left) AS duration,
        COALESCE(t.ytm, s.ytm) AS ytm,
        '中国国债' AS raw_exposure_name
    FROM raw_sse_lively_bond AS s
    LEFT JOIN raw_jisilu_treasury AS t
        ON s.bond_id = t.bond_id
       AND (t.snapshot_date, t.fetched_at) IN (
            SELECT snapshot_date, fetched_at
            FROM latest_treasury_batch
       )
    WHERE s.trade_date = (SELECT trade_date FROM latest_lively_bond)
)
SELECT
    'etf_' || fund_id AS instrument_id,
    CASE
        WHEN COALESCE(is_qdii, '') IN ('1', 'Y', 'y', 'true', 'TRUE') THEN 'qdii_etf'
        ELSE 'etf'
    END AS instrument_type,
    fund_id AS instrument_code,
    COALESCE(
        NULLIF(fund_nm, ''),
        (
            SELECT NULLIF(TRIM(r.fund_nm), '')
            FROM raw_jisilu_etf AS r
            WHERE r.fund_id = raw_jisilu_etf.fund_id
              AND NULLIF(TRIM(r.fund_nm), '') IS NOT NULL
            ORDER BY COALESCE(NULLIF(TRIM(r.nav_dt), ''), r.snapshot_date) DESC, r.snapshot_date DESC, r.fetched_at DESC
            LIMIT 1
        ),
        fund_id
    ) AS instrument_name,
    issuer_nm AS issuer_name,
    'raw_jisilu_etf' AS source_table,
    snapshot_date,
    COALESCE(NULLIF(TRIM(nav_dt), ''), snapshot_date) AS nav_dt,
    price,
    amount_yi,
    volume_wan,
    unit_total_yi,
    discount_rt AS premium_discount_rt,
    NULL AS apply_status,
    NULL AS redeem_status,
    NULL AS duration,
    NULL AS ytm,
    index_nm AS raw_exposure_name
FROM raw_jisilu_etf
WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
  AND (snapshot_date, fetched_at) IN (
    SELECT snapshot_date, fetched_at
    FROM latest_etf_batch
  )

UNION ALL

SELECT
    'qdii_' || fund_id AS instrument_id,
    'qdii' AS instrument_type,
    fund_id AS instrument_code,
    COALESCE(
        NULLIF(fund_nm_display, ''),
        NULLIF(fund_nm, ''),
        (
            SELECT COALESCE(NULLIF(r.fund_nm_display, ''), NULLIF(r.fund_nm, ''))
            FROM raw_jisilu_qdii AS r
            WHERE r.fund_id = raw_jisilu_qdii.fund_id
              AND COALESCE(NULLIF(r.fund_nm_display, ''), NULLIF(r.fund_nm, '')) IS NOT NULL
            ORDER BY COALESCE(NULLIF(TRIM(r.nav_dt), ''), r.snapshot_date) DESC, r.snapshot_date DESC, r.fetched_at DESC
            LIMIT 1
        ),
        fund_id
    ) AS instrument_name,
    issuer_nm AS issuer_name,
    'raw_jisilu_qdii' AS source_table,
    snapshot_date,
    COALESCE(NULLIF(TRIM(nav_dt), ''), snapshot_date) AS nav_dt,
    price,
    amount_yi,
    volume_wan,
    unit_total_yi,
    COALESCE(iopv_discount_rt, nav_discount_rt, discount_rt) AS premium_discount_rt,
    apply_status,
    redeem_status,
    NULL AS duration,
    NULL AS ytm,
    index_nm AS raw_exposure_name
FROM raw_jisilu_qdii
WHERE COALESCE(NULLIF(TRIM(nav_dt), ''), '') <> ''
  AND (snapshot_date, fetched_at, market) IN (
    SELECT snapshot_date, fetched_at, market
    FROM latest_qdii_batch
  )

UNION ALL

SELECT
    instrument_id,
    instrument_type,
    instrument_code,
    instrument_name,
    issuer_name,
    source_table,
    snapshot_date,
    nav_dt,
    price,
    amount_yi,
    volume_wan,
    unit_total_yi,
    premium_discount_rt,
    apply_status,
    redeem_status,
    duration,
    ytm,
    raw_exposure_name
FROM lively_bond_enriched;

DROP VIEW IF EXISTS vw_invest_exposure_latest;
CREATE VIEW vw_invest_exposure_latest AS
SELECT
    i.instrument_id,
    i.instrument_type,
    i.instrument_code,
    i.instrument_name,
    i.issuer_name,
    i.source_table,
    i.snapshot_date,
    i.nav_dt,
    i.price,
    i.amount_yi,
    i.volume_wan,
    i.unit_total_yi,
    i.premium_discount_rt,
    i.apply_status,
    i.redeem_status,
    i.duration,
    i.ytm,
    CASE
        WHEN i.instrument_type = 'treasury_bond' THEN '中国国债'
        ELSE i.raw_exposure_name
    END AS exposure_name,
    CASE
        WHEN i.instrument_type = 'treasury_bond' THEN 'cn_treasury_bond'
        ELSE LOWER(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                TRIM(COALESCE(i.raw_exposure_name, '')),
                                '指数',
                                ''
                            ),
                            'ETF',
                            ''
                        ),
                        'etf',
                        ''
                    ),
                    'LOF',
                    ''
                ),
                ' ',
                ''
            )
        )
    END AS exposure_key,
    CASE
        WHEN i.instrument_type = 'treasury_bond' THEN 'instrument_rule'
        ELSE 'name_normalized'
    END AS exposure_method,
    CASE
        WHEN i.instrument_type = 'treasury_bond' THEN 'cn'
        WHEN instr(LOWER(COALESCE(i.raw_exposure_name, '')), 'gold') > 0 THEN 'global'
        ELSE 'na'
    END AS region_tag_guess,
    CASE
        WHEN i.instrument_type = 'treasury_bond' THEN
            CASE
                WHEN i.duration IS NULL THEN 'na'
                WHEN i.duration < 3 THEN 'short'
                WHEN i.duration < 5 THEN 'intermediate'
                WHEN i.duration < 10 THEN 'long'
                ELSE 'ultra_long'
            END
        ELSE 'na'
    END AS duration_band,
    CASE
        WHEN i.instrument_type = 'treasury_bond' THEN
            CASE
                WHEN i.duration IS NULL THEN NULL
                WHEN i.duration < 3 THEN 'cn_treasury_bond|short'
                WHEN i.duration < 5 THEN 'cn_treasury_bond|intermediate'
                WHEN i.duration < 10 THEN 'cn_treasury_bond|long'
                ELSE 'cn_treasury_bond|ultra_long'
            END
        ELSE LOWER(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                TRIM(COALESCE(i.raw_exposure_name, '')),
                                '指数',
                                ''
                            ),
                            'ETF',
                            ''
                        ),
                        'etf',
                        ''
                    ),
                    'LOF',
                    ''
                ),
                ' ',
                ''
            )
        )
    END AS bucket_match_key
FROM vw_invest_instrument_latest AS i
WHERE COALESCE(TRIM(i.raw_exposure_name), '') <> ''
   OR i.instrument_type = 'treasury_bond';

DROP VIEW IF EXISTS vw_invest_bucket_latest;
CREATE VIEW vw_invest_bucket_latest AS
SELECT
    e.instrument_id,
    e.instrument_type,
    e.instrument_code,
    e.instrument_name,
    e.issuer_name,
    e.source_table,
    e.snapshot_date,
    e.nav_dt,
    e.price,
    e.amount_yi,
    e.volume_wan,
    e.unit_total_yi,
    e.premium_discount_rt,
    e.apply_status,
    e.redeem_status,
    e.duration,
    e.ytm,
    e.exposure_name,
    e.exposure_key,
    e.exposure_method,
    e.region_tag_guess,
    e.duration_band,
    e.bucket_match_key,
    COALESCE(
        m.bucket_id,
        CASE
            WHEN e.instrument_type = 'etf'
             AND e.exposure_key IS NOT NULL
             AND e.exposure_key <> ''
             AND e.exposure_key NOT LIKE 'msci%'
             AND e.exposure_key NOT LIKE 's&p%'
             AND e.exposure_key NOT LIKE 'ftse%'
             AND e.exposure_key NOT LIKE 'nasdaq%'
             AND e.exposure_key NOT LIKE 'dj%'
             AND e.exposure_key NOT LIKE 'dow%'
             AND e.exposure_key NOT LIKE 'wti%'
             AND e.exposure_key NOT LIKE 'brent%'
             AND e.exposure_key NOT LIKE 'gold%'
             AND e.exposure_key NOT LIKE 'oil%'
             AND e.exposure_key NOT LIKE 'gas%'
             AND e.exposure_key NOT LIKE '恒生%'
             AND e.exposure_key NOT LIKE '港股%'
             AND e.exposure_key NOT LIKE '香港%'
             AND e.exposure_key NOT LIKE '中概%'
             AND e.exposure_key NOT LIKE '沙特%'
             AND e.exposure_key NOT LIKE '巴西%'
             AND e.exposure_key NOT LIKE '印度%'
             AND e.exposure_key NOT LIKE '日本%'
             AND e.exposure_key NOT LIKE '德国%'
             AND e.exposure_key NOT LIKE '欧洲%'
             AND e.exposure_key NOT LIKE '美国%'
            THEN 'eq_cn_thematic'
            ELSE NULL
        END
    ) AS bucket_id,
    b.bucket_name_cn,
    b.asset_class,
    b.issuer_type,
    b.duration_band AS bucket_duration_band,
    b.region_tag,
    b.style_tag,
    COALESCE(
        m.match_method,
        CASE
            WHEN e.instrument_type = 'etf'
             AND e.exposure_key IS NOT NULL
             AND e.exposure_key <> ''
             AND e.exposure_key NOT LIKE 'msci%'
             AND e.exposure_key NOT LIKE 's&p%'
             AND e.exposure_key NOT LIKE 'ftse%'
             AND e.exposure_key NOT LIKE 'nasdaq%'
             AND e.exposure_key NOT LIKE 'dj%'
             AND e.exposure_key NOT LIKE 'dow%'
             AND e.exposure_key NOT LIKE 'wti%'
             AND e.exposure_key NOT LIKE 'brent%'
             AND e.exposure_key NOT LIKE 'gold%'
             AND e.exposure_key NOT LIKE 'oil%'
             AND e.exposure_key NOT LIKE 'gas%'
             AND e.exposure_key NOT LIKE '恒生%'
             AND e.exposure_key NOT LIKE '港股%'
             AND e.exposure_key NOT LIKE '香港%'
             AND e.exposure_key NOT LIKE '中概%'
             AND e.exposure_key NOT LIKE '沙特%'
             AND e.exposure_key NOT LIKE '巴西%'
             AND e.exposure_key NOT LIKE '印度%'
             AND e.exposure_key NOT LIKE '日本%'
             AND e.exposure_key NOT LIKE '德国%'
             AND e.exposure_key NOT LIKE '欧洲%'
             AND e.exposure_key NOT LIKE '美国%'
            THEN 'fallback_cn_thematic'
            ELSE NULL
        END
    ) AS bucket_match_method,
    COALESCE(
        m.match_confidence,
        CASE
            WHEN e.instrument_type = 'etf'
             AND e.exposure_key IS NOT NULL
             AND e.exposure_key <> ''
             AND e.exposure_key NOT LIKE 'msci%'
             AND e.exposure_key NOT LIKE 's&p%'
             AND e.exposure_key NOT LIKE 'ftse%'
             AND e.exposure_key NOT LIKE 'nasdaq%'
             AND e.exposure_key NOT LIKE 'dj%'
             AND e.exposure_key NOT LIKE 'dow%'
             AND e.exposure_key NOT LIKE 'wti%'
             AND e.exposure_key NOT LIKE 'brent%'
             AND e.exposure_key NOT LIKE 'gold%'
             AND e.exposure_key NOT LIKE 'oil%'
             AND e.exposure_key NOT LIKE 'gas%'
             AND e.exposure_key NOT LIKE '恒生%'
             AND e.exposure_key NOT LIKE '港股%'
             AND e.exposure_key NOT LIKE '香港%'
             AND e.exposure_key NOT LIKE '中概%'
             AND e.exposure_key NOT LIKE '沙特%'
             AND e.exposure_key NOT LIKE '巴西%'
             AND e.exposure_key NOT LIKE '印度%'
             AND e.exposure_key NOT LIKE '日本%'
             AND e.exposure_key NOT LIKE '德国%'
             AND e.exposure_key NOT LIKE '欧洲%'
             AND e.exposure_key NOT LIKE '美国%'
            THEN 0.40
            ELSE NULL
        END
    ) AS bucket_match_confidence,
    CASE
        WHEN COALESCE(
            m.bucket_id,
            CASE
                WHEN e.instrument_type = 'etf'
                 AND e.exposure_key IS NOT NULL
                 AND e.exposure_key <> ''
                 AND e.exposure_key NOT LIKE 'msci%'
                 AND e.exposure_key NOT LIKE 's&p%'
                 AND e.exposure_key NOT LIKE 'ftse%'
                 AND e.exposure_key NOT LIKE 'nasdaq%'
                 AND e.exposure_key NOT LIKE 'dj%'
                 AND e.exposure_key NOT LIKE 'dow%'
                 AND e.exposure_key NOT LIKE 'wti%'
                 AND e.exposure_key NOT LIKE 'brent%'
                 AND e.exposure_key NOT LIKE 'gold%'
                 AND e.exposure_key NOT LIKE 'oil%'
                 AND e.exposure_key NOT LIKE 'gas%'
                 AND e.exposure_key NOT LIKE '恒生%'
                 AND e.exposure_key NOT LIKE '港股%'
                 AND e.exposure_key NOT LIKE '香港%'
                 AND e.exposure_key NOT LIKE '中概%'
                 AND e.exposure_key NOT LIKE '沙特%'
                 AND e.exposure_key NOT LIKE '巴西%'
                 AND e.exposure_key NOT LIKE '印度%'
                 AND e.exposure_key NOT LIKE '日本%'
                 AND e.exposure_key NOT LIKE '德国%'
                 AND e.exposure_key NOT LIKE '欧洲%'
                 AND e.exposure_key NOT LIKE '美国%'
                THEN 'eq_cn_thematic'
                ELSE NULL
            END
        ) IS NULL THEN 1
        ELSE 0
    END AS needs_bucket_review
FROM vw_invest_exposure_latest AS e
LEFT JOIN map_invest_exposure_bucket AS m
    ON e.bucket_match_key = m.exposure_key
LEFT JOIN cfg_invest_bucket AS b
    ON COALESCE(
        m.bucket_id,
        CASE
            WHEN e.instrument_type = 'etf'
             AND e.exposure_key IS NOT NULL
             AND e.exposure_key <> ''
             AND e.exposure_key NOT LIKE 'msci%'
             AND e.exposure_key NOT LIKE 's&p%'
             AND e.exposure_key NOT LIKE 'ftse%'
             AND e.exposure_key NOT LIKE 'nasdaq%'
             AND e.exposure_key NOT LIKE 'dj%'
             AND e.exposure_key NOT LIKE 'dow%'
             AND e.exposure_key NOT LIKE 'wti%'
             AND e.exposure_key NOT LIKE 'brent%'
             AND e.exposure_key NOT LIKE 'gold%'
             AND e.exposure_key NOT LIKE 'oil%'
             AND e.exposure_key NOT LIKE 'gas%'
             AND e.exposure_key NOT LIKE '恒生%'
             AND e.exposure_key NOT LIKE '港股%'
             AND e.exposure_key NOT LIKE '香港%'
             AND e.exposure_key NOT LIKE '中概%'
             AND e.exposure_key NOT LIKE '沙特%'
             AND e.exposure_key NOT LIKE '巴西%'
             AND e.exposure_key NOT LIKE '印度%'
             AND e.exposure_key NOT LIKE '日本%'
             AND e.exposure_key NOT LIKE '德国%'
             AND e.exposure_key NOT LIKE '欧洲%'
             AND e.exposure_key NOT LIKE '美国%'
            THEN 'eq_cn_thematic'
            ELSE NULL
        END
    ) = b.bucket_id;

COMMIT;
