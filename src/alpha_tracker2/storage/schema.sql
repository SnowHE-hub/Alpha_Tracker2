-- Core schema for Alpha_Tracker2 US/HK (minimal for infra smoke test).

CREATE TABLE IF NOT EXISTS meta_runs (
    run_id TEXT PRIMARY KEY,
    run_ts TIMESTAMP,
    note   TEXT
);

-- Ingestion: daily OHLCV from Yahoo (US/HK). Aligned with ingest_prices INSERT columns.
CREATE TABLE IF NOT EXISTS prices_daily (
    trade_date DATE NOT NULL,
    ticker     TEXT NOT NULL,
    market     TEXT NOT NULL,  -- "US" / "HK"
    open       DOUBLE,
    high       DOUBLE,
    low        DOUBLE,
    close      DOUBLE,
    adj_close  DOUBLE,
    volume     BIGINT,
    amount     DOUBLE,
    currency   TEXT,
    source     TEXT,
    PRIMARY KEY (trade_date, ticker)
);

-- Ingestion + scoring: universe (version='UNIVERSE') and model picks. Aligned with ingest_universe INSERT.
CREATE TABLE IF NOT EXISTS picks_daily (
    trade_date DATE NOT NULL,
    version    TEXT NOT NULL,
    ticker     TEXT NOT NULL,
    name       TEXT,
    rank       INTEGER,
    score      DOUBLE,
    score_100  DOUBLE,
    reason     TEXT,
    thr_value  DOUBLE,
    pass_thr   BOOLEAN,
    picked_by  TEXT,
    PRIMARY KEY (trade_date, version, ticker)
);

-- Features: daily price/volume and backtest-style features.
CREATE TABLE IF NOT EXISTS features_daily (
    trade_date DATE NOT NULL,
    ticker     TEXT NOT NULL,

    -- Returns / momentum
    ret_1d     DOUBLE,
    ret_5d     DOUBLE,
    ret_10d    DOUBLE,
    ret_20d    DOUBLE,

    -- Volatility / risk
    vol_5d        DOUBLE,
    vol_ann_60d   DOUBLE,
    mdd_60d       DOUBLE,

    -- Moving averages / trend
    ma5        DOUBLE,
    ma10       DOUBLE,
    ma20       DOUBLE,
    ma60       DOUBLE,
    ma5_gt_ma10_gt_ma20 BOOLEAN,
    ma20_above_ma60     BOOLEAN,
    ma20_slope          DOUBLE,

    -- Liquidity / amount
    avg_amount_20 DOUBLE,

    PRIMARY KEY (trade_date, ticker)
);

-- Evaluation: 5-day forward return by as_of_date, version, bucket.
CREATE TABLE IF NOT EXISTS eval_5d_daily (
    as_of_date  DATE NOT NULL,
    version     TEXT NOT NULL,
    bucket      TEXT NOT NULL,
    fwd_ret_5d  DOUBLE,
    n_picks     INTEGER,
    horizon     INTEGER,
    PRIMARY KEY (as_of_date, version, bucket)
);

-- Portfolio: daily NAV by trade_date and portfolio (e.g. V1_top3).
CREATE TABLE IF NOT EXISTS nav_daily (
    trade_date DATE NOT NULL,
    portfolio  TEXT NOT NULL,
    nav        DOUBLE NOT NULL,
    ret        DOUBLE,
    PRIMARY KEY (trade_date, portfolio)
);

