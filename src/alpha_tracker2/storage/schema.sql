-- schema.sql (minimal + nav_daily)
-- DuckDB is the single source of truth.
-- Note: picks_daily extra columns are patched by patch_picks_daily_schema.py (as you already do).

CREATE TABLE IF NOT EXISTS meta_runs (
  run_id VARCHAR PRIMARY KEY,
  run_ts TIMESTAMP,
  note VARCHAR
);

CREATE TABLE IF NOT EXISTS picks_daily (
  trade_date DATE,
  version VARCHAR,              -- e.g. V1/V2/V3/V4
  ticker VARCHAR,
  name VARCHAR,
  score DOUBLE,
  rank INTEGER,
  PRIMARY KEY (trade_date, version, ticker)
);

CREATE TABLE IF NOT EXISTS prices_daily (
  trade_date DATE,
  ticker VARCHAR,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume DOUBLE,
  amount DOUBLE,
  source VARCHAR,          -- live/cache
  PRIMARY KEY (trade_date, ticker)
);
-- =========================
CREATE TABLE IF NOT EXISTS features_daily (
  trade_date DATE,
  ticker VARCHAR,

  -- existing (keep)
  ret_1d DOUBLE,
  mom_5d DOUBLE,
  vol_5d DOUBLE,
  ma_5 DOUBLE,

  -- returns / momentum (needed by V1/V2/V3/V4)
  ret_5d DOUBLE,
  ret_10d DOUBLE,
  ret_20d DOUBLE,

  -- moving averages & trend flags
  ma_10 DOUBLE,
  ma_20 DOUBLE,
  ma_60 DOUBLE,
  ma5_gt_ma10_gt_ma20 INTEGER,   -- 0/1
  ma20_above_ma60 INTEGER,       -- 0/1
  ma20_slope DOUBLE,             -- approx slope (see build_features.py)

  -- risk (60d)
  vol_ann_60d DOUBLE,
  mdd_60d DOUBLE,
  worst_day_60d DOUBLE,

  -- liquidity & exec risk
  avg_amount_20 DOUBLE,
  limit_up_60 INTEGER,
  limit_down_60 INTEGER,

  -- rolling backtest (V1/V4)
  bt_best_style VARCHAR,         -- LUMP / DCA5 / DCA10
  bt_mean DOUBLE,
  bt_median DOUBLE,
  bt_winrate DOUBLE,
  bt_p10 DOUBLE,
  bt_worst DOUBLE,
  bt_avg_mdd DOUBLE,
  bt_worst_mdd DOUBLE,

  source VARCHAR,                -- "calc"
  PRIMARY KEY (trade_date, ticker)
);

-- =========================
-- Step 2: NAV written back to DuckDB (nav_daily)
-- Column contract follows handover doc:
--   trade_date: NAV/return realized date (t)
--   picks_trade_date: signal date used to compute trade_date return (typically t-1 trading day); INIT for the first row
--   asof_date: same as trade_date (export/display convenience)
--   version, day_ret, nav, n_picks, n_valid
-- =========================
CREATE TABLE IF NOT EXISTS nav_daily (
  trade_date DATE,
  picks_trade_date VARCHAR,      -- 'YYYY-MM-DD' or 'INIT'
  asof_date DATE,
  version VARCHAR,
  day_ret DOUBLE,
  nav DOUBLE,
  n_picks INTEGER,
  n_valid INTEGER,
  PRIMARY KEY (trade_date, version)
);
-- =========================
-- Step 3: eval summary written back to DuckDB (eval_5d_batch_daily)
-- This table stores per-(trade_date, version) evaluation summary used by dashboard.
-- =========================
CREATE TABLE IF NOT EXISTS eval_5d_batch_daily (
  trade_date DATE,
  version VARCHAR,

  -- Generic evaluation fields (keep flexible; some may be NULL)
  coverage DOUBLE,              -- e.g. valid_days / total_days (optional)
  hit_rate DOUBLE,              -- optional
  avg_ret_5d DOUBLE,            -- optional
  median_ret_5d DOUBLE,         -- optional

  -- Keep original counts if present in CSV summary
  eval_n_picks INTEGER,
  eval_n_valid INTEGER,

  -- Freeform json/text for future expansion (optional)
  extra VARCHAR,

  PRIMARY KEY (trade_date, version)
);
-- =========================
-- Step 4: unified multi-horizon evaluation table
-- =========================
CREATE TABLE IF NOT EXISTS eval_batch_daily (
  trade_date DATE,
  version VARCHAR,
  horizon INTEGER,

  coverage DOUBLE,
  hit_rate DOUBLE,
  avg_ret DOUBLE,
  median_ret DOUBLE,

  eval_n_picks INTEGER,
  eval_n_valid INTEGER,

  extra VARCHAR,

  PRIMARY KEY (trade_date, version, horizon)
);
