-- schema_v2.sql  (增量 schema：可反复执行 + 不破坏旧数据)

-- ================
-- 0) Strategies registry
-- ================
CREATE TABLE IF NOT EXISTS strategies (
  strategy_id VARCHAR PRIMARY KEY,
  model_version VARCHAR,
  trade_rule VARCHAR,
  hold_n INTEGER,
  topk INTEGER,
  cost_bps INTEGER,
  lot_size INTEGER,
  params_json VARCHAR,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ================
-- 1) Signals / Orders / Fills
-- ================
CREATE TABLE IF NOT EXISTS signals (
  strategy_id VARCHAR,
  trade_date DATE,
  ticker VARCHAR,
  rank INTEGER,
  score DOUBLE,
  weight DOUBLE,
  meta_json VARCHAR,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
  strategy_id VARCHAR,
  trade_date DATE,
  ticker VARCHAR,
  side VARCHAR,                 -- BUY / SELL
  qty BIGINT,
  target_weight DOUBLE,
  reason VARCHAR,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fills (
  strategy_id VARCHAR,
  trade_date DATE,
  ticker VARCHAR,
  side VARCHAR,
  qty BIGINT,
  price DOUBLE,
  fee DOUBLE,
  slippage DOUBLE,
  notional DOUBLE,
  reason VARCHAR,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ================
-- 2) Positions / Trades / NAV (strategy_id 维度)
-- ================
CREATE TABLE IF NOT EXISTS positions_daily (
  asof_date DATE,
  strategy_id VARCHAR,
  ticker VARCHAR,
  shares BIGINT,
  avg_cost DOUBLE,
  price DOUBLE,
  market_value DOUBLE,
  pnl_pct DOUBLE,
  hold_days INTEGER,
  tp_half_done BOOLEAN,
  score DOUBLE,
  meta_json VARCHAR,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trades_daily (
  trade_date DATE,
  strategy_id VARCHAR,
  ticker VARCHAR,
  side VARCHAR,
  shares BIGINT,
  price DOUBLE,
  notional DOUBLE,
  cost DOUBLE,
  reason VARCHAR,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- NOTE: 你现有 nav_daily 可能已存在（列较少），迁移脚本会补列
CREATE TABLE IF NOT EXISTS nav_daily (
  trade_date DATE,
  picks_trade_date DATE,
  asof_date DATE,
  version VARCHAR,              -- legacy: 先保留
  strategy_id VARCHAR,          -- v2: 新增主维度
  day_ret DOUBLE,
  nav DOUBLE,
  day_ret_gross DOUBLE,
  nav_gross DOUBLE,
  turnover DOUBLE,
  cost_bps INTEGER,
  cost DOUBLE,
  n_picks INTEGER,
  n_valid INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ================
-- 3) Forward returns + Metrics
-- ================
CREATE TABLE IF NOT EXISTS forward_returns (
  strategy_id VARCHAR,
  signal_date DATE,
  ticker VARCHAR,
  fwd_ret_1d DOUBLE,
  fwd_ret_5d DOUBLE,
  fwd_ret_10d DOUBLE,
  fwd_ret_20d DOUBLE,
  mae DOUBLE,
  mfe DOUBLE,
  meta_json VARCHAR,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS strategy_metrics_daily (
  trade_date DATE,
  strategy_id VARCHAR,
  nav DOUBLE,
  drawdown DOUBLE,
  turnover DOUBLE,
  cost DOUBLE,
  n_positions INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS strategy_metrics_summary (
  start_date DATE,
  end_date DATE,
  strategy_id VARCHAR,
  total_return DOUBLE,
  ann_return DOUBLE,
  ann_vol DOUBLE,
  sharpe DOUBLE,
  max_drawdown DOUBLE,
  win_rate DOUBLE,
  avg_turnover DOUBLE,
  total_cost DOUBLE,
  n_days INTEGER,
  extra_json VARCHAR,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ================
-- 4) Eval batch daily（你现有是 (trade_date, version, horizon)；v2 需要 strategy_id）
-- ================
CREATE TABLE IF NOT EXISTS eval_5d_batch_daily (
  trade_date DATE,
  version VARCHAR,
  horizon INTEGER,
  strategy_id VARCHAR,          -- v2: 新增
  coverage DOUBLE,
  hit_rate DOUBLE,
  avg_ret DOUBLE,
  median_ret DOUBLE,
  eval_n_picks INTEGER,
  eval_n_valid INTEGER,
  extra VARCHAR
);

