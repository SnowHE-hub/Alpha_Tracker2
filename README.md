## Alpha_Tracker2 US/HK（基于 Yahoo 的选股与回测系统）

本项目是一个面向 **美股 + 港股** 的量化研究框架，围绕 Yahoo 数据源构建，提供从 **数据采集 → 特征工程 → 多版本选股模型（V1–V5）→ 组合与执行 → 评估 → Dashboard** 的完整闭环。

文档目标：

- 把整体 **架构设计** 和 **实施步骤** 写清楚，方便后续逐步实现。
- 完全基于你已有的 V1–V5 设计思想和数据工程要求，但 **数据源换成 Yahoo，市场换成 US/HK**。

---

## 一、整体架构与设计目标

- **统一数据源**：以 Yahoo API（如 `yfinance` / `yahooquery`）为主，获取美股/港股的日频数据。
- **统一存储**：DuckDB 作为主存储（`data/store/alpha_tracker.duckdb`），Parquet 作为数据湖缓存（`data/lake`）。
- **清晰分层**：
  - 数据层（Data / Ingestion）
  - 特征层（Features）
  - 模型层（Models / Scoring，包含 V1–V5）
  - 策略层（Strategies）
  - 组合与执行层（Portfolio & Execution）
  - 评估层（Evaluation）
  - 报告与应用层（Reporting & Dashboard）
- **插件化 & 可扩展**：
  - 数据源：`UniverseProvider`、`PriceProvider` 插件（目前实现 Yahoo，后续可扩展 Tiingo、Polygon 等）。
  - 模型：`Scorer` 插件（V1–V5、DL-TS、DL-CS 等）。
- **契约稳定**：核心表如 `prices_daily / features_daily / picks_daily / nav_daily / eval_*` 的语义稳定，方便工具脚本和 Dashboard 复用。

---

## 二、目录与模块规划（从零搭建）

目标是在当前仓库下，从零建立一个干净的项目结构：

```text
alpha_tracker2/
├── configs/
│   ├── default.yaml          # 主配置：路径、数据源(Yahoo)、模型版本、阈值等
│   └── experiments/          # 训练/实验配置（可选）
├── data/
│   ├── lake/                 # 原始/缓存数据（Parquet）
│   │   ├── universe/
│   │   └── prices/
│   ├── store/
│   │   └── alpha_tracker.duckdb
│   ├── out/                  # 导出给 dashboard / 分析的 CSV
│   └── runs/                 # run_daily 的元数据
└── src/alpha_tracker2/
    ├── __init__.py
    ├── core/
    │   ├── config.py         # 读取 default.yaml，暴露 Settings
    │   ├── trading_calendar.py  # US/HK 交易日历
    │   └── registry.py       # provider/scorer 注册中心
    ├── storage/
    │   ├── duckdb_store.py   # 统一 DuckDB 访问层
    │   └── schema.sql        # 所有表结构
    ├── ingestion/
    │   ├── base.py           # UniverseProvider / PriceProvider 协议
    │   ├── cache.py          # 通用缓存封装
    │   └── plugins/
    │       ├── yahoo_universe.py       # Yahoo / 静态文件的美股/港股 universe
    │       └── yahoo_price_provider.py # Yahoo 日线行情
    ├── features/
    │   └── price_features.py  # 价量特征与滚动回测特征
    ├── scoring/
    │   ├── base.py
    │   ├── registry.py
    │   ├── thresholds.py
    │   └── plugins/
    │       ├── v1_baseline.py
    │       ├── v2_v3_v4.py
    │       └── v5_pooled.py   # 可选：截面模型（后续阶段）
    ├── strategies/
    │   ├── base.py
    │   └── plugins/
    │       └── topk_equal_weight.py
    ├── portfolio/
    │   ├── state.py
    │   ├── rebalancer.py
    │   └── costs.py
    ├── execution/
    │   └── engine.py
    ├── evaluation/
    │   ├── forward_returns.py
    │   └── metrics.py
    ├── reporting/
    │   └── dashboard_data.py
    └── pipelines/
        ├── run_daily.py          # 一键日度调度
        ├── ingest_universe.py
        ├── ingest_prices.py
        ├── build_features.py
        ├── score_all.py
        ├── score_ensemble.py
        ├── eval_5d.py
        ├── eval_5d_batch.py
        ├── portfolio_nav.py
        ├── nav_from_positions.py
        └── make_dashboard.py
```

> 说明：上面是“目标结构”，实际实现时可以分阶段逐步补齐；暂时用不到的目录可以先不创建。

---

## 三、核心数据表设计（DuckDB）

只列出第一阶段必须表（足够支撑 V1–V4 + NAV + eval）：

- **`prices_daily`**  
  - 主键：`(trade_date, ticker)`  
  - 字段：`trade_date, ticker, market, open, high, low, close, adj_close, volume, amount, currency, source`
  - 说明：从 Yahoo 获取的日线行情，回测与收益一律用 `adj_close`。

- **`features_daily`**  
  - 主键：`(trade_date, ticker)`  
  - 字段示例：  
    - 收益/动量：`ret_1d, ret_5d, ret_10d, ret_20d`  
    - 波动/风险：`vol_5d, vol_ann_60d, mdd_60d`  
    - 趋势：`ma5, ma10, ma20, ma60, ma5_gt_ma10_gt_ma20, ma20_above_ma60, ma20_slope`  
    - 回测：`bt_mean, bt_winrate, bt_worst_mdd, ...`  
    - 量价：`avg_amount_20` 等。

- **`picks_daily`**  
  - 主键：`(trade_date, version, ticker)`  
  - 字段：`trade_date, version, ticker, name, rank, score, score_100, reason, thr_value, pass_thr, picked_by, ...`
  - 说明：V1–V5 以及 `UNIVERSE`、`ENS` 等版本的选股结果，完全沿用你已有的契约。

- **`nav_daily`**  
  - 主键：`(trade_date, portfolio, version)`（可以简化成 portfolio=version-topk 组合）  
  - 字段：`trade_date, portfolio, version, nav, ret, ...`

- **`positions_daily` / `trades_daily`**（如果做执行模拟）  
  - 结构与原设计类似，用于从实际持仓推导 NAV。

- **`eval_5d_daily`**（或类似命名）  
  - 存储 `forward_returns` 和分组评估结果。

---

## 四、数据流设计（US/HK + Yahoo）

以单日 `trade_date` 为例，`run_daily` 的主流程为：

1. **ingest_universe**
   - 从静态文件或 Yahoo 获取美股/港股 universe（如 S&P 500、NASDAQ100、HSI 等）。
   - 写入：
     - `data/lake/universe/`（Parquet 缓存）
     - DuckDB：`picks_daily(version='UNIVERSE')`

2. **ingest_prices**
   - 从 `picks_daily(UNIVERSE)` 读取 ticker 列表。
   - 使用 Yahoo 价格 provider 拉取 [start, end] 区间的日线（含 `adj_close`），带 lake 缓存：
     - lake：`data/lake/prices/{ticker}/{start}_{end}.parquet`
     - DuckDB：写入/更新 `prices_daily`（删除窗口内旧数据再插入，保证幂等）。

3. **build_features**
   - 根据 `trade_date` 和交易日历确定历史窗口（至少 260 个交易日）。
   - 从 `prices_daily` 读取窗口内的 US/HK 行情，计算价量与回测特征。
   - 写入 `features_daily`。

4. **score_all（V1–V4）**
   - 从 `features_daily`（以及必要时的 `prices_daily`）读取特征。
   - 通过 `scoring/registry.get_scorer(version)` 调用对应的 scorer 插件：
     - **V1**：纯规则、多因子 z-score 加权 baseline。
     - **V2/V3**：趋势 + 单标的 ML + 预期收益 + 风险惩罚（Aggressive vs Conservative）。
     - **V4**：在 V2 基础上加滚动回测 (`bt_*`) 与 MA 加成。
   - 使用 `scoring/thresholds` 根据历史分布计算各版本的 `thr_value` / `pass_thr`，并应用 fallback_topk 规则。
   - 统一写入 `picks_daily`。

5. **score_ensemble（可选）**
   - 对 V1–V4 的结果做投票 / streak 组合，输出 `version='ENS'` 的 picks。

6. **portfolio_nav / nav_from_positions**
   - `portfolio_nav`：直接基于 `picks_daily + prices_daily` 计算等权或指定规则的组合净值，写入 `nav_daily`。
   - `nav_from_positions`（可选）：若做执行模拟，则从 `positions_daily` + `prices_daily` 计算 NAV 进行对比。

7. **eval_5d / eval_5d_batch**
   - 根据 `picks_daily` 与未来的 `prices_daily` 计算 forward returns，生成评估指标表。

8. **make_dashboard**
   - 从 DuckDB 读取 `nav_daily`、`picks_daily`、eval 表，写入 `data/out/*.csv` 或提供查询接口。
   - 由 Streamlit Dashboard 读取这些数据进行可视化。

---

## 五、实施步骤（执行计划）

下面是从 0 到可用系统的推荐执行顺序，每一步都可以单独完成和验证。

### 第 1 阶段：基础设施与配置

1. **创建基础目录与空文件**
   - 建立 `configs/`、`data/`、`src/alpha_tracker2/` 基本结构。
   - 创建空的 `__init__.py`、`core/config.py`、`storage/duckdb_store.py`、`storage/schema.sql`。

2. **实现配置加载**
   - 在 `core/config.py` 中定义 `Settings` dataclass：包含 `lake_dir`, `store_db`, `runs_dir`, `log_level` 等。
   - 实现 `load_settings(project_root)`，从 `configs/default.yaml` 读取配置。

3. **实现 DuckDBStore 与 schema**
   - 在 `storage/duckdb_store.py` 实现：
     - `connect()`, `init_schema()`, `exec()`, `fetchall()`, `fetchone()`, `session()`.
   - 在 `storage/schema.sql` 中定义必须表：`prices_daily / features_daily / picks_daily / nav_daily / eval_5d_daily / meta_runs` 等。

4. **实现交易日历**
   - 在 `core/trading_calendar.py` 使用 `pandas_market_calendars` 或自建日历：
     - 支持 US (`market='US'`) 与 HK (`market='HK'`)。
     - 提供 `latest_trading_day()` 与 `trading_days(start, end)`。

### 第 2 阶段：Yahoo 数据接入（Universe + Prices）

5. **实现 Ingestion 协议与 Yahoo 插件**
   - `ingestion/base.py`：定义 `UniverseProvider` / `PriceProvider` 协议。
   - `ingestion/plugins/yahoo_universe.py`：
     - 支持从静态 CSV 或 Yahoo 指数成分构建 US/HK universe。
   - `ingestion/plugins/yahoo_price_provider.py`：
     - 基于 `yfinance` 抓取日线（含 `adj_close`），处理缺失与重试。

6. **实现 ingest_universe / ingest_prices pipeline**
   - `pipelines/ingest_universe.py`：
     - 计算 `trade_date`，调用 `UniverseProvider`，写 lake + `picks_daily(version='UNIVERSE')`。
   - `pipelines/ingest_prices.py`：
     - 从 `picks_daily(UNIVERSE)` 提取 tickers。
     - 用交易日历和配置确定 [start, end]。
     - 调用 `PriceProvider`，写 lake + `prices_daily`（幂等）。

7. **写一个 smoke 脚本**
   - `pipelines/smoke.py`：初始化 schema，插入一条 `meta_runs`，检查 DuckDB 读写是否正常。

### 第 3 阶段：特征工程与 V1–V4 模型

8. **实现 build_features**
   - 根据 `trade_date` 和 trading calendar，确定历史窗口（至少 260 交易日）。
   - 从 `prices_daily` 读取窗口内的数据，计算文档中定义的价量和 rolling backtest 特征。
   - 写入 `features_daily`。

9. **实现 scoring 基础模块**
   - `scoring/base.py`：定义 `Scorer` 协议和通用工具。
   - `scoring/registry.py`：根据 version 名称返回对应 scorer 实现。
   - `scoring/thresholds.py`：管理 `ab_threshold_history.json`，计算滚动分位数阈值。

10. **实现 V1–V4 scorer 插件**
    - `v1_baseline.py`：多因子 z-score 加权 baseline，无阈值，reason 为结构化 JSON。
    - `v2_v3_v4.py`：实现趋势 + 单标的 ML + 风险惩罚 + 回测加成的打分逻辑，并严格按版本区分权重与参数。

11. **实现 score_all 与（可选）score_ensemble**
    - `score_all.py`：按 `scoring.score_versions` 循环跑各版本 scorer，合并结果，计算阈值与 fallback_topk，写入 `picks_daily`。
    - `score_ensemble.py`（可选）：按投票/一致性生成 `ENS` 版本 picks。

### 第 4 阶段：NAV、评估与 Dashboard

12. **实现 forward_returns & eval_5d**
    - `evaluation/forward_returns.py`：用 `prices_daily` 计算未来 N 日收益。
    - `pipelines/eval_5d.py` / `eval_5d_batch.py`：生成评估表。

13. **实现 portfolio_nav / nav_from_positions**
    - `portfolio_nav.py`：从 `picks_daily + prices_daily` 计算组合 NAV 曲线，写入 `nav_daily`。
    - `nav_from_positions.py`（可选）：从执行结果推导 NAV，用于对比。

14. **实现 make_dashboard 与简单 Streamlit 应用**
    - `pipelines/make_dashboard.py`：从 DuckDB 生成 dashboard 需要的聚合表/CSV（写入 `data/out/`）。
    - `apps/dashboard_streamlit/app.py`（可后续添加）：展示版本对比、分位收益、回撤等。

### 第 5 阶段：扩展（V5、基本面、深度学习等，可选）

15. **扩展数据源**
    - 接入 US/HK 基本面、行业分类、新闻情绪等（可不局限于 Yahoo，需要其他 API）。
    - 新增 `fundamentals_daily / sector_* / news_sentiment_daily` 等表和对应 ingestion 脚本。

16. **实现 V5 与深度学习模型**
    - `scoring/plugins/v5_pooled.py`：截面/池化模型（LR/GBM/MLP）。
    - `training/` 目录：实现 `DL-TS`、`DL-CS` 等训练脚本与数据加载工具。

---

## 六、run_daily 使用示例（目标形态）

当上述模块逐步实现后，可以通过一个统一入口脚本完成单日或区间的完整流程：

```bash
# 单日模式（主要用于生成当日 picks + 部分评估）
python -m alpha_tracker2.pipelines.run_daily --date 2026-01-15

# 区间模式（主要用于回测与 Dashboard）
python -m alpha_tracker2.pipelines.run_daily --start 2025-01-01 --end 2025-12-31
```

`run_daily` 的职责仅是按顺序调用各 pipeline，不包含任何业务逻辑：

1. ingest_universe  
2. ingest_prices  
3. build_features  
4. score_all（+ 可选 score_ensemble）  
5. eval_5d / eval_5d_batch（可选）  
6. portfolio_nav / nav_from_positions  
7. make_dashboard  

---

## 七、小结

- 本 README 定义了 **US/HK + Yahoo** 版本的 Alpha_Tracker2 的整体架构、核心数据表和端到端数据流。
- 实施上建议按 **基础设施 → Yahoo 数据接入 → 特征与 V1–V4 → NAV & Dashboard → 扩展数据与模型** 的顺序渐进式完成。
- 后续每次编码时，可以直接对照本 README 的“模块规划”和“实施步骤”，逐块把空目录/文件填实即可。

