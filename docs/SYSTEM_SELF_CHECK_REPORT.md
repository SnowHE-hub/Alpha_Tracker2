# Alpha_Tracker2 系统自检报告

> 审查依据：README.md 架构与数据流、docs/agent_workflow.md 契约与禁改规则。  
> 审查范围：当前仓库实现 vs 文档约定。

---

## 1. Architecture compliance report（架构符合性报告）

### 1.1 分层架构

| 文档层 | 约定 | 当前实现 | 符合 |
|--------|------|----------|------|
| 数据层 | Ingestion：universe + prices，Yahoo 插件，lake 缓存 | `ingestion/base.py`、`plugins/yahoo_*`、`cache.py`（仅 prices）；`ingest_universe` / `ingest_prices` | **部分**：universe 未写 lake |
| 特征层 | build_features，价量 + 回测特征 | `features/price_features.py`、`build_features.py`；无 `bt_*` 列 | **部分**：缺 bt_* |
| 模型层 | V1–V5，Scorer 插件，registry，thresholds | `scoring/base`、`registry`、`thresholds`、`v1_baseline`、`v2_v3_v4`；无 V5 | **部分**：V5 未实现 |
| 策略层 | Strategies：TopK、分散等 | **未实现**：无 `strategies/` 目录 | **缺失** |
| 组合与执行层 | portfolio_nav，nav_from_positions，state/rebalancer/costs | `portfolio_nav.py` 有；无 `portfolio/`、`execution/`、`nav_from_positions` | **部分** |
| 评估层 | forward_returns，eval_5d，metrics | `evaluation/forward_returns.py`、`eval_5d.py`；无 `metrics.py`、无 `eval_5d_batch` | **部分** |
| 报告与应用层 | make_dashboard，data/out/*.csv，Streamlit | `make_dashboard.py`、`reporting/dashboard_data.py`、data/out 导出；无 `apps/dashboard_streamlit` | **部分**：无 Streamlit |

### 1.2 数据流（run_daily 顺序）

文档顺序：ingest_universe → ingest_prices → build_features → score_all →（score_ensemble）→ eval_5d → portfolio_nav → make_dashboard。

**当前 run_daily 顺序**：一致（1–7 步，无 score_ensemble / eval_5d_batch / nav_from_positions）。  
单日与区间模式、skip 与透传参数行为与 README 描述一致。

### 1.3 存储与配置

- **DuckDB**：唯一主存储，路径来自 config，所有写库经 `DuckDBStore`。符合。
- **Parquet lake**：`data/lake/prices/` 由 ingest_prices 写入；**universe 未写入 lake**，与 README“写入 data/lake/universe/”不一致。
- **config**：`paths.out_dir`、`paths.runs_dir`、`scoring.score_versions`、`ingestion.*` 从 YAML 读取；阈值 q/window、fallback_topk 在 score_all 中硬编码，未从 config 读取。

### 1.4 契约符合性（agent_workflow.md B 节）

| 表 | 主键/字段 | 符合 |
|----|-----------|------|
| prices_daily | (trade_date, ticker)，含 market/adj_close/amount/source 等 | 是 |
| features_daily | (trade_date, ticker)；文档含 bt_mean/bt_winrate/bt_worst_mdd | **schema 无 bt_*** |
| picks_daily | (trade_date, version, ticker)，score/score_100/reason/thr_value/pass_thr/picked_by | 是 |
| nav_daily | (trade_date, portfolio)；文档允许无 version 列、portfolio 编码含版本 | 是 |
| eval_5d_daily | (as_of_date, version, bucket)，fwd_ret_5d/n_picks/horizon | 是 |

### 1.5 禁改规则（agent_workflow.md D 节）

- run_daily 仅编排、无业务逻辑：符合。
- 写库幂等（DELETE 再 INSERT）：ingest_universe、ingest_prices、build_features、score_all、eval_5d、portfolio_nav、make_dashboard（只写 CSV）均符合。
- 数据源/模型经 registry 或 config：ingestion 用 config + core/registry；scoring 用 scoring/registry + config.score_versions。符合。
- 未发现随意改表字段语义或引入无关依赖。

---

## 2. Missing modules（缺失模块）

### 2.1 README 规划中存在但未实现的目录/文件

| 模块/文件 | 说明 |
|-----------|------|
| **strategies/** | `base.py`、`registry.py`、`plugins/topk_equal_weight.py` 等；README 规划有，当前无目录。可选阶段，可后续补齐。 |
| **portfolio/** | `state.py`、`rebalancer.py`、`costs.py`；组合状态与成本逻辑未抽离，当前仅在 portfolio_nav 内内联。 |
| **execution/** | `engine.py`、broker_sim 等；执行模拟与 nav_from_positions 未实现。 |
| **scoring/plugins/v5_pooled.py** | 截面/池化模型 V5；README 列为可选。 |
| **pipelines/score_ensemble.py** | 多版本投票/ENS 写入 picks_daily；README 列为可选。 |
| **pipelines/eval_5d_batch.py** | 区间内批量 5 日评估；README 有提及，run_daily 未调用。 |
| **pipelines/nav_from_positions.py** | 从 positions_daily 推导 NAV；可选。 |
| **evaluation/metrics.py** | 夏普、回撤等指标函数；README 有，当前无文件。 |
| **apps/dashboard_streamlit/** | Streamlit 应用；README 称“可后续添加”。 |
| **configs/experiments/** | 实验配置 YAML；README 为可选。 |

### 2.2 数据流/契约中缺失的“能力”

| 能力 | 说明 |
|------|------|
| **Universe 写 lake** | README 与数据流要求 ingest_universe 写入 `data/lake/universe/`（Parquet）；当前只写 picks_daily，无 UniverseCache.save。 |
| **features_daily 的 bt_* 列** | 文档与 agent_workflow 约定有 bt_mean、bt_winrate、bt_worst_mdd 等；schema 与 price_features 均未实现，V4 的 bt_weight 未接入真实 bt 特征。 |
| **阈值/fallback 从 config 读取** | score_all 中 ThresholdConfig(q=0.8, window=60)、fallback_topk 硬编码；文档期望从 scoring.v2_v3_v4.common 等读取。 |

---

## 3. Potential bugs（潜在缺陷）

| 位置 | 现象 | 风险 | 建议 |
|------|------|------|------|
| **evaluation/forward_returns.py** | 当 `as_of_date` 为非交易日时，`trading_days(as_of_date, end_cal)` 的首日为“下一交易日”，前向收益实为“下一交易日收盘→+N 日收盘”。 | 语义与“信号日收盘”可能不一致；若文档约定为“信号日次日开盘/收盘”则无问题。 | 在注释或 agent_workflow 中明确“信号日为非交易日时以下一交易日为起点”。 |
| **ingest_universe** | 未将 universe 写入 lake；若 provider 失败也无缓存可回退。 | 与 README“失败可回退到 lake 最近一次 universe”不一致，且无缓存可用。 | 增加 UniverseCache + 成功时写 lake，失败时读 cache。 |
| **score_all fallback_topk** | `fallback_topk = max(20, min(100, len(df)))` 写死，未从 config 读。 | 与“配置驱动”原则不符，且与设计文档 per-version fallback_topk 不一致。 | 从 `scoring.v2_v3_v4.common.topk_fallback` 或 per-version 配置读取。 |
| **V4 bt_weight** | V4 的 `bt_weight` 仅在 reason 中展示，`_compose_score` 未使用；features_daily 也无 bt_* 列。 | V4 与 V2 仅 risk_weight 不同，未体现“回测加成”。 | 待 features_daily 增加 bt_* 后，在 V4 中接入 bt_score 并参与 score。 |
| **DuckDBStore.exec 多参数** | `store.exec(sql, (a, b, c, ...))` 用于 DELETE IN (?,?,?)；DuckDB 的 execute(sql, params) 支持多参数。 | 已确认实现正确，无 bug。 | — |

---

## 4. Technical debt（技术债）

| 类别 | 描述 | 建议 |
|------|------|------|
| **重复的 project_root 查找** | `_find_project_root(Path(__file__).resolve())` 在多个 pipeline 中重复实现。 | 抽到 `core/paths.py` 或 `core/config.py` 的 `get_project_root()`，统一使用。 |
| **重复的 config 读取** | `_load_ingestion_config(project_root)`、`_resolve_versions(..., project_root)` 等各自读 YAML。 | 在 load_settings 或单独模块中暴露 ingestion/scoring 子配置，减少重复 open YAML。 |
| **TradingCalendar 仅为工作日近似** | 当前用周一–周五，未用 pandas_market_calendars 的 US/HK 真实日历。 | 按 README TODO 替换为真实交易所日历，避免节假日误差。 |
| **thresholds 语义** | `get_threshold` 用**当日** scores 的 q 分位数作为阈值并 append 历史，未用“过去 window 天历史 scores”的滚动分位数。 | 若需严格“滚动历史分位数”，在 thresholds 中基于历史 score 序列重算。 |
| **V1 因子与文档** | 设计文档含 ret_1d、mom_5d、vol_5d；当前 V1 用 ret_5d、ret_20d、avg_amount_20。 | 若需完全对齐文档，在 default.yaml 增加 scoring.v1.weights 并从 config 读；或更新文档与实现一致。 |
| **依赖与版本** | requirements.txt 无 numpy 显式版本；pyarrow/yfinance 有最低版本。 | 可补充 numpy 范围，并定期检查安全与兼容性。 |
| **测试** | 仓库内无 tests/ 或 pytest 配置。 | 为 core、ingestion、scoring、evaluation 增加单元测试，对 pipeline 做少量集成测试。 |
| **文档与 schema 同步** | README/agent_workflow 提到 features_daily 含 bt_*、nav_daily 可选 version 列。 | 要么在 schema 与实现中补齐 bt_* 并更新 doc，要么在文档中注明“当前阶段未实现 bt_* / nav 无 version 列”。 |

---

## 5. Summary

- **架构符合性**：主干数据流与分层、run_daily 编排、DuckDB/幂等/registry 使用均符合 README 与 agent_workflow；universe 未写 lake、features_daily 无 bt_*、策略/组合/执行未拆分为独立模块为已知缺口。
- **缺失模块**：strategies/、portfolio/、execution/、score_ensemble、eval_5d_batch、nav_from_positions、v5_pooled、evaluation/metrics、apps/dashboard_streamlit 等多为可选或后续阶段，可按优先级逐步补齐。
- **潜在缺陷**：universe 无 lake 回退、forward_returns 信号日语义、score_all 硬编码 fallback_topk 与阈值配置、V4 未用 bt_*，建议按上表逐项处理或文档化。
- **技术债**：project_root/config 重复、交易日历近似、阈值语义、测试与文档同步，适合在迭代中逐步还债。

建议下一步：优先补齐 **universe 写 lake** 与 **scoring 阈值/fallback 从 config 读取**，再视需要增加 **features_daily bt_* 列与 V4 接入** 或 **Streamlit dashboard**。
