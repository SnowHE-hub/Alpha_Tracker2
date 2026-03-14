# Alpha_Tracker2 项目进展总结

> 本文档汇总项目定位、已实现能力、端到端链路、已知缺口与规范文档，便于总览和后续迭代。  
> 依据：README.md、docs/SYSTEM_SELF_CHECK_REPORT.md、docs/REVIEW_AGENT_ACCEPTANCE.md、当前代码与目录结构。

---

## 一、项目定位与目标

- **定位**：面向 **美股 + 港股** 的量化研究框架，基于 Yahoo 数据源，提供从数据采集 → 特征工程 → 多版本选股（V1–V5）→ 组合与评估 → Dashboard 的完整闭环。
- **存储**：DuckDB 主存储（`data/store/alpha_tracker.duckdb`），Parquet 数据湖（`data/lake`）做缓存。
- **原则**：插件化（Provider/Scorer 注册）、配置驱动、核心表契约稳定、幂等写入。

---

## 二、已完成的模块与能力

### 2.1 基础设施与配置

| 项 | 状态 | 说明 |
|----|------|------|
| 目录与配置 | ✅ | `configs/default.yaml`、`core/config.py`（Settings、load_settings） |
| DuckDB 与 Schema | ✅ | `storage/duckdb_store.py`、`storage/schema.sql`（prices_daily、features_daily、picks_daily、nav_daily、eval_5d_daily、meta_runs） |
| 交易日历 | ✅ | `core/trading_calendar.py`（当前为工作日近似，可后续换真实交易所日历） |
| 注册中心 | ✅ | `core/registry.py`（Provider）、`scoring/registry.py`（Scorer） |

### 2.2 数据层（Ingestion）

| 项 | 状态 | 说明 |
|----|------|------|
| 协议与缓存 | ✅ | `ingestion/base.py`（UniverseProvider/PriceProvider）、`ingestion/cache.py`（价格缓存 + **UniverseCache**） |
| Yahoo Universe | ✅ | `ingestion/plugins/yahoo_universe.py`，写 picks_daily(version='UNIVERSE')，**成功写 lake、失败回退 cache** |
| Yahoo 价格 | ✅ | `ingestion/plugins/yahoo_price_provider.py`，lake + DuckDB 幂等写入 |
| Pipeline | ✅ | `pipelines/ingest_universe.py`、`pipelines/ingest_prices.py` |

### 2.3 特征层

| 项 | 状态 | 说明 |
|----|------|------|
| 价量特征 | ✅ | `features/price_features.py`（ret_*、vol_*、ma_*、avg_amount_20 等） |
| 建特征 Pipeline | ✅ | `pipelines/build_features.py`（按 trade_date 与历史窗口写 features_daily） |
| bt_* 回测特征 | ⚠️ 未实现 | schema/price_features 暂无 bt_mean、bt_winrate、bt_worst_mdd；V4 的 bt_weight 未接入 |

### 2.4 模型层（Scoring）

| 项 | 状态 | 说明 |
|----|------|------|
| 基类与阈值 | ✅ | `scoring/base.py`、`scoring/thresholds.py` |
| V1 Baseline | ✅ | `scoring/plugins/v1_baseline.py`，多因子 z-score |
| V2/V3/V4 | ✅ | `scoring/plugins/v2_v3_v4.py`，**q/window/topk_fallback 从 config 读取**（scoring.v2_v3_v4.common） |
| V5 / ENS | ⚠️ 未实现 | 可选；score_ensemble 未接入 run_daily |
| Pipeline | ✅ | `pipelines/score_all.py`，按 config 的 score_versions 调度，幂等写 picks_daily |

### 2.5 组合与评估

| 项 | 状态 | 说明 |
|----|------|------|
| 组合净值 | ✅ | `pipelines/portfolio_nav.py`，基于 picks_daily + prices_daily 写 nav_daily |
| 前向收益 | ✅ | `evaluation/forward_returns.py`，**as_of 非交易日语义已文档化** |
| 5 日评估 | ✅ | `pipelines/eval_5d.py`，写 eval_5d_daily |
| nav_from_positions / execution | ⚠️ 未实现 | 可选；positions_daily、执行模拟未做 |

### 2.6 流水线编排与出口

| 项 | 状态 | 说明 |
|----|------|------|
| run_daily | ✅ | `pipelines/run_daily.py`，仅编排：ingest_universe → ingest_prices → build_features → score_all → eval_5d → portfolio_nav → make_dashboard |
| make_dashboard | ✅ | `pipelines/make_dashboard.py`，导出 data/out/*.csv；`reporting/dashboard_data.py` |
| Streamlit 应用 | ⚠️ 可选 | README 规划有 apps/dashboard_streamlit，当前仓库有 apps/dashboard_streamlit，是否为主线未在本文档假定 |

### 2.7 端到端 Smoke 与规范（近期完成）

| 项 | 状态 | 说明 |
|----|------|------|
| smoke_e2e | ✅ | `pipelines/smoke_e2e.py`：六步 + 五表检查（存在、行数、关键列非空）；**resolved target_date**（按 prices_daily 区间 max 解析） |
| 样本输出与冷跑说明 | ✅ | `docs/smoke_e2e_sample_output.txt`（冷跑复现步骤 + resolved target 说明） |
| Agent 工作流 | ✅ | `docs/agent_workflow.md`：分支规则、Agent 分工、目录可改范围、核心表契约、验收顺序 |
| 开发循环 | ✅ | `docs/dev_loop.md`：每轮 7 步（从 dev 切 feat/* → 实现 → smoke/test → self-review → Codex review → PR 到 dev → dev 合并 main） |
| Review Agent 验收 | ✅ | 四项任务 + 新做工作均在 `docs/REVIEW_AGENT_ACCEPTANCE.md` 通过 |

---

## 三、当前能“跑通”的端到端链路

- **入口**：`run_daily.py` 单日或区间；或直接跑 `smoke_e2e --start / --end`。
- **顺序**：ingest_universe（写 lake + 失败回退）→ ingest_prices（lake + DuckDB）→ build_features → score_all（V1–V4，配置化阈值/fallback）→ eval_5d → portfolio_nav → make_dashboard。
- **验证**：`PYTHONPATH=src python -m alpha_tracker2.pipelines.smoke_e2e --start <start> --end <end> --limit 5 --topk 3` 通过即表示五张核心表有数据、关键字段非空。
- **约定**：核心表契约、禁改规则、目录 ownership 见 `docs/agent_workflow.md`；每轮开发流程见 `docs/dev_loop.md`。

---

## 四、已知缺口与可选扩展（不阻塞当前主线）

- **策略/组合/执行**：`strategies/`、`portfolio/`（state/rebalancer/costs 抽离）、`execution/`、`nav_from_positions` 为可选或后续阶段。
- **特征与模型**：features_daily 的 bt_* 列与 V4 的 bt 接入；V5、score_ensemble（ENS）；eval_5d_batch。
- **评估与报告**：evaluation/metrics.py（夏普、回撤等）；Streamlit 是否为主线按项目决策。
- **技术债**：project_root/config 重复读取、交易日历改为真实交易所、阈值“滚动历史分位数”语义、单元/集成测试、文档与 schema 同步（bt_* 等）。

详见 `docs/SYSTEM_SELF_CHECK_REPORT.md` 的缺失模块与技术债小节。

---

## 五、规范与文档索引

| 文档 | 用途 |
|------|------|
| README.md | 架构、目录规划、数据流、实施步骤 |
| docs/agent_workflow.md | 分支规则、Agent 分工、目录可改、核心表契约、验收顺序 |
| docs/dev_loop.md | 每轮开发 7 步，与 agent_workflow 配套 |
| docs/REVIEW_AGENT_TASK.md | Review Agent 任务书（历史） |
| docs/REVIEW_AGENT_ACCEPTANCE.md | Review Agent 四项任务 + 新做工作验收结论 |
| docs/smoke_e2e_sample_output.txt | Smoke 样本输出与冷跑复现说明 |
| docs/SYSTEM_SELF_CHECK_REPORT.md | 架构符合性、缺失模块、潜在缺陷、技术债 |
| docs/PROJECT_PROGRESS.md | 本文档：项目进展总结 |

---

*文档更新后请同步维护本进展总结（特别是“已完成”与“已知缺口”）。*
