# Review Agent 阶段测试报告（v0.2.0 前）

> **执行方**：Review Agent  
> **依据**：`docs/REVIEW_AGENT_STAGE_TASK.md`  
> **填写说明**：按任务书第六节模板填写；总控根据本报告做合并前决策。

---

## 1. 真实数据说明

- **区间**：`--start` **2023-01-03**  `--end` **2024-03-28**
- **标的**：`limit`=**8**，`topk`=**3**；市场（US / HK）是否均覆盖：**是**（universe 当前为 config 静态列表：AAPL、MSFT（US）、0700.HK（HK）；实际写入 3 只，US+HK 均有）
- **数据来源与复现**：Yahoo 经 ingest_universe / ingest_prices（yfinance）；复现命令：
  ```bash
  cd <project_root>
  PYTHONPATH=src python -m alpha_tracker2.pipelines.smoke_e2e --start 2023-01-03 --end 2024-03-28 --limit 8 --topk 3
  ```
- **说明**：区间覆盖超过 40 个交易日；build_features 需约 260 日历史，故 `--start` 取 2023-01-03。smoke_e2e 会按 `prices_daily` 实际最大 trade_date 解析目标日（本次为 2024-03-27）。

---

## 2. 六类测试结果

### 2.1 全量 pytest（REV-1）

- **结论**：**通过**
- **命令**：`PYTHONPATH=src pytest tests/ -x -v`
- **备注**：60 passed, 1 skipped。跳过用例：`tests/test_eval_5d_batch.py::test_integration_eval_5d_batch_real_data`（标记为真实数据集成测试，需真实 DB/区间，属已知 skip）。

### 2.2 smoke_e2e 全链路（REV-2, REV-3）

- **结论**：**通过**
- **命令**：
  ```bash
  PYTHONPATH=src python -m alpha_tracker2.pipelines.smoke_e2e --start 2023-01-03 --end 2024-03-28 --limit 8 --topk 3
  ```
- **输出摘要**：
  - ingest_universe → ingest_prices → ingest_universe(resolved date) → build_features → score_all → score_ensemble → eval_5d → portfolio_nav 全部执行无报错。
  - `smoke_e2e: using resolved target_date=2024-03-27 (prices_daily max in range; requested end=2024-03-28)`
  - `smoke_e2e: all steps and checks passed.`
  - 退出码 0。
- **备注**：五表（prices_daily, features_daily, picks_daily, eval_5d_daily, nav_daily）在约定区间内存在、行数≥1、关键字段非空；picks_daily 含 version='ENS'（score_ensemble 已执行）。

### 2.3 Streamlit（REV-4, REV-5）

- **结论**：**通过**
- **执行方式**：headless（`streamlit run apps/dashboard_streamlit/app.py --server.headless true --server.port 8504`，约 15 秒内未崩溃）
- **备注**：先执行 `make_dashboard --start 2023-01-03 --end 2024-03-28 --no-batch-eval` 生成 data/out（nav_daily.csv、picks_daily.csv、eval_5d_daily.csv、eval_summary.csv、quintile_returns.csv、ic_series.csv 等）。应用启动正常；NAV、picks、评估三块数据来源具备（data/out 非空）。

### 2.4 契约符合性（REV-6）

- **结论**：**通过**
- **已核对表**：prices_daily, features_daily, picks_daily, eval_5d_daily, nav_daily, meta_runs
- **备注**：`storage/schema.sql` 与 `docs/agent_workflow.md` B 节逐表对照。主键一致：prices_daily (trade_date, ticker)、features_daily (trade_date, ticker)、picks_daily (trade_date, version, ticker)、eval_5d_daily (as_of_date, version, bucket)、nav_daily (trade_date, portfolio)。schema 中 nav_daily 为 (trade_date, portfolio)，契约允许“或 (trade_date, portfolio)”；features_daily 已含 bt_mean, bt_winrate, bt_worst_mdd 列，与契约一致。无未文档化的表或列语义变更。

### 2.5 run_daily 与禁改抽查（REV-7～REV-9）

- **结论**：**通过**
- **抽查范围与结论**：
  - **REV-7**：`pipelines/run_daily.py` 仅解析参数、按序调用各 pipeline 的 main（ingest_universe, ingest_prices, build_features, score_all, score_ensemble, eval_5d, portfolio_nav, make_dashboard），无业务逻辑（无模型计算、无阈值计算、无 SQL 拼接）。
  - **REV-8**：score_all 通过 `scoring/registry.get_scorer`、`list_versions` 及 config `scoring.score_versions` / `scoring.v2_v3_v4` 获取版本与阈值；未在业务代码中写死版本列表或 DB 路径。
  - **REV-9**：未发现对 core 表主键或已有列语义的擅自修改；以 agent_workflow 禁改规则为准。

### 2.6 文档与可复现性（REV-10, REV-11）

- **结论**：**通过**
- **引用的文档与命令**：
  - README.md：数据准备“先 run_daily 或单独 make_dashboard”；`PYTHONPATH=src python -m alpha_tracker2.pipelines.make_dashboard --start YYYY-MM-DD --end YYYY-MM-DD`；`PYTHONPATH=src streamlit run apps/dashboard_streamlit/app.py`。
  - 本报告第一节已给出 smoke_e2e 真实数据区间与复现命令。
- **备注**：README 中 make_dashboard、Streamlit 启动命令与当前实现一致；从零跑 smoke 可按报告命令复现（需网络访问 Yahoo）。无 RUNBOOK.md，README 已涵盖主要运行步骤。

---

## 3. 缺口与建议（若有）

- **无**。REV-1～REV-11 均满足；真实数据满足任务书第二节（≥40 交易日、US+HK、limit≥5、Yahoo 真实数据）。

---

## 4. 总控结论（由总控填写或引用）

- **阶段测试**：（待总控填写）通过 / 不通过
- **合并前动作**：（待总控填写）无需修复 / 需修复 xxx / 记入已知限制 xxx
