# 批次 4 - Dashboard Agent 任务书：D-1 make_dashboard 扩展 + D-2 Streamlit 应用

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 任务 D-1、D-2，`docs/BATCH4_OVERVIEW.md`。  
> **执行方**：Dashboard Agent（或认领该角色的功能 Agent）。  
> **前置**：批次 3 已通过（eval_5d_batch 产出 quintile_returns.csv、ic_series.csv；diagnostics 产出 version_compare.csv、factor_analysis.csv）。

---

## 一、任务概览

| 任务 ID | 名称 | 内容概要 |
|---------|------|----------|
| **D-1** | make_dashboard 扩展 | 在现有 nav/eval/picks 导出基础上，增加 eval_summary、分位收益/IC 等扩展导出，供 D-2 与报告使用 |
| **D-2** | Streamlit 应用 | 实现 `apps/dashboard_streamlit` 入口，展示 NAV、picks、评估面板（分位收益、IC 等），可读 data/out 或直连 DuckDB |

建议实现顺序：**D-1 → D-2**（D-2 消费 D-1 约定的 data/out 路径与列格式；亦可先做 D-2 最小版直连 store，再与 D-1 对齐）。

---

## 二、可修改范围

- **允许修改/新增**：
  - **pipelines/make_dashboard.py**：扩展 CLI 与导出逻辑，增加 eval_summary、quintile_returns、ic_series 等导出（可在此 pipeline 内调用 eval_5d_batch 与 run_diagnostics，或约定“先跑 eval_5d_batch / run_diagnostics 再跑 make_dashboard”，由实现方选择并文档化）。
  - **reporting/**：可新增或扩展 `reporting/dashboard_data.py` 等，用于聚合 eval_5d_daily、ic_series 等生成 eval_summary；不得破坏现有 load_nav_for_dashboard、load_eval_for_dashboard、load_picks_for_dashboard 的接口语义。
  - **apps/**：新增 `apps/dashboard_streamlit/` 目录及入口 `app.py`（D-2）；可含多页或多 section（NAV / picks / 评估）。
  - **configs/default.yaml**：可选新增 `reporting` 或 `dashboard` 节点（如 data/out 子路径、默认日期范围），仅新增不删已有字段。
  - **data/out/**：D-1 产出 CSV 写至 config 约定的 out_dir（如 data/out），列名与格式在本文档或 agent_workflow 中约定。
- **禁止**：
  - 修改 nav_daily、eval_5d_daily、picks_daily 等核心表的主键或已有列语义。
  - 破坏 make_dashboard 现有对 nav_daily.csv、eval_5d_daily.csv、picks_daily.csv 的导出行为（可增加新文件，不可删除或改列语义已有导出）。
  - 在 run_daily 中加入 D-1/D-2 特有业务逻辑（仅允许在 run_daily 中增加“可选调用 make_dashboard”的编排，若总控后续要求）。

---

## 三、D-1：make_dashboard 扩展（eval_summary / quantile CSV）

### 3.1 目标

- **eval_summary**：在给定 [start, end] 内，按 version 聚合评估结果，产出**单文件**（如 `eval_summary.csv`），列至少包含：version、mean_fwd_ret_5d（或等价）、mean_ic、n_dates；可来自 eval_5d_daily 与 ic_series（或 eval_5d_batch 已写出的 ic_series.csv）的聚合，供 D-2 与报告使用。
- **分位收益 / IC 导出**：确保 **quintile_returns.csv**、**ic_series.csv** 在 make_dashboard 执行后存在于约定 out_dir（如 data/out）。实现方式二选一或兼容均可：
  - **方式 A**：make_dashboard 在给定 --start/--end 时内部调用 eval_5d_batch，使其写入 quintile_returns.csv、ic_series.csv 到同一 out_dir；
  - **方式 B**：约定“先执行 eval_5d_batch 再执行 make_dashboard”，make_dashboard 仅负责将已有 quintile_returns.csv、ic_series.csv 复制或聚合到统一 out_dir，并生成 eval_summary.csv。
- **可选**：若需版本对比与因子分析在“一键导出”中体现，可在此步骤内调用 run_diagnostics 或将其产出路径约定为同一 out_dir（version_compare.csv、factor_analysis.csv），并在文档中说明。
- **CLI**：保持现有 --start、--end、--date、--out-dir；行为为在导出 nav/eval/picks 基础上，增加上述扩展导出；幂等（重复运行不产生重复行，或覆盖写入）。

### 3.2 验收标准（D-1）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| **D1-1** | eval_summary 产出存在 | 执行 make_dashboard --start / --end 后，out_dir 下存在 eval_summary.csv；列至少含 version、mean_fwd_ret_5d（或等价）、mean_ic、n_dates；格式在任务书或 reporting 模块 docstring 中文档化 |
| **D1-2** | 分位/IC 导出可用 | 执行后 out_dir 下存在 quintile_returns.csv、ic_series.csv（列与 E-2 任务书约定一致：如 quintile_returns 含 as_of_date, version, quintile, mean_fwd_ret_5d, n_stocks；ic_series 含 as_of_date, version, ic）；可为 make_dashboard 调用 eval_5d_batch 生成，或约定先跑 eval_5d_batch 再跑 make_dashboard 并在文档中写明 |
| **D1-3** | 现有导出不变 | 原有 nav_daily.csv、eval_5d_daily.csv、picks_daily.csv 的导出行为、列名与语义不变；pytest 或现有 smoke 中涉及 make_dashboard 的部分仍通过 |
| **D1-4** | 真实数据测试 | 在真实数据区间（至少 20 个交易日）执行 make_dashboard，无报错；eval_summary、quintile_returns、ic_series 数值合理（如 mean_ic 在 [-1,1]，分位收益为小数） |

### 3.3 测试与真实数据（D-1）

- 单元/集成测试：用 fixture 或真实 data/out 调用 make_dashboard 导出逻辑，断言 eval_summary.csv、quintile_returns.csv、ic_series.csv 存在且列名符合约定。
- 真实数据底线：见 NEXT_PHASE_PLAN 第五节；验收时在真实 DB 与区间上跑 make_dashboard，检查上述文件存在且内容合理。

---

## 四、D-2：Streamlit 应用（NAV + picks + 评估面板）

### 4.1 目标

- **入口**：在仓库内提供 Streamlit 应用入口，约定为 `streamlit run apps/dashboard_streamlit/app.py`（或 README/验收文档中约定的路径）。启动后可在浏览器中查看 dashboard。
- **NAV 面板**：展示组合净值（nav_daily）；数据来源为 data/out/nav_daily.csv 或直连 DuckDB 的 nav_daily 表；支持按 portfolio 筛选或分版本展示；至少包含净值曲线或表格。
- **picks 面板**：展示选股列表（picks_daily）；数据来源为 data/out/picks_daily.csv 或直连 store；支持按 trade_date、version 筛选；至少包含日期、版本、标的、分数/排名等关键列。
- **评估面板**：展示评估结果，至少包含（1）分位收益（quintile_returns：如 quintile 1～5 的 mean_fwd_ret_5d）；（2）IC 序列（as_of_date → IC）；数据来源为 data/out/quintile_returns.csv、ic_series.csv 或 eval_summary.csv，或直连 store / eval_5d_daily。可选：版本对比（version_compare）、因子分析（factor_analysis）的简要展示。
- **数据源约定**：可读 data/out 下 CSV（与 D-1 产出一致），或直连 DuckDB；若直连，需在文档中说明并保证与 data/out 列语义一致。优先支持“先 make_dashboard 再启动 Streamlit”的用法，以便总控验收时一键导出 + 启动。

### 4.2 验收标准（D-2）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| **D2-1** | Streamlit 可启动 | 执行 `streamlit run apps/dashboard_streamlit/app.py` 无报错，浏览器可打开页面；或项目约定命令在验收文档中写明 |
| **D2-2** | NAV 面板展示 | 页面中存在 NAV/净值相关展示（曲线或表格），数据来自 nav_daily（CSV 或 store），可区分 portfolio 或 version |
| **D2-3** | picks 面板展示 | 页面中存在选股列表展示，数据来自 picks_daily（CSV 或 store），可按日期、版本筛选 |
| **D2-4** | 评估面板展示 | 页面中存在评估相关展示：至少包含分位收益（quintile）与 IC 序列（或 eval_summary 中的 mean_ic）；数据来自 data/out 或 store |
| **D2-5** | 与 D-1 产出兼容 | 当 data/out 下已有 D-1 产出的 CSV（nav_daily、eval_summary、quintile_returns、ic_series 等）时，Streamlit 能正确读取并展示；或文档明确“仅直连 store”时的数据要求 |

### 4.3 测试与真实数据（D-2）

- 集成测试：可选用真实 data/out（由 make_dashboard 或 eval_5d_batch 生成）启动 Streamlit，或使用 pytest + streamlit 的 headless 检查（若项目引入）；至少有一项验收为“启动成功 + 关键组件存在”。
- 总控验收：按 NEXT_PHASE_PLAN 第一节，Streamlit 启动并展示 NAV、picks、评估面板作为阶段验收条件之一。

---

## 五、共同要求与交付物

### 5.1 规范

- 遵守 `docs/agent_workflow.md`（目录、核心表契约、禁改规则）与 `docs/dev_loop.md`（分支、自测、PR）。
- 新增 CSV 列名与路径在本文档或 reporting 模块中文档化，便于 D-2 与后续报告消费。

### 5.2 交付物

- **代码**：扩展后的 make_dashboard.py 与 reporting 相关改动（D-1）；apps/dashboard_streamlit/app.py 及必要依赖（D-2）。
- **自检清单**：D-1、D-2 验收项逐项自检结果；make_dashboard 与 Streamlit 的启动命令、数据准备步骤（如“先跑 eval_5d_batch”“先跑 make_dashboard”）在 README 或 RUNBOOK 中说明。

### 5.3 总控验收

- **D-1**：满足 3.2 节 D1-1～D1-4。
- **D-2**：满足 4.2 节 D2-1～D2-5。
- 两项均通过则 Dashboard Agent 批次 4 交付通过，批次 4 完成。
