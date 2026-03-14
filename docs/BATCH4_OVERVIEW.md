# 批次 4 总览：D-1 make_dashboard 扩展 + D-2 Streamlit 应用

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 批次 4。  
> **前置**：批次 3（E-1、E-3、E-2）已验收通过；eval_5d_batch 产出 quintile_returns.csv、ic_series.csv，diagnostics 产出 version_compare.csv、factor_analysis.csv。

---

## 一、批次范围与 Agent 分工

本批次**仅涉及 Dashboard Agent**，任务书为一份，内含 D-1 与 D-2 两段任务与各自验收标准。

| 任务 ID | 名称 | 负责 Agent | 任务书 |
|---------|------|------------|--------|
| **D-1** | make_dashboard 扩展 | **Dashboard Agent** | `docs/BATCH4_DASHBOARD_TASK.md` |
| **D-2** | Streamlit 应用 | **Dashboard Agent** | `docs/BATCH4_DASHBOARD_TASK.md` |

建议实现顺序：**先 D-1（扩展导出与数据契约），再 D-2（Streamlit 消费 data/out 或直连 DuckDB）**。D-2 可依赖 D-1 产出的 CSV 路径与列约定，也可在无 D-1 产出时直连 store 做最小可用版本。

---

## 二、依赖与执行顺序

- **D-1**：依赖 E-2 的 eval_5d_batch 产出（eval_5d_daily、quintile_returns.csv、ic_series.csv）及可选 E-3 的 version_compare.csv、factor_analysis.csv。make_dashboard 扩展后，一次运行可产出 nav、eval、picks、eval_summary、quintile/IC 等全部 CSV，供 D-2 与报告使用。
- **D-2**：依赖 D-1 约定的 data/out 文件与列格式（或直连 DuckDB）；实现 NAV 面板、picks 面板、评估面板（分位收益、IC 等），满足总控验收“Streamlit 启动并展示 NAV、picks、评估面板”。

---

## 三、验收通过条件（汇总）

- **D-1**：满足任务书中 D-1 验收标准（eval_summary / quantile 等扩展导出、路径与格式文档化、真实数据跑通）。
- **D-2**：满足任务书中 D-2 验收标准（Streamlit 可启动、NAV + picks + 评估面板展示正常、可读 data/out 或直连 store）。
- **共同**：遵守 agent_workflow、dev_loop；不破坏 make_dashboard 现有 nav/eval/picks 导出语义；真实数据要求见 NEXT_PHASE_PLAN 第五节。

---

## 四、文档索引

| 文档 | 用途 |
|------|------|
| `docs/NEXT_PHASE_PLAN.md` | 阶段规划与真实数据要求 |
| `docs/BATCH4_OVERVIEW.md` | 本总览 |
| `docs/BATCH4_DASHBOARD_TASK.md` | Dashboard Agent：D-1、D-2 任务与验收标准 |
