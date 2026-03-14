# 批次 3 总览：E-1 metrics + E-3 diagnostics + E-2 eval_5d_batch

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 批次 3。  
> **前置**：批次 2（S-2、S-4）已验收通过，picks_daily 含 V1–V4 与 ENS，eval_5d 单日评估已存在。

---

## 一、批次范围与 Agent 分工

| 任务 ID | 名称 | 负责 Agent | 任务书 |
|---------|------|------------|--------|
| **E-1** | metrics.py（Sharpe、MDD、IC） | **Evaluation Agent** | `docs/BATCH3_EVALUATION_TASK.md` |
| **E-3** | diagnostics（版本对比 + 因子分析） | **Evaluation Agent** | `docs/BATCH3_EVALUATION_TASK.md` |
| **E-2** | eval_5d_batch（区间批量评估，分位收益 + IC 序列） | **Evaluation Agent** | `docs/BATCH3_EVALUATION_TASK.md` |

本批次**仅涉及 Evaluation Agent**，E-1、E-3、E-2 可在同一任务书中一并下达，建议顺序：先 E-1（无依赖），再 E-3 与 E-2（E-2 可复用 E-1，E-3 产出可喂给 E-2 或 D-1）。

---

## 二、依赖与执行顺序

- **E-1**：无前置依赖；产出被 E-2、E-3 调用（Sharpe/MDD/IC 等函数）。
- **E-3**：可依赖 E-1（使用 metrics 计算版本级指标）；产出“版本对比、因子分析”可喂给 E-2 或 D-1。
- **E-2**：依赖 E-1（区间评估中计算分位收益、IC 序列时使用 metrics）；可选依赖 E-3（若 E-2 需读入 diagnostics 结果）；依赖现有 picks_daily、eval_5d_daily、forward_returns 及 S-4 完成后的 ENS。
- **建议顺序**：E-1 → E-3 与 E-2 可并行或 E-3 先于 E-2；E-2 实现时调用 E-1 的 metrics。

---

## 三、验收通过条件（汇总）

- **E-1**：满足任务书中 E-1 验收标准（Sharpe、MDD、IC 等函数可用、单元测试基于给定序列或真实数据子集通过）。
- **E-3**：满足任务书中 E-3 验收标准（版本对比与因子分析产出明确、可被 E-2 或 D-1 消费）。
- **E-2**：满足任务书中 E-2 验收标准（区间内批量 5 日评估、分位收益与 IC 序列产出、真实数据测试与可选 smoke 扩展）。
- **共同**：遵守 agent_workflow、dev_loop；真实数据要求见 NEXT_PHASE_PLAN 第五节。

---

## 四、文档索引

| 文档 | 用途 |
|------|------|
| `docs/NEXT_PHASE_PLAN.md` | 阶段规划与真实数据要求 |
| `docs/BATCH3_OVERVIEW.md` | 本总览 |
| `docs/BATCH3_EVALUATION_TASK.md` | Evaluation Agent：E-1、E-3、E-2 任务与验收 |
