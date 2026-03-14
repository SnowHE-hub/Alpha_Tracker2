# 批次 1 总览：I-2 + S-1 + S-3

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 批次 1。  
> **前置**：批次 0（I-3、I-1）已验收通过（见 `docs/BATCH0_ACCEPTANCE.md`）。

---

## 一、批次范围与 Agent 分工

| 任务 ID | 名称 | 负责 Agent | 任务书 |
|---------|------|------------|--------|
| **I-2** | bt_* 特征工程 | **Infrastructure Agent** | `docs/BATCH1_INFRA_TASK.md` |
| **S-1** | V1–V3 重设计（权重可配置 + reason JSON） | **Scoring Agent** | `docs/BATCH1_SCORING_TASK.md` |
| **S-3** | 按版本阈值（q/window 各版本独立） | **Scoring Agent** | `docs/BATCH1_SCORING_TASK.md` |

- **Infrastructure Agent** 仅执行 I-2，按 `BATCH1_INFRA_TASK.md` 交付。
- **Scoring Agent** 执行 S-1 与 S-3，按 `BATCH1_SCORING_TASK.md` 交付；S-1 与 S-3 可同分支实现、一并验收。

---

## 二、依赖与执行顺序

- **I-2** 依赖批次 0 的 schema（features_daily 已有 bt_mean、bt_winrate、bt_worst_mdd 列）；与 S-1、S-3 **无强依赖**，可并行开发。
- **S-1、S-3** 依赖批次 0 的 config（scoring.v1.weights、scoring.v2_v3_v4.common 与 versions）；两者可并行或同序实现。
- **批次 1 验收**：I-2、S-1、S-3 三项均通过后，总控认定批次 1 完成；后续批次 2（S-2 V4 接入 bt_*、S-4 ENS）将依赖 I-2 与 S-1/S-3。

---

## 三、验收通过条件（汇总）

- **I-2**：满足 `BATCH1_INFRA_TASK.md` 所列验收标准（bt_* 计算与写入、真实数据测试、smoke_e2e 通过）。
- **S-1**：满足 `BATCH1_SCORING_TASK.md` 中 S-1 验收标准（V1/V2/V3/V4 权重从 config 读取、reason 含配置权重、改 config 后行为变化）。
- **S-3**：满足 `BATCH1_SCORING_TASK.md` 中 S-3 验收标准（V2/V3/V4 可配置独立 q、window，改 config 后 thr_value 按版本变化）。
- **共同**：`smoke_e2e` 全链路通过；pytest 新增/既有测试通过；遵守 `docs/agent_workflow.md` 与 `docs/dev_loop.md`。

---

## 四、文档索引

| 文档 | 用途 |
|------|------|
| `docs/NEXT_PHASE_PLAN.md` | 阶段规划与真实数据要求 |
| `docs/BATCH1_OVERVIEW.md` | 本总览 |
| `docs/BATCH1_INFRA_TASK.md` | Infrastructure Agent：I-2 任务与验收 |
| `docs/BATCH1_SCORING_TASK.md` | Scoring Agent：S-1、S-3 任务与验收 |
