# 批次 2 总览：S-2 V4 接入 bt_* + S-4 ENS 投票集成

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 批次 2。  
> **前置**：批次 1（I-2、S-1、S-3）已验收通过（见 `docs/BATCH1_ACCEPTANCE.md`）。

---

## 一、批次范围与 Agent 分工

| 任务 ID | 名称 | 负责 Agent | 任务书 |
|---------|------|------------|--------|
| **S-2** | V4 接入 bt_*（bt_score + ma_bonus） | **Scoring Agent** | `docs/BATCH2_SCORING_TASK.md` |
| **S-4** | ENS 投票集成 | **Scoring Agent** | `docs/BATCH2_SCORING_TASK.md` |

本批次**仅涉及 Scoring Agent**，S-2 与 S-4 可在同一任务书中一并下达，同分支实现、一并验收。

---

## 二、依赖与执行顺序

- **S-2** 依赖批次 1 的 I-2（features_daily 已写入 bt_mean、bt_winrate、bt_worst_mdd）及 I-1 的 `scoring.v2_v3_v4.bt_column_weights`、V4 的 bt_weight。
- **S-4** 依赖 S-2（以及 S-1、S-3）：需在 V1–V4 均写入 picks_daily 后，对多版本结果做投票/聚合并写 ENS。
- **建议顺序**：先完成 S-2（V4 使用 bt_* 与 ma_bonus），再实现 S-4（score_ensemble 读 V1–V4 写 ENS）；可同序开发、一起提 PR。

---

## 三、验收通过条件（汇总）

- **S-2**：满足 `BATCH2_SCORING_TASK.md` 中 S-2 验收标准（V4 使用 bt_* 与 bt_column_weights 参与打分、reason 含 bt_score、ma_bonus 语义明确；改 config 后 V4 行为变化）。
- **S-4**：满足 `BATCH2_SCORING_TASK.md` 中 S-4 验收标准（score_ensemble 或等效步骤产出 version='ENS' 的 picks_daily；run_daily 在 score 后调用 ensemble；smoke_e2e 含 ENS）。
- **共同**：smoke_e2e 全链路通过；pytest 新增/既有测试通过；遵守 `docs/agent_workflow.md` 与 `docs/dev_loop.md`。

---

## 四、文档索引

| 文档 | 用途 |
|------|------|
| `docs/NEXT_PHASE_PLAN.md` | 阶段规划与真实数据要求 |
| `docs/BATCH2_OVERVIEW.md` | 本总览 |
| `docs/BATCH2_SCORING_TASK.md` | Scoring Agent：S-2、S-4 任务与验收 |
