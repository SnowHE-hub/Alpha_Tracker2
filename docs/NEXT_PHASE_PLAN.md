# 下一阶段任务规划（总控）

> **文档性质**：规划文档，非立刻分配任务。总控根据本规划在适当时机下发具体任务书给各 Agent。  
> **依据**：现有系统（docs/PROJECT_PROGRESS.md）、agent_workflow.md 分工、两张阶段规划图（任务拆解 + 依赖链）。

---

## 一、阶段目标与验收标准

### 1.1 阶段目标

- **版本目标**：本阶段完成后合并 dev → main，打 tag **v0.2.0**。
- **能力台阶**：
  - **配置与契约**：per-version 权重与 bt_* 列权重可配置；schema 与 config 对齐。
  - **特征与模型**：bt_* 特征工程落地，V1–V3 重设计（权重可配置 + reason JSON），V4 接入 bt_score + ma_bonus，按版本独立阈值，ENS 投票集成。
  - **评估与诊断**：metrics.py（Sharpe/MDD/IC），eval_5d_batch（分位收益 + IC 序列），diagnostics（版本对比 + 因子分析）。
  - **出口与可观测**：make_dashboard 扩展（eval_summary / quantile CSV），Streamlit 应用（NAV + picks + 评估面板）。
  - **质量**：各模块具备单元/集成测试，且**测试使用真实数据**（见第五节）；全链路 smoke_e2e + pytest + Streamlit 启动作为总控验收条件。

### 1.2 总控验收标准（全部通过方可合并 main 并打 tag）

- **smoke_e2e 全链路**：在约定区间内六步跑通，五张表有数据、关键字段非空。
- **pytest**：执行 `pytest -x`（或项目约定命令）全部通过，且测试基于真实数据或总控认可的“真实数据子集”。
- **Streamlit 启动**：`streamlit run apps/dashboard_streamlit/app.py`（或约定入口）能正常启动并展示 NAV、picks、评估面板。
- **合并与打 tag**：上述通过后，dev → main，打 tag v0.2.0。

---

## 二、Agent 角色与负责模块（现有 + 提案）

在现有 **总控 Agent**、**Review Agent**、**功能 Agent** 基础上，按规划图对**下一阶段**按领域划分执行角色（可与“功能 Agent”对应为同一实体，按任务书指定领域即可）：

| 角色 | 负责范围 | 对应规划图 | 说明 |
|------|----------|------------|------|
| **总控 Agent** | 任务拆解、验收标准、下发任务书、最终验收、合并与打 tag | — | 不直接改业务代码；本规划由其维护。 |
| **Review Agent** | 缺口修复、自检、smoke 与契约符合性 | — | 可与总控配合做阶段末验收前的补漏。 |
| **Infrastructure Agent** | I-1 配置与 schema、I-2 bt_* 特征工程、I-3 真实交易所日历 | 灰色边框任务 | 基础设施与契约层；改动需符合 agent_workflow 禁改规则。 |
| **Scoring Agent** | S-1 V1–V3 重设计、S-2 V4 接入 bt_*、S-3 按版本阈值、S-4 ENS 投票集成 | 绿色边框任务 | 模型层与 score_all/score_ensemble；依赖 I-1、I-2。 |
| **Evaluation Agent** | E-1 metrics.py、E-2 eval_5d_batch、E-3 diagnostics | 紫色边框任务 | 评估层；E-1/E-3 产出喂给 E-2，E-2 产出喂给 D-1/D-2。 |
| **Dashboard Agent** | D-1 make_dashboard 扩展、D-2 Streamlit 应用 | 橙色边框任务 | 报告与应用层；依赖 E-2 的批量评估与 CSV。 |
| **（可选）测试/质量 Agent** | pytest 脚手架、CI 脚本、真实数据 fixture 约定 | 共同责任（绿色背景） | 若总控认为需要统一测试框架与 CI，可单独设立；否则由各 Agent 各自补齐本模块测试。 |

**说明**：Infrastructure / Scoring / Evaluation / Dashboard 在实施时可由“功能 Agent”按任务书认领，无需强制四个不同人；总控按**任务**下发，执行方按**角色**对号入座即可。

---

## 三、任务清单与依赖关系

### 3.1 任务 ID 与概要

| ID | 名称 | 负责 Agent | 产出概要 | 强依赖 |
|----|------|------------|----------|--------|
| **I-1** | 配置与 schema 扩展 | Infrastructure | per-version 权重 + bt_* 列权重可配置；schema 新增 bt_* 列（若尚未有）并更新 agent_workflow 契约 | — |
| **I-2** | bt_* 特征工程 | Infrastructure | bt_mean / bt_winrate / bt_worst_mdd（或等价）在 features 中计算并写入 features_daily | I-1（若 schema 需扩展） |
| **I-3** | 真实交易所日历 | Infrastructure | US/HK 真实交易日历（如 pandas_market_calendars 或自建），替换当前工作日近似 | —（独立，随时可做） |
| **S-1** | V1–V3 重设计 | Scoring | 权重可配置 + reason JSON 结构化；与 config 对齐 | I-1 |
| **S-2** | V4 接入 bt_* | Scoring | bt_score + ma_bonus 接入 V4 打分与 reason | I-2 |
| **S-3** | 按版本阈值 | Scoring | q / window 各版本独立（config 支持 per-version） | I-1 |
| **S-4** | ENS 投票集成 | Scoring | score_ensemble 实现并接入 run_daily，写 picks_daily(version='ENS') | S-1,S-2 等（依赖各版本 picks） |
| **E-1** | metrics.py | Evaluation | Sharpe、MDD、IC 等核心指标函数，可被 E-2 与 diagnostics 调用 | — |
| **E-2** | eval_5d_batch | Evaluation | 区间内批量 5 日评估，分位收益 + IC 序列，产出供 D-1/D-2 使用 | E-1（数据喂入）；S-4 完成后链路完整 |
| **E-3** | diagnostics | Evaluation | 版本对比 + 因子分析，产出喂给 E-2 与 D-1 | E-1 |
| **D-1** | make_dashboard 扩展 | Dashboard | eval_summary / quantile CSV 等扩展导出 | E-2 |
| **D-2** | Streamlit 应用 | Dashboard | NAV + picks + 评估面板，可读 data/out 或直连 DuckDB | D-1（或直连 E-2 产出） |

**依赖链（与规划图一致）**：

- **强依赖（必须先完成）**：I-1 → S-1, S-3；I-2 → S-2；S-2 → S-4；S-4 → E-2；E-2 → D-1, D-2。E-1、E-3 可并行；E-1/E-3 产出**喂数据**给 E-2；E-3 也可喂 D-1。
- **独立**：I-3 无前后依赖，随时可做。

### 3.2 建议执行批次（供总控排期参考，非强制）

- **批次 0**：I-3（独立）；I-1（配置与 schema）。
- **批次 1**：I-2（bt_* 特征）；S-1、S-3（V1–V3 重设计 + 按版本阈值）。
- **批次 2**：S-2（V4 bt_*）；S-4（ENS）。
- **批次 3**：E-1、E-3（metrics + diagnostics）；E-2（eval_5d_batch）。
- **批次 4**：D-1（make_dashboard 扩展）；D-2（Streamlit）。
- **贯穿**：各 Agent 在各自任务交付前补齐该模块的单元/集成测试（见第五节）。

---

## 四、技术栈与实现约束

- **主语言**：Python（与现有仓库一致）；若某子模块有性能瓶颈，允许使用更高效实现（如 Cython、Rust 扩展、或 DuckDB SQL 内联），但需在任务书中说明并保证接口与测试可复现。
- **数据源**：本阶段仍以 **Yahoo 真实数据** 为主；所有**测试必须使用真实数据**（或总控/分任务 Agent 约定的真实数据子集），禁止仅用 mock 数据通过验收（详见第五节）。
- **契约**：任何 schema 或核心表字段变更须同步更新 `storage/schema.sql` 与 `docs/agent_workflow.md` 第四节核心表契约；遵守禁改规则（已有主键/列语义、run_daily 仅编排、幂等写入、registry/config）。

---

## 五、真实数据测试要求

### 5.1 原则

- **所有测试需基于真实数据**。时间周期与数据类型可由总控在本节给出底线，或由**分任务 Agent 在任务书中自行提议**，总控审批后写入该任务书。
- 若某 Agent 提议“用某区间、某 ticker 子集作为验收数据”，需在任务交付物中注明区间、数据来源（如 Yahoo）、以及如何复现（如 `ingest_prices --start ... --end ...` 后跑测试）。

### 5.2 总控建议底线（分任务 Agent 可更严，不可更松）

- **时间周期**：至少覆盖 **连续 20 个交易日** 以上的区间（建议 1～3 个月或更长，以便评估与 IC 有统计意义）；若为单元测试的 fixture，可为上述区间的**子集**，但需在文档中说明。
- **数据类型**：至少包含 **美股 + 港股** 标的（如 US 与 HK 各若干 ticker），与当前 universe 一致；价格与特征来源为 **真实 Yahoo 数据**（经 ingest_prices / build_features 写入 DuckDB 或 fixture）。
- **可复现**：测试运行前可通过现有 pipeline（或文档中的脚本）拉取并写入数据；或约定使用仓库内**允许提交的示例数据片段**（若采用，需在 docs 中说明并纳入 .gitignore 例外或单独数据仓库）。
- **自主决定**：若分任务 Agent 希望采用更长区间、更多 ticker、或额外市场，在任务书中写明并由总控认可即可。

### 5.3 各模块测试责任

- **Infrastructure Agent**：I-1/I-2/I-3 的单元测试（如日历接口、bt_* 计算与 schema 写入）；集成测试可为“在真实数据上跑 build_features 并检查 bt_* 列存在且合理”。
- **Scoring Agent**：V1–V4、ENS 的单元测试（含权重与阈值从 config 读取）；集成测试为“在真实 features_daily 上跑 score_all/score_ensemble，检查 picks_daily 与 reason JSON”。
- **Evaluation Agent**：E-1 metrics 单元测试（给定序列算 Sharpe/MDD/IC）；E-2/E-3 集成测试为“在真实 picks_daily + prices_daily 上跑 eval_5d_batch 与 diagnostics，检查输出结构与数值合理性”。
- **Dashboard Agent**：D-1 导出结果与 D-2 启动、基本渲染的集成测试；可用真实 data/out 或小型真实数据生成的 CSV。
- **总控验收**：`pytest -x` 覆盖上述测试；smoke_e2e 使用真实数据区间；Streamlit 启动检查为手动或自动化均可，由总控在验收清单中明确。

---

## 六、交付物与任务书引用

- **本规划**：`docs/NEXT_PHASE_PLAN.md`（本文档）。总控下发具体任务时，在任务书中引用“依据 NEXT_PHASE_PLAN 中任务 I-x / S-x / E-x / D-x”。
- **任务书模板**：每个任务书应包含：任务 ID、目标、验收标准、可改目录、依赖、真实数据要求（或“见 NEXT_PHASE_PLAN 第五节 + Agent 提议”）、自检与测试要求。
- **验收报告**：阶段结束时由总控（或 Review Agent 协助）编写阶段验收报告，记录各任务通过情况、smoke_e2e + pytest + Streamlit 结果、以及 v0.2.0 tag 信息。

---

## 七、与现有规范的衔接

- **分支与流程**：本阶段所有开发仍按 `docs/dev_loop.md` 执行（从 dev 切 feat/* → 实现 → smoke/test → self-review → Codex review → PR 到 dev）；v0.2.0 验收通过后 dev → main 并打 tag。
- **目录与禁改**：遵守 `docs/agent_workflow.md` 第三节（哪些目录谁能改）、第四节（核心表契约）、禁改规则；Infrastructure 改动 schema/config 时须同步更新 agent_workflow。
- **进度与缺口**：阶段中若发现与 `docs/PROJECT_PROGRESS.md` 不一致的缺口，由对应 Agent 在任务书中注明，总控决定是否纳入本阶段或顺延下一阶段。

---

**总控结论**：本规划为下一阶段任务内容与分工的**总体安排**，不代替具体任务分配。总控将在合适时机按批次与依赖关系下发各任务书，各 Agent 按任务书实现并自测，最终由总控按第一节验收标准执行验收，通过后合并 main 并打 tag v0.2.0。
