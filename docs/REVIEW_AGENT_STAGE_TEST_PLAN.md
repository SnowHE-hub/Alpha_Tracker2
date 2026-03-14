# Review Agent 阶段整体测试计划（v0.2.0 前）

> **文档性质**：总控下发，供 Review Agent 在 dev → main 合并、打 tag v0.2.0 前执行。  
> **依据**：`docs/NEXT_PHASE_PLAN.md`（Review Agent：缺口修复、自检、smoke 与契约符合性；可与总控配合做阶段末验收前的补漏）、`docs/PERVASIVE_TEST_ACCEPTANCE.md`。  
> **任务分配与验收标准**：见 **`docs/REVIEW_AGENT_STAGE_TASK.md`**（含真实数据要求、6 类验收项 REV-1～REV-11、报告模板）；**报告产出**：`docs/REVIEW_AGENT_STAGE_TEST_REPORT.md`。

---

## 一、是否要用 Review Agent 对现阶段整体项目进行测试

**结论：建议使用。**

- **NEXT_PHASE_PLAN** 明确：总控验收通过后合并 main 并打 v0.2.0；**Review Agent** 负责“缺口修复、自检、smoke 与契约符合性”，可与总控配合做**阶段末验收前的补漏**。
- 各批次（0～4）与贯穿测试已分别验收，但阶段末仍需一次**整体视角**的检查：全量测试、全链路 smoke、契约与禁改规则符合性、文档与可运行性，避免合并后暴露出跨模块或契约类问题。
- Review Agent 不替代总控的“通过/不通过”决策，而是**在总控验收清单基础上**执行上述检查并反馈缺口，由总控决定是否在合并前修复或记录为已知限制。

---

## 二、测试什么（范围）

| 类别 | 内容 | 目的 |
|------|------|------|
| **1. 全量 pytest** | 执行 `pytest tests/ -x`（或项目约定命令），全部通过 | 确认各模块单元/集成测试无回归，与 PERVASIVE_TEST_ACCEPTANCE 一致 |
| **2. smoke_e2e 全链路** | 在约定真实数据区间执行 smoke_e2e（如 `--start 2026-01-06 --end 2026-01-15`），七步 + 五表检查通过 | 确认从 ingest → features → score_all → score_ensemble → eval_5d → portfolio_nav 可复现，五张核心表有数据且关键字段非空 |
| **3. Streamlit 启动与展示** | `streamlit run apps/dashboard_streamlit/app.py` 能启动；NAV、picks、评估三块有展示且数据来源正确（data/out 或 store） | 确认应用层出口可用，与 BATCH4 验收一致 |
| **4. 契约符合性** | 核对 `storage/schema.sql` 与 `docs/agent_workflow.md` 第四节核心表契约一致（主键、关键字段、bt_*、eval_5d_daily、nav_daily 等）；无未文档化的表/列语义变更 | 避免 schema 与文档脱节，保证禁改规则可执行 |
| **5. run_daily 与禁改规则抽查** | 确认 `run_daily.py` 仅编排调用、无业务逻辑；registry/config 无硬编码；核心表无擅自改主键/列语义 | 与 agent_workflow 禁改规则一致 |
| **6. 文档与可复现性** | RUNBOOK/README 中 smoke_e2e、Streamlit、make_dashboard 的启动命令与数据准备步骤可执行；真实数据区间或 fixture 在文档中说明 | 便于后续维护与总控复验 |

---

## 三、怎么测试（执行方式）

### 3.1 自动化可执行部分

1. **pytest**  
   ```bash
   cd <project_root>
   PYTHONPATH=src pytest tests/ -x -v
   ```  
   预期：全部通过（或仅已知 skip）；若有新增失败，记录用例与错误信息。

2. **smoke_e2e**（须满足真实数据要求：至少 40 个交易日、US+HK、limit≥5，详见 `REVIEW_AGENT_STAGE_TASK.md` 第二节）  
   ```bash
   # 示例：2～3 个月区间、limit≥5
   PYTHONPATH=src python -m alpha_tracker2.pipelines.smoke_e2e --start 2024-01-02 --end 2024-03-28 --limit 8 --topk 3
   ```  
   预期：输出 “smoke_e2e: all steps and checks passed.”，退出码 0。若失败，记录失败步骤与报错。

3. **Streamlit 启动**  
   ```bash
   streamlit run apps/dashboard_streamlit/app.py --server.headless true --server.port 8504
   ```  
   预期：进程启动且无报错退出（可用 timeout 若干秒后结束）。若需人工查看页面，可去掉 headless 在本地浏览器打开。

### 3.2 人工/文档核对部分

4. **契约符合性**：逐表对照 `storage/schema.sql` 与 `docs/agent_workflow.md` 第四节，确认主键与关键字段描述一致，新增列（如 bt_*）已写入契约。  
5. **run_daily 与禁改抽查**：打开 `pipelines/run_daily.py`，确认仅调用各 pipeline 的 main；抽查 scoring/registry、config 使用处无硬编码版本名/路径。  
6. **文档可复现性**：按 README/RUNBOOK 执行“从零跑 smoke”或“跑 make_dashboard 再开 Streamlit”的步骤，确认命令与顺序可复现。

### 3.3 产出物

- **Review Agent 阶段测试报告**：**`docs/REVIEW_AGENT_STAGE_TEST_REPORT.md`**（模板已提供，见 `REVIEW_AGENT_STAGE_TASK.md` 第六节）。  
  - 上述 1～6 项逐项结论（通过 / 不通过 / 未执行及原因）；  
  - **真实数据**：实际使用的区间、limit、topk、市场覆盖须在报告中写明；  
  - 若不通过：具体缺口、复现步骤、建议修复方向；  
  - 总控可根据报告决定：合并前修复、或记录为已知限制并合并。

---

## 四、与总控验收的关系

- **总控**：负责“通过/不通过”决策、合并 main、打 tag v0.2.0；依据为 NEXT_PHASE_PLAN 第一节验收标准及各批次/贯穿验收报告。  
- **Review Agent**：负责执行本计划中的 1～6 项，输出阶段测试报告，**补漏**而非替代总控验收。  
- 若 Review Agent 发现缺口，总控可要求对应 Agent 修复后再合并，或将缺口列入 release notes / 已知限制后再合并。
