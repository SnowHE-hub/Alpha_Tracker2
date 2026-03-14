# Review Agent 阶段整体测试 — 任务分配与验收标准

> **下发方**：总控 Agent  
> **执行方**：Review Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md`（Review Agent 职责、第五节真实数据要求）、`docs/REVIEW_AGENT_STAGE_TEST_PLAN.md`。  
> **目标**：在 dev → main 合并、打 tag v0.2.0 前，对现阶段整体项目执行一次基于**真实数据**的阶段测试，输出阶段测试报告，供总控做合并前补漏决策。

---

## 一、任务分配

| 项目 | 内容 |
|------|------|
| **角色** | Review Agent |
| **职责** | 缺口发现、自检执行、smoke 与契约符合性检查；**不替代**总控的通过/不通过决策，仅输出报告与缺口清单。 |
| **时机** | 各批次（0～4）及贯穿测试验收完成后、总控执行 v0.2.0 合并前。 |
| **产出** | `docs/REVIEW_AGENT_STAGE_TEST_REPORT.md`（阶段测试报告），格式见本文第六节。 |

---

## 二、真实数据要求（强制）

为提升验证真实性，**所有涉及数据的测试必须使用真实数据**，且数据期间与种类需满足以下要求。

### 2.1 数据期间

- **smoke_e2e**：至少覆盖 **连续 40 个交易日** 以上的区间（建议 2～3 个月）。  
  - 推荐示例：`--start 2024-01-02 --end 2024-03-28`（约 60 个交易日）或 `--start 2025-01-02 --end 2025-03-14`。  
  - **注意**：build_features 需要约 260 个交易日的历史窗口，因此若从空库跑 smoke，`--start` 须前推到足够早（如 `--start 2023-01-03 --end 2024-03-28`），或使用已有数据的 DB 且 start/end 落在已有价格区间内。  
- **make_dashboard / Streamlit 数据验证**：与 smoke_e2e 使用**同一区间**或包含该区间的更长区间，确保 data/out 内 nav、picks、eval、quintile_returns、ic_series 等具有多日、多版本数据。

### 2.2 数据种类

- **市场**：至少包含 **美股（US）与港股（HK）** 两类标的；universe 中需同时存在 US 与 HK ticker（如 AAPL、MSFT 与 0700.HK 等）。  
- **标的数量**：smoke_e2e 的 `--limit` 不少于 **5**（建议 8～10），以便 features_daily、picks_daily、eval_5d_daily、nav_daily 具有足够行数做检查。  
- **数据来源**：价格与 universe 须为 **真实 Yahoo 数据**，经 `ingest_universe`、`ingest_prices` 写入 DuckDB；禁止仅用纯 mock 或合成数据通过验收。

### 2.3 可复现性

- 在报告中标明实际使用的 **--start、--end、--limit、--topk** 及（若适用）universe 来源（如 config 中 universe_provider / static 列表）。  
- 总控或他人应能按报告中的命令与区间复现 smoke_e2e 与 make_dashboard 结果。

---

## 三、测试范围与验收标准（6 类）

### 3.1 全量 pytest

| 验收项 | 判定标准 | 不通过情形 |
|--------|----------|------------|
| **REV-1** | 全量 pytest 通过 | 执行 `PYTHONPATH=src pytest tests/ -x -v`，**全部通过**（或仅已知 skip，且在报告中列出）。 | 任一用例失败或未预期 skip。 |
| **真实数据** | 与真实数据相关的用例已运行 | 不要求本项单独使用“长区间”，但若项目中有标记为真实数据集成测试的用例，须包含在本次运行中。 | 人为排除真实数据相关用例未说明理由。 |

**执行命令**：
```bash
cd <project_root>
PYTHONPATH=src pytest tests/ -x -v
```
**预期**：输出无 FAILED；若有 SKIP，在报告中列出并说明是否为已知。

---

### 3.2 smoke_e2e 全链路

| 验收项 | 判定标准 | 不通过情形 |
|--------|----------|------------|
| **REV-2** | 七步全链路执行成功 | ingest_universe → ingest_prices → build_features → score_all → score_ensemble → eval_5d → portfolio_nav 无报错、退出码 0。 | 任一步骤 RuntimeError 或非 0 退出。 |
| **REV-3** | 五表检查通过 | prices_daily、features_daily、picks_daily（含 version='ENS'）、eval_5d_daily、nav_daily 在约定区间内存在、行数≥1、关键字段非空（见 smoke_e2e 内 _run_checks）。 | 任一表不存在或区间内无数据或关键字段大量 NULL。 |
| **真实数据** | 区间与种类符合第二节 | 使用满足 2.1、2.2 的区间与 limit（至少 40 个交易日、US+HK、limit≥5）。 | 区间过短或仅单市场或 limit 过小且未说明。 |

**执行命令（示例，按 2.1/2.2 调整）**：
```bash
# 示例：2～3 个月、limit=8、topk=3（需保证 start 前有足够历史供 build_features 窗口）
PYTHONPATH=src python -m alpha_tracker2.pipelines.smoke_e2e --start 2024-01-02 --end 2024-03-28 --limit 8 --topk 3
```
**预期**：终端输出 “smoke_e2e: all steps and checks passed.”，退出码 0。

---

### 3.3 Streamlit 启动与展示

| 验收项 | 判定标准 | 不通过情形 |
|--------|----------|------------|
| **REV-4** | 应用可启动 | `streamlit run apps/dashboard_streamlit/app.py` 启动无报错；可用 `--server.headless true` 做无头验证（若干秒内不崩溃）。 | 启动即报错或崩溃。 |
| **REV-5** | 三块展示存在且数据合理 | NAV、picks、评估（含分位收益、IC 序列或 eval_summary）三块均有展示；数据来自 data/out（需先执行 make_dashboard）或直连 store；若有 data/out，则至少 nav_daily、picks_daily、eval_summary/quintile_returns/ic_series 中若干非空。 | 缺块或数据全空且未说明（如首次未跑 make_dashboard）。 |

**执行命令**：
```bash
# 先确保 data/out 有数据（与 smoke 区间一致或包含）
PYTHONPATH=src python -m alpha_tracker2.pipelines.make_dashboard --start <与 smoke 一致的 start> --end <与 smoke 一致的 end>
streamlit run apps/dashboard_streamlit/app.py --server.headless true --server.port 8504
# 或人工在浏览器打开，检查 NAV / picks / 评估三块
```
**预期**：Streamlit 进程启动成功；若使用 headless，数秒内无异常退出。

---

### 3.4 契约符合性

| 验收项 | 判定标准 | 不通过情形 |
|--------|----------|------------|
| **REV-6** | schema 与契约文档一致 | `src/alpha_tracker2/storage/schema.sql` 与 `docs/agent_workflow.md` 中 **B. 核心表契约** 一致：各表主键、关键字段（含 bt_*、eval_5d_daily、nav_daily 等）描述与 schema 一致，无未文档化的表或列语义变更。 | 主键或关键字段不一致，或新增列未在契约中体现。 |

**执行方式**：人工逐表对照 schema.sql 与 agent_workflow B 节；在报告中列出核对过的表及结论。

---

### 3.5 run_daily 与禁改规则抽查

| 验收项 | 判定标准 | 不通过情形 |
|--------|----------|------------|
| **REV-7** | run_daily 仅编排 | `pipelines/run_daily.py` 仅解析参数并**按序调用**各 pipeline 的 main，无业务逻辑（如无模型计算、无阈值计算、无 SQL 拼接等）。 | 存在明显业务逻辑。 |
| **REV-8** | 无不当硬编码 | 抽查 scoring/registry、config 使用处：版本名、路径、API 等来自 config 或 registry，无在业务代码中写死版本列表或 DB 路径。 | 发现未通过 config/registry 的硬编码且影响契约或可维护性。 |
| **REV-9** | 核心表无擅自变更 | 未发现对 core 表主键或已有列语义的擅自修改（以 agent_workflow 禁改规则为准）。 | 存在未在任务书/契约中说明的主键或列语义变更。 |

**执行方式**：人工阅读 run_daily.py、scoring/registry 及若干 config 使用点；在报告中写明抽查范围与结论。

---

### 3.6 文档与可复现性

| 验收项 | 判定标准 | 不通过情形 |
|--------|----------|------------|
| **REV-10** | 命令可执行 | RUNBOOK/README 中关于 smoke_e2e、Streamlit、make_dashboard 的**启动命令与数据准备步骤**（如“先 ingest 再 build_features 再 smoke”）可执行且与当前代码一致。 | 文档命令报错或与实现不符。 |
| **REV-11** | 真实数据区间有说明 | 文档或本报告中明确写出用于阶段验收的**真实数据区间**（--start/--end）及数据来源、复现方式（如“执行 smoke_e2e --start ... --end ...”）。 | 未说明区间或无法按说明复现。 |

**执行方式**：按 README/RUNBOOK 执行一次“从零跑 smoke”或“make_dashboard + Streamlit”的完整流程，记录是否可复现；在报告中注明引用的文档与命令。

---

## 四、执行顺序建议

1. **真实数据准备**：按第二节选定区间与 limit，确认环境可访问 Yahoo 数据（或已有满足条件的 DuckDB）。  
2. **pytest**：执行 3.1，记录结果。  
3. **smoke_e2e**：执行 3.2，记录命令与输出。  
4. **make_dashboard**：对同一区间执行 make_dashboard，再启动 Streamlit，执行 3.3。  
5. **契约与禁改**：执行 3.4、3.5。  
6. **文档可复现**：执行 3.6。  
7. **编写报告**：按第六节填写 `REVIEW_AGENT_STAGE_TEST_REPORT.md`。

---

## 五、验收结论判定（总控用）

- **通过**：REV-1～REV-11 全部满足；真实数据满足第二节；报告完整。  
- **不通过**：任一 REV-* 不满足或真实数据不满足；报告中须写清缺口、复现步骤与建议修复方向。  
- 总控可根据报告决定：合并前修复、或记入已知限制后合并、或要求 Review Agent 补充检查。

---

## 六、阶段测试报告模板（产出物）

Review Agent 须新建或更新 **`docs/REVIEW_AGENT_STAGE_TEST_REPORT.md`**，结构如下。

```markdown
# Review Agent 阶段测试报告（v0.2.0 前）

## 1. 真实数据说明
- 区间：--start YYYY-MM-DD --end YYYY-MM-DD
- 标的：limit=?，topk=?，市场（US/HK）是否均覆盖
- 数据来源与复现：Yahoo 经 ingest_universe/ingest_prices；复现命令见下

## 2. 六类测试结果

### 2.1 全量 pytest（REV-1）
- 结论：通过 / 不通过
- 命令：PYTHONPATH=src pytest tests/ -x -v
- 备注：（若有 skip 或失败，列出用例与原因）

### 2.2 smoke_e2e 全链路（REV-2, REV-3）
- 结论：通过 / 不通过
- 命令：（实际执行的 smoke_e2e 完整命令）
- 输出摘要：（关键行或“all steps and checks passed”）
- 备注：（若失败，记录失败步骤与报错）

### 2.3 Streamlit（REV-4, REV-5）
- 结论：通过 / 不通过
- 执行方式：headless / 人工
- 备注：（若数据为空，是否已先 run make_dashboard 及区间）

### 2.4 契约符合性（REV-6）
- 结论：通过 / 不通过
- 已核对表：（列出）
- 备注：（若有不一致，列出表与差异）

### 2.5 run_daily 与禁改抽查（REV-7～REV-9）
- 结论：通过 / 不通过
- 抽查范围与结论：（简要）

### 2.6 文档与可复现性（REV-10, REV-11）
- 结论：通过 / 不通过
- 引用的文档与命令：（列出）
- 备注：（若不可复现，说明原因）

## 3. 缺口与建议（若有）
- 缺口 1：（描述）、复现步骤、建议修复方向
- …

## 4. 总控结论（由总控填写或引用）
- 阶段测试：通过 / 不通过
- 合并前动作：无需修复 / 需修复 xxx / 记入已知限制 xxx
```

---

## 七、文档索引

| 文档 | 用途 |
|------|------|
| `docs/REVIEW_AGENT_STAGE_TASK.md` | 本文档：任务分配与验收标准 |
| `docs/REVIEW_AGENT_STAGE_TEST_PLAN.md` | 测试计划（是否用 Review Agent、测试范围与方式） |
| `docs/REVIEW_AGENT_STAGE_TEST_REPORT.md` | Review Agent 产出：阶段测试报告 |
| `docs/NEXT_PHASE_PLAN.md` | 真实数据要求（第五节）、Review Agent 职责 |
