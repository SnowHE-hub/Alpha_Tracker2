# 批次 3 - Evaluation Agent 任务书：E-1 metrics + E-3 diagnostics + E-2 eval_5d_batch

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 任务 E-1、E-3、E-2，`docs/BATCH3_OVERVIEW.md`。  
> **执行方**：Evaluation Agent（或认领该角色的功能 Agent）。  
> **前置**：批次 2 已通过（picks_daily 含 V1–V4、ENS；eval_5d 单日评估与 eval_5d_daily 表已存在）。

---

## 一、任务概览

| 任务 ID | 名称 | 内容概要 |
|---------|------|----------|
| **E-1** | metrics.py | 实现 Sharpe、MDD、IC 等核心指标函数，供 E-2、E-3 及后续 D-1 调用 |
| **E-3** | diagnostics | 版本对比 + 因子分析，产出可喂给 E-2 与 D-1（如 CSV 或结构化数据） |
| **E-2** | eval_5d_batch | 区间内批量 5 日评估，产出分位收益与 IC 序列，供 D-1/D-2 使用 |

建议实现顺序：E-1 → E-3、E-2（E-3 与 E-2 可并行，E-2 调用 E-1）。

---

## 二、可修改范围

- **允许修改/新增**：
  - **evaluation/**：新增 `evaluation/metrics.py`（E-1）；新增 `evaluation/diagnostics.py` 或等价模块（E-3）；可选扩展 `evaluation/__init__.py` 导出。
  - **pipelines/**：新增 `pipelines/eval_5d_batch.py`（E-2）；可选在 run_daily 或 smoke_e2e 中增加对 eval_5d_batch 的调用（若需在每日/烟测中跑批量评估）。
  - **configs/default.yaml**：可选新增 `evaluation` 节点（如 batch 区间默认、分位数定义、IC 窗口等），仅新增不删已有字段。
  - **data/out/**：E-2、E-3 产出可写 CSV 或约定目录，供 D-1 读取；若写新表需同步 schema 与 agent_workflow 契约。
- **禁止**：
  - 修改 eval_5d_daily 表已有主键或已有列语义（可新增列或新增汇总表）；修改 run_daily 业务逻辑（仅允许增加编排调用）。
  - 破坏 eval_5d、forward_returns 的现有接口语义。

---

## 三、E-1：metrics.py（Sharpe、MDD、IC）

### 3.1 目标

- 在 **evaluation/metrics.py** 中实现可被 E-2、E-3、D-1 调用的**纯函数**（或小类）：
  - **Sharpe**：给定收益序列（如日度或 5 日 forward return 序列），计算年化夏普比率；参数可含无风险利率、年化因子（如 252 或 252/5）。
  - **MDD**：给定净值或收益序列，计算最大回撤（0～1 或百分比，约定一致即可）。
  - **IC**：给定同一截面上的**分数**（score）与**未来收益**（如 fwd_ret_5d），计算信息系数（Pearson 或 Spearman，在任务书中约定）；支持单截面与时间序列（多个 as_of_date 的 IC 组成序列）。
- 接口约定：函数签名与返回值在 docstring 中写明；输入为 pandas Series/DataFrame 或 list，输出为标量或约定结构，便于单元测试与 E-2/E-3 调用。

### 3.2 验收标准（E-1）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| **E1-1** | Sharpe 可用 | 给定收益序列（如全 0、或已知均值的序列），函数返回合理标量；单元测试断言已知序列的 Sharpe 与手算一致或与 scipy/stats 一致 |
| **E1-2** | MDD 可用 | 给定净值序列（或收益序列转换为净值），函数返回最大回撤；单元测试断言已知序列的 MDD 与手算一致 |
| **E1-3** | IC 可用 | 给定 (score, fwd_ret) 两列或两个 Series，函数返回 Pearson 或 Spearman IC；单元测试用固定数据断言 IC 值；支持“多日 IC 序列”的聚合或返回列表 |
| **E1-4** | 被 E-2 或 E-3 调用 | E-2 或 E-3 实现中至少一处调用 metrics 中的函数（如 E-2 算 IC 序列时调用 metrics.ic）；或由集成测试/文档说明调用关系 |

### 3.3 测试与真实数据（E-1）

- 单元测试：用**给定序列**（如 [0.01, -0.02, 0.015, ...]）或**真实数据子集**（至少 20 个交易日或 20 个截面）验证 Sharpe、MDD、IC；数据来源与复现方式在注释或文档中说明。
- 真实数据底线：见 NEXT_PHASE_PLAN 第五节；若仅用合成数据做单元测试，需在任务交付时说明，并有一项集成测试或 E-2 测试使用真实数据。

---

## 四、E-3：diagnostics（版本对比 + 因子分析）

### 4.1 目标

- **版本对比**：基于 picks_daily 与 eval_5d_daily（及可选 nav_daily），在给定日期或区间内，对比各 version（V1、V2、V3、V4、ENS）的选股重合度、收益表现（如 fwd_ret_5d 均值）、或使用 E-1 的 Sharpe/MDD（若为区间净值）。
- **因子分析**：基于 picks_daily 的 reason 或 features_daily，对影响 score 的因子（如 ret_5d、bt_mean 等）做简单分析（如分位数收益、因子与 fwd_ret 的 IC）；产出可为表格或 CSV，供 D-1 或报告使用。
- 实现形式：可为 **evaluation/diagnostics.py** 中的函数，或 **pipelines/run_diagnostics.py** 的 CLI；产出写 data/out 或约定路径，格式在任务书中约定（如 diagnostics_summary.csv、version_compare.csv）。

### 4.2 验收标准（E-3）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| **E3-1** | 版本对比产出存在 | 运行 diagnostics 后，产出包含“版本对比”信息（如各 version 的 fwd_ret_5d 均值、选股数、重合度等）；可为一张表或 CSV 多列 |
| **E3-2** | 因子分析产出存在 | 产出包含“因子分析”信息（如至少一个因子与 fwd_ret 的 IC 或分位收益）；可与版本对比同文件或分文件 |
| **E3-3** | 可被 E-2 或 D-1 消费 | 产出格式固定（列名或 schema 文档化），E-2 或 D-1 可读取该产出；或文档说明“供 D-1 使用”的路径与格式 |
| **E3-4** | 真实数据测试 | 在真实 picks_daily + eval_5d_daily（及可选 prices/features）上跑 diagnostics，产出无报错、数值合理 |

### 4.3 测试与真实数据（E-3）

- 单元测试：用 mock 或小 fixture 的 picks_daily、eval_5d_daily 调用 diagnostics，断言输出列或键存在。
- 集成/真实数据：在真实数据区间（至少 20 个交易日）跑 diagnostics，检查输出文件存在且内容合理。

---

## 五、E-2：eval_5d_batch（区间批量评估，分位收益 + IC 序列）

### 5.1 目标

- **区间批量**：在给定 **[start, end]** 内，对每个 as_of_date（或每个交易日）运行“单日 eval”逻辑（可复用 eval_5d 或 forward_returns），得到每个 as_of_date、version、bucket 的 fwd_ret_5d；可写入现有 eval_5d_daily（扩展 as_of_date 覆盖）或单独汇总表/CSV。
- **分位收益**：在截面维度上，按 score 分位（如 quintile）计算各分位的平均 fwd_ret_5d，产出“分位收益”表或 CSV（如 quintile 1～5 的收益）；可依赖 E-1 的指标做汇总。
- **IC 序列**：按 as_of_date 计算当日 score 与 fwd_ret_5d 的 IC，得到 **IC 时间序列**（as_of_date → IC）；可写入 CSV 或表，供 D-1 画图；计算时调用 E-1 的 IC 函数。
- **CLI**：支持 `--start`、`--end`、可选 `--versions`；可选 `--output` 指定输出路径；幂等（同一区间重复跑不重复插行，或覆盖写入）。

### 5.2 验收标准（E-2）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| **E2-1** | 区间内批量评估完成 | 指定 [start, end] 后，每个交易日（或每个 as_of_date）均有评估结果；结果写入 eval_5d_daily 或约定 CSV/表，且 as_of_date 覆盖区间内多个日期 |
| **E2-2** | 分位收益产出存在 | 产出包含“按 score 分位的平均 fwd_ret_5d”（如 quintile 1～5）；可为单独 CSV 或 eval 表扩展列；格式文档化 |
| **E2-3** | IC 序列产出存在 | 产出包含“as_of_date → IC”的序列（至少多日）；调用 E-1 的 IC 函数；可为 CSV 两列（as_of_date, ic）或等价 |
| **E2-4** | 真实数据测试 | 在真实数据区间（至少 20 个交易日、US+HK）跑 eval_5d_batch，无报错，分位收益与 IC 序列数值合理 |
| **E2-5** | 与现有 eval_5d 兼容 | 不破坏现有 eval_5d 单日逻辑与 eval_5d_daily 表主键；batch 可写同一表（多 as_of_date）或写约定产出路径 |

### 5.3 测试与真实数据（E-2）

- 单元测试：用 mock store 或小 fixture 跑 batch 逻辑，断言输出行数、列存在、IC 在 [-1,1]。
- 集成/真实数据：在真实 DB 上跑 eval_5d_batch（--start / --end），检查 eval_5d_daily 或产出 CSV 中多日数据、分位收益与 IC 序列存在且合理。

---

## 六、共同要求与交付物

### 6.1 规范

- 遵守 `docs/agent_workflow.md`（目录、核心表契约、禁改规则）与 `docs/dev_loop.md`（分支、自测、PR）。
- 不修改 eval_5d_daily 已有主键或列语义；若新增表或列，需在文档中说明并可选更新 agent_workflow 契约。

### 6.2 交付物

- **代码**：evaluation/metrics.py（E-1）；evaluation/diagnostics 或 run_diagnostics（E-3）；pipelines/eval_5d_batch.py（E-2）；可选 config 与 evaluation/__init__.py 更新。
- **自检清单**：E-1～E-2 验收项逐项自检结果，以及 pytest 命令与真实数据区间说明。

### 6.3 总控验收

- **E-1**：满足 3.2 节 E1-1～E1-4。
- **E-3**：满足 4.2 节 E3-1～E3-4。
- **E-2**：满足 5.2 节 E2-1～E2-5。
- 三项均通过则 Evaluation Agent 批次 3 交付通过，批次 3 完成。
