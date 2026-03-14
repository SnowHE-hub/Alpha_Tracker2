# 批次 2 - Scoring Agent 任务书：S-2 V4 接入 bt_* + S-4 ENS 投票集成

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 任务 S-2、S-4，`docs/BATCH2_OVERVIEW.md`。  
> **执行方**：Scoring Agent（或认领该角色的功能 Agent）。  
> **前置**：批次 1 已通过（features_daily 含 bt_* 且已写入；V1–V4 权重与 per-version 阈值来自 config）。

---

## 一、任务概览

| 任务 ID | 名称 | 内容概要 |
|---------|------|----------|
| **S-2** | V4 接入 bt_* | V4 使用 features_daily 的 bt_mean/bt_winrate/bt_worst_mdd 与 config 的 bt_column_weights 计算 bt_score，与 trend/risk 及 ma_bonus 一起参与最终 score；reason 含 bt_score 与 ma_bonus |
| **S-4** | ENS 投票集成 | 实现 score_ensemble（或等效逻辑），基于 V1–V4 的 picks_daily 做投票/聚合，写入 picks_daily(version='ENS')；run_daily 在 score_all 之后调用 ensemble 步骤 |

S-2 与 S-4 可由同一 Agent 在同一分支完成，一并交付与验收。

---

## 二、可修改范围

- **允许修改**：
  - `src/alpha_tracker2/scoring/plugins/v2_v3_v4.py`：V4 专用逻辑（或 _BaseTrendRiskScorer 在 model_name=='V4' 时分支）：拉取 features_daily 的 bt_* 列，按 `scoring.v2_v3_v4.bt_column_weights` 计算 bt_score；将 bt_score 与 ma_bonus 纳入最终 score 与 reason。
  - 新增 `src/alpha_tracker2/pipelines/score_ensemble.py`（或等效）：读 picks_daily 指定日期的 V1–V4（或 config 指定版本），做投票/聚合，写 picks_daily(version='ENS')。
  - `src/alpha_tracker2/pipelines/run_daily.py`：在 score_all 之后、eval_5d 之前增加一步调用 score_ensemble（或通过 --skip-ensemble 可选跳过）；**仅增加编排调用，不加入业务逻辑**。
  - `configs/default.yaml`：可选新增 `scoring.ensemble` 节点（如参与 ENS 的版本列表、聚合方式），仅新增不删已有字段。
  - `src/alpha_tracker2/pipelines/smoke_e2e.py`：若当前为逐步调用各 pipeline（而非 run_daily），需在 score_all 之后增加 score_ensemble 步骤，并在 picks_daily 检查中要求目标日 version='ENS' 至少有 1 行；若改为调用 run_daily 则 run_daily 内已含 ensemble，检查中需包含 ENS。
- **禁止**：
  - 修改 `storage/schema.sql` 中 picks_daily 主键或已有列语义（ENS 复用同一表，version='ENS'）。
  - 在 run_daily 中写入除“调用 score_ensemble.main”以外的业务逻辑。
  - 破坏 score_all 或 score_ensemble 的幂等写入（同 trade_date + version 先 DELETE 再 INSERT）。

---

## 三、S-2：V4 接入 bt_*（bt_score + ma_bonus）

### 3.1 目标

- **bt_score**：从 features_daily 读取 bt_mean、bt_winrate、bt_worst_mdd（V4 专用 fetch 或扩展 _fetch_features），按 `scoring.v2_v3_v4.bt_column_weights` 的权重做加权组合（如线性加权或先 z-score 再加权），得到标量 bt_score；当 bt_weight > 0 时参与 V4 最终 score。
- **ma_bonus**：在现有 trend 逻辑中已有“均线加成”（如 ma20 > ma60、ma5 > ma20 的 bonus）；在 V4 中明确其语义并在 reason 中体现；若当前已含在 trend_score 中，则 reason 中可注明“ma_bonus 已包含在 trend_score”或单独列出 ma_bonus 分量。
- **最终 score**：V4 的 score = trend_weight * trend_score - risk_weight * risk_penalty + bt_weight * bt_score（+ 若有单独 ma_bonus 项则加上）；当 bt_* 列缺失或全 NULL 时，bt_score 视为 0 或跳过，不报错。
- **reason**：V4 的 reason JSON 中需包含 bt_score、bt_mean/bt_winrate/bt_worst_mdd 的贡献或原始值（可选）、以及 ma_bonus 或说明其已含在 trend 中。

### 3.2 验收标准（S-2）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| **S2-1** | V4 使用 bt_* 参与打分 | 在 features_daily 有 bt_* 非空的前提下，跑 score_all 后 V4 的 score 与 reason 中 bt_score 存在且受 bt_column_weights 影响；若将 config 中 V4.bt_weight 置为 0，V4 score 与无 bt 时一致（或通过单元测试断言 bt_weight=0 时无 bt 项） |
| **S2-2** | bt_score 来自 bt_column_weights | 修改 bt_column_weights（如 bt_mean 权重改为 0.8），V4 的 score 或 reason 中 bt_score 变化；或单元测试用 mock features 含 bt_*，断言 bt_score 与权重一致 |
| **S2-3** | reason 含 bt_score 与 ma_bonus 说明 | picks_daily 中 V4 的 reason 为合法 JSON，含 bt_score 及（可选）bt_mean/bt_winrate/bt_worst_mdd；ma_bonus 或注明已含在 trend_score |
| **S2-4** | bt_* 缺失时不报错 | 当某 ticker 的 bt_* 全为 NULL 时，V4 仍能产出该 ticker 的 score（bt_score 视为 0 或跳过），不抛异常 |
| **S2-5** | 现有链路与 smoke 通过 | smoke_e2e 全链路通过；V1/V2/V3 行为不变；V4 写入 picks_daily 正常 |

### 3.3 测试与真实数据（S-2）

- 单元测试：用 mock 或真实 store 构造含 bt_* 的 features_daily，调用 V4 scorer，断言 score 与 reason 含 bt_score；bt_weight=0 时断言无 bt 项。
- 集成/真实数据：在真实数据上跑 build_features 再 score_all，检查 picks_daily 中 V4 的 reason 含 bt_score 且数值合理。

---

## 四、S-4：ENS 投票集成

### 4.1 目标

- **score_ensemble**：实现一个 pipeline（如 `pipelines/score_ensemble.py`），接受 `--date`（及可选 `--versions`），从 picks_daily 读取该日 V1、V2、V3、V4（或 config 指定的版本列表）的 picks，按约定规则聚合（如按 ticker 的 rank 投票、或 score_100 加权平均、或“至少 N 个版本选中”等），产出 ENS 的 ticker 列表与 score/rank；写入 picks_daily(trade_date, version='ENS', ticker, ...)，主键与现有契约一致。
- **契约**：ENS 行的 thr_value、pass_thr、picked_by 可为 NULL 或约定值（因 ENS 非单一阈值模型）；score/score_100/rank 需有合理赋值；reason 为合法 JSON，可描述聚合方式与各版本贡献。
- **run_daily 接入**：在 run_daily 中，在“4. score_all”之后、“5. eval_5d”之前，增加一步调用 score_ensemble（传入当日 date）；可通过 `--skip-ensemble` 跳过。run_daily 仅调用 `score_ensemble.main`，不写业务逻辑。
- **配置**：可选在 `configs/default.yaml` 中增加 `scoring.ensemble`（如 `input_versions: ["V1","V2","V3","V4"]`、聚合方式等）；若未配置则使用默认版本列表与默认聚合规则。

### 4.2 验收标准（S-4）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| **S4-1** | ENS 写入 picks_daily | 跑 run_daily（或先 score_all 再 score_ensemble）后，picks_daily 中存在 version='ENS' 且 trade_date 为目标日的行；行数 ≥1，且 key 字段（trade_date, version, ticker, score）非空 |
| **S4-2** | 聚合规则可复现 | ENS 的 score/rank 由 V1–V4 的 picks 确定；修改某版本 picks 后重跑 ensemble，ENS 结果变化；或单元测试用固定 picks 表断言 ENS 输出符合约定规则 |
| **S4-3** | run_daily 调用 ensemble | run_daily 在未加 --skip-ensemble 时，在 score_all 之后执行 score_ensemble；加 --skip-ensemble 时跳过 ensemble；不改变其他步骤顺序 |
| **S4-4** | smoke_e2e 含 ENS | smoke_e2e 全链路（含 run_daily 或等效六步+ensemble）通过；picks_daily 检查中 version='ENS' 在目标日至少有 1 行（若 smoke_e2e 当前不调 run_daily 而逐步调 pipeline，则需在步骤中加入 score_ensemble 并在检查中包含 ENS） |
| **S4-5** | 幂等 | 重复运行 score_ensemble 同日期不报错、不重复行（先 DELETE 该日 ENS 再 INSERT） |

### 4.3 测试与真实数据（S-4）

- 单元测试：用固定 picks_daily 数据（或 mock）调用 score_ensemble 逻辑，断言输出 ENS 行数与 score/rank 符合聚合规则。
- 集成/真实数据：在真实数据上跑 score_all 再 score_ensemble，检查 picks_daily 中 ENS 行存在且 reason 描述聚合方式。

---

## 五、共同要求与交付物

### 5.1 规范

- 遵守 `docs/agent_workflow.md`（目录、核心表契约、禁改规则）与 `docs/dev_loop.md`（分支、自测、PR）。
- 不修改 picks_daily 主键或已有列语义；ENS 复用同一表结构。
- run_daily 仅增加对 score_ensemble 的编排调用，不在 run_daily 内写投票/聚合逻辑。

### 5.2 交付物

- **代码**：上述可修改范围内的变更；若新增 score_ensemble.py，需提供 CLI（如 --date、可选 --versions）。
- **自检清单**：S-2 与 S-4 的验收项（S2-1～S2-5、S4-1～S4-5）逐项自检结果，以及 smoke_e2e 区间与 pytest 命令。

### 5.3 总控验收

- **S-2**：满足 3.2 节 S2-1～S2-5。
- **S-4**：满足 4.2 节 S4-1～S4-5。
- 两项均通过且 smoke_e2e 通过，则 Scoring Agent 批次 2 交付通过，批次 2 完成。
