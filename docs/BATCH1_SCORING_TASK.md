# 批次 1 - Scoring Agent 任务书：S-1 V1–V3 重设计 + S-3 按版本阈值

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 任务 S-1、S-3，`docs/BATCH1_OVERVIEW.md`。  
> **执行方**：Scoring Agent（或认领该角色的功能 Agent）。  
> **前置**：批次 0 已通过（config 中已有 scoring.v1.weights、scoring.v2_v3_v4.common 与 versions）。

---

## 一、任务概览

| 任务 ID | 名称 | 内容概要 |
|---------|------|----------|
| **S-1** | V1–V3 重设计 | V1/V2/V3/V4 权重从 config 读取；reason JSON 含实际使用的配置权重 |
| **S-3** | 按版本阈值 | V2/V3/V4 的 q、window（及可选 topk_fallback）支持 per-version 配置，score_all 使用 per-version 阈值 |

S-1 与 S-3 可由同一 Agent 在同一分支完成，一并交付与验收。

---

## 二、可修改范围

- **允许修改**：
  - `src/alpha_tracker2/scoring/plugins/v1_baseline.py`：V1 从 config 加载 weights 构造 V1Config（保留默认 fallback）。
  - `src/alpha_tracker2/scoring/plugins/v2_v3_v4.py`：V2/V3/V4 从 config 加载 trend_weight、risk_weight、bt_weight 构造 CoreTrendRiskConfig。
  - `src/alpha_tracker2/scoring/registry.py` 和/或 `src/alpha_tracker2/pipelines/score_all.py`：在获取 scorer 时注入从 config 读取的配置（如 get_scorer(version, project_root) 或 registry 内部读 config）；score_all 中为 V2/V3/V4 按版本加载 q、window、topk_fallback。
  - `configs/default.yaml`：仅新增或调整 scoring 下已有节点，不删除已有字段（per-version 的 q、window 若尚未有可新增）。
- **禁止**：
  - 修改 `storage/schema.sql`、picks_daily 主键或已有列语义。
  - 在 `run_daily.py` 中加入业务逻辑。
  - 破坏 score_all 的幂等写入（同 trade_date + version 先 DELETE 再 INSERT）。

---

## 三、S-1：V1–V3 重设计（权重可配置 + reason JSON）

### 3.1 目标

- **V1**：从 `configs/default.yaml` 的 `scoring.v1.weights` 读取因子权重（如 ret_5d, ret_20d, avg_amount_20）；若缺省则使用当前代码默认权重。构造 `V1Config(weights=...)` 并传给 V1BaselineScorer；**reason JSON** 中应包含实际使用的 weights（已实现则可保持）。
- **V2/V3/V4**：从 `scoring.v2_v3_v4.versions.V2/V3/V4` 读取 `trend_weight`、`risk_weight`、`bt_weight`；若某版本缺省则使用 common 或代码默认。构造 `CoreTrendRiskConfig` 并传给对应 Scorer；**reason JSON** 中应包含实际使用的 trend_weight、risk_weight、bt_weight（已实现则可保持）。
- **契约**：不改变 V1 无阈值、无 fallback 的语义；不改变 V2/V3/V4 的阈值/fallback 调用方式（阈值逻辑在 S-3 中按版本区分）。

### 3.2 验收标准（S-1）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| **S1-1** | V1 权重来自 config | 修改 `scoring.v1.weights`（如将 ret_5d 改为 0.6、ret_20d 改为 0.2），重新跑 `score_all --date <某日>`，picks_daily 中 V1 的 score 或 reason 中 weights 与 config 一致；或通过单元测试“加载 config 并实例化 V1BaselineScorer，断言 score 受权重影响” |
| **S1-2** | V2/V3/V4 权重来自 config | 修改 `scoring.v2_v3_v4.versions.V2.trend_weight`（或 V3/V4），重新跑 score_all，picks_daily 中该版本的 score/reason 反映新权重；或单元测试实例化 V2Scorer 等时传入从 config 读取的 config，断言 reason 含对应 trend_weight/risk_weight |
| **S1-3** | reason JSON 结构化且含权重 | picks_daily 中 V1 的 reason 为合法 JSON，且含 factors/weights 或等价字段；V2/V3/V4 的 reason 含 trend_weight、risk_weight、bt_weight（与当前配置一致） |
| **S1-4** | 现有链路不受损 | smoke_e2e 全链路通过；score_all 仍按 score_versions 调度 V1–V4，写入 picks_daily 无报错 |

### 3.3 测试与真实数据（S-1）

- 单元测试：加载 `default.yaml`，解析 `scoring.v1.weights` 与 `scoring.v2_v3_v4.versions`，构造 V1Config / CoreTrendRiskConfig，断言与预期一致；可选：用真实 store 或 mock 跑 scorer.score()，断言 reason 含配置权重。
- 集成/真实数据：在真实数据上跑 score_all（至少 20 个交易日、US+HK），检查 picks_daily 中 V1/V2/V3/V4 的 reason 字段为合法 JSON 且含权重信息。

---

## 四、S-3：按版本阈值（q / window 各版本独立）

### 4.1 目标

- 在 `score_all` 中，对 **V2、V3、V4** 不再共用单一 `ThresholdConfig(q, window)` 与单一 `fallback_topk`，而是**按版本**从 config 读取：
  - 优先使用 `scoring.v2_v3_v4.versions.<V2|V3|V4>.q`、`.window`、可选 `.topk_fallback`；
  - 若某版本未配置则回退到 `scoring.v2_v3_v4.common` 的 q、window、topk_fallback。
- V1 仍无阈值、无 fallback，逻辑不变。
- 每个版本使用自己的 `thr_value`（由 get_threshold 基于该版本的 q、window 与历史 scores 计算），写入 picks_daily 的 thr_value、pass_thr、picked_by 与当前语义一致。

### 4.2 配置约定

- 在 `configs/default.yaml` 的 `scoring.v2_v3_v4.versions` 下，可为 V2/V3/V4 增加可选键：
  - **q**：分位数阈值（如 0.8）
  - **window**：历史窗口天数（如 60）
  - **topk_fallback**：可选，fallback 时取 top-K
- 若某版本未指定 q/window/topk_fallback，则使用 `common` 中对应值。
- 示例（执行方可在 default.yaml 中补充，便于 S3-1 验收）：
  ```yaml
  versions:
    V2: { trend_weight: 0.4, risk_weight: 0.3, bt_weight: 0.0, q: 0.8, window: 60 }
    V3: { trend_weight: 0.35, risk_weight: 0.35, bt_weight: 0.0, q: 0.7, window: 60 }
    V4: { trend_weight: 0.3, risk_weight: 0.3, bt_weight: 0.4, q: 0.8, window: 60 }
  ```
  验收时可通过改为 V2.q: 0.9、V3.q: 0.7 等验证 per-version 行为。

### 4.3 验收标准（S-3）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| **S3-1** | 按版本读取 q、window | 在 config 中设置 `versions.V2.q: 0.9`、`versions.V3.q: 0.7`（或 window 不同），运行 score_all 后，picks_daily 中 V2 与 V3 的 thr_value 不同（或通过单元测试断言 per-version ThresholdConfig 被正确加载） |
| **S3-2** | 缺省回退到 common | 某版本不配置 q/window 时，该版本使用 common 的 q、window；行为与“仅 common”时一致 |
| **S3-3** | V1 不受影响 | V1 仍无 thr_value 或为 NULL、无 fallback 逻辑；仅 V2/V3/V4 使用阈值与 fallback |
| **S3-4** | smoke_e2e 通过 | 全链路 smoke_e2e 通过；picks_daily 五表检查满足 |

### 4.4 测试与真实数据（S-3）

- 单元测试：从 config 加载 per-version q/window/topk_fallback，断言 V2/V3/V4 得到不同 ThresholdConfig 或等效行为；或 mock get_threshold，断言按版本传入不同 q/window。
- 集成/真实数据：使用真实数据跑 score_all，修改 config 中 V2.q 与 V3.q，再次运行，比较两次 picks_daily 中 V2 与 V3 的 thr_value 变化。

---

## 五、共同要求与交付物

### 5.1 规范

- 遵守 `docs/agent_workflow.md`（目录、核心表契约、禁改规则）与 `docs/dev_loop.md`（分支、自测、PR）。
- 不破坏 score_all 幂等性；不修改 run_daily 业务逻辑。

### 5.2 交付物

- **代码**：上述可修改范围内的变更；config 仅新增或调整 scoring 节点，不删已有字段。
- **自检清单**：S-1 与 S-3 的验收项（S1-1～S1-4、S3-1～S3-4）逐项自检结果，以及 smoke_e2e 区间与 pytest 命令。

### 5.3 总控验收

- **S-1**：满足 3.2 节 S1-1～S1-4。
- **S-3**：满足 4.3 节 S3-1～S3-4。
- 两项均通过且 smoke_e2e 通过，则 Scoring Agent 批次 1 交付通过。与 I-2 一并通过后，批次 1 完成。
