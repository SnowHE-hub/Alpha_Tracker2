# Review Agent 任务书（总控下发）

你是 **Review Agent**，负责在自检报告（`docs/SYSTEM_SELF_CHECK_REPORT.md`）结论基础上，完成指定的**修复/补充任务**，并执行**端到端 Smoke 验收**，确保系统“真的能跑”而不仅是“看起来能跑”。

总控 Agent 仅做任务拆解与验收标准定义；你负责实现、自测与交付。

---

## 一、角色与边界

- **你可修改/新增**：  
  - `ingestion/`（含 cache、plugins、pipelines/ingest_universe、ingest_prices）  
  - `scoring/`（含 score_all、thresholds 使用方式）  
  - `configs/default.yaml`（仅增加 scoring 相关配置项，不删已有）  
  - 新增脚本：`scripts/smoke_e2e.py` 或 `pipelines/smoke_e2e.py`（见第五节）  
  - 可选：`core/` 下增加 `get_project_root()` 等工具（不破坏现有接口）
- **你不得**：  
  - 修改 `storage/schema.sql` 中已有表的主键或已有列语义（可新增列）  
  - 在 `run_daily.py` 中加入业务逻辑  
  - 删除或破坏现有 pipeline 的幂等写入逻辑
- **必须遵守**：`docs/agent_workflow.md` 中的契约与禁改规则。

---

## 二、任务来源与优先级

以下任务来自自检报告中的 **必须修复/补充** 与 **高优先级改进**，按执行顺序排列。

---

## 三、具体任务与验收标准

### 任务 1：Universe 写 lake + 失败回退

**背景**：自检报告指出 ingest_universe 未将 universe 写入 lake，与 README“写入 data/lake/universe/”及“失败可回退到 lake 最近一次”不一致。

**要求**：

1. 在 `ingestion/cache.py` 中增加 **UniverseCache**（或等价类）：
   - 方法：`save(trade_date: date, df: pd.DataFrame) -> Path`，将当日 universe DataFrame 写入 `lake_dir / "universe" / "{trade_date}" / universe.parquet`（或单文件命名含日期）。
   - 方法：`load(trade_date: date) -> pd.DataFrame | None`，若该日文件存在则返回，否则返回 None。
   - 方法（可选）：`load_latest() -> pd.DataFrame | None`，返回最近一次保存的 universe（按目录/文件日期取最新）。
2. 在 `pipelines/ingest_universe.py` 中：
   - 调用 provider 成功后，将结果 DataFrame 写入 **UniverseCache**（即写 lake）。
   - 调用 provider 失败时，先尝试 **UniverseCache.load(trade_date)**，若无则 **load_latest()**；若仍无则再抛出异常。
3. 不改变 picks_daily 的写入逻辑与主键。

**验收标准**：

- 成功跑一次 `ingest_universe --date 2026-01-15` 后，`data/lake/universe/` 下存在对应日期的 Parquet（或约定路径下的文件）。
- 在**人为制造 provider 异常**（如断网或 mock 抛错）且 lake 中已有历史 universe 时，ingest_universe 能回退到 cache 并成功写 picks_daily（可手测或单测 mock）。

---

### 任务 2：Scoring 阈值与 fallback 从 config 读取

**背景**：自检报告指出 score_all 中 `ThresholdConfig(q=0.8, window=60)` 与 `fallback_topk` 硬编码，应与 config 一致。

**要求**：

1. 在 `configs/default.yaml` 的 `scoring` 下增加（或扩展现有）：
   - `v2_v3_v4`（或等价）节点，包含：
     - `common.q`（默认 0.8）
     - `common.window`（默认 60）
     - `common.topk_fallback`（默认 50）
   - 若需 per-version 可再加 `versions.V2/V3/V4.threshold.q|window|topk_fallback`，否则 common 即可。
2. 在 `pipelines/score_all.py` 中：
   - 从 `default.yaml` 读取上述配置（通过 `load_settings` 或单独读 YAML），构造 `ThresholdConfig(q=..., window=...)` 和 fallback_topk。
   - 对 V2/V3/V4 使用配置中的 q、window；fallback 时使用配置的 topk_fallback（或 per-version 若已实现）。
3. 不改变 V1 行为（V1 仍无阈值、无 fallback 逻辑）。

**验收标准**：

- 修改 `default.yaml` 中 `scoring.v2_v3_v4.common.q` 为 0.9 后，再跑 `score_all --date 2026-01-15`，V2/V3/V4 的 thr_value 或通过率随之变化（可人工对比 q=0.8 与 0.9 的写入结果）。
- 将 `topk_fallback` 设为 5，在“当日全部分数低于阈值”的场景下，picks_daily 中该版本仅写入 5 行（fallback 行数等于 topk_fallback）。

---

### 任务 3：forward_returns 信号日语义文档化

**背景**：自检报告指出当 as_of_date 为非交易日时，实际使用“下一交易日”为起点，需明确约定避免误解。

**要求**：

1. 在 `evaluation/forward_returns.py` 的 `compute_forward_returns` 的 docstring 中，明确写出：
   - “若 as_of_date 非交易日，则使用**该日之后的第一个交易日**作为起点日（start_date），起点日收盘价作为买入价；end_date 为起点日之后第 horizon 个交易日。”
2. 可选：在 `docs/agent_workflow.md` 的评估相关小节或 E 节“使用方式”中加一句：eval 前向收益的 as_of 语义以 forward_returns 模块 docstring 为准。

**验收标准**：

- 阅读 `compute_forward_returns` 的 docstring 即可明确 as_of 非交易日时的行为，无需猜。

---

### 任务 4：端到端 Smoke 测试脚本与执行

**目标**：验证系统不是“看起来能跑”，而是**真的能跑**——在稍长一段时间区间内完整跑通流水线，并检查各表已生成、行数合理、关键字段非空。

**要求**：

1. **新增 Smoke 脚本**（二选一或都做）：
   - 方案 A：在 `scripts/smoke_e2e.py`（若不存在则创建 `scripts/` 目录）。
   - 方案 B：在 `pipelines/smoke_e2e.py`。
   - 脚本应：
     - 接受参数：`--start`、`--end`（必填），或 `--date`（单日，等价 start=end）；可选 `--limit`、`--topk` 等透传。
     - **按顺序调用**（通过 subprocess 或 in-process 调用各 pipeline 的 main）：
       1. ingest_universe（用 end 或 date 作为 trade_date）
       2. ingest_prices（区间 [start, end]，或单日时用 last-n 推导窗口）
       3. build_features（用 end 或 date）
       4. score_all（用 end 或 date）
       5. eval_5d（as_of 用 end 或 date）
       6. portfolio_nav（start, end）
     - 使用**同一份** DuckDB（config 中的 store_db）和 project root。
     - 不调用 make_dashboard（可选，smoke 重点验证 DB 表）。
2. **检查逻辑**（在脚本内或独立“检查”步骤）：
   - **表存在**：以下表在 DuckDB 中存在且可查询：  
     `prices_daily`、`features_daily`、`picks_daily`、`eval_5d_daily`、`nav_daily`。
   - **行数合理**：
     - `prices_daily`：在 [start, end] 内至少有若干行（≥ 1，且 ticker 数 ≥ 1）。
     - `features_daily`：在目标 trade_date（end 或 date）至少有 1 行。
     - `picks_daily`：在目标 trade_date 下，version='UNIVERSE' 至少有 1 行；version 在 ('V1','V2','V3','V4') 中至少有一个有 ≥ 1 行。
     - `eval_5d_daily`：as_of_date = 目标日 至少有 1 行（可允许 fwd_ret_5d 为 NULL 若未来数据不足）。
     - `nav_daily`：在 [start, end] 内至少有 1 行，且 portfolio 数 ≥ 1。
   - **关键字段非空**（抽样或全表检查，由你定）：
     - `prices_daily`：`trade_date`, `ticker`, `adj_close` 非 NULL。
     - `features_daily`：`trade_date`, `ticker` 非 NULL；至少一个特征列（如 `ret_1d`）在部分行非 NULL。
     - `picks_daily`：`trade_date`, `version`, `ticker`, `score` 非 NULL（reason 可空）。
     - `eval_5d_daily`：`as_of_date`, `version`, `bucket` 非 NULL。
     - `nav_daily`：`trade_date`, `portfolio`, `nav` 非 NULL。
   - 若任一项不通过，脚本应以非零退出码退出，并打印明确错误信息（例如 “SMOKE FAIL: prices_daily has 0 rows in range”）。
3. **建议运行区间**：为“充分验证”，建议使用**至少约 5–10 个交易日**的区间，例如：
   - `--start 2026-01-06 --end 2026-01-15`（或你本地有数据的任意连续交易日）。
   - 若 Yahoo 数据不可用，可在任务中说明“需在具备 prices_daily 数据的环境执行”，并给出示例命令。

**验收标准**：

- 在项目根目录执行（示例）：
  ```bash
  PYTHONPATH=src python scripts/smoke_e2e.py --start 2026-01-06 --end 2026-01-15 --limit 5 --topk 3
  ```
  或：
  ```bash
  PYTHONPATH=src python -m alpha_tracker2.pipelines.smoke_e2e --start 2026-01-06 --end 2026-01-15 --limit 5 --topk 3
  ```
- 脚本**无报错**跑完上述 6 步。
- 脚本内（或随后执行的检查）确认：
  - 五张表 **prices_daily, features_daily, picks_daily, eval_5d_daily, nav_daily** 均**有数据**（满足上表“行数合理”与“关键字段非空”）。
- 若任一项检查未通过，脚本以非零退出码退出并输出明确失败原因；修复后再次运行可通过。

---

## 四、交付物与自检清单

完成后请交付：

1. **代码**：上述任务涉及的新增/修改文件（含 UniverseCache、ingest_universe 修改、score_all 配置读取、forward_returns docstring、smoke_e2e 脚本）。
2. **配置**：`configs/default.yaml` 的 diff 或说明（仅新增 scoring 节点，不删已有）。
3. **自检结果**：
   - 任务 1：已写 lake 且失败回退已测（或说明测试方式）。
   - 任务 2：已从 config 读 q/window/topk_fallback 并验证行为。
   - 任务 3：docstring 已更新（及可选 agent_workflow 更新）。
   - 任务 4：在给定区间执行 smoke 的**完整终端输出**（可贴到回复或存为 `docs/smoke_e2e_sample_output.txt`），以及“五张表均有数据、关键字段非空”的结论。

---

## 五、验收总结（总控将据此判定通过与否）

- **任务 1 通过**：lake/universe 有写入，且失败时能回退到 cache 并写 picks_daily。
- **任务 2 通过**：score_all 的 q、window、topk_fallback 来自 config，且修改 config 后行为随之变化。
- **任务 3 通过**：forward_returns 的 docstring 已明确 as_of 非交易日语义。
- **任务 4 通过**：smoke 脚本在约定区间内完整执行 6 步无报错，且五张表（prices_daily, features_daily, picks_daily, eval_5d_daily, nav_daily）均生成、行数合理、关键字段非空；失败时脚本非零退出并给出明确信息。

**只有四项全部通过，总控才认定 Review Agent 本轮完成，系统具备“真的能跑”的端到端验证。**
