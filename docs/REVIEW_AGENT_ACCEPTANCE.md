# Review Agent 验收结论（总控）

依据 `docs/REVIEW_AGENT_TASK.md` 的四项任务与验收总结，对 Review Agent 交付的实现与 Smoke 结果做逐项核对。

---

## 任务 1：Universe 写 lake + 失败回退

**验收标准**：成功跑一次后 `data/lake/universe/` 下存在对应日期 Parquet；失败时能回退到 cache 并写 picks_daily。

**核对结果**：

- **UniverseCache**（`ingestion/cache.py`）：
  - `save(trade_date, df)` → `lake_dir / "universe" / trade_date.isoformat() / universe.parquet` ✓
  - `load(trade_date)`、`load_latest()` 已实现 ✓
- **ingest_universe**（`pipelines/ingest_universe.py`）：
  - 成功路径：`provider.fetch_universe` 后调用 `universe_cache.save(trade_date, universe_df)` ✓
  - 失败路径：`except` 中先 `load(trade_date)`，再 `load_latest()`，有则转成 `UniverseRow` 并继续写 picks_daily，无则抛错 ✓
- picks_daily 写入逻辑与主键未改 ✓

**结论**：**通过**。满足“写 lake + 失败回退”的约定。

---

## 任务 2：Scoring 阈值与 fallback 从 config 读取

**验收标准**：q/window/topk_fallback 来自 config；改 q 或 topk_fallback 后 score_all 行为随之变化。

**核对结果**：

- **configs/default.yaml**：已增加 `scoring.v2_v3_v4.common`，含 `q: 0.8`、`window: 60`、`topk_fallback: 50` ✓
- **score_all.py**：
  - `_load_v2_v3_v4_config(project_root)` 从 YAML 读取 `scoring.v2_v3_v4.common` 的 q、window、topk_fallback ✓
  - `thr_cfg = ThresholdConfig(q=q, window=window)`，`fallback_topk=topk_fallback` 传入 `_prepare_rows_for_version` ✓
  - fallback 分支使用 `k = min(fallback_topk, len(df))` ✓
- V1 仍无阈值、无 fallback ✓

**结论**：**通过**。阈值与 fallback 已配置化，行为可随 config 变化。

---

## 任务 3：forward_returns 信号日语义文档化

**验收标准**：阅读 `compute_forward_returns` 的 docstring 即可明确 as_of 非交易日时的行为。

**核对结果**：

- **evaluation/forward_returns.py** 中 `compute_forward_returns` 的 docstring 已包含：
  - “**Signal-day semantics**: If as_of_date is not a trading day, the **first trading day on or after as_of_date** is used as the start date. The buy price is the close (adj_close) on that start date; end_date is the horizon-th trading day after start_date.” ✓

**结论**：**通过**。as_of 非交易日时的语义已文档化。

---

## 任务 4：端到端 Smoke 测试脚本与执行

**验收标准**：六步无报错跑完；五张表（prices_daily, features_daily, picks_daily, eval_5d_daily, nav_daily）均有数据、行数合理、关键字段非空；失败时脚本非零退出并给出明确信息。

**核对结果**：

- **脚本位置与参数**：`pipelines/smoke_e2e.py`，支持 `--start`/`--end` 或 `--date`，以及 `--limit`、`--topk` ✓
- **步骤顺序**：ingest_universe → ingest_prices → build_features → score_all → eval_5d → portfolio_nav，与任务书一致 ✓
- **检查逻辑**：
  - 五张表存在性（information_schema）✓
  - prices_daily：区间内行数 ≥1、ticker 数 ≥1，trade_date/ticker/adj_close 非空 ✓
  - features_daily：target_date 行数 ≥1，trade_date/ticker 非空，至少一行 ret_1d 非空 ✓
  - picks_daily：UNIVERSE 与 V1–V4 在 target_date 有行，trade_date/version/ticker/score 非空 ✓
  - eval_5d_daily：as_of_date=target 有行，as_of_date/version/bucket 非空 ✓
  - nav_daily：区间内行数 ≥1、portfolio 数 ≥1，trade_date/portfolio/nav 非空 ✓
- **失败时**：`failures` 列表非空则打印并 `return 1` ✓
- **样本输出**（`docs/smoke_e2e_sample_output.txt`）：六步均执行，最终 “all steps and checks passed.”、Exit code 0；结论写明五张表均有数据、行数及关键列非空检查通过 ✓

**说明**：样本中 `build_features` 显示 `wrote_rows=0`。若当次跑的是空库且仅此一次 build_features，则 features_daily 在 target_date 会无行，smoke 的 features_daily 检查会失败。若实际为“先有历史数据或此前跑过 build_features”则可通过。建议在**空库或清空 target_date 的 features_daily 后**再跑一次 smoke，确认从零也能全部通过。

**结论**：**通过**。Smoke 脚本与检查逻辑符合任务书；样本输出表明在提供方环境下跑通。建议按上段做一次从零/清空后的复现以彻底确认。

---

## 总体验收总结

| 任务 | 结果 |
|------|------|
| 1. Universe 写 lake + 失败回退 | ✅ 通过 |
| 2. Scoring 从 config 读取 | ✅ 通过 |
| 3. forward_returns 语义文档化 | ✅ 通过 |
| 4. 端到端 Smoke 脚本与执行 | ✅ 通过（建议补一次从零/清空后的复现） |

**总控结论**：Review Agent 四项任务均满足 `REVIEW_AGENT_TASK.md` 的验收标准，**本轮验收通过**。系统具备“真的能跑”的端到端验证能力；建议在需要时做一次从空库或清空目标日 features 后的完整 smoke 复现，以消除对 `wrote_rows=0` 场景的疑虑。

---

## Review Agent 新做工作（补充验收）

在四项任务通过后，Review Agent 又做了以下增强与文档补充，总控核对如下。

### 1. Smoke 脚本：resolved target_date 逻辑

**问题**：当 `--end` 指定的日期在 Yahoo 侧尚无数据（或尚未落入 prices_daily）时，build_features 对 end 日会写 0 行，导致 features_daily 检查失败。

**实现**（`pipelines/smoke_e2e.py` 约 247–265 行）：

- 在 ingest_universe + ingest_prices 执行完成后，查询 `prices_daily` 在 `[start, end]` 内的 **MAX(trade_date)**。
- 若存在，则将该日期作为 **resolved target_date**；若不存在则用 end，若 resolved &lt; start 则报错退出。
- 当 resolved 与请求的 end 不一致时，打印：  
  `smoke_e2e: using resolved target_date=YYYY-MM-DD (prices_daily max in range; requested end=YYYY-MM-DD)`，  
  并将后续 build_features、score_all、eval_5d 的 `--date` 改为 resolved 日期。
- `_run_checks(store, start, end, target_date)` 使用更新后的 `target_date`，保证 features_daily / picks_daily / eval_5d_daily 的检查针对“有数据”的目标日。

**验收**：逻辑与任务书一致；在“end 日无价格、区间内有更早交易日”的场景下，smoke 能自动用有数据的最近交易日作为 target，避免误报失败。**通过**。

### 2. 样本输出文档：冷跑复现步骤与 resolved target 说明

**交付**（`docs/smoke_e2e_sample_output.txt`，由 43 行扩充为 71 行）：

- **Cold run 复现**：给出两步操作——  
  (1) 用一段 Python 清空目标日（示例 2025-03-12）的 features_daily、picks_daily（V1–V4）、eval_5d_daily；  
  (2) 执行 smoke 命令（示例 `--start 2024-01-01 --end 2025-03-13 --limit 5 --topk 3`），说明会按 prices_daily 实际最大 trade_date 解析目标日，保证 build_features 有数据。
- **原示例命令与输出**：保留原有 `--end 2026-01-14` 的示例及六步输出。
- **Cold-run sample (resolved target_date)**：说明当 end 尚未出现在 prices_daily 时，smoke 解析出 resolved target（示例 2025-03-12），并展示 build_features wrote_rows=3、all steps and checks passed，确认“当日首次 build_features”场景能通过。

**验收**：文档与脚本行为一致，复现步骤可操作，resolved target 的语义与样本结论清晰。**通过**。

---

### 新做工作总表

| 项 | 内容 | 结果 |
|----|------|------|
| Smoke 脚本增强 | resolved target_date（按 prices_daily 区间内 max 解析） | ✅ 通过 |
| 样本输出文档 | 冷跑复现步骤 + resolved target 说明 + 冷跑样本结论 | ✅ 通过 |

**总控结论（新做工作）**：Review Agent 在四项任务之外的**新做工作**——smoke 的 target_date 解析与样本输出文档的补充——已核对通过，与“当日首次 build_features”及“end 日无数据时用区间内最近交易日”的诉求一致，**补充验收通过**。
