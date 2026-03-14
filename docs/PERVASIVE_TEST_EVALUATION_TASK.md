# 贯穿 - Evaluation Agent：E-1 / E-2 / E-3 测试补齐

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 第五节 5.3（Evaluation Agent）、`docs/PERVASIVE_TEST_OVERVIEW.md`。  
> **执行方**：Evaluation Agent（或认领该角色的功能 Agent）。  
> **前置**：批次 3（E-1、E-2、E-3）已交付并验收通过。

---

## 一、目标

确保 **E-1 metrics**、**E-2 eval_5d_batch**、**E-3 diagnostics** 具备符合 NEXT_PHASE_PLAN 第五节的单元测试（给定序列算 Sharpe/MDD/IC）及集成测试（在真实 picks_daily + prices_daily 上跑 eval_5d_batch 与 diagnostics，检查输出结构与数值合理性）。

---

## 二、责任范围与现有测试

| 任务 | 内容 | 现有测试文件（需保持通过或补齐） |
|------|------|----------------------------------|
| **E-1** | metrics.py（Sharpe、MDD、IC） | `tests/test_evaluation_metrics.py`（已知序列 Sharpe/MDD/IC、ic_series、aggregate_ic_series） |
| **E-2** | eval_5d_batch | `tests/test_eval_5d_batch.py`（IC 调用关系、quintile_returns/ic_series CSV schema、tickers_for_bucket） |
| **E-3** | diagnostics | `tests/test_evaluation_diagnostics.py`（version_compare/factor_analysis 列、run_diagnostics 写出文件） |

---

## 三、验收标准（Evaluation）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| **EVAL-1** | E-1 单元测试存在且通过 | `pytest tests/test_evaluation_metrics.py -v` 全部通过；覆盖给定序列的 Sharpe、MDD、IC（Pearson/Spearman）、ic_series、aggregate_ic_series；与手算或 scipy 一致 |
| **EVAL-2** | E-2 单元/集成测试存在且通过 | `pytest tests/test_eval_5d_batch.py -v` 全部通过；覆盖 eval_5d_batch 调用 metrics.ic、quintile_returns.csv 与 ic_series.csv 列约定、输出行数/列存在 |
| **EVAL-3** | E-3 单元/集成测试存在且通过 | `pytest tests/test_evaluation_diagnostics.py -v` 全部通过；覆盖 version_compare 与 factor_analysis 产出列、run_diagnostics 写出 version_compare.csv 与 factor_analysis.csv |
| **EVAL-4** | 真实数据集成测试 | 至少一项测试在真实 picks_daily + prices_daily（或总控认可的 fixture）上跑 eval_5d_batch 或 run_diagnostics，检查输出文件存在、数值在合理范围（如 IC∈[-1,1]、分位收益为小数）；区间至少 20 个交易日 |
| **EVAL-5** | 真实数据底线 | 同 NEXT_PHASE_PLAN 5.2；若单元测试使用合成序列，需在交付物中说明，并有一项集成或 E-2 测试使用真实数据 |

---

## 四、可修改范围

- **允许**：在 `tests/` 下新增或修改与 E-1、E-2、E-3 相关的测试；使用现有 store fixture（如 store_with_eval_and_picks）；在 evaluation 模块内仅限为便于测试的只读接口。
- **禁止**：为通过测试而放宽 E-1/E-2/E-3 的指标语义或产出契约；删除已有、已通过批次 3 验收的用例。

---

## 五、自检与交付

- 执行：`pytest tests/test_evaluation_metrics.py tests/test_eval_5d_batch.py tests/test_evaluation_diagnostics.py -v`，全部通过。
- 在交付物中注明：哪些为真实数据集成测试、区间与复现方式。
- 总控验收时与全量 pytest、smoke_e2e 一并检查；通过则 Evaluation 贯穿测试项通过。

---

## 六、交付说明（Evaluation Agent）

- **真实数据集成测试**：`tests/test_eval_5d_batch.py::test_integration_eval_5d_batch_real_data`（标记为 `@pytest.mark.integration`）。
  - 使用项目真实 `picks_daily` + `prices_daily`（`config paths.store_db`），当 `picks_daily` 中至少有 20 个不同 `trade_date` 且该日期区间内 US 交易日 ≥ 20 时执行；否则跳过。
  - 运行 `eval_5d_batch` 的 `--start` / `--end` 为 picks_daily 的 min/max 日期，产出写入临时目录；断言 `quintile_returns.csv`、`ic_series.csv` 存在，且 IC∈[-1,1]、分位收益为数值。
- **复现方式**：在仓库根目录先跑通 ingest + build_features + score_all 覆盖至少 20 个交易日，再执行  
  `pytest tests/test_eval_5d_batch.py -v -k integration`  
  或全量  
  `pytest tests/test_evaluation_metrics.py tests/test_eval_5d_batch.py tests/test_evaluation_diagnostics.py -v`
