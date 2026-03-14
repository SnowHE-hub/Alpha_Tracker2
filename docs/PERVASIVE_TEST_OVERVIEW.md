# 贯穿：各模块单元/集成测试补齐 — 总览

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 第五节（真实数据测试要求）、3.2 节“贯穿”项。  
> **目标**：在各自任务交付基础上，确保各 Agent 负责模块具备符合第五节要求的单元/集成测试，并满足总控验收 `pytest -x`、smoke_e2e、Streamlit 启动。

---

## 一、范围与 Agent 分工

本“贯穿”任务按 **NEXT_PHASE_PLAN 5.3 各模块测试责任** 拆分为四类 Agent，各自补齐/确认本模块测试。总控不直接改业务代码，仅做计划、验收标准与最终验收。

| 负责 Agent | 对应模块/任务 | 任务书 | 验收标准文档 |
|------------|----------------|--------|----------------|
| **Infrastructure Agent** | I-1 配置与 schema、I-2 bt_* 特征、I-3 真实交易所日历 | `docs/PERVASIVE_TEST_INFRA_TASK.md` | 同任务书 |
| **Scoring Agent** | S-1～S-4（V1–V4、ENS） | `docs/PERVASIVE_TEST_SCORING_TASK.md` | 同任务书 |
| **Evaluation Agent** | E-1 metrics、E-2 eval_5d_batch、E-3 diagnostics | `docs/PERVASIVE_TEST_EVALUATION_TASK.md` | 同任务书 |
| **Dashboard Agent** | D-1 make_dashboard、D-2 Streamlit | `docs/PERVASIVE_TEST_DASHBOARD_TASK.md` | 同任务书 |

各 Agent 按**各自任务书**执行：补齐缺失用例、确保测试使用真实数据或总控认可的“真实数据子集”，并满足任务书内验收项。总控验收时汇总各 Agent 自检结果并执行全量 pytest、smoke_e2e、Streamlit 检查。

---

## 二、总控验收条件（汇总）

以下**全部通过**后，视为“贯穿”测试补齐通过，可与阶段总验收（v0.2.0）衔接：

1. **pytest**：在项目根目录执行约定命令（如 `pytest -x` 或 `pytest tests/`）**全部通过**；测试覆盖 Infrastructure、Scoring、Evaluation、Dashboard 各模块，且符合第五节“真实数据”底线（或各任务书中经总控认可的替代说明）。
2. **smoke_e2e**：在约定真实数据区间执行 smoke_e2e，**全链路通过**；五张核心表有数据、关键字段非空。
3. **Streamlit 启动**：`streamlit run apps/dashboard_streamlit/app.py`（或约定入口）能正常启动并展示 NAV、picks、评估面板；可为手动或自动化检查，由总控在验收清单中明确。

---

## 三、真实数据底线（重申）

- **时间**：至少连续 **20 个交易日** 以上区间；单元 fixture 可为该区间子集，需在文档中说明。
- **数据**：至少 **美股 + 港股** 标的，与当前 universe 一致；价格与特征来源为 **真实 Yahoo 数据**（经 ingest_prices / build_features 写入 DuckDB 或 fixture）。
- **可复现**：测试运行前可通过现有 pipeline 或文档脚本拉取并写入数据；或约定仓库内允许提交的示例数据片段并在 docs 中说明。

各 Agent 可在各自任务书中提议更严要求，不可低于上述底线。

---

## 四、按 Agent 的命令安排（分派用）

- **Infrastructure Agent**：认领 `docs/PERVASIVE_TEST_INFRA_TASK.md`。确保 I-1/I-2/I-3 对应测试（test_trading_calendar、test_config、test_schema、test_price_features_bt、test_build_features_bt_integration）存在且通过，集成测试符合真实数据底线；自检命令：`pytest tests/test_trading_calendar.py tests/test_config.py tests/test_schema.py tests/test_price_features_bt.py tests/test_build_features_bt_integration.py -v`。
- **Scoring Agent**：认领 `docs/PERVASIVE_TEST_SCORING_TASK.md`。补齐 V1–V4、ENS 的单元/集成测试（含 config 权重与阈值、score_ensemble 与 picks_daily(version='ENS')）；至少一项集成在真实 features_daily 上跑 score_all/score_ensemble；自检命令由任务书约定（如 `pytest tests/test_scoring_*.py tests/test_score_ensemble.py -v`）。
- **Evaluation Agent**：认领 `docs/PERVASIVE_TEST_EVALUATION_TASK.md`。保持 E-1/E-2/E-3 现有测试通过，并确保至少一项集成测试使用真实 picks_daily + prices 跑 eval_5d_batch 或 diagnostics；自检命令：`pytest tests/test_evaluation_metrics.py tests/test_eval_5d_batch.py tests/test_evaluation_diagnostics.py -v`。
- **Dashboard Agent**：认领 `docs/PERVASIVE_TEST_DASHBOARD_TASK.md`。保持 test_make_dashboard 通过；确认 Streamlit 启动与 NAV/picks/评估展示可验收（手动或自动化）；自检命令：`pytest tests/test_make_dashboard.py -v` 及 `streamlit run apps/dashboard_streamlit/app.py`。

总控汇总：各 Agent 交付后执行全量 `pytest -x`、smoke_e2e、Streamlit 启动检查，全部通过则“贯穿”测试补齐通过。

---

## 五、文档索引

| 文档 | 用途 |
|------|------|
| `docs/NEXT_PHASE_PLAN.md` | 阶段规划与第五节真实数据要求 |
| `docs/PERVASIVE_TEST_OVERVIEW.md` | 本总览 |
| `docs/PERVASIVE_TEST_INFRA_TASK.md` | Infrastructure Agent 测试补齐任务与验收 |
| `docs/PERVASIVE_TEST_SCORING_TASK.md` | Scoring Agent 测试补齐任务与验收 |
| `docs/PERVASIVE_TEST_EVALUATION_TASK.md` | Evaluation Agent 测试补齐任务与验收 |
| `docs/PERVASIVE_TEST_DASHBOARD_TASK.md` | Dashboard Agent 测试补齐任务与验收 |
