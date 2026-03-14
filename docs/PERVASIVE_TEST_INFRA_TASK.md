# 贯穿 - Infrastructure Agent：I-1 / I-2 / I-3 测试补齐

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 第五节 5.3（Infrastructure Agent）、`docs/PERVASIVE_TEST_OVERVIEW.md`。  
> **执行方**：Infrastructure Agent（或认领该角色的功能 Agent）。  
> **前置**：批次 0（I-3、I-1）、批次 1（I-2）已交付并验收通过。

---

## 一、目标

确保 **I-1 配置与 schema**、**I-2 bt_* 特征工程**、**I-3 真实交易所日历** 具备符合 NEXT_PHASE_PLAN 第五节的单元/集成测试，且测试基于真实数据或总控认可的“真实数据子集”。

---

## 二、责任范围与现有测试

| 任务 | 内容 | 现有测试文件（需保持通过或补齐） |
|------|------|----------------------------------|
| **I-3** | 真实交易所日历 | `tests/test_trading_calendar.py`（US 节假日、HK 节假日、latest_trading_day、trading_days 顺序与边界） |
| **I-1** | 配置与 schema 扩展 | `tests/test_config.py`、`tests/test_schema.py`（配置加载、schema 与契约一致） |
| **I-2** | bt_* 特征工程 | `tests/test_price_features_bt.py`（bt_mean/bt_winrate/bt_worst_mdd 计算、范围、配置）；`tests/test_build_features_bt_integration.py`（build_features 写入 bt_* 列） |

---

## 三、验收标准（Infrastructure）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| **INF-1** | I-3 日历单元测试存在且通过 | `pytest tests/test_trading_calendar.py -v` 全部通过；至少覆盖 US 已知休市日（如 2024-07-04）、HK 已知休市日（如农历新年）、latest_trading_day 在休市日行为、trading_days 升序与含端点 |
| **INF-2** | I-1 配置/schema 测试存在且通过 | `pytest tests/test_config.py tests/test_schema.py -v` 全部通过；配置与 schema 符合 agent_workflow 与批次 0 约定 |
| **INF-3** | I-2 bt_* 单元测试存在且通过 | `pytest tests/test_price_features_bt.py -v` 全部通过；覆盖 bt_winrate∈[0,1]、bt_worst_mdd≤0、bt_mean 量级、至少一条非空与 config 窗口影响 |
| **INF-4** | I-2 集成测试存在且通过 | `pytest tests/test_build_features_bt_integration.py -v` 通过；在真实数据或约定 fixture 上跑 build_features（或等价），检查 features_daily 中 bt_* 列存在且数值合理 |
| **INF-5** | 真实数据底线 | 集成测试或单元 fixture 使用至少 20 个交易日、US 或 HK ticker；数据来源为 ingest_prices/build_features 或文档中说明的示例数据；禁止仅用纯 mock 通过验收 |

---

## 四、可修改范围

- **允许**：在 `tests/` 下新增或修改与 I-1、I-2、I-3 相关的测试；在 `src/alpha_tracker2/` 内仅限为便于测试暴露的只读接口或 fixture 辅助（不改变现有公共接口语义）。
- **禁止**：为通过测试而放宽 I-1/I-2/I-3 的业务逻辑或契约；删除已有、已通过验收的测试用例。

---

## 五、自检与交付

- 执行：`pytest tests/test_trading_calendar.py tests/test_config.py tests/test_schema.py tests/test_price_features_bt.py tests/test_build_features_bt_integration.py -v`，全部通过。
- 在任务交付物中注明：上述测试是否使用真实数据、区间与复现方式（或“见 NEXT_PHASE_PLAN 第五节 + 本任务书 INF-5”）。
- 总控验收时与 smoke_e2e 一并检查；通过则 Infrastructure 贯穿测试项通过。
