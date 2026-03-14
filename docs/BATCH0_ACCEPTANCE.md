# 批次 0 验收报告（Infrastructure Agent 交付）

> **验收方**：总控 Agent  
> **依据**：`docs/BATCH0_TASK.md` 验收标准。  
> **结论**：I-3 与 I-1 **均通过**，批次 0 完成。

---

## 一、验收概要

| 任务 | 验收结果 | 说明 |
|------|----------|------|
| **I-3** 真实交易所日历 | ✅ 通过 | 接口未变，US/HK 节假日单元测试通过，smoke_e2e 全链路通过 |
| **I-1** 配置与 Schema 扩展 | ✅ 通过 | schema 新增 bt_* 三列，配置结构存在且可读，契约文档已更新，smoke_e2e 通过 |

---

## 二、I-3 逐项核对

| 验收项 | 判定 | 依据 |
|--------|------|------|
| **I3-1** 接口未变 | ✅ | `TradingCalendar` 仍提供 `latest_trading_day(market)`、`trading_days(start, end, market)`，签名与语义不变；build_features、ingest_prices、eval_5d、score_all、ingest_universe、portfolio_nav、forward_returns、smoke 均未改调用代码 |
| **I3-2** US 节假日正确 | ✅ | `tests/test_trading_calendar.py`：`test_trading_days_us_excludes_independence_day_2024` 断言 2024-07-04 不在 trading_days(2024-07-01, 2024-07-05, "US")；`test_latest_trading_day_us_on_holiday_patched` 断言“今日”为 2024-07-04 时 latest_trading_day("US") 返回 2024-07-03；pytest 全部通过 |
| **I3-3** HK 节假日正确 | ✅ | `test_trading_days_hk_excludes_lunar_new_year` 断言 2024 年农历新年至少一个休市日在 trading_days(..., "HK") 中不出现；pytest 通过 |
| **I3-4** smoke 与 pipeline 不受损 | ✅ | 执行 `PYTHONPATH=src python -m alpha_tracker2.pipelines.smoke_e2e --start 2024-01-01 --end 2025-03-13 --limit 5 --topk 3`，六步跑通，resolved target_date=2025-03-12，all steps and checks passed，退出码 0 |

**实现要点**：`core/trading_calendar.py` 使用 `pandas_market_calendars`（NYSE、XHKG），按 (market, start, end) 缓存；`requirements.txt` 已增加 `pandas_market_calendars>=4.0.0`。

---

## 三、I-1 逐项核对

| 验收项 | 判定 | 依据 |
|--------|------|------|
| **I1-1** features_daily 新增三列 | ✅ | `storage/schema.sql` 中 features_daily 已含 `bt_mean`、`bt_winrate`、`bt_worst_mdd`（DOUBLE，可 NULL）；`tests/test_schema.py::test_features_daily_has_bt_columns` 通过；`DuckDBStore.init_schema()` 内含迁移逻辑，对已有库可 idempotent 增加三列 |
| **I1-2** 配置结构存在且可读 | ✅ | `configs/default.yaml` 已新增：`scoring.v1.weights`（ret_5d, ret_20d, avg_amount_20）；`scoring.v2_v3_v4.versions`（V2/V3/V4 的 trend_weight、risk_weight、bt_weight）；`scoring.v2_v3_v4.bt_column_weights`（bt_mean, bt_winrate, bt_worst_mdd）；`tests/test_config.py` 三项配置测试及 `test_load_settings_still_works` 均通过 |
| **I1-3** 契约文档已更新 | ✅ | `docs/agent_workflow.md` 第 B 节 features_daily 已补充 bt_mean、bt_winrate、bt_worst_mdd 说明，并注明“bt_* 由 I-2 特征工程写入，V4 在 S-2 中接入” |
| **I1-4** 现有链路不受损 | ✅ | 同 I3-4，smoke_e2e 全链路通过；build_features 写入 3 行，score_all / eval_5d / portfolio_nav 正常；features_daily 表结构含 bt_* 列，INSERT 未报错（新列 NULL 或由迁移补齐） |

**实现要点**：未删改已有 default.yaml 字段；未改 run_daily 业务逻辑；主键与已有列语义未变。

---

## 四、测试与执行记录

- **pytest**：`PYTHONPATH=src pytest tests/ -v -x`，**12 passed**（test_config 4，test_schema 1，test_trading_calendar 7）。
- **smoke_e2e**：`--start 2024-01-01 --end 2025-03-13 --limit 5 --topk 3`，真实数据区间，**all steps and checks passed**，退出码 0。

---

## 五、总控结论

- **I-3**：满足 BATCH0_TASK 2.5 节 I3-1～I3-4，**通过**。
- **I-1**：满足 BATCH0_TASK 3.6 节 I1-1～I1-4，**通过**。

**批次 0 验收通过**。Infrastructure Agent 交付物符合任务书要求，可合并入 dev，作为后续 I-2、S-1、S-3 的依赖基础。总控据此关闭批次 0，可进入批次 1 任务设计与分配。
