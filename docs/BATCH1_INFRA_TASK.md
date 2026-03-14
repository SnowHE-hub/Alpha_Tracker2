# 批次 1 - Infrastructure Agent 任务书：I-2 bt_* 特征工程

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 任务 I-2、`docs/BATCH1_OVERVIEW.md`。  
> **执行方**：Infrastructure Agent（或认领该角色的功能 Agent）。  
> **前置**：批次 0 已通过（features_daily 已含 bt_mean、bt_winrate、bt_worst_mdd 列）。

---

## 一、目标

在特征层实现 **bt_mean**、**bt_winrate**、**bt_worst_mdd** 的计算，并由 `build_features` 写入 `features_daily`，为后续 S-2（V4 接入 bt_*）提供数据。语义与 `docs/agent_workflow.md` 及 schema 约定一致。

---

## 二、可修改范围

- **允许修改**：
  - `src/alpha_tracker2/features/price_features.py`：在 `compute_price_features` 中增加 bt_* 的计算逻辑；可选扩展 `PriceFeatureConfig`（如 bt 窗口长度）。
  - `src/alpha_tracker2/pipelines/build_features.py`：`_write_features` 及调用处，使 INSERT 包含 `bt_mean`、`bt_winrate`、`bt_worst_mdd`（从 `compute_price_features` 产出写入）。
- **禁止**：
  - 修改 `storage/schema.sql` 已有列或主键（bt_* 已在批次 0 增加）。
  - 修改 `run_daily.py`、scoring 模块、eval 模块。
  - 修改 core 表契约文档中 bt_* 的**语义**（可补充实现说明）。

---

## 三、bt_* 语义与计算要求

- **bt_mean**（DOUBLE，可 NULL）：在**滚动窗口**内，标的日收益（如 ret_1d）的**均值**，作为“模拟滚动收益”的近似。窗口长度与现有特征一致（如 60 日）或通过 config 指定；若数据不足则 NULL。
- **bt_winrate**（DOUBLE，可 NULL）：滚动窗口内**日收益为正的天数占比**，取值 [0, 1]。若窗口内无有效收益则 NULL。
- **bt_worst_mdd**（DOUBLE，可 NULL）：滚动窗口内**价格序列的最大回撤**（即 (价格 / 区间内最高价 - 1) 的最小值），非正数。可与现有 `mdd_60d` 同源或共用窗口；若无法计算则 NULL。

实现时可在 `price_features.py` 内按 ticker 滚动计算，与 ret_1d、mdd_60d 等使用同一窗口或单独参数；需在 docstring 或注释中说明窗口与数据来源。

---

## 四、验收标准（I-2）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| **I2-1** | bt_* 已计算并写入 | `build_features --date <某日>` 运行后，`features_daily` 中该日至少部分行的 `bt_mean`、`bt_winrate`、`bt_worst_mdd` 非 NULL（允许部分 ticker 因数据不足为 NULL） |
| **I2-2** | 数值范围合理 | `bt_winrate` 在 [0, 1]；`bt_worst_mdd` ≤ 0；`bt_mean` 为收益量级（可与 ret_5d/ret_20d 同数量级） |
| **I2-3** | 幂等与兼容 | 重复运行 `build_features` 同日期同 ticker 不报错、不重复行；未改动的其他特征列行为不变 |
| **I2-4** | 真实数据测试 | 单元或集成测试使用**真实数据**（至少 20 个交易日、US 或 HK ticker，来自 ingest_prices/build_features 或约定 fixture）；测试通过且覆盖 bt_* 非空与范围 |
| **I2-5** | smoke_e2e 通过 | 执行 `smoke_e2e --start ... --end ...` 全链路通过；features_daily 检查仍满足（目标日行数 ≥1、关键列非空）；build_features 步骤写入行数 ≥1 且含 bt_* 列 |

---

## 五、测试与真实数据要求

- **真实数据**：按 `docs/NEXT_PHASE_PLAN.md` 第五节，至少连续 **20 个交易日**、含 **US 或 HK** ticker；数据来源为 Yahoo（经 ingest_prices 写入）或项目约定的真实数据子集。
- **单元测试**：可对 `compute_price_features` 传入带 adj_close 的 DataFrame 与 trading_days，断言输出 DataFrame 含 `bt_mean`、`bt_winrate`、`bt_worst_mdd` 列，且 winrate ∈ [0,1]、bt_worst_mdd ≤ 0；若使用 fixture，需在文档或注释中说明数据来源与复现方式。
- **集成测试**：在真实 DB 或临时 DB 上跑 `build_features` 指定日期，查询 features_daily 该日至少一行三列均非 NULL（或按约定允许部分 NULL），并断言范围。

---

## 六、交付物与规范

- **代码**：上述可修改范围内的变更；若增加配置项（如 bt 窗口），在 `configs/default.yaml` 中仅新增、不删已有项。
- **自检清单**：交付时列出 I2-1～I2-5 逐项自检结果，并注明 smoke_e2e 区间与 pytest 命令。
- 遵守 `docs/agent_workflow.md`（目录、契约、禁改）与 `docs/dev_loop.md`（分支、自测、PR）。

---

## 七、总控验收说明

总控将按上表 I2-1～I2-5 验收；全部通过则 I-2 完成。若与 Scoring Agent 同批交付，批次 1 需 I-2、S-1、S-3 均通过后方可合并 dev。
