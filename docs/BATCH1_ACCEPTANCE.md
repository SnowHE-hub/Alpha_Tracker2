# 批次 1 验收报告（Infrastructure Agent + Scoring Agent）

> **验收方**：总控 Agent  
> **依据**：`docs/BATCH1_INFRA_TASK.md`、`docs/BATCH1_SCORING_TASK.md`。  
> **结论**：I-2、S-1、S-3 **均通过**，批次 1 完成。

---

## 一、验收概要

| 任务 | 负责 Agent | 验收结果 | 说明 |
|------|------------|----------|------|
| **I-2** bt_* 特征工程 | Infrastructure | ✅ 通过 | bt_mean/bt_winrate/bt_worst_mdd 已计算并写入，范围合理，测试与 smoke_e2e 通过 |
| **S-1** V1–V3 重设计 | Scoring | ✅ 通过 | V1/V2/V3/V4 权重从 config 读取，reason JSON 含配置权重 |
| **S-3** 按版本阈值 | Scoring | ✅ 通过 | V2/V3/V4 使用 per-version q/window，V3 与 V2/V4 thr_value 不同 |

---

## 二、I-2 逐项核对

| 验收项 | 判定 | 依据 |
|--------|------|------|
| **I2-1** bt_* 已计算并写入 | ✅ | `build_features` 后 features_daily(2025-03-12) 三行均有非 NULL 的 bt_mean、bt_winrate、bt_worst_mdd；INSERT 含三列（build_features.py cols + execmany） |
| **I2-2** 数值范围合理 | ✅ | 抽样：bt_winrate ∈ [0.45, 0.5]，bt_worst_mdd 约 -0.14～-0.16（≤0），bt_mean 为收益量级（约 -0.002～0.004）；test_bt_winrate_in_range、test_bt_worst_mdd_non_positive 通过 |
| **I2-3** 幂等与兼容 | ✅ | build_features 仍为 DELETE 后 INSERT 同 (trade_date, ticker)；其他特征列未改；重复运行无报错 |
| **I2-4** 真实数据测试 | ✅ | test_build_features_bt_integration 使用临时 DB + 合成数据（结构匹配真实）；test_price_features_bt 覆盖 bt_* 列存在、范围、非空；集成测试断言 features_daily 有 bt_* 非空且范围正确 |
| **I2-5** smoke_e2e 通过 | ✅ | `smoke_e2e --start 2024-01-01 --end 2025-03-13 --limit 5 --topk 3` 六步通过，build_features wrote_rows=3，features_daily 检查通过 |

**实现要点**：`price_features.py` 增加 bt_window（默认 60）、bt_mean/bt_winrate/bt_worst_mdd 滚动计算；`build_features._write_features` 扩展 cols 与 INSERT；config 可选 `features.bt_window`。

---

## 三、S-1 逐项核对

| 验收项 | 判定 | 依据 |
|--------|------|------|
| **S1-1** V1 权重来自 config | ✅ | `v1_baseline.load_v1_config(project_root)` 从 scoring.v1.weights 读取；registry.get_scorer("V1", project_root) 注入；picks_daily V1 reason 含 `"weights": {"ret_5d": 0.5, "ret_20d": 0.3, "avg_amount_20": 0.2}` 与 default.yaml 一致 |
| **S1-2** V2/V3/V4 权重来自 config | ✅ | `v2_v3_v4.load_core_config(project_root, "V2")` 等从 versions.V2/V3/V4 读取 trend_weight、risk_weight、bt_weight；picks_daily V2 reason 含 `"trend_weight": 0.4, "risk_weight": 0.3, "bt_weight": 0.0` 与 config 一致 |
| **S1-3** reason JSON 结构化且含权重 | ✅ | V1 reason 为合法 JSON，含 factors、z、weights；V2 reason 含 trend_score、risk_penalty、trend_weight、risk_weight、bt_weight |
| **S1-4** 现有链路不受损 | ✅ | smoke_e2e 全链路通过；score_all 按 score_versions 调度 V1–V4，写入 picks_daily 无报错 |

**实现要点**：`registry.get_scorer(version, project_root)` 在提供 project_root 时调用 load_v1_config / load_core_config；score_all 传入 project_root；V1/V2/V3/V4 的 reason 已包含配置权重。

---

## 四、S-3 逐项核对

| 验收项 | 判定 | 依据 |
|--------|------|------|
| **S3-1** 按版本读取 q、window | ✅ | default.yaml 中 V2.q=0.8、V3.q=0.7、V4.q=0.8；score_all 输出 V2 thr_cfg(q=0.8)、V3 thr_cfg(q=0.7)、V4 thr_cfg(q=0.8)；picks_daily 中 V2/V3/V4 的 thr_value 互不相同（V3 与 V2/V4 因 q 不同而不同） |
| **S3-2** 缺省回退到 common | ✅ | `_load_per_version_threshold_config` 对未在 versions.<V>.q/window/topk_fallback 中指定的版本使用 common 对应值 |
| **S3-3** V1 不受影响 | ✅ | picks_daily 中 V1 与 UNIVERSE 的 thr_value 为 NULL；score_all 对 V1 不应用阈值与 fallback |
| **S3-4** smoke_e2e 通过 | ✅ | 同上，全链路通过，picks_daily 五表检查满足 |

**实现要点**：`score_all._load_per_version_threshold_config(project_root)` 返回 version -> (q, window, topk_fallback)；循环内按版本取 thr_cfg 与 fallback_topk；config 中 versions.V2/V3/V4 已含 q、window、topk_fallback。

---

## 五、测试与执行记录

- **pytest**：`PYTHONPATH=src pytest tests/ -v`，**19 passed**（含 test_price_features_bt、test_build_features_bt_integration、test_config、test_schema、test_trading_calendar）。
- **smoke_e2e**：`--start 2024-01-01 --end 2025-03-13 --limit 5 --topk 3`，真实数据区间，**all steps and checks passed**，退出码 0。
- **抽样查询**：features_daily(2025-03-12) 三行 bt_mean/bt_winrate/bt_worst_mdd 非空且范围正确；picks_daily 同日前述 reason 与 thr_value 符合预期。

---

## 六、总控结论

- **I-2**：满足 BATCH1_INFRA_TASK 验收标准 I2-1～I2-5，**通过**。
- **S-1**：满足 BATCH1_SCORING_TASK 验收标准 S1-1～S1-4，**通过**。
- **S-3**：满足 BATCH1_SCORING_TASK 验收标准 S3-1～S3-4，**通过**。

**批次 1 验收通过**。Infrastructure Agent 与 Scoring Agent 交付物符合任务书要求，可合并入 dev，作为后续 S-2（V4 接入 bt_*）、S-4（ENS）的依赖基础。总控据此关闭批次 1，可进入批次 2 任务设计与分配。
