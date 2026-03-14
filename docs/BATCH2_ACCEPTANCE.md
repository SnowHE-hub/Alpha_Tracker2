# 批次 2 验收报告（Scoring Agent）

> **验收方**：总控 Agent  
> **依据**：`docs/BATCH2_SCORING_TASK.md`。  
> **结论**：S-2 与 S-4 **均通过**，批次 2 完成。

---

## 一、验收概要

| 任务 | 验收结果 | 说明 |
|------|----------|------|
| **S-2** V4 接入 bt_* | ✅ 通过 | V4 使用 bt_* 与 bt_column_weights 计算 bt_score，与 trend/risk、ma_bonus 参与 score；reason 含 bt_score、ma_bonus、bt_* 原始值 |
| **S-4** ENS 投票集成 | ✅ 通过 | score_ensemble 写入 picks_daily(version='ENS')；run_daily 在 score_all 后调用；smoke_e2e 含 7 步与 ENS 检查 |

---

## 二、S-2 逐项核对

| 验收项 | 判定 | 依据 |
|--------|------|------|
| **S2-1** V4 使用 bt_* 参与打分 | ✅ | V4Scorer.score() 使用 _fetch_features(include_bt=True)、_compute_bt_score()、_compose_score(..., bt_score=bt_score)；registry 传入 load_bt_column_weights(project_root)；picks_daily V4 reason 含 bt_score（如 1.31）且与 trend/risk 共同影响 score |
| **S2-2** bt_score 来自 bt_column_weights | ✅ | load_bt_column_weights 从 scoring.v2_v3_v4.bt_column_weights 读取；_compute_bt_score(df, weights) 对 bt_mean/bt_winrate/bt_worst_mdd 做 z-score 后加权；reason 含 bt_mean、bt_winrate、bt_worst_mdd 原始值 |
| **S2-3** reason 含 bt_score 与 ma_bonus 说明 | ✅ | V4 reason 为合法 JSON，含 bt_score、ma_bonus、ma_bonus_included_in_trend_score: true、bt_mean/bt_winrate/bt_worst_mdd |
| **S2-4** bt_* 缺失时不报错 | ✅ | _fetch_features(include_bt=True) 在 schema 无 bt_* 时 retry 不含 bt 列；_compute_bt_score 在 available 为空或全 NULL 时返回 0；_compose_score 中 bt_score.fillna(0.0) |
| **S2-5** 现有链路与 smoke 通过 | ✅ | smoke_e2e 七步通过；V1/V2/V3 行为未改；V4 写入 picks_daily 正常 |

**实现要点**：V4 专用 _fetch_features(include_bt=True)、load_bt_column_weights、_compute_bt_score（z-score 后加权）、_compute_ma_bonus；_compose_score 增加 bt_score 参数；V4Scorer 覆写 score() 并注入 bt_column_weights。

---

## 三、S-4 逐项核对

| 验收项 | 判定 | 依据 |
|--------|------|------|
| **S4-1** ENS 写入 picks_daily | ✅ | score_ensemble 执行后 picks_daily 存在 version='ENS'、trade_date=2025-03-12 的 3 行；trade_date、version、ticker、score、rank 非空 |
| **S4-2** 聚合规则可复现 | ✅ | score_ensemble 从 picks_daily 读 V1–V4 的 score_100，按 ticker 取均值后排序得 rank，写入 ENS；reason 含 input_versions 与聚合说明；修改输入版本或 picks 后重跑结果会变 |
| **S4-3** run_daily 调用 ensemble | ✅ | run_daily 在 step 4 (score_all) 之后执行 step 4b (score_ensemble.main)，带 --skip-ensemble 时跳过；meta 中 skips.ensemble 记录 |
| **S4-4** smoke_e2e 含 ENS | ✅ | smoke_e2e 为 7 步（含 score_ensemble）；picks_daily 检查要求 version='ENS' 在 target 日至少 1 行；本次运行 score_ensemble 写入 3 行，检查通过 |
| **S4-5** 幂等 | ✅ | score_ensemble 先 `DELETE FROM picks_daily WHERE trade_date = ? AND version = 'ENS'` 再 INSERT；重复运行同日期不报错、无重复行 |

**实现要点**：pipelines/score_ensemble.py 支持 --date、--versions；从 config scoring.ensemble.input_versions 读默认版本列表；按 score_100 均值聚合、排序写 ENS；run_daily 与 smoke_e2e 已接入。

---

## 四、测试与执行记录

- **pytest**：`PYTHONPATH=src pytest tests/ -v`，**19 passed**（与批次 1 一致，无新增失败）。
- **smoke_e2e**：`--start 2024-01-01 --end 2025-03-13 --limit 5 --topk 3`，七步（含 score_ensemble），**all steps and checks passed**，退出码 0。
- **抽样查询**：V4 reason 含 bt_score、ma_bonus、bt_mean/bt_winrate/bt_worst_mdd；ENS 三行 score/rank 合理（AAPL 62.5 rank 1，0700.HK 50.0 rank 2，MSFT 37.5 rank 3）。

---

## 五、总控结论

- **S-2**：满足 BATCH2_SCORING_TASK 验收标准 S2-1～S2-5，**通过**。
- **S-4**：满足 BATCH2_SCORING_TASK 验收标准 S4-1～S4-5，**通过**。

**批次 2 验收通过**。Scoring Agent 交付物符合任务书要求，可合并入 dev，作为后续 E-2、D-1/D-2 的依赖基础。总控据此关闭批次 2，可进入批次 3 任务设计与分配。
