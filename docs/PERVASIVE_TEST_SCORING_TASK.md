# 贯穿 - Scoring Agent：V1–V4、ENS 单元/集成测试补齐

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 第五节 5.3（Scoring Agent）、`docs/PERVASIVE_TEST_OVERVIEW.md`。  
> **执行方**：Scoring Agent（或认领该角色的功能 Agent）。  
> **前置**：批次 1（S-1、S-3）、批次 2（S-2、S-4）已交付并验收通过。

---

## 一、目标

确保 **V1–V4** 与 **ENS** 具备符合 NEXT_PHASE_PLAN 第五节的单元测试（含权重与阈值从 config 读取）及集成测试（在真实 features_daily 上跑 score_all/score_ensemble，检查 picks_daily 与 reason JSON）。

---

## 二、责任范围与期望测试

| 任务 | 内容 | 期望测试覆盖 |
|------|------|--------------|
| **S-1 / S-3** | V1–V3 重设计、按版本阈值 | 单元测试：各版本 scorer 从 config 读取权重/阈值；输出 score、reason JSON 结构 |
| **S-2** | V4 接入 bt_* | 单元测试：V4 使用 bt_* 列与 bt_column_weights；bt_* 缺失时不报错；reason 含 bt_score、ma_bonus 说明 |
| **S-4** | ENS 投票集成 | 单元测试：score_ensemble 逻辑（投票/聚合）；集成测试：score_all + score_ensemble 后 picks_daily 含 version='ENS'，行数与预期一致 |
| **集成** | 全链路 | 在真实 features_daily（或约定 fixture，至少 20 个交易日、US+HK）上跑 score_all 与 score_ensemble，检查 picks_daily 各 version 有数据、reason 为合法 JSON、关键列非空 |

---

## 三、验收标准（Scoring）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| **SCR-1** | V1–V4 单元测试存在且通过 | 存在针对 scoring 插件（V1/V2/V3/V4）的单元测试；覆盖权重与阈值从 config 读取、score 与 reason 结构；`pytest tests/ -k "scor" 或约定 test_score 文件` 通过 |
| **SCR-2** | ENS 单元或集成测试存在且通过 | 存在 score_ensemble 或 picks_daily(version='ENS') 的测试；断言 ENS 行存在、来源为 V1–V4 聚合/投票 |
| **SCR-3** | 集成测试使用真实数据 | 至少一项集成测试在真实 features_daily（或总控认可的 fixture）上跑 score_all（及 score_ensemble），检查 picks_daily 与 reason JSON；数据区间至少 20 个交易日、含 US 或 HK ticker |
| **SCR-4** | smoke_e2e 中 scoring 步骤通过 | smoke_e2e 全链路包含 score_all（及 score_ensemble）；picks_daily 检查含多 version 与 ENS；与现有 BATCH2 验收一致 |
| **SCR-5** | 真实数据底线 | 同 NEXT_PHASE_PLAN 5.2；若使用 fixture，需在文档中说明区间、数据来源与复现方式 |

---

## 四、可修改范围

- **允许**：在 `tests/` 下新增 `test_scoring_*.py` 或 `test_score_*.py`（或按项目约定命名）；使用现有 store/schema fixture 或真实 DB 子集；在 scoring 模块内仅限为便于测试的只读/可注入依赖（不改变现有公共接口语义）。
- **禁止**：为通过测试而放宽 S-1～S-4 的业务逻辑；删除已有、已通过验收的 smoke 或批次验收用例。

---

## 五、自检与交付

- 列出所有与 Scoring 相关的测试文件及运行命令（如 `pytest tests/test_scoring_*.py tests/test_score_ensemble.py -v`），确保全部通过。
- 在交付物中注明：集成测试使用的数据区间、来源（如“smoke 区间 + features_daily”）及复现步骤。
- 总控验收时与全量 pytest、smoke_e2e 一并检查；通过则 Scoring 贯穿测试项通过。
