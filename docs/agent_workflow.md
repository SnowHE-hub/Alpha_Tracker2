# Agent 工作流与施工规范

本文件是本仓库在 Cursor 中协作的**项目宪法**，约定分支规则、Agent 分工、目录可改范围、核心表契约与验收顺序。**所有 Agent 在修改代码前必须先阅读并遵守本文件**。与开发循环的配合见 `docs/dev_loop.md`。

---

## 一、分支规则

- **main**：生产稳定分支，仅接受自 `dev` 的合并；不直接提交、不直接 PR。
- **dev**：集成分支，功能开发完成并经过 Codex review 的 PR 合并到此；用于日常联调与 smoke 验证。
- **feat/***：功能分支，从 `dev` 拉取，命名示例 `feat/universe-lake`、`feat/score-config`。单分支只做单一功能/任务，完成后 PR 回 `dev`。
- **规则**：
  - 新需求一律从 `dev` 切 `feat/<描述>`，禁止从 `main` 直接切功能分支。
  - 合并顺序：`feat/*` → PR → `dev`；`dev` 稳定后 → 合并 `main`。
  - 禁止在 `dev`/`main` 上强制推送；`feat/*` 可 rebase 到最新 `dev`。

---

## 二、Agent 分工

- **总控 Agent**：拆解任务、下发任务书（如 `docs/REVIEW_AGENT_TASK.md`）、定义验收标准、做最终验收（如 `docs/REVIEW_AGENT_ACCEPTANCE.md`）。不直接改业务代码。
- **Review Agent**：在总控下发的任务范围内，完成修复/补充（如 universe lake、scoring 配置、forward_returns 文档、smoke_e2e），并自测与交付；必须遵守本文件的契约与禁改规则。
- **功能 Agent（含 Cursor Agent）**：在指定目录与契约内实现新功能（新 ingestion、新特征、新 scorer、新 pipeline 等）；修改前需确认不触及“禁改规则”与“基础设施层”。
- **自检 / 验收**：由总控或指定 Agent 执行；验收顺序见第五节。

---

## 三、哪些目录谁能改

以下约定**谁可以改哪些目录**，避免多人/多 Agent 踩踏。任务书可在此基础上进一步收窄（如“仅允许改 ingestion/ 与 pipelines/ingest_universe.py”）。

| 层级 | 目录/文件 | 可改方 | 说明 |
|------|-----------|--------|------|
| **基础设施与契约** | `src/alpha_tracker2/core/`、`src/alpha_tracker2/storage/`、`configs/`、`docs/` | 仅架构/基础设施类任务 | 含 `core/config.py`、`storage/schema.sql`、`configs/default.yaml`、`docs/agent_workflow.md`。新增表、新配置项、新 Provider 注册等在此；**禁止**随意改已有表主键或已有列语义。 |
| **业务逻辑** | `ingestion/`、`features/`、`scoring/`、`strategies/`、`portfolio/`、`execution/`、`evaluation/`、`reporting/`、`pipelines/` | 功能 Agent / Review Agent（按任务书） | 新 pipeline、新 scorer、新特征、新评估逻辑等；须通过 registry/config 接入，不破坏幂等与 run_daily 编排。 |
| **应用与脚本** | `apps/`、`scripts/`、`tools/` | 功能 Agent | Dashboard、一次性脚本、验收/诊断工具；不改变核心表契约。 |
| **数据与输出** | `data/`（含 `lake/`、`store/`、`out/`、`runs/`） | 仅本地 pipeline 运行 | 不提交生产数据到 Git；示例/测试数据若提交需在文档中说明。 |

**禁改规则（所有人必须遵守）**：

- 不得修改 `storage/schema.sql` 中**已有表的主键或已有列语义**（可新增列、新增表，并同步更新本文档与 schema）。
- 不得在 `pipelines/run_daily.py` 中加入业务逻辑；仅允许解析参数、按序调用各 pipeline 的 `main()`、记录 run meta。
- 不得破坏 DuckDB 的**幂等写入**（同主键范围重复运行不产生重复/冲突记录）。
- 不得绕过 **registry / config** 硬编码数据源、版本名、API key 等；新增能力须在对应 registry 与 `configs/default.yaml` 中注册。

---

## 四、核心表契约（DuckDB）

所有 Agent 必须尊重以下表的**主键与字段语义**。允许**新增字段**或**新增表**，不得在未说明的情况下更改已有字段含义。完整 DDL 以 `src/alpha_tracker2/storage/schema.sql` 为准。

| 表名 | 主键 | 关键字段与约定 |
|------|------|----------------|
| **prices_daily** | (trade_date, ticker) | trade_date, ticker, market, open/high/low/close, **adj_close**（收益/回测一律用 adj_close）, volume, amount, currency, source |
| **features_daily** | (trade_date, ticker) | 价量/动量/波动/均线等；不写模型版本信息；版本/策略信息仅出现在 picks_daily 等下游表 |
| **picks_daily** | (trade_date, version, ticker) | trade_date, version（如 UNIVERSE, V1–V5, ENS）, ticker, name, score, score_100, rank, reason, thr_value, pass_thr, picked_by；Scorer 只负责 score/reason，入选与 pass_thr/picked_by 由 score_all+thresholds 决定 |
| **nav_daily** | (trade_date, portfolio) | trade_date, portfolio, nav, ret；组合层输出，不记录单只股票 |
| **eval_5d_daily** | (as_of_date, version, bucket) | as_of_date, version, bucket, fwd_ret_5d, n_picks, horizon；前向收益 as_of 语义以 `evaluation/forward_returns.py` 中 `compute_forward_returns` 的 docstring 为准 |

---

## 五、验收顺序

为保证“真的能跑”且符合契约，验收按以下顺序执行（与 `docs/dev_loop.md` 中的每轮流程一致）：

1. **实现完成**：功能在 `feat/*` 上实现完毕，符合任务书与本文档（目录可改范围 + 核心表契约 + 禁改规则）。
2. **本地 smoke/test**：在项目根目录执行端到端 smoke（如 `PYTHONPATH=src python -m alpha_tracker2.pipelines.smoke_e2e --start ... --end ...`），以及任务要求的单元/集成测试；全部通过方可进入下一步。
3. **Cursor self-review**：由实现方（或 Cursor Agent）自检：是否触及 schema 主键/列语义、run_daily、幂等、registry/config；若有，需在 PR 中说明并确认兼容。
4. **Codex review**：由指定人员或工具对 PR 做代码评审；通过后合并到 `dev`。
5. **PR 到 dev**：将 `feat/*` 合并到 `dev`，不在 `dev` 上直接做大改动。
6. **dev 稳定后合并 main**：在 `dev` 上确认 smoke/回归无问题后，由维护者将 `dev` 合并到 `main`，并打 tag（如需）。

只有按上述顺序完成，该轮交付才视为验收通过；Agent 任务书的“验收总结”应引用本顺序。

---

*本文档可随项目演进补充，修改视为架构层变更，需谨慎并同步更新所有引用（如任务书、RUNBOOK）。*
