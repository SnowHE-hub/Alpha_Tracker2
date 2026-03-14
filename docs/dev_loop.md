# 开发循环（每轮固定流程）

本文档约定**每轮功能开发**的固定步骤，与 `docs/agent_workflow.md` 中的分支规则、Agent 分工、验收顺序一致。所有参与开发的 Agent 与开发者均按此循环执行。

---

## 每轮固定流程

### 1. 从 dev 切 feat/*

- 在 `dev` 上拉取最新：`git checkout dev && git pull origin dev`
- 新建功能分支：`git checkout -b feat/<功能描述>`  
  示例：`feat/universe-lake`、`feat/score-config`、`feat/v5-scorer`
- 禁止从 `main` 直接切功能分支

### 2. Cursor Agent 实现

- 在 `feat/*` 上按任务书（或需求说明）实现功能
- 遵守 `docs/agent_workflow.md`：目录可改范围、核心表契约、禁改规则
- 新增能力通过 registry + `configs/default.yaml` 注册，不硬编码

### 3. 本地 smoke / test

- **端到端 Smoke**（必做）：
  ```bash
  PYTHONPATH=src python -m alpha_tracker2.pipelines.smoke_e2e --start <start> --end <end> [--limit 5] [--topk 3]
  ```
  确保六步跑通、五张表有数据、关键字段非空（详见 `docs/smoke_e2e_sample_output.txt`）。
- 任务要求的单元测试或集成测试一并跑通
- 未通过则在本步修复，不进入下一步

### 4. Cursor self-review

- 自检是否触及：
  - `storage/schema.sql` 已有表主键或列语义
  - `pipelines/run_daily.py`（仅允许编排，不加业务逻辑）
  - DuckDB 幂等写入
  - registry / config 使用方式
- 若有上述改动，在 PR 中明确说明并确认兼容性

### 5. Codex review

- 将 `feat/*` 提交并推送，发起 PR 到 `dev`
- 由指定人员或 Codex 进行代码评审
- 按评审意见修改后再次通过 smoke/test

### 6. PR 到 dev

- PR 通过 Codex review 后合并到 `dev`
- 不在 `dev` 上直接做大改动；所有功能均经 `feat/*` → PR → `dev`

### 7. dev 稳定后合并 main

- 在 `dev` 上做回归/smoke 确认无问题
- 由维护者将 `dev` 合并到 `main`
- 视需要打 tag（如 `v1.2.0`）

---

## 小结

| 步骤 | 动作 |
|------|------|
| 1 | 从 dev 切 feat/* |
| 2 | Cursor Agent 实现 |
| 3 | 本地 smoke / test |
| 4 | Cursor self-review |
| 5 | Codex review |
| 6 | PR 到 dev |
| 7 | dev 稳定后合并 main |

**这两份文档（`docs/agent_workflow.md` + `docs/dev_loop.md`）构成后续所有 Agent 的“施工规范”，不得跳过或调乱顺序。**
