# PR 说明（供 Codex 测试与合并）

> 本分支：`feat/codex-test` → 目标：合并前由 Codex 再次测试整个项目，通过后再合并。

---

## 一、推送与建 PR 步骤（需你在本机执行）

### 1. 推送到 GitHub

本地已提交，需你在有 GitHub 认证的环境执行推送：

```bash
cd /home/lenovo/projects/alpha_tracker2_clean
git push -u origin feat/codex-test
```

若使用 SSH：

```bash
git remote set-url origin git@github.com:SnowHE-hub/Alpha_Tracker2.git
git push -u origin feat/codex-test
```

### 2. 创建 PR

- 打开：https://github.com/SnowHE-hub/Alpha_Tracker2/compare  
- Base 选 `main`（或你的默认分支），compare 选 `feat/codex-test`  
- 标题建议：**chore: 阶段交付 v0.2.0 前 — 请 Codex 全量测试后合并**  
- 描述可粘贴下方「二、PR 描述（给 Codex）」整段。

---

## 二、PR 描述（给 Codex）

```markdown
## 说明
本 PR 为 v0.2.0 阶段交付内容，已在本机完成 Review Agent 阶段测试。请 Codex 再次对**整个项目**做一次测试，全部通过后再合并。

## 请执行以下测试

1. **全量 pytest**
   ```bash
   cd <repo_root>
   PYTHONPATH=src pytest tests/ -x -v
   ```
   预期：全部通过（或仅已知 skip）。

2. **smoke_e2e 全链路**
   ```bash
   PYTHONPATH=src python -m alpha_tracker2.pipelines.smoke_e2e --start 2026-01-06 --end 2026-01-15 --limit 5 --topk 3
   ```
   预期：输出 "smoke_e2e: all steps and checks passed."，退出码 0。

3. **Streamlit 启动**
   ```bash
   streamlit run apps/dashboard_streamlit/app.py --server.headless true
   ```
   预期：能正常启动，无报错退出。

## 通过标准
- 上述三项均通过 → 请合并本 PR。
- 任一项失败 → 请评论失败步骤与报错，暂不合并。
```

---

## 三、本 PR 包含内容摘要

- 批次 0～4 任务书、验收报告及阶段规划文档（NEXT_PHASE_PLAN、BATCH*、PERVASIVE_TEST_*、REVIEW_AGENT_STAGE_*）
- Evaluation：metrics.py、diagnostics、eval_5d_batch、run_diagnostics
- Scoring：score_ensemble、V4 bt_*、ENS
- Dashboard：make_dashboard 扩展（eval_summary/quintile/IC）、Streamlit 应用
- 贯穿测试相关任务书与验收文档
- 新增 tests/ 与 apps/dashboard_streamlit/
