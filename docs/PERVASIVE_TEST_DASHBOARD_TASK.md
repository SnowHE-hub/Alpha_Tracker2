# 贯穿 - Dashboard Agent：D-1 / D-2 测试补齐

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 第五节 5.3（Dashboard Agent）、`docs/PERVASIVE_TEST_OVERVIEW.md`。  
> **执行方**：Dashboard Agent（或认领该角色的功能 Agent）。  
> **前置**：批次 4（D-1、D-2）已交付并验收通过。

---

## 一、目标

确保 **D-1 make_dashboard 扩展** 与 **D-2 Streamlit 应用** 具备符合 NEXT_PHASE_PLAN 第五节的集成测试：D-1 导出结果与 D-2 启动、基本渲染可验证；可用真实 data/out 或小型真实数据生成的 CSV。

---

## 二、责任范围与现有测试

| 任务 | 内容 | 现有测试文件（需保持通过或补齐） |
|------|------|----------------------------------|
| **D-1** | make_dashboard（eval_summary、quintile/IC 导出） | `tests/test_make_dashboard.py`（build_eval_summary 列、mean_ic 来自 ic_series、eval_summary/quintile_returns/ic_series schema） |
| **D-2** | Streamlit 启动与展示 | 无强制 pytest（Streamlit 启动为总控验收项）；可选：headless 或 UI 自动化；或文档化“手动启动 + 检查 NAV/picks/评估面板” |

---

## 三、验收标准（Dashboard）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| **DASH-1** | D-1 导出测试存在且通过 | `pytest tests/test_make_dashboard.py -v` 全部通过；覆盖 eval_summary.csv 列（version、mean_fwd_ret_5d、mean_ic、n_dates）、quintile_returns 与 ic_series 列约定、build_eval_summary 与 ic_series 结合 |
| **DASH-2** | D-1 与真实 data/out 兼容 | 在真实数据区间执行 make_dashboard 后，eval_summary、quintile_returns、ic_series 存在且格式正确；或由现有 BATCH4 验收覆盖 |
| **DASH-3** | D-2 启动可验证 | `streamlit run apps/dashboard_streamlit/app.py` 能正常启动（可用 --server.headless true 做自动化或文档化手动步骤）；总控验收清单中明确“Streamlit 启动并展示 NAV、picks、评估面板” |
| **DASH-4** | 真实数据底线 | 测试使用真实 data/out（由 make_dashboard 或 eval_5d_batch 生成）或文档中说明的小型真实数据生成的 CSV；禁止仅用空 CSV 通过验收 |
| **DASH-5** | 与全量 pytest 一致 | 上述测试纳入项目约定 `pytest` 命令，无单独排除；总控执行 `pytest -x` 时包含 test_make_dashboard |

---

## 四、可修改范围

- **允许**：在 `tests/` 下新增或修改与 make_dashboard、Streamlit 数据加载相关的测试；可选增加 Streamlit 的 headless/无头检测脚本（若项目采纳）；在 reporting、apps 内仅限为便于测试的只读或 fixture。
- **禁止**：为通过测试而放宽 D-1 导出列约定或 D-2 展示要求；删除已有、已通过批次 4 验收的用例。

---

## 五、自检与交付

- 执行：`pytest tests/test_make_dashboard.py -v`，全部通过。
- 执行：`streamlit run apps/dashboard_streamlit/app.py`（或约定命令），确认可启动并展示 NAV、picks、评估面板；在交付物中注明为“手动”或“自动化”检查。
- 总控验收时与全量 pytest、Streamlit 启动检查一并执行；通过则 Dashboard 贯穿测试项通过。

---

## 六、自检清单与验收命令（Dashboard Agent）

| 检查项 | 命令 | 说明 |
|--------|------|------|
| **D-1 测试** | `pytest tests/test_make_dashboard.py -v` | 必须全部通过；含 eval_summary 列、build_eval_summary+ic_series、schema、以及当存在 data/out 时对真实 CSV 的校验 |
| **全量 pytest** | `PYTHONPATH=src pytest -x` | 含 test_make_dashboard，无排除 |
| **Streamlit 启动（自动化）** | `PYTHONPATH=src streamlit run apps/dashboard_streamlit/app.py --server.headless true --server.port 8502` | 启动成功即通过；可加 timeout 后终止 |
| **Streamlit 启动（手动）** | `PYTHONPATH=src streamlit run apps/dashboard_streamlit/app.py` | 浏览器打开后检查：NAV 面板（净值曲线/表）、Picks 面板（选股表+日期/版本筛选）、评估面板（eval_summary、quintile、IC 序列） |

**数据准备**：先执行 `make_dashboard` 再启动 Streamlit，以便 data/out 下有 nav_daily、eval_summary、quintile_returns、ic_series 等 CSV，供 D-2 展示。
