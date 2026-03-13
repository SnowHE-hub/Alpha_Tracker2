# alpha_tracker2 Runbook (Single Source of Truth)

This repo contains **two** runnable "shapes":

## 1) Mainline (NEW): `src/alpha_tracker2/*` pipelines
**Purpose:** research / evaluation closed-loop, multi-version (V1–V4) comparability.

### Official entrypoints
- One-click daily runner (recommended):
  - `python run_daily.py --date YYYY-MM-DD`
  - or `python -m alpha_tracker2.pipelines.run_daily --date YYYY-MM-DD`

### Standard pipeline order
1. `ingest_universe` → writes `picks_daily(version='UNIVERSE')`
2. `ingest_prices` → writes `prices_daily` + Parquet cache
3. `build_features` → writes `features_daily`
4. `score_all` → writes `picks_daily(version in V1..V4)`
5. `eval_5d` (optional) → writes eval tables
6. `portfolio_nav` → outputs NAV CSV (and/or table depending on module)
7. `make_dashboard` → outputs dashboard CSV into `data/out/`

### Output locations (mainline)
- `data/lake/` (Parquet): large cached panels
  - `data/lake/universe/YYYY-MM-DD/universe.parquet`
  - `data/lake/prices/{ticker}/*.parquet`
- `data/store/alpha_tracker.duckdb` (DuckDB): fast query store
  - canonical tables: `picks_daily`, `prices_daily`, `features_daily`, `meta_runs`, ...
- `data/out/` (CSV): dashboard-ready aggregated outputs
- `data/runs/` (JSON): run metadata written by `run_daily`

## 2) Legacy (OLD): vendor runner + tracker
**Purpose:** older baseline that bundles "compare → per-strategy tracker → nav aggregation".

Files commonly seen in the old workflow (NOT part of mainline):
- `run_daily.py` (legacy)
- `four_version_compare.py`
- `portfolio_live_tracker.py`
- an older `apps/dashboard_streamlit/app.py`

Keep legacy code for reference / regression checks, but do **not** mix its outputs
with the mainline pipelines unless you explicitly create an adapter layer.


# Alpha_Tracker2 RUNBOOK（Single Source of Truth）

## 0. 本文档的目的

本 RUNBOOK 用于明确：

* Alpha_Tracker2 **唯一官方运行方式**；
* 主线与 legacy 的边界；
* 数据产物的权责划分；
* 已踩坑位的强约束规避方案。

> 任何新环境 / 新对话框 / 新协作者，必须以本 RUNBOOK 为准。

---

## 1. 项目存在的两种“形态”

### 1.1 Mainline（NEW，官方主线）

**路径**：`src/alpha_tracker2/*`

**用途**：

* 研究 / 评估闭环
* 多版本（V1–V4）对齐比较

**官方入口**：

* `python run_daily.py --date YYYY-MM-DD`
* `python -m alpha_tracker2.pipelines.run_daily --date YYYY-MM-DD`

---

### 1.2 Legacy（OLD，仅参考）

**用途**：

* 历史基线验证
* 逻辑对照

**文件示例**：

* four_version_compare.py
* portfolio_live_tracker.py
* 旧版 run_daily.py / app.py

⚠️ 规则：

* Legacy 不允许写入 data/store
* Legacy 输出不得与 mainline dashboard 混用

---

## 2. 官方 Pipeline 顺序（Mainline）

1. ingest_universe → universe_daily
2. ingest_prices → prices_daily + Parquet
3. build_features → features_daily
4. score_all → picks_daily（V1–V4）
5. eval_5d（可选）
6. portfolio_nav
7. make_dashboard

---

## 3. 数据目录职责划分

### data/lake/

* 大体量 parquet
* 可重复构建

### data/store/alpha_tracker.duckdb

* 单一事实源
* dashboard / eval / nav 必须从此读取

### data/out/

* dashboard-ready CSV

### data/runs/

* 每次 run_daily 的元信息 JSON

---

## 4. picks_daily 契约（核心表）

**主键**：trade_date + version + ticker

**关键字段语义**：

* score：原始模型分
* score_100：0–100 归一化展示分
* thr_value：阈值（仅 V2–V4）
* pass_thr：是否通过阈值
* picked_by：BASELINE_RANK / THRESHOLD / FALLBACK_TOPK
* reason：结构化可解释文本

⚠️ 强规则：

* scorer 不得决定是否入选
* fallback 只能由 score_all 决定

---

## 5. 阈值机制强约束

* 阈值来源：ab_threshold_history.json
* 更新基于 **全量候选分数**
* 禁止基于已筛选样本更新

---

## 6. 已知坑位与强制规避

### PowerShell

* 禁止使用 <<EOF
* 必须写成 .py 文件执行

### pandas

* 禁止 fillna(None)
* 使用 string dtype + where

### Streamlit

* 只能使用：`streamlit run app.py`
* 禁止 python app.py

### 路径

* 所有路径只能来自 configs/default.yaml

### 数据库

* 只允许 DuckDBStore 写入

---

## 7. 版本与输出隔离

* 所有输出必须带 version 维度
* 主键设计防止覆盖

---

## 8. 结论

> 本 RUNBOOK 是 Alpha_Tracker2 的 **唯一运行事实源**。
> 任何偏离，均视为错误用法。
