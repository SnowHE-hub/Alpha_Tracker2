## Alpha_Tracker2 US/HK – Agent 主控文档（Project Charter）

本文件是本仓库在 Cursor 中协作的“项目宪法”，约定项目目标、核心表契约、目录 ownership 与禁改规则。所有 Agent 在修改代码前都应先阅读并遵守本文件。

---

## A. 项目目标（从 README 浓缩）

- **数据源**：以 **Yahoo API**（如 `yfinance` / `yahooquery` 封装）为主，获取 **美股（US）+ 港股（HK）** 的日线数据。
- **存储架构**：
  - **DuckDB** 为唯一主存储（`data/store/alpha_tracker.duckdb`）。
  - **Parquet lake** 为原始/缓存层（`data/lake/**`），避免重复请求外部 API。
- **分层架构**：
  - 数据层（Ingestion：universe + prices + 可选扩展）
  - 特征层（Features：价量 + 滚动回测 + 可选扩展特征）
  - 模型层（Scoring：V1–V5 + 可选深度学习）
  - 策略层（Strategies：TopK、分散、轮动等）
  - 组合与执行层（Portfolio & Execution）
  - 评估层（Evaluation）
  - 应用层（Reporting & Dashboard）
- **端到端闭环**：
  - 从 **ingest_universe / ingest_prices** 起，经过 **build_features → score_all/score_ensemble → portfolio_nav / nav_from_positions → eval_5d(_batch) → make_dashboard**，形成从数据到 dashboard 的完整可复现实验与回测链路。

---

## B. 核心表契约（DuckDB）

所有 Agent 必须尊重以下表的**主键和字段语义**；增加字段可以，但不得在未特殊说明的情况下更改已有字段的含义。

> 注意：字段列表为“关键字段”，并非完整 schema，详细 DDL 以 `storage/schema.sql` 为准。

### 1. `prices_daily`

- **主键**：`(trade_date, ticker)`
- **关键字段**：
  - `trade_date`：交易日（本地日期，UTC 对齐后按市场日历定义）
  - `ticker`：标的代码（Yahoo 格式，如 `AAPL`, `0700.HK`）
  - `market`：市场标识（如 `"US"`, `"HK"`）
  - `open, high, low, close`：原始日线价格（未复权）
  - `adj_close`：复权收盘价，**所有收益/回测一律基于 adj_close**
  - `volume`：成交量（股数）
  - `amount`：近似成交额（通常为 `adj_close * volume` 或数据源给出）
  - `currency`：货币（如 `"USD"`, `"HKD"`）
  - `source`：数据来源标识（如 `"yahoo"`）

### 2. `features_daily`

- **主键**：`(trade_date, ticker)`
- **关键字段（价量与回测特征为主）**：
  - 收益/动量：`ret_1d, ret_5d, ret_10d, ret_20d`
  - 波动与风险：`vol_5d, vol_ann_60d, mdd_60d`
  - 趋势与均线：`ma5, ma10, ma20, ma60, ma5_gt_ma10_gt_ma20, ma20_above_ma60, ma20_slope`
  - 滚动回测：`bt_mean`（DOUBLE，滚动窗口内模拟收益均值）、`bt_winrate`（DOUBLE，滚动窗口内胜率 [0,1]）、`bt_worst_mdd`（DOUBLE，滚动窗口内最差回撤，非正数或 NULL）；与选股设计文档 V2–V4 对齐
  - 量价流动性：`avg_amount_20` 等

> 约定：`features_daily` 不直接写入模型版本信息。所有与“版本/策略”相关的信息，只出现在 `picks_daily` 等下游表中。**bt_\*** 由 I-2 特征工程写入，V4 在 S-2 中接入。

### 3. `picks_daily`

- **主键**：`(trade_date, version, ticker)`
- **关键字段**：
  - `trade_date`：信号/选股生成日
  - `version`：模型或策略版本（如 `"UNIVERSE"`, `"V1"`, `"V2"`, `"V3"`, `"V4"`, `"V5"`, `"ENS"` 等）
  - `ticker`：标的代码
  - `name`：标的名称（可为空）
  - `score`：原始打分（实数，一般未归一化）
  - `score_100`：0–100 的归一化分数（仅用于排序/展示，不改变排序逻辑）
  - `rank`：当日该版本内的排序（1 为最好）
  - `reason`：解释字段（推荐 JSON 字符串，包含各因子/模块的贡献与说明）
  - `thr_value`：该版本当日使用的分位数阈值（V2/V3/V4/V5 等）
  - `pass_thr`：布尔，是否通过阈值（DuckDB boolean，可为 NULL）
  - `picked_by`：枚举字符串（如 `"BASELINE_RANK"`, `"THRESHOLD"`, `"FALLBACK_TOPK"` 等）

> 约定：**Scorer 只负责 score 与 reason**，是否入选、pass_thr 与 picked_by 由 `score_all + thresholds` 统一决定。

### 4. `nav_daily`

- **主键**：`(trade_date, portfolio, version)`  
  > 或根据最终实现约定为 `(trade_date, portfolio)`，但必须在 schema 中显式记录。
- **关键字段**：
  - `trade_date`：组合净值日期
  - `portfolio`：组合标识（如 `"V2_top3"`, `"ENS_top5"` 等）
  - `version`：对应模型版本（可与 portfolio 合并编码，但语义需稳定）
  - `nav`：单位净值（起点一般为 1.0）
  - `ret`：当日收益率
  - （可选）回撤、波动、换手等聚合指标列

> 约定：`nav_daily` 是 **组合层** 的输出，不记录单只股票信息。

### 5. `eval_5d_daily`（名称可稍有差异，但语义固定）

- **主键**：建议为 `(as_of_date, version, bucket)` 或 `(trade_date, version, bucket)`，具体以 schema.sql 为准。
- **关键字段**：
  - `asof` 或 `trade_date`：评估基准日
  - `version`：模型版本
  - `bucket`：分组（如 score 分位、rank 区间、topK 等描述）
  - `fwd_ret_5d`：未来 5 日平均收益率
  - 其他评估指标：胜率、分位收益、IC 等，可按需要扩展

> 约定：评估层读取 `picks_daily + prices_daily`，不直接依赖模型内部实现。  
> 前向收益的 as_of 日语义（非交易日时以“下一交易日”为起点等）以 `evaluation/forward_returns.py` 中 `compute_forward_returns` 的 docstring 为准。

---

## C. 目录 Ownership（Agent 职责边界）

为避免不同 Agent 在关键基础设施上“互相踩脚”，约定如下职责边界。除非任务明确要求，否则一般按下面的 ownership 执行。

- **基础设施与契约层（慎改）**
  - 目录：`src/alpha_tracker2/core/`, `src/alpha_tracker2/storage/`, `configs/`, `docs/`
  - 典型文件：
    - `core/config.py`, `core/trading_calendar.py`, `core/registry.py`
    - `storage/duckdb_store.py`, `storage/schema.sql`
    - `configs/default.yaml`
    - `docs/agent_workflow.md`（本文件）
  - **建议仅由“架构/基础设施类任务”的 Agent 修改**，例如：
    - 新增数据源类型（增加 Provider / Registry 条目）
    - 增加新的表（在 schema.sql 中添加 DDL）
    - 扩展配置项（default.yaml 中新增字段）

- **业务逻辑层（常规开发区域）**
  - 目录：  
    - `src/alpha_tracker2/ingestion/`  
    - `src/alpha_tracker2/features/`  
    - `src/alpha_tracker2/scoring/`  
    - `src/alpha_tracker2/strategies/`  
    - `src/alpha_tracker2/portfolio/`  
    - `src/alpha_tracker2/execution/`  
    - `src/alpha_tracker2/evaluation/`  
    - `src/alpha_tracker2/reporting/`  
    - `src/alpha_tracker2/pipelines/`
  - 一般 Agent 可以在这些目录中实现/修改：
    - 新的 ingestion 脚本（如新的 Yahoo 指数 universe）
    - 新特征构造逻辑
    - 新 scorer / 新版本（在 registry 中登记）
    - 新评估指标、新 dashboard 数据聚合

- **数据与输出目录（不可直接提交生产数据）**
  - 目录：`data/`（包括 `lake/`, `store/`, `out/`, `runs/`）
  - **约定**：
    - 这些目录主要由 pipeline 在本地运行时读写。
    - 一般不应在 Git 中提交数据文件（除非是示例/测试数据，并且经过明确说明）。

---

## D. 禁改规则（Hard Rules）

以下规则对所有 Agent 生效，除非用户在任务描述中明确说明“允许破例”，否则一律视为禁止。

1. **不得随意更改已有表字段语义**
   - 不得改变核心表（如 `prices_daily`, `features_daily`, `picks_daily`, `nav_daily`, `eval_5d_daily`）已有字段的含义。
   - 需要扩展时，应优先 **新增字段** 或 **新增表**，并在 `schema.sql` 与本文档中更新说明。

2. **不得绕过 registry / config 直接硬编码实现**
   - 新增数据源、模型、策略等，必须通过对应的 **registry + 配置** 注册：
     - 数据源：通过 `core/registry.py` 中的 Provider 注册。
     - 模型：通过 `scoring/registry.py` 以 version 名称注册。
   - 不得在业务代码中直接写死路径、版本名、API key 等，应从 `configs/default.yaml` 读取。

3. **不得在 `run_daily` 中加入业务逻辑**
   - `pipelines/run_daily.py` 仅负责：
     - 解析命令行参数；
     - 按顺序调用各 pipeline 的 `main()`；
     - 记录一次运行的 meta 信息。
   - 禁止把模型逻辑、数据处理细节写进 `run_daily`；任何实际业务逻辑必须放在各自独立的 pipeline/模块中。

4. **不得破坏 DuckDB 的幂等写入逻辑**
   - Ingestion 与 pipeline 写入 DuckDB 时，应遵守以下原则：
     - 对于同一 `(主键)` 范围的重复运行是安全的，不会产生重复或冲突记录。
     - 常见模式：**先 DELETE 目标窗口内旧数据，再 INSERT 新数据；或使用 INSERT OR REPLACE**。
   - 禁止引入仅依赖“自增 ID 或隐式顺序”的写入逻辑，避免数据不一致。

5. **不得在未经确认的情况下修改 schema.sql 中已存在表结构**
   - 尤其是：
     - 删除字段 / 重命名字段 / 更改字段类型。
   - 如确有必要，必须：
     - 在任务描述中明确说明迁移方案；
     - 更新本文档的核心表契约部分；
     - 确保所有相关 pipeline 已同步调整。

6. **不得在核心模块中引入与项目无关的外部依赖**
   - 所有新增依赖应与：
     - Yahoo 数据访问、
     - 数据处理（pandas/numpy 等），
     - 建模（scikit-learn/lightgbm/深度学习框架）直接相关。
   - 与项目目标无关的依赖（如与 Web 框架、GUI 无关的库）应避免出现在 `src/alpha_tracker2/` 中。

---

## E. 使用方式

- 每个 Cursor Agent 在接到任务时，应先检查任务是否会触及：
  - 核心表契约（B），
  - 基础设施与配置（C 中的基础设施部分），
  - 或任一禁改规则（D）。
- 如任务涉及这些区域，应在实现过程中：
  - 显式说明更改点与兼容性影响；
  - 优先选择“新增”而不是“修改/删除”已有契约；
  - 保证 `run_daily` 的端到端链路和幂等性不被破坏。

本文件可随项目演进进行补充和微调，但修改时需格外谨慎，并视为一次“架构层变更”。

