# 批次 0 任务书：I-3 真实交易所日历 + I-1 配置与 Schema 扩展

> **下发方**：总控 Agent  
> **依据**：`docs/NEXT_PHASE_PLAN.md` 批次 0、任务 I-3 与 I-1。  
> **执行方**：Infrastructure Agent（或认领该角色的功能 Agent）。  
> **本文档为任务设计与验收标准**，执行方按此实现并自检，总控按验收标准判定通过与否。

---

## 一、批次概览

| 任务 ID | 名称 | 依赖 | 可并行 |
|---------|------|------|--------|
| **I-3** | 真实交易所日历 | 无 | 与 I-1 可并行 |
| **I-1** | 配置与 schema 扩展 | 无 | 与 I-3 可并行 |

两任务可同批开发、分别 PR，验收时两项均通过则批次 0 通过。

---

## 二、任务 I-3：真实交易所日历

### 2.1 目标

将当前“周一至周五工作日近似”的 `TradingCalendar` 替换为 **US（NYSE/NASDAQ）与 HK（HKEX）真实交易所日历**，使 `latest_trading_day`、`trading_days` 与真实休市日一致，避免节假日误差。

### 2.2 可修改范围

- **允许修改**：
  - `src/alpha_tracker2/core/trading_calendar.py`（实现可替换，**公共接口不可变**）
  - 可选：`core/config.py` 若需增加日历相关配置（如缓存目录、交易所名）
  - `requirements.txt` 或 `pyproject.toml`：增加日历依赖（如 `pandas_market_calendars` 或等价库）
- **禁止**：
  - 修改 `TradingCalendar` 的**公共接口**：`latest_trading_day(self, market: str = "US") -> date`、`trading_days(self, start: date, end: date, market: str = "US") -> List[date]` 的签名与语义（返回类型、含义不变）
  - 修改其他模块的业务逻辑（仅允许调用方继续使用现有接口）

### 2.3 接口契约（必须保持）

- `latest_trading_day(self, market: str = "US") -> date`  
  - 返回该市场“今天或今天之前的最近一个交易日”。若 today 为交易日则返回 today。
- `trading_days(self, start: date, end: date, market: str = "US") -> List[date]`  
  - 返回 [start, end] 内（含两端）的交易日列表，按日期升序；`market` 支持 `"US"` 与 `"HK"`。

### 2.4 实现要求

- **US**：使用纽交所/纳斯达克统一日历（如 `pandas_market_calendars.get_calendar("NYSE")` 或等价），包含美国股市节假日（如 New Year, MLK, Presidents, Good Friday, Memorial, Juneteenth, Independence, Labor, Thanksgiving, Christmas 等）。
- **HK**：使用港交所日历（如 `pandas_market_calendars.get_calendar("XHKG")` 或等价），包含港交所休市日。
- 若依赖库在区间外无数据（如仅提供近年），需在 docstring 或文档中说明有效区间；在有效区间内行为必须正确。
- 性能：首次获取某区间交易日列表时可缓存，避免重复计算；实现方式自定。

### 2.5 验收标准（I-3）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| I3-1 | 接口未变 | 现有调用方（build_features, ingest_prices, eval_5d, score_all, ingest_universe, portfolio_nav, forward_returns, smoke）无需改代码即可运行 |
| I3-2 | US 节假日正确 | 单元测试：给定 2024-07-04（美国独立日）为休市，`trading_days(2024-07-01, 2024-07-05, "US")` 不包含 2024-07-04；`latest_trading_day("US")` 在 2024-07-04 返回 2024-07-03（或等价） |
| I3-3 | HK 节假日正确 | 单元测试：至少一个已知港交所休市日（如农历新年某日）在 `trading_days(..., "HK")` 中不出现 |
| I3-4 | smoke 与 pipeline 不受损 | 执行现有 `smoke_e2e`（或 `smoke`）及 run_daily 相关步骤不报错；结果与“使用真实日历”的预期一致（不要求数值与旧实现完全一致，但不得因日历错误导致空数据或异常） |

### 2.6 测试与真实数据

- **单元测试**：使用**已知日期**（如 2024 年美国/香港节假日）验证 `trading_days`、`latest_trading_day` 的返回；不依赖“今天”的测试便于 CI 复现。
- **集成**：现有 `pipelines/smoke.py` 若包含日历相关断言，须通过；`smoke_e2e` 在真实数据区间内跑通即可。
- 真实数据要求：本任务不强制使用 Yahoo 拉数；若集成测试沿用现有 smoke，则沿用现有数据约定即可。

---

## 三、任务 I-1：配置与 Schema 扩展

### 3.1 目标

- **Schema**：在 `features_daily` 中新增 **bt_*** 回测特征列**（为 I-2 特征工程与 S-2 V4 接入提供表结构），并同步更新核心表契约文档。
- **配置**：支持 **per-version 权重** 与 **bt_* 列权重** 可配置，供后续 S-1（V1–V3 权重可配置）、S-3（按版本阈值）、S-2/S-4（V4 bt 权重）使用；不在本任务内实现 scorer 读取逻辑，仅完成配置结构与 schema。

### 3.2 可修改范围

- **允许修改**：
  - `src/alpha_tracker2/storage/schema.sql`：为 `features_daily` **新增列**（不删、不改已有列）
  - `configs/default.yaml`：**仅新增**配置节点与字段，不删除已有字段
  - `docs/agent_workflow.md`：更新 **B. 核心表契约** 中 `features_daily` 的描述，增加 bt_* 列说明
  - 可选：`core/config.py` 若需增加配置加载的 dataclass/字段（如读取 scoring.v1.weights、bt_column_weights），仅做“可被读取”的扩展，不在本任务内在 scoring/features 中消费
- **禁止**：
  - 修改 `features_daily` 已有列的类型或语义、修改主键
  - 修改 `picks_daily`、`prices_daily`、`nav_daily`、`eval_5d_daily` 的表结构（除非仅为文档说明）
  - 在 `run_daily.py` 中加入业务逻辑

### 3.3 Schema 变更要求

- 在 `features_daily` 中新增以下列（类型与含义约定如下，名字可微调但语义一致）：
  - **bt_mean**：DOUBLE，滚动窗口内模拟收益均值（或等价定义，I-2 实现具体计算）
  - **bt_winrate**：DOUBLE，滚动窗口内胜率 [0,1]（或等价）
  - **bt_worst_mdd**：DOUBLE，滚动窗口内最差回撤（非正数，或 NULL 表示无）
- 新增列均允许 NULL；主键仍为 `(trade_date, ticker)`。

### 3.4 配置扩展要求

在 `configs/default.yaml` 中**新增**以下结构（与现有 `scoring` 并列或子节点，不覆盖已有 `score_versions`、`v2_v3_v4.common`）：

- **scoring.v1.weights**（可选键名，与现有 V1 逻辑兼容）：  
  - 格式：`因子名 -> 权重`，例如 `ret_5d: 0.5, ret_20d: 0.3, avg_amount_20: 0.2`  
  - 用途：后续 S-1 中 V1 从 config 读取权重；本任务仅确保该节点存在且可被 `load_settings` 或现有配置加载方式读取。

- **scoring.v2_v3_v4.versions**（可选）：  
  - 支持 per-version 覆盖，例如 `V2`/`V3`/`V4` 下可配置 `trend_weight`、`risk_weight`、`bt_weight`（默认可缺省，表示沿用 common 或代码默认）。  
  - 本任务仅增加 YAML 结构示例与说明，不强制 score_all 在本任务中读取（可留待 S-1/S-3）。

- **scoring.v2_v3_v4.bt_column_weights** 或 **features.bt_weights**（二选一或都支持）：  
  - 格式：`bt_mean`、`bt_winrate`、`bt_worst_mdd` 对应的权重（用于 V4 的 bt_score 或特征加权），例如 `bt_mean: 0.5, bt_winrate: 0.3, bt_worst_mdd: 0.2`。  
  - 本任务仅确保配置存在且可被读取，不在 I-2 之前实现计算逻辑。

- **scoring.v2_v3_v4** 下已有 **common.q / window / topk_fallback** 保持不变；若增加 **versions.V2/V3/V4** 的 **q、window** 覆盖，也属本任务配置扩展范围（为 S-3 按版本阈值做准备）。

### 3.5 文档更新要求

- 在 `docs/agent_workflow.md` 的 **B. 核心表契约** → **features_daily** 小节中：
  - 明确列出新增的 `bt_mean`、`bt_winrate`、`bt_worst_mdd` 及其含义（与 3.3 一致）；
  - 注明“bt_* 由 I-2 特征工程写入，V4 在 S-2 中接入”。

### 3.6 验收标准（I-1）

| # | 验收项 | 判定方式 |
|---|--------|----------|
| I1-1 | features_daily 新增三列 | `schema.sql` 中 `features_daily` 包含 `bt_mean`、`bt_winrate`、`bt_worst_mdd`（类型 DOUBLE，可为 NULL） |
| I1-2 | 配置结构存在且可读 | 读取 `configs/default.yaml` 能解析出 `scoring.v1.weights`、`scoring.v2_v3_v4` 下 bt_column_weights（或 features.bt_weights）及可选 versions 结构；可用单元测试“加载 YAML 并 assert 键存在” |
| I1-3 | 契约文档已更新 | `docs/agent_workflow.md` 中 features_daily 契约包含 bt_* 三列说明 |
| I1-4 | 现有链路不受损 | 执行 `smoke_e2e` 全链路通过（build_features 等可暂时不写 bt_* 列，仅需表结构存在、INSERT 不报错）；`score_all`、`eval_5d`、`portfolio_nav` 行为与改动前一致 |

### 3.7 测试与真实数据

- **单元测试**：配置加载测试（Python 读 default.yaml，检查新节点存在）；可选：DuckDB 执行 `CREATE TABLE`/迁移脚本后对 `features_daily` 做 `DESCRIBE` 或插入一行含 bt_* 的测试数据再查询。
- **集成**：smoke_e2e 使用真实数据区间（至少 20 个交易日、US+HK ticker），全链路通过即满足 I1-4。
- 不在本任务内实现 `build_features` 对 bt_* 的填充（留待 I-2）；若现有 build_features 的 INSERT 未包含新列，需在 I-1 中保证 schema 支持新列为 NULL 或默认值，且 pipeline 不报错。

---

## 四、共同要求与交付物

### 4.1 规范遵守

- 遵守 `docs/agent_workflow.md`：目录可改范围、核心表契约、禁改规则（不改已有主键/列语义、run_daily 仅编排、幂等、registry/config）。
- 遵守 `docs/dev_loop.md`：从 dev 切 feat/*、自测、self-review、PR 到 dev。

### 4.2 交付物

- **代码与配置**：满足上述可修改范围内的变更；若新增依赖，在 `requirements.txt` 或 `pyproject.toml` 中注明。
- **文档**：agent_workflow 的更新（仅 I-1）；若 I-3 有日历有效区间或依赖说明，在 `core/trading_calendar.py` docstring 或简短 README 小节说明即可。
- **自检清单**：执行方交付时提供简短自检结果（如“I3-1～I3-4 / I1-1～I1-4 逐项通过”），并注明用于验收的 smoke_e2e 区间与 pytest 命令。

### 4.3 验收通过条件

- **I-3**：满足 2.5 节 I3-1～I3-4。
- **I-1**：满足 3.6 节 I1-1～I1-4。
- 两项均通过后，总控认定批次 0 完成，可合并入 dev，并作为后续 I-2、S-1、S-3 的依赖基础。

---

**总控**：请 Infrastructure Agent（或认领者）按本文档实现 I-3 与 I-1，自检后按 dev_loop 提 PR；总控将按上表验收标准进行验收。
