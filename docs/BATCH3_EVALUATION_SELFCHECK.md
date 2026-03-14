# 批次 3 Evaluation 自检清单

## E-1 metrics.py

| 验收项 | 结果 | 说明 |
|--------|------|------|
| E1-1 Sharpe 可用 | 通过 | `tests/test_evaluation_metrics.py`: test_sharpe_* 断言已知序列与手算一致 |
| E1-2 MDD 可用 | 通过 | test_mdd_* 断言净值/收益序列 MDD |
| E1-3 IC 可用 | 通过 | test_ic_* 断言 Pearson/Spearman、多日 IC 序列 |
| E1-4 被 E-2 或 E-3 调用 | 通过 | E-2 `eval_5d_batch.py` 中 `from alpha_tracker2.evaluation.metrics import ic` 并用于 IC 序列 |

**pytest**: `PYTHONPATH=src pytest tests/test_evaluation_metrics.py -v`

---

## E-3 diagnostics

| 验收项 | 结果 | 说明 |
|--------|------|------|
| E3-1 版本对比产出存在 | 通过 | `version_compare.csv` 含 version, mean_fwd_ret_5d, avg_n_picks, n_dates, n_pick_days |
| E3-2 因子分析产出存在 | 通过 | `factor_analysis.csv` 含 factor_name, mean_ic, std_ic, n_dates |
| E3-3 可被 E-2 或 D-1 消费 | 通过 | 列名与路径文档化于 `evaluation/diagnostics.py` 及本自检；路径默认 `config paths.out_dir` |
| E3-4 真实数据测试 | 通过 | `PYTHONPATH=src python -m alpha_tracker2.pipelines.run_diagnostics --start 2026-01-01 --end 2026-01-14` 无报错 |

**pytest**: `PYTHONPATH=src pytest tests/test_evaluation_diagnostics.py -v`  
**真实数据区间**: `--start 2026-01-01 --end 2026-01-14`（或已有 picks_daily/eval_5d_daily 的任意 20+ 交易日）

---

## E-2 eval_5d_batch

| 验收项 | 结果 | 说明 |
|--------|------|------|
| E2-1 区间内批量评估完成 | 通过 | 结果写入 `eval_5d_daily`，as_of_date 覆盖 [start,end] 内每个交易日 |
| E2-2 分位收益产出存在 | 通过 | `quintile_returns.csv` 含 as_of_date, version, quintile, mean_fwd_ret_5d, n_stocks |
| E2-3 IC 序列产出存在 | 通过 | `ic_series.csv` 含 as_of_date, version, ic；调用 E-1 的 `metrics.ic` |
| E2-4 真实数据测试 | 通过 | `--start 2026-01-01 --end 2026-01-14` 无报错；有未来价格时 quintile/IC 有数据 |
| E2-5 与现有 eval_5d 兼容 | 通过 | 未改 eval_5d_daily 主键/列语义；batch 写同一表多 as_of_date，幂等 DELETE 再 INSERT |

**pytest**: `PYTHONPATH=src pytest tests/test_eval_5d_batch.py -v`  
**真实数据区间**: `PYTHONPATH=src python -m alpha_tracker2.pipelines.eval_5d_batch --start 2026-01-01 --end 2026-01-14`

---

## 产出路径与格式（供 D-1）

- **data/out/version_compare.csv**: version, mean_fwd_ret_5d, avg_n_picks, n_dates, n_pick_days  
- **data/out/factor_analysis.csv**: factor_name, mean_ic, std_ic, n_dates  
- **data/out/quintile_returns.csv**: as_of_date, version, quintile, mean_fwd_ret_5d, n_stocks  
- **data/out/ic_series.csv**: as_of_date, version, ic  

路径可由 `--output-dir` / `config paths.out_dir` 覆盖。
