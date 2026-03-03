# Desk BI 自动化工作流设计（v0.1）

## 1. 目标

建立一条可持续运行的 BI 自动化链路：

`bi-pivot-fetch-agent` -> `bi-analysis-agent`（后续）

本设计先定义两者之间的**数据契约**和**执行边界**，避免后续分析阶段因字段不稳定、重复计数、口径不一致而返工。

---

## 2. 范围与非范围

### 范围（MVP）
- 每日从 BI Pivot 拉取前一自然日数据（默认 `Asia/Shanghai`）。
- 生成可供分析的标准化数据集（含质量校验结果）。
- 由分析 Agent 读取标准化数据，产出日报和动作建议。

### 非范围（当前不做）
- 直接执行广告平台改价、改预算、暂停计划等线上操作。
- 复杂多目标自动优化策略（先做人审可执行建议）。

---

## 3. Agent 职责边界

## 3.1 `bi-pivot-fetch-agent`
- 只负责：认证、拉取、标准化、质量校验、落盘。
- 不负责：业务结论、优化建议、预算策略。

## 3.2 `bi-analysis-agent`（后续）
- 只负责：读取标准化数据并分析，输出报告和建议。
- 不负责：二次抓取、修复上游数据、直接投放执行。

---

## 4. 目录与产物规范

每次抓取一个 `run_id` 目录：

```text
/Users/algorix/Documents/project/openfang/desk/data/bi-fetch/<YYYY-MM-DD_HH-MM>/
  raw/
    <source_name>/
      *_app-settings.json
      *_groupby_*.json
      *_axis_*.json
      *_groupby_total.json
  normalized/
    fact_account_daily.json
    fact_campaign_daily.json
    fact_creative_daily.json
    totals.json
    schema.json
  logs/
    ingestion-summary.md
    qa.json
    run-meta.json
```

说明：
- `raw/` 存完整上游响应，便于追溯。
- `normalized/` 给下游分析 Agent 直接消费。
- `logs/qa.json` 是质量门禁结果；`run-meta.json` 是运行元信息。

---

## 5. 数据契约（Fetch -> Analysis）

## 5.1 `run-meta.json`（必需）
- `run_id`
- `target_date`
- `timezone`
- `source_name`
- `data_cube`
- `status`（`success` / `partial_success` / `failed`）
- `row_counts`（按粒度统计）
- `generated_at`

## 5.2 `totals.json`（必需）
- 仅保留单日总量，避免重复计数。
- 示例字段：
  - `date`
  - `spend`
  - `impressions`
  - `clicks`
  - `conversions`
  - `revenue`
  - `currency`

## 5.3 粒度事实表（至少一张必需）
- `fact_account_daily.json`
- `fact_campaign_daily.json`
- `fact_creative_daily.json`

每行建议包含：
- `date`, `source`, `account_id`, `timezone`
- `<grain>_id`, `<grain>_name`（如 campaign/creative）
- `spend`, `impressions`, `clicks`, `conversions`, `revenue`
- `raw_dimension_id`, `raw_measure_ids`

> 规则：分析 Agent 聚合时优先使用 `totals.json` 或单一粒度事实表，不跨粒度混加。

---

## 6. 指标映射配置建议

在 `/Users/algorix/Documents/project/openfang/desk/config/bi-sources.json` 为每个 source 增加可配置映射：

```json
{
  "metric_map": {
    "spend": "SUMtadv-00e",
    "impressions": "SUMtimp-350",
    "clicks": "SUMtcli-b4c",
    "conversions": "SUMttot-6a9",
    "revenue": "SUMttot-ba5"
  }
}
```

说明：
- 各业务可自定义 measure ID，避免脚本硬编码。
- Analysis Agent 只认语义字段（spend/clicks...），不直接依赖原始 measure ID。

---

## 7. 质量门禁（QA）

`qa.json` 建议包含以下检查：

- `auth_ok`：认证是否成功。
- `raw_files_complete`：原始关键接口响应是否齐全。
- `required_metrics_present`：指标映射对应 measure 都可取到。
- `grain_consistency`：单粒度汇总与 `totals.json` 误差在阈值内（如 <= 0.5%）。
- `non_negative_metrics`：花费/曝光/点击等非负。
- `final_status`：`pass` / `warn` / `fail`。

门禁策略：
- `fail`：分析 Agent 不执行，直接产出 blocker。
- `warn`：允许执行，但报告开头标注风险。

---

## 8. 工作流定义（后续落地）

新增工作流文件：

`/Users/algorix/Documents/project/openfang/desk/workflows/bi-fetch-and-analysis.json`

建议流程：
1. `bi-pivot-fetch-agent` 执行抓取与标准化。
2. 校验 `qa.json.final_status`。
3. `bi-analysis-agent` 读取 `normalized/` 产出分析。

---

## 9. `bi-analysis-agent` 输出契约（预留）

建议输出到：

- `/Users/algorix/Documents/project/openfang/desk/reports/bi-daily/<YYYY-MM-DD>.md`
- `/Users/algorix/Documents/project/openfang/desk/reports/bi-daily/<YYYY-MM-DD>-actions.json`
- `/Users/algorix/Documents/project/openfang/desk/reports/bi-daily/<YYYY-MM-DD>-diagnostics.json`

其中：
- `md`：面向人读的日报。
- `actions.json`：可被后续执行/审批系统消费。
- `diagnostics.json`：记录口径、异常、置信度、数据缺口。

---

## 10. 验收标准（MVP）

- 连续 7 天每日可产出一套完整文件（raw/normalized/logs）。
- `qa.json.final_status=pass` 的 run 才进入分析步骤。
- 分析报告中核心指标（spend/clicks/conversions/revenue）与 `totals.json` 一致。
- 任一关键输入缺失时，不猜数据，输出 blocker 原因与缺失路径。

---

## 11. 下一步实施顺序

1. 升级 `bi-pivot-fetch-agent`：先把 `metric_map + totals + qa + run-meta` 落地。  
2. 新建 `bi-fetch-and-analysis` 工作流定义。  
3. 创建 `bi-analysis-agent`（只消费标准化语义字段，不读原始 measure ID）。  
4. 联调 3 天真实数据后，再加异常告警和自动重试策略。  
