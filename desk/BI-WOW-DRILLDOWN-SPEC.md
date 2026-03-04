# Desk BI 周环比异常钻取设计（v0.1）

## 1. 目标

在现有 `bi-pivot-fetch-agent -> bi-analysis-agent` 基础上，增加“先广后细”的异常钻取机制：

- 首轮先看宏观维度（结构面）；
- 出现显著异常时，再逐层下钻到业务对象维度；
- 最终输出可解释的异常归因，而不是只给整体 WoW。

---

## 2. 设计原则

1. 先广后细：优先判断结构变化，再定位到账户/活动/创意。
2. 条件触发：仅在满足异常阈值时进入下一层，避免过度抓取。
3. 有上限：最多下钻 2 层，避免无限递归和成本失控。
4. 可追溯：每次下钻都记录触发原因、触发维度、影响贡献。

---

## 3. 维度分层与优先级

维度来源：`/pivot/app-settings/load` 返回的 `dataCubes[].dimensions`（当前 dataCube=`7b07`）。

## 3.1 L0（宏观层，默认首轮）

- `tad_cha-7ef`（產品類型）
- `country`（地理位置）
- `device_type`（設備類型）
- `platform`（操作系統）

目标：先判断异常来自产品结构、地域结构、设备结构还是平台结构。

## 3.2 L1（业务对象层）

- `tuser_i-387`（廣告帳戶ID）
- `tcpg_id-174`（廣告活動ID）

目标：定位到“哪个账户/活动”拉动了异常。

## 3.3 L2（细粒度执行层）

- `tcr_id-94f`（廣告文案ID）
- `placement_id`（廣告位ID）
- `format`（廣告位類型）
- `bundle`（流量來源）

目标：给出可执行的细粒度诊断依据（创意/版位/流量源）。

---

## 4. 异常触发规则（默认值）

为避免“小基数导致 WoW 虚高”，触发条件使用“比例 + 绝对值 + 基线”三重门槛。

## 4.1 总体触发（是否进入下一层）

满足以下全部条件才触发下钻：

- `abs(wow) >= 0.30`
- `abs(delta) >= 1000`
- `baseline >= 3000`

其中：

- `wow = (current - baseline) / baseline`
- `delta = current - baseline`

## 4.2 桶级触发（挑哪些维度值继续下钻）

在当前层每个维度值（bucket）计算贡献：

- `bucket_contrib = abs(bucket_delta) / abs(total_delta)`

建议触发条件（满足任一）：

- `bucket_contrib >= 0.40`
- `abs(bucket_wow) >= 0.60 且 abs(bucket_delta) >= 300`

---

## 5. 工作流（v2）建议

1. **Step-1：L0 抓取**
   - `bi-pivot-fetch-agent` 按 L0 维度抓取 target 与 baseline。
2. **Step-2：L0 分析**
   - `bi-analysis-agent` 产出总体 WoW + 结构异常判断。
3. **Step-3：条件下钻 L1**
   - 若触发总体异常，仅对异常 bucket 回流抓取 L1。
4. **Step-4：条件下钻 L2**
   - 若 L1 仍未解释充分，继续对异常 bucket 下钻 L2。
5. **Step-5：最终报告**
   - 输出“总体结论 + 分层归因路径 + 证据表”。

限制：

- 最多下钻 2 层（L0 -> L1 -> L2）。
- 每层最多新增 3 个维度。
- 单次 workflow 最多回流 2 次。

---

## 6. 配置扩展建议（`bi-sources.json`）

建议在 source 下新增：

```json
{
  "analysis": {
    "dimension_tiers": {
      "L0": ["tad_cha-7ef", "country", "device_type", "platform"],
      "L1": ["tuser_i-387", "tcpg_id-174"],
      "L2": ["tcr_id-94f", "placement_id", "format", "bundle"]
    },
    "anomaly_thresholds": {
      "overall_wow_abs": 0.3,
      "overall_delta_abs": 1000,
      "overall_baseline_min": 3000,
      "bucket_contrib_min": 0.4,
      "bucket_wow_abs": 0.6,
      "bucket_delta_abs": 300
    },
    "drilldown_limits": {
      "max_rounds": 2,
      "max_new_dimensions_per_round": 3
    }
  }
}
```

---

## 7. 报告输出增强（建议）

在 `bi-wow` 报告中增加：

- `是否触发下钻`、`触发原因`
- `下钻层级路径`（例如：`L0(country=TH) -> L1(tcpg_id=97123) -> L2(tcr_id=xxxx)`）
- `每层 Top 异常 bucket`（current / baseline / delta / wow / 贡献度）
- `未解释部分占比`（residual）

---

## 8. 验收标准

1. 正常波动日：仅执行 L0，不触发回流。
2. 异常波动日：可自动触发 L1/L2，并输出明确归因链路。
3. 报告中的总体 delta 与分层汇总 delta 可对齐（允许极小误差）。
4. 任一层数据缺失时，不猜数，直接输出 blocker 与缺失路径。

---

## 9. 实施顺序

1. 先加配置：`dimension_tiers + thresholds + limits`。
2. 再改 fetch：支持按层维度抓取（L0/L1/L2）。
3. 再改 analysis：增加异常判定与回流建议字段。
4. 最后改 workflow：增加“条件下钻”步骤与最终汇总。

