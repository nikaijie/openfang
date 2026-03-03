#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATA_BASE="${ROOT_DIR}/desk/data/bi-fetch"
REPORT_BASE="${ROOT_DIR}/desk/reports/bi-wow"
CONFIG_FILE="${DESK_BI_CONFIG_FILE:-${ROOT_DIR}/desk/config/bi-sources.json}"

TARGET_DATE_ARG="${1:-}"
BASELINE_DATE_ARG="${2:-}"

if [[ ! -d "${DATA_BASE}" ]]; then
  echo "Error: bi-fetch data base not found: ${DATA_BASE}" >&2
  exit 1
fi

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "Error: missing config file at ${CONFIG_FILE}" >&2
  exit 1
fi

mkdir -p "${REPORT_BASE}"

GLOBAL_TZ="$(jq -r '.timezone // "Asia/Shanghai"' "${CONFIG_FILE}")"
read -r TARGET_DATE BASELINE_DATE < <(python3 - "${GLOBAL_TZ}" "${TARGET_DATE_ARG}" "${BASELINE_DATE_ARG}" <<'PY'
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sys

tz = ZoneInfo(sys.argv[1])
target_arg = sys.argv[2]
baseline_arg = sys.argv[3]

if target_arg:
    target = datetime.strptime(target_arg, "%Y-%m-%d").date()
else:
    target = datetime.now(tz).date() - timedelta(days=1)

if baseline_arg:
    baseline = datetime.strptime(baseline_arg, "%Y-%m-%d").date()
else:
    baseline = target - timedelta(days=7)

print(target.isoformat(), baseline.isoformat())
PY
)

FILE_MAP_JSON="$(python3 - "${DATA_BASE}" "${TARGET_DATE}" "${BASELINE_DATE}" <<'PY'
import glob
import json
import os
import sys

data_base, target_date, baseline_date = sys.argv[1:]

def latest_files_for_date(date_str):
    pattern = os.path.join(data_base, "*", "normalized", f"*_{date_str}.json")
    files = sorted(glob.glob(pattern), reverse=True)
    mapping = {}
    suffix = f"_{date_str}.json"
    for path in files:
        name = os.path.basename(path)
        if not name.endswith(suffix):
            continue
        source = name[:-len(suffix)]
        if source not in mapping:
            mapping[source] = path
    return mapping

print(json.dumps({
    "target": latest_files_for_date(target_date),
    "baseline": latest_files_for_date(baseline_date),
}))
PY
)"

TARGET_SOURCES="$(jq -r '.target | keys[]?' <<<"${FILE_MAP_JSON}")"
if [[ -z "${TARGET_SOURCES}" ]]; then
  echo "Error: no normalized files found for target date ${TARGET_DATE}" >&2
  echo "Expected under: ${DATA_BASE}/<run_id>/normalized/*_${TARGET_DATE}.json" >&2
  exit 1
fi

SOURCE_SUMMARY_NDJSON="$(mktemp)"
MISSING_BASELINE_SOURCES=()
SOURCES_ANALYZED=()

for source_name in ${TARGET_SOURCES}; do
  TARGET_FILE="$(jq -r --arg source "${source_name}" '.target[$source] // empty' <<<"${FILE_MAP_JSON}")"
  BASELINE_FILE="$(jq -r --arg source "${source_name}" '.baseline[$source] // empty' <<<"${FILE_MAP_JSON}")"

  if [[ -z "${BASELINE_FILE}" ]]; then
    MISSING_BASELINE_SOURCES+=("${source_name}")
    continue
  fi

  if [[ ! -f "${TARGET_FILE}" || ! -f "${BASELINE_FILE}" ]]; then
    MISSING_BASELINE_SOURCES+=("${source_name}")
    continue
  fi

  COMPARE_DIMENSION="$(jq -r --arg source "${source_name}" '.sources[] | select(.name == $source) | .portal.compare_dimension // .portal.dimension_ids[0] // empty' "${CONFIG_FILE}" | head -n1)"
  CAMPAIGN_DIMENSION="$(jq -r --arg source "${source_name}" '.sources[] | select(.name == $source) | .portal.campaign_dimension // .portal.dimension_ids[1] // empty' "${CONFIG_FILE}" | head -n1)"

  if [[ -z "${COMPARE_DIMENSION}" ]]; then
    COMPARE_DIMENSION="$(jq -r '.[0].dimension_id // empty' "${TARGET_FILE}")"
  fi
  if [[ -z "${CAMPAIGN_DIMENSION}" ]]; then
    CAMPAIGN_DIMENSION="${COMPARE_DIMENSION}"
  fi

  COMPARE_DIMENSION_TITLE="$(jq -r --arg dim "${COMPARE_DIMENSION}" '[.[] | select(.dimension_id == $dim) | .dimension_title][0] // empty' "${TARGET_FILE}")"
  CAMPAIGN_DIMENSION_TITLE="$(jq -r --arg dim "${CAMPAIGN_DIMENSION}" '[.[] | select(.dimension_id == $dim) | .dimension_title][0] // empty' "${TARGET_FILE}")"
  if [[ -z "${COMPARE_DIMENSION_TITLE}" ]]; then
    COMPARE_DIMENSION_TITLE="${COMPARE_DIMENSION}"
  fi
  if [[ -z "${CAMPAIGN_DIMENSION_TITLE}" ]]; then
    CAMPAIGN_DIMENSION_TITLE="${CAMPAIGN_DIMENSION}"
  fi
  METRIC_ID="$(jq -r '.[0].metric_id // "unknown_metric"' "${TARGET_FILE}")"
  METRIC_TITLE="$(jq -r '.[0].metric_title // "value"' "${TARGET_FILE}" | sed 's/[[:space:]]*$//')"

  CURRENT_TOTAL="$(jq --arg dim "${COMPARE_DIMENSION}" '[.[] | select(.dimension_id == $dim) | (.value_num // 0 | tonumber? // 0)] | add // 0' "${TARGET_FILE}")"
  BASELINE_TOTAL="$(jq --arg dim "${COMPARE_DIMENSION}" '[.[] | select(.dimension_id == $dim) | (.value_num // 0 | tonumber? // 0)] | add // 0' "${BASELINE_FILE}")"
  CURRENT_ROWS="$(jq --arg dim "${COMPARE_DIMENSION}" '[.[] | select(.dimension_id == $dim)] | length' "${TARGET_FILE}")"
  BASELINE_ROWS="$(jq --arg dim "${COMPARE_DIMENSION}" '[.[] | select(.dimension_id == $dim)] | length' "${BASELINE_FILE}")"

  DELTA="$(python3 - "${CURRENT_TOTAL}" "${BASELINE_TOTAL}" <<'PY'
import sys
current = float(sys.argv[1])
baseline = float(sys.argv[2])
print(current - baseline)
PY
)"
  WOW_RATIO="$(python3 - "${CURRENT_TOTAL}" "${BASELINE_TOTAL}" <<'PY'
import sys
current = float(sys.argv[1])
baseline = float(sys.argv[2])
if baseline == 0:
    print("null")
else:
    print((current - baseline) / baseline)
PY
)"

  TOP_MOVERS_JSON="$(jq -n \
    --arg dim "${CAMPAIGN_DIMENSION}" \
    --slurpfile cur "${TARGET_FILE}" \
    --slurpfile prev "${BASELINE_FILE}" '
      def values_by_dim($rows; $dimension):
        reduce ($rows[] | select(.dimension_id == $dimension)) as $r ({};
          .[(($r.dimension_value // "null") | tostring)] = (($r.value_num // 0) | tonumber? // 0)
        );
      (values_by_dim($cur[0]; $dim)) as $current
      | (values_by_dim($prev[0]; $dim)) as $baseline
      | (($current | keys_unsorted) + ($baseline | keys_unsorted) | unique) as $keys
      | [ $keys[] as $k
          | {
              dimension_value: $k,
              current: ($current[$k] // 0),
              baseline: ($baseline[$k] // 0),
              delta: (($current[$k] // 0) - ($baseline[$k] // 0)),
              wow: (if (($baseline[$k] // 0) == 0) then null else ((($current[$k] // 0) - ($baseline[$k] // 0)) / ($baseline[$k] // 0)) end)
            }
        ]
      | sort_by((.delta | if . < 0 then -. else . end))
      | reverse
      | .[:5]
    ')"

  jq -n \
    --arg source "${source_name}" \
    --arg metric_id "${METRIC_ID}" \
    --arg metric_title "${METRIC_TITLE}" \
    --arg compare_dimension "${COMPARE_DIMENSION}" \
    --arg compare_dimension_title "${COMPARE_DIMENSION_TITLE}" \
    --arg campaign_dimension "${CAMPAIGN_DIMENSION}" \
    --arg campaign_dimension_title "${CAMPAIGN_DIMENSION_TITLE}" \
    --arg target_file "${TARGET_FILE}" \
    --arg baseline_file "${BASELINE_FILE}" \
    --argjson current_total "${CURRENT_TOTAL}" \
    --argjson baseline_total "${BASELINE_TOTAL}" \
    --argjson delta "${DELTA}" \
    --argjson wow "${WOW_RATIO}" \
    --argjson current_rows "${CURRENT_ROWS}" \
    --argjson baseline_rows "${BASELINE_ROWS}" \
    --argjson top_movers "${TOP_MOVERS_JSON}" '
    {
      source: $source,
      metric_id: $metric_id,
      metric_title: $metric_title,
      compare_dimension: $compare_dimension,
      compare_dimension_title: $compare_dimension_title,
      campaign_dimension: $campaign_dimension,
      campaign_dimension_title: $campaign_dimension_title,
      current_total: $current_total,
      baseline_total: $baseline_total,
      delta: $delta,
      wow: $wow,
      current_rows: $current_rows,
      baseline_rows: $baseline_rows,
      target_file: $target_file,
      baseline_file: $baseline_file,
      top_movers: $top_movers
    }' >> "${SOURCE_SUMMARY_NDJSON}"

  SOURCES_ANALYZED+=("${source_name}")
done

if [[ ! -s "${SOURCE_SUMMARY_NDJSON}" ]]; then
  rm -f "${SOURCE_SUMMARY_NDJSON}"
  echo "Error: no comparable sources found for target=${TARGET_DATE} vs baseline=${BASELINE_DATE}" >&2
  echo "Missing baseline sources: ${MISSING_BASELINE_SOURCES[*]:-none}" >&2
  exit 1
fi

SOURCE_SUMMARIES_JSON="$(jq -s '.' "${SOURCE_SUMMARY_NDJSON}")"
rm -f "${SOURCE_SUMMARY_NDJSON}"

MISSING_BASELINE_JSON="$(printf '%s\n' "${MISSING_BASELINE_SOURCES[@]-}" | jq -R . | jq -s 'map(select(length > 0))')"
SOURCES_ANALYZED_JSON="$(printf '%s\n' "${SOURCES_ANALYZED[@]-}" | jq -R . | jq -s 'map(select(length > 0))')"

TOTAL_CURRENT="$(jq '[.[].current_total] | add // 0' <<<"${SOURCE_SUMMARIES_JSON}")"
TOTAL_BASELINE="$(jq '[.[].baseline_total] | add // 0' <<<"${SOURCE_SUMMARIES_JSON}")"
PRIMARY_METRIC_ID="$(jq -r '.[0].metric_id // "unknown_metric"' <<<"${SOURCE_SUMMARIES_JSON}")"
PRIMARY_METRIC_TITLE="$(jq -r '.[0].metric_title // "value"' <<<"${SOURCE_SUMMARIES_JSON}")"
TOTAL_DELTA="$(python3 - "${TOTAL_CURRENT}" "${TOTAL_BASELINE}" <<'PY'
import sys
current = float(sys.argv[1])
baseline = float(sys.argv[2])
print(current - baseline)
PY
)"
TOTAL_WOW="$(python3 - "${TOTAL_CURRENT}" "${TOTAL_BASELINE}" <<'PY'
import sys
current = float(sys.argv[1])
baseline = float(sys.argv[2])
if baseline == 0:
    print("null")
else:
    print((current - baseline) / baseline)
PY
)"

REPORT_JSON="${REPORT_BASE}/${TARGET_DATE}-vs-${BASELINE_DATE}.json"
REPORT_MD="${REPORT_BASE}/${TARGET_DATE}-vs-${BASELINE_DATE}.md"
GENERATED_AT="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

jq -n \
  --arg target_date "${TARGET_DATE}" \
  --arg baseline_date "${BASELINE_DATE}" \
  --arg timezone "${GLOBAL_TZ}" \
  --arg generated_at "${GENERATED_AT}" \
  --arg metric_id "${PRIMARY_METRIC_ID}" \
  --arg metric_title "${PRIMARY_METRIC_TITLE}" \
  --argjson totals "$(jq -n \
    --argjson current "${TOTAL_CURRENT}" \
    --argjson baseline "${TOTAL_BASELINE}" \
    --argjson delta "${TOTAL_DELTA}" \
    --argjson wow "${TOTAL_WOW}" \
    '{current: $current, baseline: $baseline, delta: $delta, wow: $wow}')" \
  --argjson sources "${SOURCE_SUMMARIES_JSON}" \
  --argjson analyzed_sources "${SOURCES_ANALYZED_JSON}" \
  --argjson missing_baseline_sources "${MISSING_BASELINE_JSON}" '
  {
    target_date: $target_date,
    baseline_date: $baseline_date,
    timezone: $timezone,
    generated_at: $generated_at,
    metric: {
      id: $metric_id,
      title: $metric_title
    },
    totals: $totals,
    sources: $sources,
    analyzed_sources: $analyzed_sources,
    missing_baseline_sources: $missing_baseline_sources
  }' > "${REPORT_JSON}"

{
  echo "# BI 周环比报告 (${TARGET_DATE} vs ${BASELINE_DATE})"
  echo
  echo "- 时区: ${GLOBAL_TZ}"
  echo "- 已分析数据源: $(jq -r '.analyzed_sources | join(", ")' "${REPORT_JSON}")"
  echo "- 缺失基准数据源: $(jq -r '.missing_baseline_sources | if length == 0 then "无" else join(", ") end' "${REPORT_JSON}")"
  echo "- 指标: $(jq -r '.metric.title' "${REPORT_JSON}")"
  echo
  echo "## 指标定义"
  echo
  echo "- 指标名称: $(jq -r '.metric.title' "${REPORT_JSON}")"
  echo "- 指标ID: $(jq -r '.metric.id' "${REPORT_JSON}")"
  echo
  echo "## 总体周环比（按对比维度汇总）"
  echo
  echo "- 当前值: $(jq -r '.totals.current' "${REPORT_JSON}")"
  echo "- 基准值: $(jq -r '.totals.baseline' "${REPORT_JSON}")"
  echo "- 变化量: $(jq -r '.totals.delta' "${REPORT_JSON}")"
  echo "- 周环比: $(jq -r '.totals.wow' "${REPORT_JSON}")"
  echo
  echo "## 数据源明细"
  echo
  jq -r '
    .sources[]
    | "- \(.source): 指标=\(.metric_title), 当前=\(.current_total), 基准=\(.baseline_total), 变化量=\(.delta), 周环比=\(.wow), 对比维度=\(.compare_dimension_title)"
  ' "${REPORT_JSON}"
  echo
  echo "## 主要变动项（按活动维度）"
  echo
  jq -r '
    .sources[]
    | . as $source
    | "### \($source.source) (\($source.campaign_dimension_title))\n"
      + (
          if ($source.top_movers | length) == 0
          then "- 无"
          else
            ($source.top_movers
              | map("- [\($source.metric_title)] \(.dimension_value): 当前=\(.current), 基准=\(.baseline), 变化量=\(.delta), 周环比=\(.wow)")
              | join("\n"))
          end
        )
  ' "${REPORT_JSON}"
} > "${REPORT_MD}"

echo "目标日期: ${TARGET_DATE}"
echo "基准日期: ${BASELINE_DATE}"
echo "已分析数据源: ${SOURCES_ANALYZED[*]}"
if [[ ${#MISSING_BASELINE_SOURCES[@]} -gt 0 ]]; then
  echo "缺失基准数据源: ${MISSING_BASELINE_SOURCES[*]}"
else
  echo "缺失基准数据源: 无"
fi
echo "报告路径: ${REPORT_MD}"
echo "JSON路径: ${REPORT_JSON}"
echo "关键指标摘要: current_spend=${TOTAL_CURRENT}, baseline_spend=${TOTAL_BASELINE}, wow=${TOTAL_WOW}"
