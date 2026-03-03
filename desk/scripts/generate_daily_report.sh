#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATA_FETCH_BASE="${ROOT_DIR}/desk/data/data-fetch"
REPORT_BASE="${ROOT_DIR}/desk/reports/daily"

TARGET_DATE="${1:-$(date -u -v-1d '+%Y-%m-%d' 2>/dev/null || date -u -d 'yesterday' '+%Y-%m-%d')}"
INPUT_FILE_ARG="${2:-}"
INPUT_FILE_ENV="${INPUT_FILE:-}"

if [[ ! -d "${DATA_FETCH_BASE}" ]]; then
  echo "Error: data-fetch base not found: ${DATA_FETCH_BASE}" >&2
  exit 1
fi

mkdir -p "${REPORT_BASE}"

REPORT_INPUT_FILE=""
if [[ -n "${INPUT_FILE_ARG}" ]]; then
  REPORT_INPUT_FILE="${INPUT_FILE_ARG}"
elif [[ -n "${INPUT_FILE_ENV}" ]]; then
  REPORT_INPUT_FILE="${INPUT_FILE_ENV}"
else
  for run_dir in $(find "${DATA_FETCH_BASE}" -mindepth 1 -maxdepth 1 -type d | sort -r); do
    candidate="${run_dir}/normalized/${TARGET_DATE}.json"
    if [[ -f "${candidate}" ]] && [[ -s "${candidate}" ]]; then
      REPORT_INPUT_FILE="${candidate}"
      break
    fi
  done
fi

if [[ -z "${REPORT_INPUT_FILE}" ]]; then
  echo "Error: normalized input not found for ${TARGET_DATE}" >&2
  echo "Expected: ${DATA_FETCH_BASE}/<run_id>/normalized/${TARGET_DATE}.json" >&2
  exit 1
fi

if [[ ! -f "${REPORT_INPUT_FILE}" ]]; then
  echo "Error: input file does not exist: ${REPORT_INPUT_FILE}" >&2
  exit 1
fi

REPORT_MD="${REPORT_BASE}/${TARGET_DATE}.md"
ACTIONS_JSON="${REPORT_BASE}/${TARGET_DATE}-actions.json"

METRICS_JSON="$(jq -c '
  def sum_num($k): (map(.[$k] // 0 | tonumber? // 0) | add) // 0;
  def nz($x): if $x == 0 then null else $x end;
  def ratio($a; $b): if ($b // 0) == 0 then null else ($a / $b) end;
  . as $rows
  | {
      row_count: ($rows | length),
      spend: ($rows | sum_num("spend")),
      impressions: ($rows | sum_num("impressions")),
      clicks: ($rows | sum_num("clicks")),
      conversions: ($rows | sum_num("conversions")),
      revenue: (($rows | map(.revenue // 0 | tonumber? // 0) | add) // 0),
      ctr: (ratio(($rows | sum_num("clicks")); ($rows | sum_num("impressions")))),
      cvr: (ratio(($rows | sum_num("conversions")); ($rows | sum_num("clicks")))),
      cpc: (ratio(($rows | sum_num("spend")); ($rows | sum_num("clicks")))),
      cpa: (ratio(($rows | sum_num("spend")); ($rows | sum_num("conversions")))),
      roas: (ratio((($rows | map(.revenue // 0 | tonumber? // 0) | add) // 0); ($rows | sum_num("spend"))))
    }' "${REPORT_INPUT_FILE}")"

TOP3_JSON="$(jq -c '
  map(select((.campaign_id // "") != "overview"))
  | sort_by((.spend // 0 | tonumber? // 0))
  | reverse
  | .[:3]
  | map({
      campaign_id: .campaign_id,
      spend: (.spend // 0),
      clicks: (.clicks // 0),
      conversions: (.conversions // 0)
    })' "${REPORT_INPUT_FILE}")"

BOTTOM3_JSON="$(jq -c '
  map(select((.campaign_id // "") != "overview"))
  | sort_by((.spend // 0 | tonumber? // 0))
  | .[:3]
  | map({
      campaign_id: .campaign_id,
      spend: (.spend // 0),
      clicks: (.clicks // 0),
      conversions: (.conversions // 0)
    })' "${REPORT_INPUT_FILE}")"

SPEND="$(echo "${METRICS_JSON}" | jq -r '.spend')"
IMPRESSIONS="$(echo "${METRICS_JSON}" | jq -r '.impressions')"
CLICKS="$(echo "${METRICS_JSON}" | jq -r '.clicks')"
CONVERSIONS="$(echo "${METRICS_JSON}" | jq -r '.conversions')"
REVENUE="$(echo "${METRICS_JSON}" | jq -r '.revenue')"
CTR="$(echo "${METRICS_JSON}" | jq -r '.ctr')"
CVR="$(echo "${METRICS_JSON}" | jq -r '.cvr')"
CPC="$(echo "${METRICS_JSON}" | jq -r '.cpc')"
CPA="$(echo "${METRICS_JSON}" | jq -r '.cpa')"
ROAS="$(echo "${METRICS_JSON}" | jq -r '.roas')"
ROW_COUNT="$(echo "${METRICS_JSON}" | jq -r '.row_count')"

jq -n \
  --arg date "${TARGET_DATE}" \
  --arg input_file "${REPORT_INPUT_FILE}" \
  --argjson metrics "${METRICS_JSON}" \
  --argjson top3 "${TOP3_JSON}" \
  --argjson bottom3 "${BOTTOM3_JSON}" \
'def mk_action($id; $priority; $title; $rule; $owner):
  {id: $id, priority: $priority, title: $title, rule: $rule, owner: $owner};
{
  date: $date,
  input_file: $input_file,
  metrics: $metrics,
  top_campaigns: $top3,
  bottom_campaigns: $bottom3,
  actions: (
    [
      (if ($metrics.cpa != null and $metrics.cpa > 50)
       then mk_action("reduce_high_cpa_budget"; "high"; "Reduce budget on high-CPA campaigns"; "Pause or reduce campaigns with CPA above account median by >30%"; "campaign-ops")
       else empty end),
      (if ($metrics.roas != null and $metrics.roas < 1.5)
       then mk_action("improve_roas"; "high"; "Prioritize ROAS recovery"; "Shift spend from low ROAS campaigns to stable converters"; "campaign-ops")
       else empty end),
      (if ($metrics.ctr != null and $metrics.ctr < 0.01)
       then mk_action("refresh_creatives"; "medium"; "Refresh creatives for low CTR"; "Prepare 3 new creative angles for weakest ad groups"; "creative-ops")
       else empty end),
      mk_action("daily_checklist"; "medium"; "Run daily operations checklist"; "Validate spend pacing, conversion tracking, and campaign statuses"; "ops")
    ]
  )
}' > "${ACTIONS_JSON}"

{
  echo "# Desk Daily Analysis (${TARGET_DATE})"
  echo
  echo "- input_file: ${REPORT_INPUT_FILE}"
  echo "- row_count: ${ROW_COUNT}"
  echo
  echo "## KPI Summary"
  echo
  echo "- spend: ${SPEND}"
  echo "- impressions: ${IMPRESSIONS}"
  echo "- clicks: ${CLICKS}"
  echo "- conversions: ${CONVERSIONS}"
  echo "- revenue: ${REVENUE}"
  echo "- ctr: ${CTR}"
  echo "- cvr: ${CVR}"
  echo "- cpc: ${CPC}"
  echo "- cpa: ${CPA}"
  echo "- roas: ${ROAS}"
  echo
  echo "## Top 3 Campaigns By Spend"
  echo
  echo "$(echo "${TOP3_JSON}" | jq -r '.[] | "- \(.campaign_id): spend=\(.spend), clicks=\(.clicks), conversions=\(.conversions)"')"
  echo
  echo "## Bottom 3 Campaigns By Spend"
  echo
  echo "$(echo "${BOTTOM3_JSON}" | jq -r '.[] | "- \(.campaign_id): spend=\(.spend), clicks=\(.clicks), conversions=\(.conversions)"')"
  echo
  echo "## Suggested Actions"
  echo
  echo "$(jq -r '.actions[] | "- [\(.priority)] \(.title): \(.rule) (owner=\(.owner))"' "${ACTIONS_JSON}")"
} > "${REPORT_MD}"

echo "Target date: ${TARGET_DATE}"
echo "Input normalized file: ${REPORT_INPUT_FILE}"
echo "Report path: ${REPORT_MD}"
echo "Actions path: ${ACTIONS_JSON}"
echo "Key metrics summary: spend=${SPEND}, clicks=${CLICKS}, conversions=${CONVERSIONS}, cpa=${CPA}, roas=${ROAS}"
