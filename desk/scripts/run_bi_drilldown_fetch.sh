#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIG_FILE="${DESK_BI_CONFIG_FILE:-${ROOT_DIR}/desk/config/bi-sources.json}"
REPORT_DIR="${ROOT_DIR}/desk/reports/bi-wow"
FETCH_SCRIPT="${ROOT_DIR}/desk/scripts/fetch_bi_pivot.sh"
REPORT_JSON_ARG="${1:-}"

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "Error: missing config file at ${CONFIG_FILE}" >&2
  exit 1
fi

if [[ ! -x "${FETCH_SCRIPT}" ]]; then
  echo "Error: missing executable fetch script at ${FETCH_SCRIPT}" >&2
  exit 1
fi

if [[ -n "${REPORT_JSON_ARG}" ]]; then
  REPORT_JSON="${REPORT_JSON_ARG}"
else
  REPORT_JSON="$(ls -1t "${REPORT_DIR}"/*-vs-*.json 2>/dev/null | head -n1 || true)"
fi

if [[ -z "${REPORT_JSON}" || ! -f "${REPORT_JSON}" ]]; then
  echo "Error: no drilldown report json found under ${REPORT_DIR}" >&2
  exit 1
fi

DRILLDOWN_REQUIRED="$(jq -r '.drilldown.required // false' "${REPORT_JSON}")"
TARGET_DATE="$(jq -r '.target_date // empty' "${REPORT_JSON}")"
BASELINE_DATE="$(jq -r '.baseline_date // empty' "${REPORT_JSON}")"
REQUEST_COUNT="$(jq '.drilldown.requests // [] | length' "${REPORT_JSON}")"

if [[ "${DRILLDOWN_REQUIRED}" != "true" ]]; then
  echo "Drilldown fetch completed: false"
  echo "DRILLDOWN_FETCH_COMPLETED=false"
  echo "Reason: drilldown.required is false"
  echo "Report JSON: ${REPORT_JSON}"
  exit 0
fi

if [[ -z "${TARGET_DATE}" || -z "${BASELINE_DATE}" ]]; then
  echo "Error: target_date or baseline_date missing in report: ${REPORT_JSON}" >&2
  exit 1
fi

if [[ "${REQUEST_COUNT}" -eq 0 ]]; then
  echo "Drilldown fetch completed: false"
  echo "DRILLDOWN_FETCH_COMPLETED=false"
  echo "Reason: drilldown.required=true but requests is empty"
  echo "Report JSON: ${REPORT_JSON}"
  exit 0
fi

declare -a RUN_BASES=()
declare -a REQUESTS_EXECUTED=()

for ((i=0; i<REQUEST_COUNT; i++)); do
  SOURCE_NAME="$(jq -r --argjson i "${i}" '.drilldown.requests[$i].source // "unknown_source"' "${REPORT_JSON}")"
  NEXT_TIER="$(jq -r --argjson i "${i}" '.drilldown.requests[$i].next_tier // empty' "${REPORT_JSON}")"
  FILTERS_JSON="$(jq -c --argjson i "${i}" '.drilldown.requests[$i].filters // {}' "${REPORT_JSON}")"

  if [[ -z "${NEXT_TIER}" ]]; then
    echo "Warning: skip request index ${i} due to empty next_tier." >&2
    continue
  fi

  REQUESTS_EXECUTED+=("${SOURCE_NAME}:${NEXT_TIER}")

  for target_date in "${TARGET_DATE}" "${BASELINE_DATE}"; do
    RUN_TOKEN="$(TZ='Asia/Shanghai' date '+%Y-%m-%d_%H-%M-%S')_${NEXT_TIER}_${target_date}_r$((i+1))"
    CMD_OUTPUT="$(DESK_BI_CONFIG_FILE="${CONFIG_FILE}" DESK_BI_DIMENSION_TIER="${NEXT_TIER}" DESK_BI_DRILLDOWN_FILTERS_JSON="${FILTERS_JSON}" DESK_BI_RUN_ID="${RUN_TOKEN}" bash "${FETCH_SCRIPT}" "${target_date}")"
    echo "${CMD_OUTPUT}"
    RUN_BASE="$(awk -F': ' '/^- run_base: /{print $2}' <<<"${CMD_OUTPUT}" | tail -n1)"
    if [[ -n "${RUN_BASE}" ]]; then
      RUN_BASES+=("${RUN_BASE}")
    fi
  done
done

echo "Drilldown fetch completed: true"
echo "DRILLDOWN_FETCH_COMPLETED=true"
echo "Report JSON: ${REPORT_JSON}"
echo "Target date: ${TARGET_DATE}"
echo "Baseline date: ${BASELINE_DATE}"
echo "Requests declared: ${REQUEST_COUNT}"
echo "Requests executed: ${#REQUESTS_EXECUTED[@]}"
if [[ ${#REQUESTS_EXECUTED[@]} -gt 0 ]]; then
  echo "Executed request keys: ${REQUESTS_EXECUTED[*]}"
fi
echo "Run base paths:"
if [[ ${#RUN_BASES[@]} -eq 0 ]]; then
  echo "- none"
else
  for run_base in "${RUN_BASES[@]}"; do
    echo "- ${run_base}"
  done
fi
