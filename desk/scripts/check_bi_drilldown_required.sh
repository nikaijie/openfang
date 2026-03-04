#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_DIR="${ROOT_DIR}/desk/reports/bi-wow"
REPORT_JSON_ARG="${1:-}"

if [[ -n "${REPORT_JSON_ARG}" ]]; then
  REPORT_JSON="${REPORT_JSON_ARG}"
else
  REPORT_JSON="$(ls -1t "${REPORT_DIR}"/*-vs-*.json 2>/dev/null | head -n1 || true)"
fi

if [[ -z "${REPORT_JSON}" || ! -f "${REPORT_JSON}" ]]; then
  echo "DRILLDOWN_REQUIRED_FLAG=false"
  echo "DRILLDOWN_REQUEST_COUNT=0"
  echo "DRILLDOWN_REPORT_JSON=missing"
  echo "DRILLDOWN_REASON=no_report_json"
  exit 0
fi

DRILLDOWN_REQUIRED="$(jq -r '.drilldown.required // false' "${REPORT_JSON}")"
DRILLDOWN_REQUEST_COUNT="$(jq '.drilldown.requests // [] | length' "${REPORT_JSON}")"

if [[ "${DRILLDOWN_REQUIRED}" == "true" && "${DRILLDOWN_REQUEST_COUNT}" -gt 0 ]]; then
  FLAG="true"
else
  FLAG="false"
fi

echo "DRILLDOWN_REQUIRED_FLAG=${FLAG}"
echo "DRILLDOWN_REQUEST_COUNT=${DRILLDOWN_REQUEST_COUNT}"
echo "DRILLDOWN_REPORT_JSON=${REPORT_JSON}"
