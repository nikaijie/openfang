#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEFAULT_CONFIG_FILE="${ROOT_DIR}/desk/config/ad-sources.json"
CONFIG_FILE="${DESK_CONFIG_FILE:-${DEFAULT_CONFIG_FILE}}"

if ! command -v jq >/dev/null 2>&1; then
  echo "Error: jq is required but not installed." >&2
  exit 1
fi

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "Error: missing config file at ${CONFIG_FILE}" >&2
  echo "Hint: set DESK_CONFIG_FILE to your config path, e.g. export DESK_CONFIG_FILE='/path/to/desk/config/ad-sources.json'" >&2
  exit 1
fi

TARGET_DATE="${1:-$(date -u -v-1d '+%Y-%m-%d' 2>/dev/null || date -u -d 'yesterday' '+%Y-%m-%d')}"
SOURCE_NAME="$(jq -r '.sources[] | select(.enabled == true) | .name' "${CONFIG_FILE}" | head -n1)"
ACCOUNT_ID="$(jq -r '.sources[] | select(.enabled == true) | .account_id // "unknown-account"' "${CONFIG_FILE}" | head -n1)"
BASE_URL_FROM_CFG="$(jq -r '.sources[] | select(.enabled == true) | .portal.base_url // empty' "${CONFIG_FILE}" | head -n1)"
BASE_URL="${DESK_PORTAL_BASE_URL:-${BASE_URL_FROM_CFG:-http://newrixdemo.console.rixbeedesk.com}}"
REPORT_PATH="/dashboard/over-adv"

if [[ -z "${SOURCE_NAME}" || "${SOURCE_NAME}" == "null" ]]; then
  echo "Error: no enabled source found in ${CONFIG_FILE}" >&2
  exit 1
fi

RUN_ID="$(TZ='Asia/Shanghai' date '+%Y-%m-%d_%H-%M')"
RUN_BASE="${ROOT_DIR}/desk/data/data-fetch/${RUN_ID}"
RAW_BASE="${RUN_BASE}/raw/${SOURCE_NAME}"
NORM_BASE="${RUN_BASE}/normalized"
LOG_BASE="${RUN_BASE}/logs"

mkdir -p "${RAW_BASE}" "${NORM_BASE}" "${LOG_BASE}"

SESSION_COOKIE_FROM_CFG="$(jq -r '.sources[] | select(.enabled == true) | .auth.session_cookie // empty' "${CONFIG_FILE}" | head -n1)"
SESSION_COOKIE="${DESK_PORTAL_SESSION_COOKIE:-${SESSION_COOKIE_FROM_CFG}}"

if [[ -z "${SESSION_COOKIE}" ]]; then
  echo "Error: missing session cookie." >&2
  echo "Set DESK_PORTAL_SESSION_COOKIE or set auth.session_cookie in ${CONFIG_FILE}" >&2
  exit 1
fi

X_CURRENCY_CFG="$(jq -r '.sources[] | select(.enabled == true) | .portal.headers.x_currency // empty' "${CONFIG_FILE}" | head -n1)"
X_LANGUAGE_CFG="$(jq -r '.sources[] | select(.enabled == true) | .portal.headers.x_language // empty' "${CONFIG_FILE}" | head -n1)"
X_PAGE_UID_CFG="$(jq -r '.sources[] | select(.enabled == true) | .portal.headers.x_page_uid // empty' "${CONFIG_FILE}" | head -n1)"
X_TIME_ZONE_CFG="$(jq -r '.sources[] | select(.enabled == true) | .portal.headers.x_time_zone // empty' "${CONFIG_FILE}" | head -n1)"
X_VERSION_CFG="$(jq -r '.sources[] | select(.enabled == true) | .portal.headers.x_version // empty' "${CONFIG_FILE}" | head -n1)"
X_SIGN_CFG="$(jq -r '.sources[] | select(.enabled == true) | .portal.headers.x_sign // empty' "${CONFIG_FILE}" | head -n1)"

X_CURRENCY="${DESK_PORTAL_X_CURRENCY:-${X_CURRENCY_CFG:-USD}}"
X_LANGUAGE="${DESK_PORTAL_X_LANGUAGE:-${X_LANGUAGE_CFG:-zh-CN}}"
X_PAGE_UID="${DESK_PORTAL_X_PAGE_UID:-${X_PAGE_UID_CFG:-0}}"
X_TIME_ZONE="${DESK_PORTAL_X_TIME_ZONE:-${X_TIME_ZONE_CFG:-Etc/UTC}}"
X_VERSION="${DESK_PORTAL_X_VERSION:-${X_VERSION_CFG:-310c1127f7fa96973af505dc56814c6c6a0f870b}}"
X_SIGN="${DESK_PORTAL_X_SIGN:-${X_SIGN_CFG}}"

declare -a SUCCEEDED=()
declare -a FAILED=()

fetch_endpoint() {
  local endpoint="$1"
  local payload="$2"
  local out_file="$3"
  local tmp_file
  local code=""
  local try
  tmp_file="$(mktemp)"

  for try in 1 2 3; do
    if [[ -n "${X_SIGN}" ]]; then
      code="$(curl -sS -o "${tmp_file}" -w "%{http_code}" \
        "${BASE_URL}${endpoint}" \
        -X POST \
        -H "Accept: application/json, text/plain, */*" \
        -H "Content-Type: application/json" \
        -H "Origin: ${BASE_URL}" \
        -H "Referer: ${BASE_URL}${REPORT_PATH}" \
        -H "x-currency: ${X_CURRENCY}" \
        -H "x-language: ${X_LANGUAGE}" \
        -H "x-page-u-id: ${X_PAGE_UID}" \
        -H "x-time-zone: ${X_TIME_ZONE}" \
        -H "x-version: ${X_VERSION}" \
        -H "x-sign: ${X_SIGN}" \
        -H "Cookie: ${SESSION_COOKIE}" \
        --data-raw "${payload}" || true)"
    else
      code="$(curl -sS -o "${tmp_file}" -w "%{http_code}" \
        "${BASE_URL}${endpoint}" \
        -X POST \
        -H "Accept: application/json, text/plain, */*" \
        -H "Content-Type: application/json" \
        -H "Origin: ${BASE_URL}" \
        -H "Referer: ${BASE_URL}${REPORT_PATH}" \
        -H "x-currency: ${X_CURRENCY}" \
        -H "x-language: ${X_LANGUAGE}" \
        -H "x-page-u-id: ${X_PAGE_UID}" \
        -H "x-time-zone: ${X_TIME_ZONE}" \
        -H "x-version: ${X_VERSION}" \
        -H "Cookie: ${SESSION_COOKIE}" \
        --data-raw "${payload}" || true)"
    fi

    if [[ "${code}" == "200" ]]; then
      mv "${tmp_file}" "${out_file}"
      return 0
    fi
    sleep 2
  done

  echo "Endpoint failed after retries: ${endpoint} (http=${code})" >&2
  rm -f "${tmp_file}"
  return 1
}

OVERVIEW_FILE="${RAW_BASE}/${TARGET_DATE}_getOverview.json"
TOP_CAMPAIGN_FILE="${RAW_BASE}/${TARGET_DATE}_getTopCampaign.json"
DYNAMIC_FILE="${RAW_BASE}/${TARGET_DATE}_getDynamicMetric.json"

if fetch_endpoint "/api/dashboard/getOverview" "{}" "${OVERVIEW_FILE}"; then
  SUCCEEDED+=("/api/dashboard/getOverview")
else
  FAILED+=("/api/dashboard/getOverview")
  echo '{}' > "${OVERVIEW_FILE}"
fi

if fetch_endpoint "/api/dashboard/getTopCampaign" "{}" "${TOP_CAMPAIGN_FILE}"; then
  SUCCEEDED+=("/api/dashboard/getTopCampaign")
else
  FAILED+=("/api/dashboard/getTopCampaign")
  echo '{}' > "${TOP_CAMPAIGN_FILE}"
fi

if fetch_endpoint "/api/dashboard/getDynamicMetric" '{"metric":"click_rate"}' "${DYNAMIC_FILE}"; then
  SUCCEEDED+=("/api/dashboard/getDynamicMetric")
else
  FAILED+=("/api/dashboard/getDynamicMetric")
  echo '{}' > "${DYNAMIC_FILE}"
fi

NORM_FILE="${NORM_BASE}/${TARGET_DATE}.json"
jq -n \
  --arg date "${TARGET_DATE}" \
  --arg source "${SOURCE_NAME}" \
  --arg account_id "${ACCOUNT_ID}" \
  --slurpfile ov "${OVERVIEW_FILE}" \
  --slurpfile tc "${TOP_CAMPAIGN_FILE}" \
'def pick($obj; $keys): reduce $keys[] as $k (null; . // $obj[$k]);
 def to_num: (tonumber? // 0);
 def ov: ($ov[0].data // $ov[0].result // $ov[0]);
 def tc_raw: ($tc[0].data // $tc[0].result // $tc[0]);
 def tc_list:
   if (tc_raw | type) == "array" then tc_raw
   elif (tc_raw | type) == "object" and ((tc_raw.list | type) == "array") then tc_raw.list
   else []
   end;
 [
   {
     date: $date,
     source: $source,
     account_id: $account_id,
     campaign_id: "overview",
     spend: (pick(ov; ["spend","consume","cost","amount"]) | to_num),
     impressions: (pick(ov; ["impressions","impression","show","views"]) | to_num),
     clicks: (pick(ov; ["clicks","click","clk"]) | to_num),
     conversions: (pick(ov; ["conversions","conversion","orders","cv"]) | to_num),
     revenue: (pick(ov; ["revenue","gmv","sales","income"]) | tonumber?)
   }
 ] + (
   tc_list | map({
     date: $date,
     source: $source,
     account_id: $account_id,
     campaign_id: (pick(.; ["campaign_id","campaignId","id","name"]) | tostring),
     spend: (pick(.; ["spend","consume","cost","amount"]) | to_num),
     impressions: (pick(.; ["impressions","impression","show","views"]) | to_num),
     clicks: (pick(.; ["clicks","click","clk"]) | to_num),
     conversions: (pick(.; ["conversions","conversion","orders","cv"]) | to_num),
     revenue: (pick(.; ["revenue","gmv","sales","income"]) | tonumber?)
   })
 )' > "${NORM_FILE}"

LOG_FILE="${LOG_BASE}/ingestion-${TARGET_DATE}.md"
ROW_COUNT="$(jq 'length' "${NORM_FILE}")"
{
  echo "# Desk Ingestion Summary (${TARGET_DATE})"
  echo
  echo "- run_id: ${RUN_ID}"
  echo "- run_base: ${RUN_BASE}"
  echo "- source: ${SOURCE_NAME}"
  echo "- account_id: ${ACCOUNT_ID}"
  echo "- base_url: ${BASE_URL}"
  echo "- config_file: ${CONFIG_FILE}"
  echo "- succeeded_endpoints: ${SUCCEEDED[*]:-none}"
  echo "- failed_endpoints: ${FAILED[*]:-none}"
  echo "- normalized_file: ${NORM_FILE}"
  echo "- normalized_rows: ${ROW_COUNT}"
} > "${LOG_FILE}"

echo "Target date: ${TARGET_DATE}"
if [[ ${#SUCCEEDED[@]} -gt 0 ]]; then
  echo "Sources succeeded: ${SOURCE_NAME}"
else
  echo "Sources succeeded: none"
fi
if [[ ${#FAILED[@]} -gt 0 ]]; then
  echo "Sources failed: ${SOURCE_NAME}"
else
  echo "Sources failed: none"
fi
echo "Output file paths:"
echo "- run_base: ${RUN_BASE}"
echo "- raw: ${RAW_BASE}"
echo "- normalized: ${NORM_FILE}"
echo "- log: ${LOG_FILE}"
echo "Row count written: ${ROW_COUNT}"
