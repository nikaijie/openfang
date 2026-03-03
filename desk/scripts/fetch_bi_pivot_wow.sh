#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEFAULT_CONFIG_FILE="${ROOT_DIR}/desk/config/bi-sources.json"
CONFIG_FILE="${DESK_BI_CONFIG_FILE:-${DEFAULT_CONFIG_FILE}}"
FETCH_SCRIPT="${ROOT_DIR}/desk/scripts/fetch_bi_pivot.sh"

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "Error: missing config file at ${CONFIG_FILE}" >&2
  exit 1
fi

if [[ ! -x "${FETCH_SCRIPT}" ]]; then
  echo "Error: missing executable fetch script at ${FETCH_SCRIPT}" >&2
  exit 1
fi

validate_date() {
  local date_value="$1"
  python3 - "${date_value}" <<'PY'
from datetime import datetime
import sys

value = sys.argv[1]
try:
    datetime.strptime(value, "%Y-%m-%d")
except ValueError:
    raise SystemExit(1)
PY
}

TARGET_DATES=()
if [[ "$#" -gt 0 ]]; then
  for arg in "$@"; do
    if ! validate_date "${arg}"; then
      echo "Error: invalid date '${arg}', expected YYYY-MM-DD." >&2
      exit 1
    fi
    TARGET_DATES+=("${arg}")
  done
else
  GLOBAL_TZ="$(jq -r '.timezone // "Asia/Shanghai"' "${CONFIG_FILE}")"
  TARGET_DATE_MODE="$(jq -r '.target_date_mode // "yesterday"' "${CONFIG_FILE}")"

  read -r YESTERDAY LAST_WEEK < <(python3 - "${GLOBAL_TZ}" <<'PY'
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sys

tz = ZoneInfo(sys.argv[1])
today = datetime.now(tz).date()
yesterday = today - timedelta(days=1)
last_week = yesterday - timedelta(days=7)
print(yesterday.isoformat(), last_week.isoformat())
PY
  )

  case "${TARGET_DATE_MODE}" in
    yesterday_and_last_week|yesterday+last_week|wow)
      TARGET_DATES=("${YESTERDAY}" "${LAST_WEEK}")
      ;;
    *)
      TARGET_DATES=("${YESTERDAY}")
      ;;
  esac
fi

RUN_PREFIX="$(TZ='Asia/Shanghai' date '+%Y-%m-%d_%H-%M')"
declare -a SUCCEEDED_DATES=()
declare -a FAILED_DATES=()
declare -a RUN_BASE_PATHS=()
EXIT_CODE=0

for target_date in "${TARGET_DATES[@]}"; do
  RUN_ID="${RUN_PREFIX}_${target_date}"
  RUN_BASE="${ROOT_DIR}/desk/data/bi-fetch/${RUN_ID}"
  RUN_BASE_PATHS+=("${RUN_BASE}")

  echo "=== BI Pivot fetch start: target_date=${target_date}, run_id=${RUN_ID} ==="
  if DESK_BI_RUN_ID="${RUN_ID}" DESK_BI_CONFIG_FILE="${CONFIG_FILE}" bash "${FETCH_SCRIPT}" "${target_date}"; then
    SUCCEEDED_DATES+=("${target_date}")
  else
    FAILED_DATES+=("${target_date}")
    EXIT_CODE=1
  fi
done

echo "Target dates requested: ${TARGET_DATES[*]}"
if [[ ${#SUCCEEDED_DATES[@]} -gt 0 ]]; then
  echo "Target dates succeeded: ${SUCCEEDED_DATES[*]}"
else
  echo "Target dates succeeded: none"
fi
if [[ ${#FAILED_DATES[@]} -gt 0 ]]; then
  echo "Target dates failed: ${FAILED_DATES[*]}"
else
  echo "Target dates failed: none"
fi
echo "Run base paths:"
for run_base in "${RUN_BASE_PATHS[@]}"; do
  echo "- ${run_base}"
done

exit "${EXIT_CODE}"
