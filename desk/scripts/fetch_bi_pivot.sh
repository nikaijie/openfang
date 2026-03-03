#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEFAULT_CONFIG_FILE="${ROOT_DIR}/desk/config/bi-sources.json"
CONFIG_FILE="${DESK_BI_CONFIG_FILE:-${DEFAULT_CONFIG_FILE}}"
TARGET_DATE_ARG="${1:-}"

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: ${cmd} is required but not installed." >&2
    exit 1
  fi
}

require_cmd jq
require_cmd curl
require_cmd python3

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "Error: missing config file at ${CONFIG_FILE}" >&2
  echo "Hint: set DESK_BI_CONFIG_FILE to your config path." >&2
  exit 1
fi

GLOBAL_TZ="$(jq -r '.timezone // "Asia/Shanghai"' "${CONFIG_FILE}")"
RUN_ID="${DESK_BI_RUN_ID:-$(TZ='Asia/Shanghai' date '+%Y-%m-%d_%H-%M')}"
RUN_BASE="${ROOT_DIR}/desk/data/bi-fetch/${RUN_ID}"
RAW_BASE="${RUN_BASE}/raw"
NORM_BASE="${RUN_BASE}/normalized"
LOG_BASE="${RUN_BASE}/logs"

mkdir -p "${RAW_BASE}" "${NORM_BASE}" "${LOG_BASE}"

ENABLED_SOURCE_INDEXES=()
while IFS= read -r source_idx; do
  [[ -n "${source_idx}" ]] || continue
  ENABLED_SOURCE_INDEXES+=("${source_idx}")
done < <(jq -r '.sources | to_entries[] | select(.value.enabled == true) | .key' "${CONFIG_FILE}")
if [[ ${#ENABLED_SOURCE_INDEXES[@]} -eq 0 ]]; then
  echo "Error: no enabled BI source found in ${CONFIG_FILE}" >&2
  exit 1
fi

declare -a SOURCES_SUCCEEDED=()
declare -a SOURCES_FAILED=()
TOTAL_ROWS_ALL=0
LAST_TARGET_DATE=""

parse_data_config_value() {
  local html_file="$1"
  local key="$2"

  python3 - "$html_file" "$key" <<'PY'
import base64
import json
import re
import sys

html_file = sys.argv[1]
key = sys.argv[2]
html = open(html_file, 'r', encoding='utf-8', errors='ignore').read()
match = re.search(r'data-config=([^>]+)></div>', html)
if not match:
    print("")
    raise SystemExit

raw = match.group(1).strip().strip('"').strip("'")
raw += "=" * (-len(raw) % 4)
try:
    payload = json.loads(base64.b64decode(raw).decode('utf-8', errors='ignore'))
except Exception:
    print("")
    raise SystemExit

value = payload.get(key, "")
print(value if isinstance(value, str) else "")
PY
}

select_dimension_ids() {
  local data_cube_doc_json="$1"
  local configured_dimensions_json="$2"
  local max_dimensions="$3"

  DATA_CUBE_DOC_JSON="${data_cube_doc_json}" \
  CONFIG_DIMENSIONS_JSON="${configured_dimensions_json}" \
  MAX_DIMENSIONS="${max_dimensions}" \
  python3 - <<'PY'
import json
import os


def parse_json_env(name, default):
    raw = os.environ.get(name, "")
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def add_unique(items, value):
    if value and value not in items:
        items.append(value)


def normalize_text(value):
    return (value or "").strip().lower()


def resolve_configured_dimension(entry, dimensions, valid_name_set):
    if not isinstance(entry, str):
        return ""
    entry = entry.strip()
    if not entry:
        return ""

    if entry in valid_name_set:
        return entry

    target = normalize_text(entry)

    # Allow config to specify dimension title from UI.
    for dim in dimensions:
        title = normalize_text(dim.get("title"))
        if title and title == target:
            return dim.get("name") or ""

    # Allow config to specify formula token (e.g. user_id / cpg_id / cr_id).
    for dim in dimensions:
        formula = normalize_text(dim.get("formula"))
        if target and target in formula:
            return dim.get("name") or ""

    return ""


def pick_dimension_id(dimensions, formula_token, name_prefix):
    # Prefer *_id formula dimensions (stable keys for downstream analysis).
    for dim in dimensions:
        name = dim.get("name") or ""
        formula = (dim.get("formula") or "").lower()
        if formula_token in formula and "_id" in formula:
            return name

    # Fallback: any dimension with matching formula token.
    for dim in dimensions:
        name = dim.get("name") or ""
        formula = (dim.get("formula") or "").lower()
        if formula_token in formula:
            return name

    # Last fallback: known dimension prefix.
    for dim in dimensions:
        name = dim.get("name") or ""
        if name.startswith(name_prefix):
            return name

    return ""


data_cube_doc = parse_json_env("DATA_CUBE_DOC_JSON", {})
configured_dims = parse_json_env("CONFIG_DIMENSIONS_JSON", [])
dimensions = data_cube_doc.get("dimensions") or []
valid_names = [d.get("name") for d in dimensions if d.get("name")]
valid_name_set = set(valid_names)

try:
    max_dimensions = int(os.environ.get("MAX_DIMENSIONS", "3"))
except ValueError:
    max_dimensions = 3
if max_dimensions <= 0:
    max_dimensions = 3

selected = []

# 1) Use explicitly configured dimensions first.
for dim_id in configured_dims:
    add_unique(selected, resolve_configured_dimension(dim_id, dimensions, valid_name_set))

# 2) Ensure key business grains exist (account / campaign / creative).
for dim_id in [
    pick_dimension_id(dimensions, "user_id", "tuser_i-"),
    pick_dimension_id(dimensions, "cpg_id", "tcpg_id-"),
    pick_dimension_id(dimensions, "cr_id", "tcr_id-"),
]:
    add_unique(selected, dim_id)

# 3) Add default facet dimensions as soft fallback.
default_facet_splits = data_cube_doc.get("defaultFacetSplits") or []
if default_facet_splits and isinstance(default_facet_splits[0], list):
    for split in default_facet_splits[0]:
        if isinstance(split, dict):
            add_unique(selected, split.get("dimension", ""))

# Keep only dimensions that actually exist in current dataCube metadata.
selected = [dim_id for dim_id in selected if dim_id in valid_name_set]

# If still empty, fallback to first N dimensions from metadata.
if not selected:
    selected = valid_names[:max_dimensions]

selected = selected[:max_dimensions]
print(json.dumps(selected))
PY
}

resolve_measure_ids() {
  local data_cube_doc_json="$1"
  local primary_measure_cfg="$2"
  local sort_measure_cfg="$3"

  DATA_CUBE_DOC_JSON="${data_cube_doc_json}" \
  PRIMARY_MEASURE_CFG="${primary_measure_cfg}" \
  SORT_MEASURE_CFG="${sort_measure_cfg}" \
  python3 - <<'PY'
import json
import os


def parse_json_env(name, default):
    raw = os.environ.get(name, "")
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def normalize_text(value):
    return (value or "").strip().lower()


def find_measure_name(candidate, measures):
    if not candidate:
        return ""
    c = candidate.strip()
    if not c:
        return ""

    for m in measures:
        if (m.get("name") or "") == c:
            return c

    target = normalize_text(c)
    for m in measures:
        if normalize_text(m.get("title")) == target:
            return m.get("name") or ""

    return ""


def find_measure_by_hint(measures, hints):
    for hint in hints:
        token = normalize_text(hint)
        if not token:
            continue
        for m in measures:
            hay = f"{m.get('title', '')} {m.get('formula', '')}"
            if token in normalize_text(hay):
                return m.get("name") or ""
    return ""


def measure_title(name, measures):
    for m in measures:
        if (m.get("name") or "") == name:
            return m.get("title") or ""
    return ""


data_cube_doc = parse_json_env("DATA_CUBE_DOC_JSON", {})
measures = data_cube_doc.get("measures") or []

primary_cfg = os.environ.get("PRIMARY_MEASURE_CFG", "")
sort_cfg = os.environ.get("SORT_MEASURE_CFG", "")

primary = find_measure_name(primary_cfg, measures)
primary_from_config = bool(primary)
if not primary:
    primary = find_measure_by_hint(
        measures,
        [
            "廣告費用(nt$)",
            "廣告費用",
            "spend",
            "cost",
            "adv_gross_rev",
        ],
    )
if not primary and measures:
    primary = measures[0].get("name") or ""

sort = find_measure_name(sort_cfg, measures)
sort_from_config = bool(sort)
if not sort:
    sort = primary

result = {
    "primary": primary,
    "primary_title": measure_title(primary, measures),
    "primary_from_config": primary_from_config,
    "sort": sort,
    "sort_title": measure_title(sort, measures),
    "sort_from_config": sort_from_config,
}
print(json.dumps(result))
PY
}

for source_idx in "${ENABLED_SOURCE_INDEXES[@]}"; do
  SOURCE_JSON="$(jq -c ".sources[${source_idx}]" "${CONFIG_FILE}")"

  SOURCE_NAME="$(jq -r '.name' <<<"${SOURCE_JSON}")"
  ACCOUNT_ID="$(jq -r '.account_id // "unknown-account"' <<<"${SOURCE_JSON}")"

  BASE_URL="$(jq -r '.portal.base_url // empty' <<<"${SOURCE_JSON}")"
  REFERER="$(jq -r '.portal.referer // (.portal.base_url + .portal.path)' <<<"${SOURCE_JSON}")"
  VIEW_NAME="$(jq -r '.portal.view_name // .portal.data_cube // empty' <<<"${SOURCE_JSON}")"
  VIEW_TITLE="$(jq -r '.portal.view_title // empty' <<<"${SOURCE_JSON}")"
  DATA_CUBE="$(jq -r '.portal.data_cube // .portal.view_name // empty' <<<"${SOURCE_JSON}")"

  if [[ -z "${BASE_URL}" || -z "${VIEW_NAME}" || -z "${DATA_CUBE}" ]]; then
    echo "Source '${SOURCE_NAME}' is missing required portal fields (base_url/view_name/data_cube)." >&2
    SOURCES_FAILED+=("${SOURCE_NAME}")
    continue
  fi

  SOURCE_TZ="$(jq -r --arg tz "${GLOBAL_TZ}" '.portal.timezone // $tz' <<<"${SOURCE_JSON}")"
  if [[ -n "${TARGET_DATE_ARG}" ]]; then
    TARGET_DATE="${TARGET_DATE_ARG}"
  else
    TARGET_DATE="$(python3 - "${SOURCE_TZ}" <<'PY'
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sys

tz_name = sys.argv[1]
tz = ZoneInfo(tz_name)
print((datetime.now(tz).date() - timedelta(days=1)).isoformat())
PY
)"
  fi
  LAST_TARGET_DATE="${TARGET_DATE}"

  read -r START_UTC END_UTC < <(python3 - "${TARGET_DATE}" "${SOURCE_TZ}" <<'PY'
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import sys

target_date = sys.argv[1]
tz = ZoneInfo(sys.argv[2])
start_local = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=tz)
end_local = start_local + timedelta(days=1)
print(start_local.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'), end_local.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'))
PY
)

  SOURCE_RAW_DIR="${RAW_BASE}/${SOURCE_NAME}"
  SOURCE_LOG_DIR="${LOG_BASE}/${SOURCE_NAME}"
  mkdir -p "${SOURCE_RAW_DIR}" "${SOURCE_LOG_DIR}"

  APP_SETTINGS_FILE="${SOURCE_RAW_DIR}/${TARGET_DATE}_app-settings.json"
  TOTAL_GROUPBY_FILE="${SOURCE_RAW_DIR}/${TARGET_DATE}_groupby_total.json"

  COOKIE_JAR="${SOURCE_LOG_DIR}/cookies.txt"
  : > "${COOKIE_JAR}"

  USERNAME_CFG="$(jq -r '.auth.username // empty' <<<"${SOURCE_JSON}")"
  PASSWORD_CFG="$(jq -r '.auth.password // empty' <<<"${SOURCE_JSON}")"
  USERNAME_ENV_NAME="$(jq -r '.auth.username_env // empty' <<<"${SOURCE_JSON}")"
  PASSWORD_ENV_NAME="$(jq -r '.auth.password_env // empty' <<<"${SOURCE_JSON}")"

  if [[ -n "${USERNAME_CFG}" ]]; then
    USERNAME="${USERNAME_CFG}"
  elif [[ -n "${USERNAME_ENV_NAME}" ]]; then
    USERNAME="${!USERNAME_ENV_NAME:-}"
  else
    USERNAME=""
  fi

  if [[ -n "${PASSWORD_CFG}" ]]; then
    PASSWORD="${PASSWORD_CFG}"
  elif [[ -n "${PASSWORD_ENV_NAME}" ]]; then
    PASSWORD="${!PASSWORD_ENV_NAME:-}"
  else
    PASSWORD=""
  fi

  SESSION_COOKIE_ENV_NAME="$(jq -r '.auth.session_cookie_env // empty' <<<"${SOURCE_JSON}")"
  X_SRF_ENV_NAME="$(jq -r '.auth.x_srf_env // empty' <<<"${SOURCE_JSON}")"
  SESSION_COOKIE_CFG="$(jq -r '.auth.session_cookie // empty' <<<"${SOURCE_JSON}")"
  X_SRF_CFG="$(jq -r '.auth.x_srf // empty' <<<"${SOURCE_JSON}")"

  if [[ -n "${SESSION_COOKIE_ENV_NAME}" && -n "${!SESSION_COOKIE_ENV_NAME:-}" ]]; then
    COOKIE_HEADER="${!SESSION_COOKIE_ENV_NAME}"
  else
    COOKIE_HEADER="${SESSION_COOKIE_CFG}"
  fi

  if [[ -n "${X_SRF_ENV_NAME}" && -n "${!X_SRF_ENV_NAME:-}" ]]; then
    X_SRF_TOKEN="${!X_SRF_ENV_NAME}"
  else
    X_SRF_TOKEN="${X_SRF_CFG}"
  fi

  AUTO_LOGIN="$(jq -r '.auth.auto_login // true' <<<"${SOURCE_JSON}")"

  IM_VERSION="$(jq -r '.portal.im_version // "2023.01.0"' <<<"${SOURCE_JSON}")"
  SORT_MEASURE_CFG="$(jq -r '.portal.sort_measure // .portal.spend_measure // empty' <<<"${SOURCE_JSON}")"
  PRIMARY_MEASURE_CFG="$(jq -r '.portal.primary_measure // .portal.spend_measure // (.portal.measures[0] // "SUMtadv-00e")' <<<"${SOURCE_JSON}")"
  MAX_ROWS="$(jq -r '.portal.max_rows // 5000' <<<"${SOURCE_JSON}")"
  MAX_DIMENSIONS_RAW="$(jq -r '.portal.max_dimensions // 3' <<<"${SOURCE_JSON}")"
  EXCLUDE_DIMENSION="$(jq -r '.portal.exclude_dimension // empty' <<<"${SOURCE_JSON}")"
  EXCLUDE_VALUES_JSON="$(jq -c '.portal.exclude_values // []' <<<"${SOURCE_JSON}")"
  CONFIG_DIMENSIONS_JSON="$(jq -c '.portal.dimension_ids // (if (.portal.split_dimension // empty) != "" then [ .portal.split_dimension ] else [] end)' <<<"${SOURCE_JSON}")"

  if [[ "${MAX_DIMENSIONS_RAW}" =~ ^[0-9]+$ ]] && [[ "${MAX_DIMENSIONS_RAW}" -gt 0 ]]; then
    MAX_DIMENSIONS="${MAX_DIMENSIONS_RAW}"
  else
    MAX_DIMENSIONS=3
  fi

  refresh_auth() {
    local login_html login_xsrf login_payload login_response_file login_status home_html

    if [[ -z "${USERNAME}" || -z "${PASSWORD}" ]]; then
      return 1
    fi

    login_html="$(mktemp)"
    curl -sS -c "${COOKIE_JAR}" -b "${COOKIE_JAR}" "${BASE_URL}/login" > "${login_html}"

    login_xsrf="$(parse_data_config_value "${login_html}" "xsrfLogin")"
    rm -f "${login_html}"

    if [[ -z "${login_xsrf}" ]]; then
      return 1
    fi

    login_payload="$(jq -nc --arg name "${USERNAME}" --arg pass "${PASSWORD}" '{name:$name, pass:$pass, rememberMe:true}')"
    login_response_file="$(mktemp)"

    login_status="$(curl -sS -o "${login_response_file}" -w "%{http_code}" \
      -c "${COOKIE_JAR}" -b "${COOKIE_JAR}" \
      "${BASE_URL}/imply/login" \
      -H 'Accept: application/json, text/plain, */*' \
      -H 'Content-Type: application/json' \
      -H "Origin: ${BASE_URL}" \
      -H "Referer: ${BASE_URL}/login" \
      -H "x-srf-login: ${login_xsrf}" \
      -H "x-srf: ${login_xsrf}" \
      --data-raw "${login_payload}" || true)"

    if [[ "${login_status}" != "200" ]]; then
      rm -f "${login_response_file}"
      return 1
    fi

    if ! jq -e '.status == "OK"' "${login_response_file}" >/dev/null 2>&1; then
      rm -f "${login_response_file}"
      return 1
    fi

    rm -f "${login_response_file}"

    local connect_sid
    connect_sid="$(awk '$6=="connect.sid" {print $7}' "${COOKIE_JAR}" | tail -n1)"
    if [[ -z "${connect_sid}" ]]; then
      return 1
    fi

    COOKIE_HEADER="connect.sid=${connect_sid}"

    home_html="$(mktemp)"
    curl -sS -b "${COOKIE_HEADER}" "${BASE_URL}/pivot/home" > "${home_html}"

    X_SRF_TOKEN="$(parse_data_config_value "${home_html}" "xsrfToken")"
    rm -f "${home_html}"

    [[ -n "${X_SRF_TOKEN}" ]]
  }

  post_json() {
    local endpoint="$1"
    local payload="$2"
    local out_file="$3"
    local code

    code="$(curl -sS -o "${out_file}" -w "%{http_code}" \
      "${BASE_URL}${endpoint}" \
      -X POST \
      -H 'Accept: application/json, text/plain, */*' \
      -H 'Content-Type: application/json' \
      -H "Origin: ${BASE_URL}" \
      -H "Referer: ${REFERER}" \
      -H 'x-allow-cancel: 1' \
      -H "x-im-version: ${IM_VERSION}" \
      -H "x-srf: ${X_SRF_TOKEN}" \
      -b "${COOKIE_HEADER}" \
      --data-raw "${payload}" || true)"

    if [[ "${code}" != "200" ]]; then
      return 1
    fi

    if ! jq -e . "${out_file}" >/dev/null 2>&1; then
      return 1
    fi

    if jq -e '.status? == "BAD_AUTH" or .status? == "ACCOUNT_DISABLED"' "${out_file}" >/dev/null 2>&1; then
      return 1
    fi

    if jq -e '.error? != null' "${out_file}" >/dev/null 2>&1; then
      return 1
    fi

    return 0
  }

  post_json_with_retry() {
    local endpoint="$1"
    local payload="$2"
    local out_file="$3"
    local tries

    for tries in 1 2 3; do
      if post_json "${endpoint}" "${payload}" "${out_file}"; then
        return 0
      fi

      if [[ "${AUTO_LOGIN}" == "true" && "${tries}" == "1" ]]; then
        refresh_auth >/dev/null 2>&1 || true
      fi
      sleep 2
    done

    return 1
  }

  if [[ -z "${COOKIE_HEADER}" || -z "${X_SRF_TOKEN}" ]]; then
    if [[ "${AUTO_LOGIN}" == "true" ]]; then
      if ! refresh_auth; then
        echo "Source '${SOURCE_NAME}' auth bootstrap failed (missing/expired session and auto login failed)." >&2
        SOURCES_FAILED+=("${SOURCE_NAME}")
        continue
      fi
    else
      echo "Source '${SOURCE_NAME}' missing session_cookie/x_srf and auto_login=false." >&2
      SOURCES_FAILED+=("${SOURCE_NAME}")
      continue
    fi
  fi

  FILTER_CLAUSES_JSON="$(jq -nc \
    --arg start "${START_UTC}" \
    --arg end "${END_UTC}" \
    --arg exclude_dimension "${EXCLUDE_DIMENSION}" \
    --argjson exclude_values "${EXCLUDE_VALUES_JSON}" '
      [
        {
          dimension: "__time",
          action: "overlap",
          values: {
            setType: "TIME_RANGE",
            elements: [
              { start: $start, end: $end }
            ]
          }
        }
      ]
      +
      (if ($exclude_dimension | length) > 0 and (($exclude_values | length) > 0)
       then [
         {
           dimension: $exclude_dimension,
           action: "overlap",
           values: {
             setType: "STRING",
             elements: $exclude_values
           },
           exclude: true,
           mvFilterOnly: false
         }
       ]
       else []
       end)
    ')"

  if ! post_json_with_retry "/app-settings/load" "" "${APP_SETTINGS_FILE}"; then
    echo "Source '${SOURCE_NAME}' failed to fetch /app-settings/load." >&2
    SOURCES_FAILED+=("${SOURCE_NAME}")
    continue
  fi

  DATA_CUBE_DOC="$(jq -c --arg cube "${DATA_CUBE}" '.clientSettings.dataCubes[]? | select(.name == $cube)' "${APP_SETTINGS_FILE}" | head -n1)"
  if [[ -z "${DATA_CUBE_DOC}" || "${DATA_CUBE_DOC}" == "null" ]]; then
    DATA_CUBE_DOC="$(jq -c '.clientSettings.dataCubes[0] // null' "${APP_SETTINGS_FILE}")"
  fi

  if [[ -z "${DATA_CUBE_DOC}" || "${DATA_CUBE_DOC}" == "null" ]]; then
    echo "Source '${SOURCE_NAME}' has no dataCube metadata in app-settings." >&2
    SOURCES_FAILED+=("${SOURCE_NAME}")
    continue
  fi

  DIMENSIONS_JSON="$(select_dimension_ids "${DATA_CUBE_DOC}" "${CONFIG_DIMENSIONS_JSON}" "${MAX_DIMENSIONS}")"

  if [[ "$(jq 'length' <<<"${DIMENSIONS_JSON}")" -eq 0 ]]; then
    echo "Source '${SOURCE_NAME}' has no configured dimensions to fetch." >&2
    SOURCES_FAILED+=("${SOURCE_NAME}")
    continue
  fi

  MEASURE_RESOLUTION_JSON="$(resolve_measure_ids "${DATA_CUBE_DOC}" "${PRIMARY_MEASURE_CFG}" "${SORT_MEASURE_CFG}")"
  PRIMARY_MEASURE="$(jq -r '.primary // empty' <<<"${MEASURE_RESOLUTION_JSON}")"
  SORT_MEASURE="$(jq -r '.sort // empty' <<<"${MEASURE_RESOLUTION_JSON}")"
  PRIMARY_MEASURE_TITLE="$(jq -r '.primary_title // empty' <<<"${MEASURE_RESOLUTION_JSON}")"
  SORT_MEASURE_TITLE="$(jq -r '.sort_title // empty' <<<"${MEASURE_RESOLUTION_JSON}")"
  PRIMARY_FROM_CONFIG="$(jq -r '.primary_from_config' <<<"${MEASURE_RESOLUTION_JSON}")"
  SORT_FROM_CONFIG="$(jq -r '.sort_from_config' <<<"${MEASURE_RESOLUTION_JSON}")"

  if [[ -z "${PRIMARY_MEASURE}" ]]; then
    echo "Source '${SOURCE_NAME}' failed to resolve primary measure from /app-settings/load." >&2
    SOURCES_FAILED+=("${SOURCE_NAME}")
    continue
  fi

  if [[ -z "${SORT_MEASURE}" ]]; then
    SORT_MEASURE="${PRIMARY_MEASURE}"
  fi

  if [[ -z "${PRIMARY_MEASURE_TITLE}" ]]; then
    PRIMARY_MEASURE_TITLE="${PRIMARY_MEASURE}"
  fi
  if [[ -z "${SORT_MEASURE_TITLE}" ]]; then
    SORT_MEASURE_TITLE="${SORT_MEASURE}"
  fi

  if [[ "${PRIMARY_FROM_CONFIG}" != "true" ]]; then
    echo "Source '${SOURCE_NAME}' primary_measure '${PRIMARY_MEASURE_CFG}' unresolved, fallback to '${PRIMARY_MEASURE}' via /app-settings/load." >&2
  fi
  if [[ -n "${SORT_MEASURE_CFG}" && "${SORT_FROM_CONFIG}" != "true" ]]; then
    echo "Source '${SOURCE_NAME}' sort_measure '${SORT_MEASURE_CFG}' unresolved, fallback to '${SORT_MEASURE}' via /app-settings/load." >&2
  fi

  TOTAL_PAYLOAD="$(jq -nc \
    --arg view_name "${VIEW_NAME}" \
    --arg view_title "${VIEW_TITLE}" \
    --arg data_cube "${DATA_CUBE}" \
    --arg timezone "${SOURCE_TZ}" \
    --arg measure "${PRIMARY_MEASURE}" \
    --argjson clauses "${FILTER_CLAUSES_JSON}" '
      {
        viewName: $view_name,
        viewTitle: $view_title,
        feature: "data-cube-two-facet",
        dataCube: $data_cube,
        timezone: $timezone,
        queryDefinition: {
          splits: [],
          measures: [{measure: $measure}],
          pageFilter: {clauses: $clauses},
          commonFilter: {clauses: $clauses}
        },
        queryType: "groupBy"
      }
    ')"

  if ! post_json_with_retry "/pivot/facet?q=groupBy" "${TOTAL_PAYLOAD}" "${TOTAL_GROUPBY_FILE}"; then
    echo "Warning: source '${SOURCE_NAME}' failed total groupBy fetch." >&2
    echo '{"result":{"attributes":[],"data":[]}}' > "${TOTAL_GROUPBY_FILE}"
  fi

  DIMENSIONS=()
  while IFS= read -r dimension_id; do
    [[ -n "${dimension_id}" ]] || continue
    DIMENSIONS+=("${dimension_id}")
  done < <(jq -r '.[]' <<<"${DIMENSIONS_JSON}")
  echo "Source '${SOURCE_NAME}' selected dimensions: ${DIMENSIONS[*]:-none}" >&2
  declare -a DIM_SUCCEEDED=()
  declare -a DIM_FAILED=()

  for dimension_id in "${DIMENSIONS[@]}"; do
    AXIS_FILE="${SOURCE_RAW_DIR}/${TARGET_DATE}_axis_${dimension_id}.json"
    GROUPBY_FILE="${SOURCE_RAW_DIR}/${TARGET_DATE}_groupby_${dimension_id}.json"

    AXIS_PAYLOAD="$(jq -nc \
      --arg view_name "${VIEW_NAME}" \
      --arg view_title "${VIEW_TITLE}" \
      --arg data_cube "${DATA_CUBE}" \
      --arg timezone "${SOURCE_TZ}" \
      --arg dimension "$dimension_id" \
      --arg sort_measure "${SORT_MEASURE}" \
      --argjson clauses "${FILTER_CLAUSES_JSON}" \
      --argjson max_rows "${MAX_ROWS}" '
        {
          viewName: $view_name,
          viewTitle: $view_title,
          feature: "data-cube-two-axis",
          dataCube: $data_cube,
          timezone: $timezone,
          splitCombine: {
            dimension: $dimension,
            sortType: "measure",
            sortMeasure: $sort_measure,
            direction: "descending",
            limit: 1000
          },
          filter: {clauses: $clauses},
          maxRows: $max_rows
        }
      ')"

    if ! post_json_with_retry "/pivot/facet/axis?axis=${dimension_id}" "${AXIS_PAYLOAD}" "${AXIS_FILE}"; then
      DIM_FAILED+=("${dimension_id}")
      continue
    fi

    AXIS_VALUES_JSON="$(jq -c '.result.elements // []' "${AXIS_FILE}")"

    if [[ "$(jq 'length' <<<"${AXIS_VALUES_JSON}")" -eq 0 ]]; then
      echo '{"result":{"attributes":[],"data":[]}}' > "${GROUPBY_FILE}"
      DIM_SUCCEEDED+=("${dimension_id}")
      continue
    fi

    COMMON_CLAUSES_JSON="$(jq -nc \
      --argjson base "${FILTER_CLAUSES_JSON}" \
      --arg dimension "${dimension_id}" \
      --argjson values "${AXIS_VALUES_JSON}" '
        $base + [
          {
            dimension: $dimension,
            action: "overlap",
            values: {
              setType: "STRING",
              elements: $values
            }
          }
        ]
      ')"

    GROUPBY_PAYLOAD="$(jq -nc \
      --arg view_name "${VIEW_NAME}" \
      --arg view_title "${VIEW_TITLE}" \
      --arg data_cube "${DATA_CUBE}" \
      --arg timezone "${SOURCE_TZ}" \
      --arg dimension "${dimension_id}" \
      --arg measure "${PRIMARY_MEASURE}" \
      --argjson page_clauses "${FILTER_CLAUSES_JSON}" \
      --argjson common_clauses "${COMMON_CLAUSES_JSON}" '
        {
          viewName: $view_name,
          viewTitle: $view_title,
          feature: "data-cube-two-facet",
          dataCube: $data_cube,
          timezone: $timezone,
          queryDefinition: {
            splits: [
              {
                dimension: $dimension,
                sortType: "measure",
                direction: "ascending"
              }
            ],
            measures: [{measure: $measure}],
            pageFilter: {clauses: $page_clauses},
            commonFilter: {clauses: $common_clauses}
          },
          queryType: "groupBy"
        }
      ')"

    if post_json_with_retry "/pivot/facet?q=groupBy" "${GROUPBY_PAYLOAD}" "${GROUPBY_FILE}"; then
      DIM_SUCCEEDED+=("${dimension_id}")
    else
      DIM_FAILED+=("${dimension_id}")
    fi
  done

  NORM_FILE="${NORM_BASE}/${SOURCE_NAME}_${TARGET_DATE}.json"
  NORM_TMP="${NORM_BASE}/${SOURCE_NAME}_${TARGET_DATE}.ndjson"
  : > "${NORM_TMP}"

  for dimension_id in "${DIM_SUCCEEDED[@]}"; do
    GROUPBY_FILE="${SOURCE_RAW_DIR}/${TARGET_DATE}_groupby_${dimension_id}.json"
    if [[ ! -f "${GROUPBY_FILE}" ]]; then
      continue
    fi

    DIMENSION_TITLE="$(jq -r --arg d "${dimension_id}" '.dimensions[]? | select(.name == $d) | .title // empty' <<<"${DATA_CUBE_DOC}" | head -n1)"
    if [[ -z "${DIMENSION_TITLE}" ]]; then
      DIMENSION_TITLE="${dimension_id}"
    fi

    jq -c \
      --arg date "${TARGET_DATE}" \
      --arg source "${SOURCE_NAME}" \
      --arg account_id "${ACCOUNT_ID}" \
      --arg timezone "${SOURCE_TZ}" \
      --arg start_utc "${START_UTC}" \
      --arg end_utc "${END_UTC}" \
      --arg data_cube "${DATA_CUBE}" \
      --arg dimension_id "${dimension_id}" \
      --arg dimension_title "${DIMENSION_TITLE}" \
      --arg metric_id "${PRIMARY_MEASURE}" \
      --arg metric_title "${PRIMARY_MEASURE_TITLE}" '
        (.result.data // [])[]
        | {
            date: $date,
            source: $source,
            account_id: $account_id,
            timezone: $timezone,
            window_start_utc: $start_utc,
            window_end_utc: $end_utc,
            data_cube: $data_cube,
            dimension_id: $dimension_id,
            dimension_title: $dimension_title,
            dimension_value: (.[$dimension_id] // null),
            metric_id: $metric_id,
            metric_title: $metric_title,
            value: (.[$metric_id] // null),
            value_num: (.[$metric_id] | tonumber? // null)
          }
      ' "${GROUPBY_FILE}" >> "${NORM_TMP}"
  done

  if [[ -s "${NORM_TMP}" ]]; then
    jq -s '.' "${NORM_TMP}" > "${NORM_FILE}"
  else
    echo '[]' > "${NORM_FILE}"
  fi
  rm -f "${NORM_TMP}"

  ROW_COUNT="$(jq 'length' "${NORM_FILE}")"
  TOTAL_ROWS_ALL=$((TOTAL_ROWS_ALL + ROW_COUNT))

  SOURCE_LOG_FILE="${SOURCE_LOG_DIR}/ingestion-${TARGET_DATE}.md"
  {
    echo "# BI Pivot Ingestion Summary (${TARGET_DATE})"
    echo
    echo "- run_id: ${RUN_ID}"
    echo "- source: ${SOURCE_NAME}"
    echo "- account_id: ${ACCOUNT_ID}"
    echo "- timezone: ${SOURCE_TZ}"
    echo "- window_start_utc: ${START_UTC}"
    echo "- window_end_utc: ${END_UTC}"
    echo "- base_url: ${BASE_URL}"
    echo "- config_file: ${CONFIG_FILE}"
    echo "- primary_measure_configured: ${PRIMARY_MEASURE_CFG:-none}"
    echo "- primary_measure_resolved: ${PRIMARY_MEASURE} (${PRIMARY_MEASURE_TITLE})"
    echo "- sort_measure_configured: ${SORT_MEASURE_CFG:-none}"
    echo "- sort_measure_resolved: ${SORT_MEASURE} (${SORT_MEASURE_TITLE})"
    echo "- dimensions_selected: ${DIMENSIONS[*]:-none}"
    echo "- dimensions_succeeded: ${DIM_SUCCEEDED[*]:-none}"
    echo "- dimensions_failed: ${DIM_FAILED[*]:-none}"
    echo "- normalized_file: ${NORM_FILE}"
    echo "- normalized_rows: ${ROW_COUNT}"
  } > "${SOURCE_LOG_FILE}"

  if [[ ${#DIM_SUCCEEDED[@]} -gt 0 ]]; then
    SOURCES_SUCCEEDED+=("${SOURCE_NAME}")
  else
    SOURCES_FAILED+=("${SOURCE_NAME}")
  fi

done

RUN_SUMMARY_FILE="${LOG_BASE}/ingestion-summary.md"
{
  echo "# BI Fetch Run Summary"
  echo
  echo "- run_id: ${RUN_ID}"
  echo "- config_file: ${CONFIG_FILE}"
  echo "- target_date: ${LAST_TARGET_DATE:-unknown}"
  echo "- sources_succeeded: ${SOURCES_SUCCEEDED[*]:-none}"
  echo "- sources_failed: ${SOURCES_FAILED[*]:-none}"
  echo "- total_rows_written: ${TOTAL_ROWS_ALL}"
  echo "- run_base: ${RUN_BASE}"
} > "${RUN_SUMMARY_FILE}"

echo "Target date: ${LAST_TARGET_DATE:-unknown}"
if [[ ${#SOURCES_SUCCEEDED[@]} -gt 0 ]]; then
  echo "Sources succeeded: ${SOURCES_SUCCEEDED[*]}"
else
  echo "Sources succeeded: none"
fi
if [[ ${#SOURCES_FAILED[@]} -gt 0 ]]; then
  echo "Sources failed: ${SOURCES_FAILED[*]}"
else
  echo "Sources failed: none"
fi
echo "Output file paths:"
echo "- run_base: ${RUN_BASE}"
echo "- raw: ${RAW_BASE}"
echo "- normalized: ${NORM_BASE}"
echo "- log: ${LOG_BASE}"
echo "Row count written: ${TOTAL_ROWS_ALL}"

if [[ ${#SOURCES_SUCCEEDED[@]} -eq 0 ]]; then
  exit 1
fi
