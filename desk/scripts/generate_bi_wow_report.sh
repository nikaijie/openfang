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

  SOURCE_CONFIG_JSON="$(jq -c --arg source "${source_name}" '.sources[] | select(.name == $source)' "${CONFIG_FILE}" | head -n1)"
  if [[ -z "${SOURCE_CONFIG_JSON}" || "${SOURCE_CONFIG_JSON}" == "null" ]]; then
    SOURCE_CONFIG_JSON='{}'
  fi
  SOURCE_ANALYSIS_JSON="$(jq -c '.analysis // {}' <<<"${SOURCE_CONFIG_JSON}")"

  COMPARE_DIMENSION="$(jq -r '.portal.compare_dimension // .portal.dimension_ids[0] // empty' <<<"${SOURCE_CONFIG_JSON}")"
  CAMPAIGN_DIMENSION="$(jq -r '.portal.campaign_dimension // .portal.dimension_ids[1] // empty' <<<"${SOURCE_CONFIG_JSON}")"

  TARGET_DIMENSIONS_JSON="$(jq '[.[].dimension_id | select(. != null) | tostring] | unique' "${TARGET_FILE}")"
  PRIMARY_AVAILABLE_DIMENSION="$(jq -r '
    [.[].dimension_id | select(. != null) | tostring]
    | group_by(.)
    | map({id: .[0], count: length})
    | sort_by(.count)
    | reverse
    | .[0].id // empty
  ' "${TARGET_FILE}")"
  SECONDARY_AVAILABLE_DIMENSION="$(jq -r --arg primary "${PRIMARY_AVAILABLE_DIMENSION}" '
    [.[].dimension_id | select(. != null) | tostring]
    | group_by(.)
    | map({id: .[0], count: length})
    | sort_by(.count)
    | reverse
    | map(select(.id != $primary))
    | .[0].id // empty
  ' "${TARGET_FILE}")"

  if [[ -z "${COMPARE_DIMENSION}" ]]; then
    COMPARE_DIMENSION="${PRIMARY_AVAILABLE_DIMENSION}"
  fi
  COMPARE_ROWS_PRESENT="$(jq --arg dim "${COMPARE_DIMENSION}" '[.[] | select(.dimension_id == $dim)] | length' "${TARGET_FILE}")"
  if [[ -z "${COMPARE_DIMENSION}" || "${COMPARE_ROWS_PRESENT}" -eq 0 ]]; then
    COMPARE_DIMENSION="${PRIMARY_AVAILABLE_DIMENSION}"
  fi

  if [[ -z "${CAMPAIGN_DIMENSION}" ]]; then
    CAMPAIGN_DIMENSION="${SECONDARY_AVAILABLE_DIMENSION}"
  fi
  CAMPAIGN_ROWS_PRESENT="$(jq --arg dim "${CAMPAIGN_DIMENSION}" '[.[] | select(.dimension_id == $dim)] | length' "${TARGET_FILE}")"
  if [[ -z "${CAMPAIGN_DIMENSION}" || "${CAMPAIGN_ROWS_PRESENT}" -eq 0 ]]; then
    CAMPAIGN_DIMENSION="${SECONDARY_AVAILABLE_DIMENSION}"
  fi
  if [[ -z "${CAMPAIGN_DIMENSION}" ]]; then
    CAMPAIGN_DIMENSION="${COMPARE_DIMENSION}"
  fi

  if [[ -z "${COMPARE_DIMENSION}" ]]; then
    echo "Warning: source '${source_name}' has no dimension rows in target file ${TARGET_FILE}" >&2
    MISSING_BASELINE_SOURCES+=("${source_name}")
    continue
  fi

  COMPARE_DIMENSION_TITLE="$(jq -r --arg dim "${COMPARE_DIMENSION}" '[.[] | select(.dimension_id == $dim) | .dimension_title][0] // empty' "${TARGET_FILE}")"
  CAMPAIGN_DIMENSION_TITLE="$(jq -r --arg dim "${CAMPAIGN_DIMENSION}" '[.[] | select(.dimension_id == $dim) | .dimension_title][0] // empty' "${TARGET_FILE}")"
  if [[ -z "${COMPARE_DIMENSION_TITLE}" ]]; then
    COMPARE_DIMENSION_TITLE="${COMPARE_DIMENSION}"
  fi
  if [[ -z "${CAMPAIGN_DIMENSION_TITLE}" ]]; then
    CAMPAIGN_DIMENSION_TITLE="${CAMPAIGN_DIMENSION}"
  fi
  read -r COMPARE_DIMENSION_TITLE_HUMAN CAMPAIGN_DIMENSION_TITLE_HUMAN < <(COMPARE_DIMENSION="${COMPARE_DIMENSION}" CAMPAIGN_DIMENSION="${CAMPAIGN_DIMENSION}" COMPARE_DIMENSION_TITLE="${COMPARE_DIMENSION_TITLE}" CAMPAIGN_DIMENSION_TITLE="${CAMPAIGN_DIMENSION_TITLE}" python3 - <<'PY'
import os
import re

compare_dim = os.environ.get("COMPARE_DIMENSION", "")
campaign_dim = os.environ.get("CAMPAIGN_DIMENSION", "")
compare_title = os.environ.get("COMPARE_DIMENSION_TITLE", "")
campaign_title = os.environ.get("CAMPAIGN_DIMENSION_TITLE", "")

id_to_human = {
    "tad_cha-7ef": "产品类型",
    "country": "地理位置",
    "device_type": "设备类型",
    "platform": "平台",
    "tuser_i-387": "广告账户",
    "tcpg_id-174": "广告活动",
    "tcr_id-94f": "广告文案",
    "placement_id": "广告位",
    "format": "广告位类型",
    "bundle": "流量来源",
}

def humanize(dim_id: str, title: str) -> str:
    if dim_id in id_to_human:
        return id_to_human[dim_id]
    base = (title or dim_id or "").strip()
    base = re.sub(r"\s*ID\s*$", "", base, flags=re.IGNORECASE).strip()
    base = re.sub(r"（\s*ID\s*）$", "", base, flags=re.IGNORECASE).strip()
    base = re.sub(r"\(\s*ID\s*\)$", "", base, flags=re.IGNORECASE).strip()
    return base or dim_id or ""

print(humanize(compare_dim, compare_title), humanize(campaign_dim, campaign_title))
PY
)
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
  TOP_MOVERS_JSON="$(jq '
    map(
      . + {
        dimension_value_display:
          (if (.dimension_value | type) == "string" and (.dimension_value | length) > 16
           then ((.dimension_value[0:8]) + "..." + (.dimension_value[(.dimension_value|length)-6:]))
           else .dimension_value
           end)
      }
    )
  ' <<<"${TOP_MOVERS_JSON}")"

  COMPARE_BUCKETS_JSON="$(jq -n \
    --arg dim "${COMPARE_DIMENSION}" \
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
              baseline: ($baseline[$k] // 0)
            }
          | .delta = (.current - .baseline)
          | .wow = (if .baseline == 0 then null else (.delta / .baseline) end)
        ] as $rows
      | ($rows | map(.delta) | add // 0) as $total_delta
      | ($total_delta | if . < 0 then -. else . end) as $abs_total_delta
      | $rows
      | map(. + {contribution: (if $abs_total_delta == 0 then null else ((.delta | if . < 0 then -. else . end) / $abs_total_delta) end)})
      | sort_by((.delta | if . < 0 then -. else . end))
      | reverse
    ')"
  COMPARE_BUCKETS_JSON="$(jq '
    map(
      . + {
        dimension_value_display:
          (if (.dimension_value | type) == "string" and (.dimension_value | length) > 16
           then ((.dimension_value[0:8]) + "..." + (.dimension_value[(.dimension_value|length)-6:]))
           else .dimension_value
           end)
      }
    )
  ' <<<"${COMPARE_BUCKETS_JSON}")"
  COMPARE_BUCKETS_TOP_JSON="$(jq '.[0:5]' <<<"${COMPARE_BUCKETS_JSON}")"

  OVERALL_WOW_ABS="$(jq -r '.anomaly_thresholds.overall_wow_abs // 0.3' <<<"${SOURCE_ANALYSIS_JSON}")"
  OVERALL_DELTA_ABS="$(jq -r '.anomaly_thresholds.overall_delta_abs // 1000' <<<"${SOURCE_ANALYSIS_JSON}")"
  OVERALL_BASELINE_MIN="$(jq -r '.anomaly_thresholds.overall_baseline_min // 3000' <<<"${SOURCE_ANALYSIS_JSON}")"
  BUCKET_CONTRIB_MIN="$(jq -r '.anomaly_thresholds.bucket_contrib_min // 0.4' <<<"${SOURCE_ANALYSIS_JSON}")"
  BUCKET_WOW_ABS="$(jq -r '.anomaly_thresholds.bucket_wow_abs // 0.6' <<<"${SOURCE_ANALYSIS_JSON}")"
  BUCKET_DELTA_ABS="$(jq -r '.anomaly_thresholds.bucket_delta_abs // 300' <<<"${SOURCE_ANALYSIS_JSON}")"

  ANOMALY_DECISION_JSON="$(python3 - "${CURRENT_TOTAL}" "${BASELINE_TOTAL}" "${WOW_RATIO}" "${OVERALL_WOW_ABS}" "${OVERALL_DELTA_ABS}" "${OVERALL_BASELINE_MIN}" <<'PY'
import json
import sys

current = float(sys.argv[1])
baseline = float(sys.argv[2])
wow_raw = sys.argv[3]
wow_thr = float(sys.argv[4])
delta_thr = float(sys.argv[5])
baseline_min = float(sys.argv[6])

delta = current - baseline
wow = None if wow_raw == "null" else float(wow_raw)
reasons = []

if abs(delta) < delta_thr:
    reasons.append(f"abs(delta)={abs(delta):.6f} < threshold={delta_thr:.6f}")

if baseline < baseline_min:
    reasons.append(f"baseline={baseline:.6f} < threshold={baseline_min:.6f}")

if wow is None:
    reasons.append("wow is null (baseline equals 0)")
elif abs(wow) < wow_thr:
    reasons.append(f"abs(wow)={abs(wow):.6f} < threshold={wow_thr:.6f}")

is_anomaly = abs(delta) >= delta_thr and baseline >= baseline_min and wow is not None and abs(wow) >= wow_thr

if is_anomaly:
    reasons = [
        f"abs(delta)={abs(delta):.6f} >= threshold={delta_thr:.6f}",
        f"baseline={baseline:.6f} >= threshold={baseline_min:.6f}",
        f"abs(wow)={abs(wow):.6f} >= threshold={wow_thr:.6f}",
    ]

print(json.dumps({
    "is_anomaly": is_anomaly,
    "reasons": reasons,
    "thresholds": {
        "overall_wow_abs": wow_thr,
        "overall_delta_abs": delta_thr,
        "overall_baseline_min": baseline_min,
    },
}))
PY
)"

  CURRENT_TIER="$(TARGET_DIMS_JSON="${TARGET_DIMENSIONS_JSON}" SOURCE_ANALYSIS_JSON="${SOURCE_ANALYSIS_JSON}" python3 - <<'PY'
import json
import os

target_dims_raw = os.environ.get("TARGET_DIMS_JSON", "[]")
analysis_raw = os.environ.get("SOURCE_ANALYSIS_JSON", "{}")

try:
    target_dims = set(json.loads(target_dims_raw))
except Exception:
    target_dims = set()

try:
    analysis = json.loads(analysis_raw)
except Exception:
    analysis = {}

tiers = analysis.get("dimension_tiers") or {}
default_tier = (analysis.get("default_tier") or "").upper()
rank = {"L0": 0, "L1": 1, "L2": 2}

best_tier = ""
best_score = -1
for tier_name, dims in tiers.items():
    tier = str(tier_name).upper()
    dim_set = {str(d) for d in (dims or [])}
    score = len(target_dims & dim_set)
    if score > best_score or (score == best_score and rank.get(tier, 99) < rank.get(best_tier, 99)):
        best_tier = tier
        best_score = score

if best_score > 0:
    print(best_tier)
elif default_tier in ("L0", "L1", "L2"):
    print(default_tier)
else:
    print("none")
PY
)"

  L1_TIER_SIZE="$(jq -r '.dimension_tiers.L1 // [] | length' <<<"${SOURCE_ANALYSIS_JSON}")"
  L2_TIER_SIZE="$(jq -r '.dimension_tiers.L2 // [] | length' <<<"${SOURCE_ANALYSIS_JSON}")"
  ANOMALY_FLAG="$(jq -r '.is_anomaly' <<<"${ANOMALY_DECISION_JSON}")"
  NEXT_TIER=""
  if [[ "${ANOMALY_FLAG}" == "true" ]]; then
    case "${CURRENT_TIER}" in
      L0)
        if [[ "${L1_TIER_SIZE}" -gt 0 ]]; then
          NEXT_TIER="L1"
        fi
        ;;
      L1)
        if [[ "${L2_TIER_SIZE}" -gt 0 ]]; then
          NEXT_TIER="L2"
        fi
        ;;
      L2)
        NEXT_TIER=""
        ;;
      *)
        if [[ "${L1_TIER_SIZE}" -gt 0 ]]; then
          NEXT_TIER="L1"
        elif [[ "${L2_TIER_SIZE}" -gt 0 ]]; then
          NEXT_TIER="L2"
        fi
        ;;
    esac
  fi

  DRILLDOWN_BUCKET_VALUES_JSON="$(COMPARE_BUCKETS_JSON="${COMPARE_BUCKETS_JSON}" python3 - "${BUCKET_CONTRIB_MIN}" "${BUCKET_WOW_ABS}" "${BUCKET_DELTA_ABS}" <<'PY'
import json
import os
import sys

bucket_contrib_min = float(sys.argv[1])
bucket_wow_abs = float(sys.argv[2])
bucket_delta_abs = float(sys.argv[3])

try:
    rows = json.loads(os.environ.get("COMPARE_BUCKETS_JSON", "[]"))
except Exception:
    rows = []

selected = []
for row in rows:
    value = str(row.get("dimension_value", ""))
    if not value or value == "null":
        continue
    delta = abs(float(row.get("delta", 0) or 0))
    wow_val = row.get("wow")
    wow_abs = abs(float(wow_val)) if wow_val is not None else None
    contrib = row.get("contribution")
    contrib_val = abs(float(contrib)) if contrib is not None else 0.0

    if contrib_val >= bucket_contrib_min or (wow_abs is not None and wow_abs >= bucket_wow_abs and delta >= bucket_delta_abs):
        if value not in selected:
            selected.append(value)
    if len(selected) >= 3:
        break

print(json.dumps(selected))
PY
)"

  DRILLDOWN_BUCKET_REASONS_JSON="$(COMPARE_BUCKETS_JSON="${COMPARE_BUCKETS_JSON}" DRILLDOWN_BUCKET_VALUES_JSON="${DRILLDOWN_BUCKET_VALUES_JSON}" python3 - "${BUCKET_CONTRIB_MIN}" "${BUCKET_WOW_ABS}" "${BUCKET_DELTA_ABS}" <<'PY'
import json
import os
import sys

bucket_contrib_min = float(sys.argv[1])
bucket_wow_abs = float(sys.argv[2])
bucket_delta_abs = float(sys.argv[3])

try:
    rows = json.loads(os.environ.get("COMPARE_BUCKETS_JSON", "[]"))
except Exception:
    rows = []

try:
    selected_values = json.loads(os.environ.get("DRILLDOWN_BUCKET_VALUES_JSON", "[]"))
except Exception:
    selected_values = []

row_map = {}
for row in rows:
    value = str(row.get("dimension_value", ""))
    if value and value != "null":
        row_map[value] = row

result = []
for value in selected_values:
    key = str(value)
    row = row_map.get(key, {})
    current = float(row.get("current", 0) or 0)
    baseline = float(row.get("baseline", 0) or 0)
    delta = float(row.get("delta", 0) or 0)
    wow_val = row.get("wow")
    wow_num = float(wow_val) if wow_val is not None else None
    contribution = row.get("contribution")
    contribution_num = float(contribution) if contribution is not None else None

    delta_abs = abs(delta)
    wow_abs = abs(wow_num) if wow_num is not None else None
    contribution_abs = abs(contribution_num) if contribution_num is not None else 0.0

    reasons = []
    if contribution_abs >= bucket_contrib_min:
        reasons.append(
            f"bucket_contribution={contribution_abs:.6f} >= threshold={bucket_contrib_min:.6f}"
        )
    if wow_abs is not None and wow_abs >= bucket_wow_abs and delta_abs >= bucket_delta_abs:
        reasons.append(
            f"abs(bucket_wow)={wow_abs:.6f} >= threshold={bucket_wow_abs:.6f} and abs(bucket_delta)={delta_abs:.6f} >= threshold={bucket_delta_abs:.6f}"
        )
    if not reasons:
        reasons.append("selected_by_ranked_bucket_rule")

    result.append(
        {
            "value": key,
            "current": current,
            "baseline": baseline,
            "delta": delta,
            "wow": wow_num,
            "contribution": contribution_num,
            "reasons": reasons,
        }
    )

print(json.dumps(result))
PY
)"

  DRILLDOWN_REQUIRED="false"
  DRILLDOWN_FILTERS_JSON='{}'
  DRILLDOWN_STOP_REASON="anomaly_not_triggered"
  if [[ "${ANOMALY_FLAG}" == "true" && -n "${NEXT_TIER}" ]]; then
    DRILLDOWN_REQUIRED="true"
    DRILLDOWN_STOP_REASON=""
    if [[ "$(jq 'length' <<<"${DRILLDOWN_BUCKET_VALUES_JSON}")" -gt 0 ]]; then
      DRILLDOWN_FILTERS_JSON="$(jq -n --arg dim "${COMPARE_DIMENSION}" --argjson values "${DRILLDOWN_BUCKET_VALUES_JSON}" '{($dim): $values}')"
    fi
  else
    if [[ "${ANOMALY_FLAG}" == "true" && "${CURRENT_TIER}" == "L2" ]]; then
      DRILLDOWN_STOP_REASON="max_tier_reached"
    fi
    DRILLDOWN_BUCKET_VALUES_JSON='[]'
    DRILLDOWN_BUCKET_REASONS_JSON='[]'
  fi

  jq -n \
    --arg source "${source_name}" \
    --arg metric_id "${METRIC_ID}" \
    --arg metric_title "${METRIC_TITLE}" \
    --arg compare_dimension "${COMPARE_DIMENSION}" \
    --arg compare_dimension_title "${COMPARE_DIMENSION_TITLE}" \
    --arg compare_dimension_title_human "${COMPARE_DIMENSION_TITLE_HUMAN}" \
    --arg campaign_dimension "${CAMPAIGN_DIMENSION}" \
    --arg campaign_dimension_title "${CAMPAIGN_DIMENSION_TITLE}" \
    --arg campaign_dimension_title_human "${CAMPAIGN_DIMENSION_TITLE_HUMAN}" \
    --arg target_file "${TARGET_FILE}" \
    --arg baseline_file "${BASELINE_FILE}" \
    --arg current_tier "${CURRENT_TIER}" \
    --arg recommended_next_tier "${NEXT_TIER}" \
    --argjson current_total "${CURRENT_TOTAL}" \
    --argjson baseline_total "${BASELINE_TOTAL}" \
    --argjson delta "${DELTA}" \
    --argjson wow "${WOW_RATIO}" \
    --argjson current_rows "${CURRENT_ROWS}" \
    --argjson baseline_rows "${BASELINE_ROWS}" \
    --argjson top_movers "${TOP_MOVERS_JSON}" \
    --argjson compare_buckets_top "${COMPARE_BUCKETS_TOP_JSON}" \
    --argjson anomaly "${ANOMALY_DECISION_JSON}" \
    --argjson drilldown_required "${DRILLDOWN_REQUIRED}" \
    --argjson drilldown_filters "${DRILLDOWN_FILTERS_JSON}" \
    --argjson drilldown_bucket_values "${DRILLDOWN_BUCKET_VALUES_JSON}" \
    --argjson drilldown_bucket_reasons "${DRILLDOWN_BUCKET_REASONS_JSON}" \
    --arg drilldown_stop_reason "${DRILLDOWN_STOP_REASON}" \
    --argjson bucket_thresholds "$(jq -n --argjson contrib "${BUCKET_CONTRIB_MIN}" --argjson wow_abs "${BUCKET_WOW_ABS}" --argjson delta_abs "${BUCKET_DELTA_ABS}" '{bucket_contrib_min: $contrib, bucket_wow_abs: $wow_abs, bucket_delta_abs: $delta_abs}')" '
    {
      source: $source,
      metric_id: $metric_id,
      metric_title: $metric_title,
      compare_dimension: $compare_dimension,
      compare_dimension_title: $compare_dimension_title,
      compare_dimension_title_human: $compare_dimension_title_human,
      campaign_dimension: $campaign_dimension,
      campaign_dimension_title: $campaign_dimension_title,
      campaign_dimension_title_human: $campaign_dimension_title_human,
      current_tier: $current_tier,
      recommended_next_tier: $recommended_next_tier,
      current_total: $current_total,
      baseline_total: $baseline_total,
      delta: $delta,
      wow: $wow,
      current_rows: $current_rows,
      baseline_rows: $baseline_rows,
      target_file: $target_file,
      baseline_file: $baseline_file,
      top_movers: $top_movers,
      compare_buckets_top: $compare_buckets_top,
      anomaly: ($anomaly + {bucket_thresholds: $bucket_thresholds}),
      drilldown: {
        required: $drilldown_required,
        next_tier: $recommended_next_tier,
        filters: $drilldown_filters,
        bucket_values: $drilldown_bucket_values,
        bucket_reasons: $drilldown_bucket_reasons,
        stop_reason: $drilldown_stop_reason,
        reasons: $anomaly.reasons
      }
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

DRILLDOWN_REQUIRED="$(jq -r '[.[].drilldown.required] | any' <<<"${SOURCE_SUMMARIES_JSON}")"
DRILLDOWN_REQUESTS_JSON="$(jq -c --arg target_date "${TARGET_DATE}" '
  [
    .[]
    | select(.drilldown.required == true)
    | {
        source: .source,
        target_date: $target_date,
        current_tier: .current_tier,
        next_tier: .drilldown.next_tier,
        filters: .drilldown.filters,
        bucket_values: .drilldown.bucket_values,
        bucket_reasons: .drilldown.bucket_reasons,
        reasons: .drilldown.reasons,
        compare_dimension: .compare_dimension,
        compare_dimension_title: .compare_dimension_title,
        compare_dimension_title_human: .compare_dimension_title_human
      }
  ]
' <<<"${SOURCE_SUMMARIES_JSON}")"

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
  --argjson missing_baseline_sources "${MISSING_BASELINE_JSON}" \
  --argjson drilldown "$(jq -n \
    --argjson required "${DRILLDOWN_REQUIRED}" \
    --argjson requests "${DRILLDOWN_REQUESTS_JSON}" \
    '{required: $required, requests: $requests}')" '
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
    missing_baseline_sources: $missing_baseline_sources,
    drilldown: $drilldown
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
    | "- \(.source): 指标=\(.metric_title), 当前=\(.current_total), 基准=\(.baseline_total), 变化量=\(.delta), 周环比=\(.wow), 对比维度=\(.compare_dimension_title_human), 当前层级=\(.current_tier), 异常触发=\(.anomaly.is_anomaly)"
  ' "${REPORT_JSON}"
  echo
  echo "## 异常下钻建议"
  echo
  if [[ "$(jq -r '.drilldown.required' "${REPORT_JSON}")" == "true" ]]; then
    jq -r '
      .drilldown.requests[]
      | "- \(.source): 当前层=\(.current_tier), 建议下钻层=\(.next_tier), 过滤条件=\(.filters | @json), 触发原因=\(.reasons | join(" | "))"
    ' "${REPORT_JSON}"
  else
    echo "- 无（本次未触发下钻）"
  fi
  echo
  echo "## 主要变动项（按活动维度）"
  echo
  jq -r '
    .sources[]
    | . as $source
    | "### \($source.source) (\($source.campaign_dimension_title_human))\n"
      + (
          if ($source.top_movers | length) == 0
          then "- 无"
          else
            ($source.top_movers
              | map("- [\($source.metric_title)] \(.dimension_value_display // .dimension_value): 当前=\(.current), 基准=\(.baseline), 变化量=\(.delta), 周环比=\(.wow)")
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
echo "是否触发下钻: ${DRILLDOWN_REQUIRED}"
echo "下钻请求数: $(jq 'length' <<<"${DRILLDOWN_REQUESTS_JSON}")"
echo "DRILLDOWN_REQUIRED: ${DRILLDOWN_REQUIRED}"
echo "DRILLDOWN_REQUESTS_JSON: ${DRILLDOWN_REQUESTS_JSON}"
echo "关键指标摘要: current_spend=${TOTAL_CURRENT}, baseline_spend=${TOTAL_BASELINE}, wow=${TOTAL_WOW}"
