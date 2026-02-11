#!/usr/bin/env bash
set -euo pipefail

# -----------------------
# Config (override via env)
# -----------------------
STATE_MACHINE_ARN="${STATE_MACHINE_ARN:-arn:aws:states:us-east-1:945919380353:stateMachine:hazard-risk-agent-controller}"
RUN_ID="${RUN_ID:-phase5-smoke-003}"
MODE="${MODE:-manual}"
SLEEP_SECS="${SLEEP_SECS:-10}"
MAX_HISTORY="${MAX_HISTORY:-1000}"

# If 1, pass a minimal catalog config so CatalogAgent doesn't expect noaa_events_raw etc.
SMOKE_MINIMAL="${SMOKE_MINIMAL:-1}"

EXEC_NAME="${RUN_ID}-$(date +%s)"

# -----------------------
# Input JSON
# -----------------------
if [[ "$SMOKE_MINIMAL" == "1" ]]; then
  INPUT_JSON="$(cat <<JSON
{
  "run_id": "${RUN_ID}",
  "mode": "${MODE}",
  "catalog": {
    "crawlers": ["bronze-noaa-details","bronze-noaa-fatalities","bronze-noaa-locations"],
    "ensure_tables": [
      {"database":"bronze_hazard_raw","table":"details"},
      {"database":"bronze_hazard_raw","table":"fatalities"},
      {"database":"bronze_hazard_raw","table":"locations"}
    ]
  }
}
JSON
)"
else
  INPUT_JSON="$(cat <<JSON
{
  "run_id": "${RUN_ID}",
  "mode": "${MODE}"
}
JSON
)"
fi

echo ""
echo "Starting Step Functions execution..."

START_OUT="$(aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --name "$EXEC_NAME" \
  --input "$INPUT_JSON" \
  --output json)"

EXEC_ARN="$(echo "$START_OUT" | python3 -c 'import sys,json; print(json.load(sys.stdin)["executionArn"])')"

echo "Execution name : $EXEC_NAME"
echo "Execution ARN  : $EXEC_ARN"
echo ""

# -----------------------
# Helpers
# -----------------------
describe_status() {
  aws stepfunctions describe-execution \
    --execution-arn "$EXEC_ARN" \
    --query 'status' \
    --output text 2>/dev/null || true
}

get_last_state_entered() {
  # Get the most recent "*StateEntered" event name (TaskStateEntered/PassStateEntered/etc.)
  aws stepfunctions get-execution-history \
    --execution-arn "$EXEC_ARN" \
    --max-results "$MAX_HISTORY" \
    --output json \
    --query 'events[?ends_with(type, `StateEntered`)] | [-1].stateEnteredEventDetails.name' \
    2>/dev/null || true
}

get_current_task_resource() {
  # Most recent TaskScheduled details (Lambda invoke etc.)
  aws stepfunctions get-execution-history \
    --execution-arn "$EXEC_ARN" \
    --max-results "$MAX_HISTORY" \
    --output json \
    --query 'events[?type==`TaskScheduled`] | [-1].taskScheduledEventDetails.parameters' \
    2>/dev/null || true
}

get_latest_failure_event() {
  # Pull the most informative recent failure-ish event and return it as JSON
  # Preference order: TaskFailed, LambdaFunctionFailed, FailStateEntered, ExecutionFailed
  aws stepfunctions get-execution-history \
    --execution-arn "$EXEC_ARN" \
    --max-results "$MAX_HISTORY" \
    --output json \
    --query 'events[?type==`TaskFailed` || type==`LambdaFunctionFailed` || type==`FailStateEntered` || type==`ExecutionFailed`] | [-1]' \
    2>/dev/null || true
}

pretty_failure() {
  # Reads single event JSON from stdin and prints a compact summary
  python3 - <<'PY'
import sys,json
s=sys.stdin.read().strip()
if not s or s == "None":
  print("")
  raise SystemExit(0)
try:
  e=json.loads(s)
except Exception:
  # Not JSON; just print raw
  print(s)
  raise SystemExit(0)

typ=e.get("type")
ts=e.get("timestamp")
out={"type":typ,"time":ts}

if typ=="TaskFailed":
  d=e.get("taskFailedEventDetails") or {}
  out["state"]=e.get("stateName")
  out["error"]=d.get("error")
  out["cause"]=d.get("cause")
elif typ=="LambdaFunctionFailed":
  d=e.get("lambdaFunctionFailedEventDetails") or {}
  out["error"]=d.get("error")
  out["cause"]=d.get("cause")
elif typ=="FailStateEntered":
  d=e.get("stateEnteredEventDetails") or {}
  out["state"]=d.get("name")
  out["input"]=d.get("input")
elif typ=="ExecutionFailed":
  d=e.get("executionFailedEventDetails") or {}
  out["details"]=d

print(json.dumps(out, default=str))
PY
}

# -----------------------
# Poll loop
# -----------------------
LAST_PRINTED_STATE=""
LAST_PRINTED_TASK=""
LAST_PRINTED_FAIL=""

while true; do
  STATUS="$(describe_status)"
  TS="$(date +"%Y-%m-%d %H:%M:%S")"

  STATE="$(get_last_state_entered)"
  # AWS CLI returns "None" sometimes when query yields no value
  if [[ -z "${STATE:-}" || "$STATE" == "None" ]]; then
    STATE="<not yet available>"
  fi

  echo "[$TS] status=$STATUS  last_state=$STATE"

  TASK_PARAMS="$(get_current_task_resource)"
  if [[ -n "${TASK_PARAMS:-}" && "$TASK_PARAMS" != "None" && "$TASK_PARAMS" != "$LAST_PRINTED_TASK" ]]; then
    # Attempt to extract FunctionName if present
    FN="$(echo "$TASK_PARAMS" | python3 -c 'import sys,re; s=sys.stdin.read(); m=re.search(r"\"FunctionName\"\s*:\s*\"([^\"]+)\"", s); print(m.group(1) if m else "")' 2>/dev/null || true)"
    if [[ -n "${FN:-}" ]]; then
      echo "[$TS] current_lambda=$FN"
    else
      echo "[$TS] current_task_params=$(echo "$TASK_PARAMS" | head -c 180)"
    fi
    LAST_PRINTED_TASK="$TASK_PARAMS"
  fi

  if [[ "$STATUS" == "FAILED" || "$STATUS" == "TIMED_OUT" || "$STATUS" == "ABORTED" ]]; then
    FAIL_EVT="$(get_latest_failure_event || true)"
    FAIL_PRETTY="$(echo "$FAIL_EVT" | pretty_failure || true)"

    # Fallback: if your helper didn't find anything, query Step Functions history directly
    if [[ -z "${FAIL_PRETTY//[[:space:]]/}" ]]; then
      FAIL_PRETTY="$(
        aws stepfunctions get-execution-history \
          --execution-arn "$EXEC_ARN" \
          --max-results 1000 \
          --reverse-order \
          --output json \
          --query 'events[?type==`TaskFailed` || type==`LambdaFunctionFailed` || type==`ExecutionFailed` || type==`FailStateEntered`].[type, id, previousEventId, taskFailedEventDetails.error, taskFailedEventDetails.cause, lambdaFunctionFailedEventDetails.error, lambdaFunctionFailedEventDetails.cause, executionFailedEventDetails.error, executionFailedEventDetails.cause, stateEnteredEventDetails.name]' \
        2>/dev/null
      )"
    fi

    echo ""
    echo "Execution finished with status: $STATUS"
    echo "Execution ARN: $EXEC_ARN"
    echo "Last state: $STATE"
    echo ""
    echo "Most recent failure event:"
    echo "${FAIL_PRETTY:-<none>}"

    # IMPORTANT: failure should be a non-zero exit code
    exit 1
  fi


  if [[ "$STATUS" == "SUCCEEDED" ]]; then
    echo ""
    echo "Execution finished with status: SUCCEEDED"
    echo "Execution ARN: $EXEC_ARN"
    echo "Last state: $STATE"
    exit 0
  fi

  sleep "$SLEEP_SECS"
done

