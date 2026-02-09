# #!/usr/bin/env bash
# set -euo pipefail
# export AWS_PAGER=""

# STATE_MACHINE_ARN="arn:aws:states:us-east-1:945919380353:stateMachine:hazard-risk-agent-controller"
# RUN_ID="phase5-smoke-003"
# MODE="manual"

# EXEC_NAME="${RUN_ID}-$(date +%s)"
# INPUT_JSON="{\"run_id\":\"${RUN_ID}\",\"mode\":\"${MODE}\"}"

# echo ""
# echo "Starting Step Functions execution..."
# START_OUT="$(aws stepfunctions start-execution \
#   --state-machine-arn "$STATE_MACHINE_ARN" \
#   --name "$EXEC_NAME" \
#   --input "$INPUT_JSON" \
#   --output json 2>&1 || true)"

# if ! echo "$START_OUT" | python3 -c 'import sys,json; json.load(sys.stdin)' >/dev/null 2>&1; then
#   echo ""
#   echo "[ERROR] start-execution did not return JSON. Raw output:"
#   echo "$START_OUT"
#   exit 1
# fi

# EXEC_ARN="$(echo "$START_OUT" | python3 -c 'import sys,json; print(json.load(sys.stdin)["executionArn"])')"

# echo "Execution name : $EXEC_NAME"
# echo "Execution ARN  : $EXEC_ARN"
# echo ""

# SLEEP_SECS=10

# get_status() {
#   aws stepfunctions describe-execution \
#     --execution-arn "$EXEC_ARN" \
#     --query 'status' \
#     --output text 2>/dev/null || echo "UNKNOWN"
# }

# get_last_state() {
#   aws stepfunctions get-execution-history \
#     --execution-arn "$EXEC_ARN" \
#     --reverse-order \
#     --max-results 50 \
#     --query 'events[?ends_with(type, `StateEntered`)]|[0].stateEnteredEventDetails.name' \
#     --output text 2>/dev/null || echo "<history unavailable>"
# }

# get_failstate_input() {
#   aws stepfunctions get-execution-history \
#     --execution-arn "$EXEC_ARN" \
#     --reverse-order \
#     --max-results 200 \
#     --query 'events[?type==`FailStateEntered`]|[0].stateEnteredEventDetails.input' \
#     --output text 2>/dev/null || echo ""
# }

# get_last_taskfailed() {
#   aws stepfunctions get-execution-history \
#     --execution-arn "$EXEC_ARN" \
#     --reverse-order \
#     --max-results 200 \
#     --query 'events[?type==`TaskFailed`]|[0].{state:stateName,error:taskFailedEventDetails.error,cause:taskFailedEventDetails.cause,time:timestamp}' \
#     --output json 2>/dev/null || echo ""
# }

# get_last_lambdafailed() {
#   aws stepfunctions get-execution-history \
#     --execution-arn "$EXEC_ARN" \
#     --reverse-order \
#     --max-results 200 \
#     --query 'events[?type==`LambdaFunctionFailed`]|[0].{error:lambdaFunctionFailedEventDetails.error,cause:lambdaFunctionFailedEventDetails.cause,time:timestamp}' \
#     --output json 2>/dev/null || echo ""
# }

# while true; do
#   STATUS="$(get_status)"
#   TS="$(date +"%Y-%m-%d %H:%M:%S")"
#   LAST_STATE="$(get_last_state)"

#   if [[ "$LAST_STATE" == "None" || -z "$LAST_STATE" ]]; then
#     LAST_STATE="<not yet available>"
#   fi

#   echo "[$TS] status=$STATUS  last_state=$LAST_STATE"

#   case "$STATUS" in
#     RUNNING)
#       sleep "$SLEEP_SECS"
#       ;;
#     SUCCEEDED|FAILED|TIMED_OUT|ABORTED)
#       echo ""
#       echo "Execution finished with status: $STATUS"
#       echo "Execution ARN: $EXEC_ARN"
#       echo "Last state: $LAST_STATE"
#       echo ""

#       FS_INPUT="$(get_failstate_input)"
#       if [[ -n "${FS_INPUT:-}" && "$FS_INPUT" != "None" ]]; then
#         echo "FailStateEntered input (most useful):"
#         echo "$FS_INPUT"
#         echo ""
#       fi

#       LAST_TASKFAILED="$(get_last_taskfailed)"
#       if [[ -n "${LAST_TASKFAILED:-}" && "$LAST_TASKFAILED" != "null" && "$LAST_TASKFAILED" != "None" ]]; then
#         echo "Most recent TaskFailed (if any):"
#         echo "$LAST_TASKFAILED"
#         echo ""
#       fi

#       LAST_LAMBDAFAILED="$(get_last_lambdafailed)"
#       if [[ -n "${LAST_LAMBDAFAILED:-}" && "$LAST_LAMBDAFAILED" != "null" && "$LAST_LAMBDAFAILED" != "None" ]]; then
#         echo "Most recent LambdaFunctionFailed (if any):"
#         echo "$LAST_LAMBDAFAILED"
#         echo ""
#       fi

#       exit 0
#       ;;
#     *)
#       sleep "$SLEEP_SECS"
#       ;;
#   esac
# done

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
    FAIL_EVT="$(get_latest_failure_event)"
    FAIL_PRETTY="$(echo "$FAIL_EVT" | pretty_failure || true)"
    echo ""
    echo "Execution finished with status: $STATUS"
    echo "Execution ARN: $EXEC_ARN"
    echo "Last state: $STATE"
    echo ""
    echo "Most recent failure event:"
    echo "${FAIL_PRETTY:-<none>}"
    exit 0
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

