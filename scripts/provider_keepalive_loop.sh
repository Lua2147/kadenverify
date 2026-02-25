#!/usr/bin/env bash
set -euo pipefail

RUN="${RUN:-/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211}"
APP="${APP:-/opt/mundi-princeps/apps/email-verifier}"
API_KEY="${API_KEY:-kadenwood_verify_2026}"

REVERIFY_LOG="${REVERIFY_LOG:-$RUN/provider_reverify.log}"
QUEUE_LOG="${QUEUE_LOG:-$RUN/queue_after_reverify.log}"
KEEP_LOG="${KEEP_LOG:-$RUN/provider_keepalive.log}"

# Throughput tuning
BATCH_SIZE="${BATCH_SIZE:-750}"
CONCURRENCY="${CONCURRENCY:-96}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-0}"

# Reverify stop tuning
GAIN_STOP_ABS="${GAIN_STOP_ABS:-40}"
GAIN_STOP_RATE="${GAIN_STOP_RATE:-0.00015}"
GAIN_STOP_STREAK="${GAIN_STOP_STREAK:-2}"
MIN_PENDING_FOR_STOP="${MIN_PENDING_FOR_STOP:-50000}"

# Guardrail: block suspicious state shrink before promotion
MIN_STATE_RATIO="${MIN_STATE_RATIO:-0.80}"
MIN_STATE_DROP_ROWS="${MIN_STATE_DROP_ROWS:-5000}"

PROVIDER_DIR="$RUN/provider_loop"
FORENSICS_DIR="$PROVIDER_DIR/reverify_guardrail_forensics"

STATE_FILE="$PROVIDER_DIR/provider_reverify_state.csv"
USABLE_FILE="$PROVIDER_DIR/provider_reverify_additional_usable.csv"
SUMMARY_FILE="$PROVIDER_DIR/provider_reverify_summary.txt"
QA_FILE="$PROVIDER_DIR/provider_reverify_qa.json"

STATE_NEXT="$PROVIDER_DIR/provider_reverify_state.next.csv"
USABLE_NEXT="$PROVIDER_DIR/provider_reverify_additional_usable.next.csv"
SUMMARY_NEXT="$PROVIDER_DIR/provider_reverify_summary.next.txt"
QA_NEXT="$PROVIDER_DIR/provider_reverify_qa.next.json"

STATE_LAST_GOOD="$PROVIDER_DIR/provider_reverify_state.last_good.csv"
USABLE_LAST_GOOD="$PROVIDER_DIR/provider_reverify_additional_usable.last_good.csv"
SUMMARY_LAST_GOOD="$PROVIDER_DIR/provider_reverify_summary.last_good.txt"
QA_LAST_GOOD="$PROVIDER_DIR/provider_reverify_qa.last_good.json"

utc_now() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

log_keep() {
  echo "[keeper] $*" >> "$KEEP_LOG"
}

csv_rows() {
  local file="$1"
  if [[ ! -s "$file" ]]; then
    echo 0
    return 0
  fi

  python3 - "$file" <<'PY'
import csv
import sys

path = sys.argv[1]
n = 0
with open(path, "r", encoding="utf-8-sig", newline="") as f:
    r = csv.reader(f)
    next(r, None)
    for _ in r:
        n += 1
print(n)
PY
}

should_block_state_promotion() {
  local prev_rows="$1"
  local next_rows="$2"
  local min_ratio="$3"
  local min_drop_rows="$4"

  python3 - "$prev_rows" "$next_rows" "$min_ratio" "$min_drop_rows" <<'PY'
import sys

prev_rows = int(sys.argv[1])
next_rows = int(sys.argv[2])
min_ratio = float(sys.argv[3])
min_drop_rows = int(sys.argv[4])

if prev_rows <= 0:
    print(0)
    raise SystemExit(0)

if next_rows <= 0:
    print(1)
    raise SystemExit(0)

if next_rows >= prev_rows:
    print(0)
    raise SystemExit(0)

drop = prev_rows - next_rows
ratio = next_rows / prev_rows
print(1 if (drop >= min_drop_rows and ratio < min_ratio) else 0)
PY
}

snapshot_last_good() {
  local src="$1"
  local dst="$2"
  if [[ -f "$src" ]]; then
    ln -f "$src" "$dst"
  fi
}

archive_next_files() {
  local tag="$1"
  mkdir -p "$FORENSICS_DIR"

  [[ -f "$STATE_NEXT" ]] && mv -f "$STATE_NEXT" "$FORENSICS_DIR/provider_reverify_state.$tag.csv"
  [[ -f "$USABLE_NEXT" ]] && mv -f "$USABLE_NEXT" "$FORENSICS_DIR/provider_reverify_additional_usable.$tag.csv"
  [[ -f "$SUMMARY_NEXT" ]] && mv -f "$SUMMARY_NEXT" "$FORENSICS_DIR/provider_reverify_summary.$tag.txt"
  [[ -f "$QA_NEXT" ]] && mv -f "$QA_NEXT" "$FORENSICS_DIR/provider_reverify_qa.$tag.json"
}

promote_next_files() {
  mv -f "$STATE_NEXT" "$STATE_FILE"
  mv -f "$USABLE_NEXT" "$USABLE_FILE"
  mv -f "$SUMMARY_NEXT" "$SUMMARY_FILE"
  mv -f "$QA_NEXT" "$QA_FILE"
}

mkdir -p "$(dirname "$KEEP_LOG")" "$PROVIDER_DIR" "$FORENSICS_DIR"

while true; do
  echo "0" > "$RUN/queue_after_reverify.pid"
  echo "0" > "$RUN/run_provider_round2.pid"
  log_keep "cycle_start $(utc_now)"

  rm -f "$STATE_NEXT" "$USABLE_NEXT" "$SUMMARY_NEXT" "$QA_NEXT"

  PREV_STATE_ROWS="$(csv_rows "$STATE_FILE")"
  PREV_USABLE_ROWS="$(csv_rows "$USABLE_FILE")"

  cd "$APP"
  .venv/bin/python -u -m waterfall_pipeline.reverify_loop \
    "$RUN/provider_loop/provider_candidates_verified.csv" \
    "$RUN/waterfall_unknown_undeliverable.csv" \
    "$STATE_NEXT" \
    "$USABLE_NEXT" \
    "$SUMMARY_NEXT" \
    "$API_KEY" \
    --api-url http://127.0.0.1:8025 \
    --batch-size "$BATCH_SIZE" \
    --concurrency "$CONCURRENCY" \
    --max-iters 6 \
    --cooldown-seconds "$COOLDOWN_SECONDS" \
    --gain-stop-abs "$GAIN_STOP_ABS" \
    --gain-stop-rate "$GAIN_STOP_RATE" \
    --gain-stop-streak "$GAIN_STOP_STREAK" \
    --min-pending-for-stop "$MIN_PENDING_FOR_STOP" \
    --qa-report "$QA_NEXT" \
    >> "$REVERIFY_LOG" 2>&1 &

  RPID=$!
  echo "$RPID" > "$RUN/provider_reverify.pid"
  echo "$RPID" > "$RUN/run_reverify.pid"
  log_keep "reverify_pid=$RPID prev_state_rows=$PREV_STATE_ROWS prev_usable_rows=$PREV_USABLE_ROWS"

  set +e
  wait "$RPID"
  REVERIFY_EXIT="$?"
  set -e

  echo "0" > "$RUN/provider_reverify.pid"
  echo "0" > "$RUN/run_reverify.pid"

  NEXT_STATE_ROWS="$(csv_rows "$STATE_NEXT")"
  NEXT_USABLE_ROWS="$(csv_rows "$USABLE_NEXT")"

  PROMOTE=1
  BLOCK_REASON=""
  if [[ "$REVERIFY_EXIT" -ne 0 ]]; then
    PROMOTE=0
    BLOCK_REASON="reverify_exit_nonzero:$REVERIFY_EXIT"
  elif [[ ! -s "$STATE_NEXT" || ! -s "$USABLE_NEXT" ]]; then
    PROMOTE=0
    BLOCK_REASON="missing_next_outputs"
  else
    BLOCKED="$(should_block_state_promotion "$PREV_STATE_ROWS" "$NEXT_STATE_ROWS" "$MIN_STATE_RATIO" "$MIN_STATE_DROP_ROWS")"
    if [[ "$BLOCKED" == "1" ]]; then
      PROMOTE=0
      BLOCK_REASON="state_shrink_guard prev=$PREV_STATE_ROWS next=$NEXT_STATE_ROWS min_ratio=$MIN_STATE_RATIO min_drop_rows=$MIN_STATE_DROP_ROWS"
    fi
  fi

  if [[ "$PROMOTE" == "1" ]]; then
    snapshot_last_good "$STATE_FILE" "$STATE_LAST_GOOD"
    snapshot_last_good "$USABLE_FILE" "$USABLE_LAST_GOOD"
    snapshot_last_good "$SUMMARY_FILE" "$SUMMARY_LAST_GOOD"
    snapshot_last_good "$QA_FILE" "$QA_LAST_GOOD"

    promote_next_files
    log_keep "promoted state_rows=$PREV_STATE_ROWS->$NEXT_STATE_ROWS usable_rows=$PREV_USABLE_ROWS->$NEXT_USABLE_ROWS"
  else
    TAG="$(date -u +%Y%m%dT%H%M%SZ)"
    archive_next_files "$TAG"
    log_keep "promotion_blocked reason=$BLOCK_REASON state_rows=$PREV_STATE_ROWS->$NEXT_STATE_ROWS usable_rows=$PREV_USABLE_ROWS->$NEXT_USABLE_ROWS"
  fi

  if [[ "$PROMOTE" == "1" ]]; then
    echo "$$" > "$RUN/queue_after_reverify.pid"
    bash "$APP/scripts/waterfall_queue_round2.sh" "$RUN" "$API_KEY" "$QUEUE_LOG" >> "$KEEP_LOG" 2>&1 || true
    echo "0" > "$RUN/queue_after_reverify.pid"
  else
    echo "0" > "$RUN/queue_after_reverify.pid"
    log_keep "queue_round2_skipped reason=promotion_blocked"
  fi

  log_keep "cycle_done $(utc_now)"
  sleep 2
done
