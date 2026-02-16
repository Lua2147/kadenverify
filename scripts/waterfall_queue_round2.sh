#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="${1:?usage: waterfall_queue_round2.sh <run_dir> <kadenverify_api_key> [log_file]}"
API_KEY="${2:?usage: waterfall_queue_round2.sh <run_dir> <kadenverify_api_key> [log_file]}"
LOG_FILE="${3:-$RUN_DIR/queue_after_reverify.log}"

APP_DIR="/opt/mundi-princeps/apps/email-verifier"
mkdir -p "$(dirname "$LOG_FILE")"

{
  echo "[queue] start $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  cd "$APP_DIR"
  .venv/bin/python -m waterfall_pipeline.runner queue-round2 \
    --run-dir "$RUN_DIR" \
    --kadenverify-api-key "$API_KEY" \
    --wait-reverify \
    --poll-seconds 60
  echo "[queue] done $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >> "$LOG_FILE" 2>&1
