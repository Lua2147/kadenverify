#!/usr/bin/env bash
set -euo pipefail

RUN="${RUN:-/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211}"
APP="${APP:-/opt/mundi-princeps/apps/email-verifier}"
SHEET_ID="${SHEET_ID:-1I7B2Dg4XcZRHrJ_zowH1gxtFXYIm2_LtQXLgSA4yXVw}"
TEMPLATE_TAB="${TEMPLATE_TAB:-✅quick_wins_net_new_valid_catchall_2026-02-18}"
TARGET_TAB="${TARGET_TAB:-quick_wins_net_new_valid_catchall_2026-02-23}"
TOKEN_FILE="${TOKEN_FILE:-/opt/mundi-princeps/config/token.json}"
CHECK_EVERY_SECONDS="${CHECK_EVERY_SECONDS:-900}"
CHUNK="${CHUNK:-1000}"

SENDABLE_CSV="$RUN/provider_loop/provider_reverify_additional_usable.csv"
MASTER_CSV="$RUN/provider_loop/provider_reverify_additional_usable.csv"
LOG_FILE="${LOG_FILE:-$RUN/reverify_sheet_sync.log}"

mkdir -p "$(dirname "$LOG_FILE")"

log() {
  echo "[sync] $(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "$LOG_FILE"
}

while true; do
  if [[ ! -s "$SENDABLE_CSV" ]]; then
    log "sendable_missing path=$SENDABLE_CSV"
    sleep "$CHECK_EVERY_SECONDS"
    continue
  fi

  set +e
  CHECK_OUT="$("$APP/.venv/bin/python" "$APP/scripts/check_needed_net_new.py" \
    --sheet-id "$SHEET_ID" \
    --sendable-csv "$SENDABLE_CSV" \
    --token-file "$TOKEN_FILE" 2>&1)"
  CHECK_RC=$?
  set -e

  echo "$CHECK_OUT" >> "$LOG_FILE"

  if [[ "$CHECK_RC" -ne 0 ]]; then
    log "check_failed rc=$CHECK_RC"
    sleep "$CHECK_EVERY_SECONDS"
    continue
  fi

  NEEDED="$(printf '%s\n' "$CHECK_OUT" | awk -F= '/^needed_net_new=/{print $2}' | tail -n 1)"
  if [[ -z "${NEEDED:-}" ]]; then
    log "parse_needed_failed"
    sleep "$CHECK_EVERY_SECONDS"
    continue
  fi

  if [[ "$NEEDED" -gt 0 ]]; then
    log "needed_net_new=$NEEDED append_start"
    set +e
    APPEND_OUT="$("$APP/.venv/bin/python" "$APP/scripts/add_feb19_net_new.py" \
      --sheet-id "$SHEET_ID" \
      --template-tab "$TEMPLATE_TAB" \
      --target-tab "$TARGET_TAB" \
      --sendable-csv "$SENDABLE_CSV" \
      --master-csv "$MASTER_CSV" \
      --chunk "$CHUNK" \
      --token-file "$TOKEN_FILE" 2>&1)"
    APPEND_RC=$?
    set -e
    echo "$APPEND_OUT" >> "$LOG_FILE"
    if [[ "$APPEND_RC" -eq 0 ]]; then
      log "append_done needed_was=$NEEDED"
    else
      log "append_failed rc=$APPEND_RC"
    fi
  else
    log "needed_net_new=0"
  fi

  sleep "$CHECK_EVERY_SECONDS"
done
