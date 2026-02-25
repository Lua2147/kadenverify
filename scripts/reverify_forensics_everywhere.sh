#!/usr/bin/env bash
set -euo pipefail

# Run reverify forensic scans locally and/or on remote hosts.
#
# Usage:
#   scripts/reverify_forensics_everywhere.sh
#   scripts/reverify_forensics_everywhere.sh local mundi root@149.28.37.34

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCANNER="$SCRIPT_DIR/reverify_forensics_scan.py"

if [[ ! -f "$SCANNER" ]]; then
  echo "scanner not found: $SCANNER" >&2
  exit 1
fi

if [[ "$#" -eq 0 ]]; then
  HOSTS=("local" "mundi")
else
  HOSTS=("$@")
fi

ROOTS=(
  "/data/local-machine-backup"
  "/opt/mundi-princeps"
  "/tmp"
  "$HOME"
)

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${OUT_DIR:-$PWD/reverify_forensics_reports_$STAMP}"
mkdir -p "$OUT_DIR"

run_local() {
  local json_out="$OUT_DIR/reverify_forensics_local_$STAMP.json"
  echo "[forensics] local -> $json_out"
  python3 "$SCANNER" \
    --roots "${ROOTS[@]}" \
    --max-files 3000 \
    --min-drop-ratio 0.80 \
    --min-drop-rows 5000 \
    --json-out "$json_out"
}

run_remote() {
  local host="$1"
  local remote_scanner="/tmp/reverify_forensics_scan.py"
  local remote_json="/tmp/reverify_forensics_${STAMP}.json"
  local local_json="$OUT_DIR/reverify_forensics_${host//[^a-zA-Z0-9._-]/_}_$STAMP.json"

  echo "[forensics] $host -> $local_json"
  rsync -a "$SCANNER" "$host:$remote_scanner"
  ssh "$host" "python3 $remote_scanner --roots /data/local-machine-backup /opt/mundi-princeps /tmp \$HOME --max-files 3000 --min-drop-ratio 0.80 --min-drop-rows 5000 --json-out $remote_json >/tmp/reverify_forensics_${STAMP}.stdout"
  rsync -a "$host:$remote_json" "$local_json"
  rsync -a "$host:/tmp/reverify_forensics_${STAMP}.stdout" "$OUT_DIR/reverify_forensics_${host//[^a-zA-Z0-9._-]/_}_$STAMP.stdout"
}

for host in "${HOSTS[@]}"; do
  if [[ "$host" == "local" || "$host" == "localhost" ]]; then
    run_local
  else
    run_remote "$host"
  fi
done

echo "[forensics] done. reports_dir=$OUT_DIR"
