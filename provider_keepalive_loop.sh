#!/usr/bin/env bash
set -euo pipefail

RUN="${RUN:-/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211}"
APP="${APP:-/opt/mundi-princeps/apps/email-verifier}"
API_KEY="${API_KEY:-kadenwood_verify_2026}"

REVERIFY_LOG="${REVERIFY_LOG:-$RUN/provider_reverify.log}"
QUEUE_LOG="${QUEUE_LOG:-$RUN/queue_after_reverify.log}"
KEEP_LOG="${KEEP_LOG:-$RUN/provider_keepalive.log}"

# Throughput tuning
BATCH_SIZE="${BATCH_SIZE:-500}"
CONCURRENCY="${CONCURRENCY:-64}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-0}"
REQUEST_TIMEOUT_SECONDS="${REQUEST_TIMEOUT_SECONDS:-180}"
BATCH_MAX_ATTEMPTS="${BATCH_MAX_ATTEMPTS:-2}"
MAX_PENDING_PER_ITER="${MAX_PENDING_PER_ITER:-250000}"
MAX_ITERS="${MAX_ITERS:-6}"
PARALLEL_SHARDS="${PARALLEL_SHARDS:-1}"

# Reverify stop tuning
GAIN_STOP_ABS="${GAIN_STOP_ABS:-40}"
GAIN_STOP_RATE="${GAIN_STOP_RATE:-0.00015}"
GAIN_STOP_STREAK="${GAIN_STOP_STREAK:-2}"
MIN_PENDING_FOR_STOP="${MIN_PENDING_FOR_STOP:-50000}"
UNKNOWN_STREAK_LOCK="${UNKNOWN_STREAK_LOCK:-3}"
UNKNOWN_RETRY_GAP_ITERS="${UNKNOWN_RETRY_GAP_ITERS:-12}"
GOOD_RESULTS="${GOOD_RESULTS:-deliverable,accept_all}"
FIRST_PASS_ALL="${FIRST_PASS_ALL:-0}"
FIRST_PASS_FORCE_LOAD_ONCE="${FIRST_PASS_FORCE_LOAD_ONCE:-0}"
FIRST_PASS_RETRY_GAP_ITERS="${FIRST_PASS_RETRY_GAP_ITERS:-1000000}"

# Hot/cold prioritization tuning
HOT_SOURCE_PREFIXES="${HOT_SOURCE_PREFIXES:-drive_parallel_hold}"
HOT_PRIORITY_QUOTA="${HOT_PRIORITY_QUOTA:-80000}"
FRESH_UNKNOWN_STREAK_MAX="${FRESH_UNKNOWN_STREAK_MAX:-0}"
HOLD_PROMOTE_PER_CYCLE="${HOLD_PROMOTE_PER_CYCLE:-5000}"
HOLD_PROMOTE_ON_ROTATE="${HOLD_PROMOTE_ON_ROTATE:-20000}"

# Low-gain rotate tuning
LOW_GAIN_ROTATE_ABS="${LOW_GAIN_ROTATE_ABS:-75}"
LOW_GAIN_ROTATE_RATE="${LOW_GAIN_ROTATE_RATE:-0.0005}"
LOW_GAIN_ROTATE_STREAK="${LOW_GAIN_ROTATE_STREAK:-2}"

# Guardrail: block suspicious state shrink before promotion
MIN_STATE_RATIO="${MIN_STATE_RATIO:-0.80}"
MIN_STATE_DROP_ROWS="${MIN_STATE_DROP_ROWS:-5000}"

PROVIDER_DIR="$RUN/provider_loop"
FORENSICS_DIR="$PROVIDER_DIR/reverify_guardrail_forensics"
VERIFIED_FILE="$PROVIDER_DIR/provider_candidates_verified.csv"
WATERFALL_FILE="$RUN/waterfall_unknown_undeliverable.csv"
LOW_VALUE_PURGE_ON_START="${LOW_VALUE_PURGE_ON_START:-1}"
LOW_VALUE_PURGE_MARKER="${LOW_VALUE_PURGE_MARKER:-$PROVIDER_DIR/.low_value_purge_done}"

STATE_FILE="$PROVIDER_DIR/provider_reverify_state.csv"
USABLE_FILE="$PROVIDER_DIR/provider_reverify_additional_usable.csv"
SUMMARY_FILE="$PROVIDER_DIR/provider_reverify_summary.txt"
QA_FILE="$PROVIDER_DIR/provider_reverify_qa.json"
HOLD_STATE_FILE="$PROVIDER_DIR/provider_reverify_state.drive_parallel_hold.csv"
HOLD_VERIFIED_FILE="$PROVIDER_DIR/provider_candidates_verified.drive_parallel_hold.csv"
FIRST_PASS_FORCE_LOAD_MARKER="${FIRST_PASS_FORCE_LOAD_MARKER:-$PROVIDER_DIR/.first_pass_force_loaded}"

STATE_NEXT="$PROVIDER_DIR/provider_reverify_state.next.csv"
USABLE_NEXT="$PROVIDER_DIR/provider_reverify_additional_usable.next.csv"
SUMMARY_NEXT="$PROVIDER_DIR/provider_reverify_summary.next.txt"
QA_NEXT="$PROVIDER_DIR/provider_reverify_qa.next.json"

STATE_LAST_GOOD="$PROVIDER_DIR/provider_reverify_state.last_good.csv"
USABLE_LAST_GOOD="$PROVIDER_DIR/provider_reverify_additional_usable.last_good.csv"
SUMMARY_LAST_GOOD="$PROVIDER_DIR/provider_reverify_summary.last_good.txt"
QA_LAST_GOOD="$PROVIDER_DIR/provider_reverify_qa.last_good.json"
LOW_GAIN_STREAK_FILE="$PROVIDER_DIR/.low_gain_rotate_streak"
ROTATE_NEXT_FILE="$PROVIDER_DIR/.rotate_hot_slice_next"
KPI_FILE="${KPI_FILE:-$RUN/provider_reverify_kpis.csv}"
KPI_HOURLY_FILE="${KPI_HOURLY_FILE:-$RUN/provider_reverify_kpis_hourly.json}"

if [[ "$FIRST_PASS_ALL" == "1" ]]; then
  MAX_ITERS=1
  UNKNOWN_STREAK_LOCK=1
  UNKNOWN_RETRY_GAP_ITERS="$FIRST_PASS_RETRY_GAP_ITERS"
fi

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

promote_drive_hold_rows() {
  local max_rows="$1"
  python3 - "$HOLD_STATE_FILE" "$HOLD_VERIFIED_FILE" "$STATE_FILE" "$VERIFIED_FILE" "$max_rows" <<'PY'
import csv
import json
import sys
from pathlib import Path

hold_state = Path(sys.argv[1])
hold_verified = Path(sys.argv[2])
state_file = Path(sys.argv[3])
verified_file = Path(sys.argv[4])
max_rows = max(0, int(sys.argv[5]))

stats = {
    "requested": max_rows,
    "promoted": 0,
    "promoted_verified": 0,
    "remaining_hold_state": 0,
    "remaining_hold_verified": 0,
}

if max_rows <= 0:
    stats["remaining_hold_state"] = -1
    stats["remaining_hold_verified"] = -1
    print(json.dumps(stats, separators=(",", ":")))
    raise SystemExit(0)

state_header = []
if state_file.exists() and state_file.stat().st_size > 0:
    with state_file.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        state_header = list(r.fieldnames or [])
if not state_header:
    state_header = [
        "contact_key",
        "email",
        "source",
        "prev_result",
        "current_result",
        "resolved_iter",
        "unknown_streak",
        "next_retry_iter",
    ]

verified_header = []
if verified_file.exists() and verified_file.stat().st_size > 0:
    with verified_file.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        verified_header = list(r.fieldnames or [])
if not verified_header:
    verified_header = ["contact_key", "email", "source", "verify_result"]

selected_rows: list[dict] = []
selected_keys: set[str] = set()
selected_emails: set[str] = set()

if hold_state.exists() and hold_state.stat().st_size > 0:
    tmp_hold_state = hold_state.with_suffix(hold_state.suffix + ".promote_tmp")
    with hold_state.open("r", encoding="utf-8-sig", newline="") as src, tmp_hold_state.open(
        "w", encoding="utf-8", newline=""
    ) as dst:
        reader = csv.DictReader(src)
        hold_state_fields = list(reader.fieldnames or [])
        if hold_state_fields:
            writer = csv.DictWriter(dst, fieldnames=hold_state_fields)
            writer.writeheader()
            for row in reader:
                ck = (row.get("contact_key") or "").strip().lower()
                em = (row.get("email") or "").strip().lower()
                if (
                    len(selected_rows) < max_rows
                    and ck
                    and "@" in em
                    and ck not in selected_keys
                    and em not in selected_emails
                ):
                    selected_rows.append(dict(row))
                    selected_keys.add(ck)
                    selected_emails.add(em)
                else:
                    writer.writerow(row)
        else:
            dst.write("")
    tmp_hold_state.replace(hold_state)

if selected_rows:
    file_has_data = state_file.exists() and state_file.stat().st_size > 0
    with state_file.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=state_header, extrasaction="ignore")
        if not file_has_data:
            w.writeheader()
        for src in selected_rows:
            out = {k: "" for k in state_header}
            out["contact_key"] = (src.get("contact_key") or "").strip().lower()
            out["email"] = (src.get("email") or "").strip().lower()
            out["source"] = (src.get("source") or "drive_parallel_hold").strip().lower()
            if "prev_result" in out:
                out["prev_result"] = "unknown"
            if "current_result" in out:
                out["current_result"] = "unknown"
            if "resolved_iter" in out:
                out["resolved_iter"] = ""
            if "unknown_streak" in out:
                out["unknown_streak"] = "0"
            if "next_retry_iter" in out:
                out["next_retry_iter"] = ""
            w.writerow(out)
    stats["promoted"] = len(selected_rows)

if hold_verified.exists() and hold_verified.stat().st_size > 0:
    tmp_hold_verified = hold_verified.with_suffix(hold_verified.suffix + ".promote_tmp")
    with hold_verified.open("r", encoding="utf-8-sig", newline="") as src, tmp_hold_verified.open(
        "w", encoding="utf-8", newline=""
    ) as dst:
        reader = csv.DictReader(src)
        hold_verified_fields = list(reader.fieldnames or [])
        if hold_verified_fields:
            writer = csv.DictWriter(dst, fieldnames=hold_verified_fields)
            writer.writeheader()
            promote_verified_rows = []
            for row in reader:
                ck = (row.get("contact_key") or "").strip().lower()
                if ck and ck in selected_keys:
                    promote_verified_rows.append(dict(row))
                else:
                    writer.writerow(row)
        else:
            promote_verified_rows = []
            dst.write("")
    tmp_hold_verified.replace(hold_verified)

    if promote_verified_rows:
        file_has_data = verified_file.exists() and verified_file.stat().st_size > 0
        with verified_file.open("a", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=verified_header, extrasaction="ignore")
            if not file_has_data:
                w.writeheader()
            for src in promote_verified_rows:
                out = {k: "" for k in verified_header}
                out["contact_key"] = (src.get("contact_key") or "").strip().lower()
                out["email"] = (src.get("email") or "").strip().lower()
                out["source"] = (src.get("source") or "drive_parallel_hold").strip().lower()
                if "verify_result" in out:
                    out["verify_result"] = "unknown"
                w.writerow(out)
        stats["promoted_verified"] = len(promote_verified_rows)

for p, key in ((hold_state, "remaining_hold_state"), (hold_verified, "remaining_hold_verified")):
    if not p.exists() or p.stat().st_size == 0:
        stats[key] = 0
        continue
    n = 0
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.reader(f)
        next(r, None)
        for _ in r:
            n += 1
    stats[key] = n

print(json.dumps(stats, separators=(",", ":")))
PY
}

parse_cycle_metrics() {
  local summary_path="$1"
  local qa_path="$2"
  python3 - "$summary_path" "$qa_path" <<'PY'
import json
import re
import sys
from pathlib import Path

summary_path = Path(sys.argv[1])
qa_path = Path(sys.argv[2])
out = {
    "iter": 0,
    "pending": 0,
    "eligible": 0,
    "queried": 0,
    "verify_miss": 0,
    "gains": 0,
    "gain_rate": 0.0,
    "remaining": 0,
    "stop_reason": "",
}

if summary_path.exists() and summary_path.stat().st_size > 0:
    text = summary_path.read_text(encoding="utf-8", errors="ignore")
    for line in text.splitlines():
        if line.startswith("stop_reason="):
            out["stop_reason"] = line.split("=", 1)[1].strip()
    iter_lines = [ln.strip() for ln in text.splitlines() if ln.strip().startswith("- iter=")]
    if iter_lines:
        last = iter_lines[-1]
        kv = dict(re.findall(r"([a-z_]+)=([0-9.]+)", last))
        out["iter"] = int(float(kv.get("iter", "0")))
        out["pending"] = int(float(kv.get("pending", "0")))
        out["eligible"] = int(float(kv.get("eligible", "0")))
        out["queried"] = int(float(kv.get("queried", "0")))
        out["verify_miss"] = int(float(kv.get("verify_miss", "0")))
        out["gains"] = int(float(kv.get("gains", "0")))
        out["gain_rate"] = float(kv.get("gain_rate", "0"))
        out["remaining"] = int(float(kv.get("remaining", "0")))

if (not out["stop_reason"]) and qa_path.exists() and qa_path.stat().st_size > 0:
    try:
        qa = json.loads(qa_path.read_text(encoding="utf-8", errors="ignore"))
        out["stop_reason"] = str(qa.get("stop_reason") or "")
    except Exception:
        pass

print(json.dumps(out, separators=(",", ":")))
PY
}

append_cycle_kpis() {
  local cycle_metrics_json="$1"
  local cycle_secs="$2"
  local promoted_rows="$3"
  local rotate_boost="$4"
  python3 - "$KPI_FILE" "$KPI_HOURLY_FILE" "$cycle_metrics_json" "$cycle_secs" "$promoted_rows" "$rotate_boost" \
    "$BATCH_SIZE" "$CONCURRENCY" "$REQUEST_TIMEOUT_SECONDS" "$BATCH_MAX_ATTEMPTS" "$MAX_PENDING_PER_ITER" <<'PY'
import csv
import json
import math
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

kpi_file = Path(sys.argv[1])
hourly_file = Path(sys.argv[2])
metrics = json.loads(sys.argv[3] or "{}")
cycle_secs = max(1, int(float(sys.argv[4] or "1")))
promoted_rows = int(float(sys.argv[5] or "0"))
rotate_boost = int(float(sys.argv[6] or "0"))
batch_size = int(float(sys.argv[7] or "0"))
concurrency = int(float(sys.argv[8] or "0"))
request_timeout = int(float(sys.argv[9] or "0"))
batch_attempts = int(float(sys.argv[10] or "0"))
max_pending = int(float(sys.argv[11] or "0"))

pending = int(metrics.get("pending", 0) or 0)
eligible = int(metrics.get("eligible", 0) or 0)
queried = int(metrics.get("queried", 0) or 0)
verify_miss = int(metrics.get("verify_miss", 0) or 0)
gains = int(metrics.get("gains", 0) or 0)
gain_rate = float(metrics.get("gain_rate", 0.0) or 0.0)
remaining = int(metrics.get("remaining", 0) or 0)
stop_reason = str(metrics.get("stop_reason", "") or "")

completed = max(0, queried - verify_miss)
cycle_mins = cycle_secs / 60.0
completed_per_min = (completed / cycle_mins) if cycle_mins > 0 else 0.0
gains_per_10k = ((gains / queried) * 10000.0) if queried > 0 else 0.0
verify_miss_rate = (verify_miss / queried) if queried > 0 else 0.0

row = {
    "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "cycle_secs": cycle_secs,
    "batch_size": batch_size,
    "concurrency": concurrency,
    "request_timeout_seconds": request_timeout,
    "batch_max_attempts": batch_attempts,
    "max_pending_per_iter": max_pending,
    "promoted_hold_rows": promoted_rows,
    "rotate_boost": rotate_boost,
    "pending": pending,
    "eligible": eligible,
    "queried": queried,
    "verify_miss": verify_miss,
    "gains": gains,
    "gain_rate": f"{gain_rate:.6f}",
    "remaining": remaining,
    "stop_reason": stop_reason,
    "completed_per_min": f"{completed_per_min:.2f}",
    "gains_per_10k": f"{gains_per_10k:.2f}",
    "verify_miss_rate": f"{verify_miss_rate:.6f}",
}

fieldnames = list(row.keys())
exists = kpi_file.exists() and kpi_file.stat().st_size > 0
kpi_file.parent.mkdir(parents=True, exist_ok=True)
with kpi_file.open("a", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    if not exists:
        w.writeheader()
    w.writerow(row)

cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
recent = []
with kpi_file.open("r", encoding="utf-8", newline="") as f:
    r = csv.DictReader(f)
    for rec in r:
        ts = rec.get("timestamp_utc", "")
        try:
            dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if dt >= cutoff:
            recent.append(rec)

def avg_float(key: str) -> float:
    vals = []
    for rec in recent:
        try:
            vals.append(float(rec.get(key, "0") or 0.0))
        except Exception:
            pass
    if not vals:
        return 0.0
    return sum(vals) / len(vals)

hourly = {
    "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "window_minutes": 60,
    "samples": len(recent),
    "avg_completed_per_min": round(avg_float("completed_per_min"), 2),
    "avg_gains_per_10k": round(avg_float("gains_per_10k"), 2),
    "avg_verify_miss_rate": round(avg_float("verify_miss_rate"), 6),
    "avg_gain_rate": round(avg_float("gain_rate"), 6),
}
hourly_file.parent.mkdir(parents=True, exist_ok=True)
hourly_file.write_text(json.dumps(hourly, separators=(",", ":")), encoding="utf-8")
print(json.dumps(hourly, separators=(",", ":")))
PY
}

json_field() {
  local json_blob="$1"
  local key="$2"
  python3 - "$json_blob" "$key" <<'PY'
import json
import sys

blob = sys.argv[1]
key = sys.argv[2]
try:
    obj = json.loads(blob)
except Exception:
    print("")
    raise SystemExit(0)
val = obj.get(key, "")
if isinstance(val, float):
    print(f"{val:.6f}")
else:
    print(val)
PY
}

purge_low_value_rows() {
  local file="$1"
  local label="$2"

  python3 - "$file" "$label" <<'PY'
import csv
import json
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
label = sys.argv[2]

stats = {
    "label": label,
    "path": str(path),
    "status": "unknown",
    "scanned": 0,
    "kept": 0,
    "dropped": 0,
    "dropped_academic_domain": 0,
    "dropped_keyword": 0,
    "dropped_naics": 0,
}

if not path.exists() or path.stat().st_size == 0:
    stats["status"] = "missing_or_empty"
    print(json.dumps(stats, separators=(",", ":")))
    raise SystemExit(0)

academic_domain_re = re.compile(r"(?:\.edu$|\.ac\.[a-z]{2}$)", re.I)
keyword_blocklist = (
    "higher education",
    "university",
    "state university",
    "college",
    "community college",
    "polytechnic",
    "junior college",
    "registrar",
    "admissions",
    "alumni",
)
naics_block_prefixes = ("6112", "6113")


def row_domain(row: dict) -> str:
    email = (row.get("email") or "").strip().lower()
    if "@" in email:
        return email.split("@", 1)[1].strip()

    domain = (row.get("domain") or "").strip().lower()
    if domain:
        return domain

    key = (row.get("contact_key") or "").strip().lower()
    parts = key.split("|")
    if len(parts) == 3 and parts[2]:
        return parts[2].strip()
    return ""


tmp = path.with_suffix(path.suffix + ".purge_tmp")
with path.open("r", encoding="utf-8-sig", newline="") as src, tmp.open("w", encoding="utf-8", newline="") as dst:
    reader = csv.DictReader(src)
    fieldnames = list(reader.fieldnames or [])
    if not fieldnames:
        stats["status"] = "no_header"
        print(json.dumps(stats, separators=(",", ":")))
        raise SystemExit(0)

    writer = csv.DictWriter(dst, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        stats["scanned"] += 1
        domain = row_domain(row)
        blob = " | ".join(
            (
                row.get("contact_key") or "",
                row.get("source") or "",
                row.get("source_file") or "",
                row.get("company") or "",
                row.get("industry") or "",
                row.get("naics") or "",
                domain,
            )
        ).lower()

        drop = False
        if domain and academic_domain_re.search(domain):
            stats["dropped_academic_domain"] += 1
            drop = True
        elif any(token in blob for token in keyword_blocklist):
            stats["dropped_keyword"] += 1
            drop = True
        else:
            naics_codes = re.findall(r"\d{3,6}", blob)
            if any(code.startswith(naics_block_prefixes) for code in naics_codes):
                stats["dropped_naics"] += 1
                drop = True

        if drop:
            stats["dropped"] += 1
            continue

        stats["kept"] += 1
        writer.writerow(row)

if stats["dropped"] > 0:
    tmp.replace(path)
    stats["status"] = "purged"
else:
    tmp.unlink(missing_ok=True)
    stats["status"] = "unchanged"

print(json.dumps(stats, separators=(",", ":")))
PY
}

run_low_value_purge_once() {
  if [[ "$LOW_VALUE_PURGE_ON_START" != "1" ]]; then
    return 0
  fi
  if [[ -f "$LOW_VALUE_PURGE_MARKER" ]]; then
    log_keep "low_value_purge skipped marker=$LOW_VALUE_PURGE_MARKER"
    return 0
  fi

  local state_stats=""
  local verified_stats=""
  local waterfall_stats=""
  state_stats="$(purge_low_value_rows "$STATE_FILE" "provider_reverify_state")"
  verified_stats="$(purge_low_value_rows "$VERIFIED_FILE" "provider_candidates_verified")"
  waterfall_stats="$(purge_low_value_rows "$WATERFALL_FILE" "waterfall_unknown_undeliverable")"
  log_keep "low_value_purge state=$state_stats"
  log_keep "low_value_purge verified=$verified_stats"
  log_keep "low_value_purge waterfall=$waterfall_stats"
  date -u +%Y-%m-%dT%H:%M:%SZ > "$LOW_VALUE_PURGE_MARKER"
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
  CYCLE_START_EPOCH="$(date +%s)"
  echo "0" > "$RUN/queue_after_reverify.pid"
  echo "0" > "$RUN/run_provider_round2.pid"
  log_keep "cycle_start $(utc_now)"

  ROTATE_BOOST=0
  if [[ -f "$ROTATE_NEXT_FILE" ]]; then
    ROTATE_BOOST=1
    rm -f "$ROTATE_NEXT_FILE"
  fi

  rm -f "$STATE_NEXT" "$USABLE_NEXT" "$SUMMARY_NEXT" "$QA_NEXT"
  run_low_value_purge_once

  FORCE_LOAD_THIS_CYCLE=0
  if [[ "$FIRST_PASS_FORCE_LOAD_ONCE" == "1" && ! -f "$FIRST_PASS_FORCE_LOAD_MARKER" ]]; then
    FORCE_LOAD_THIS_CYCLE=1
  fi
  FORCE_LOAD_FROM_VERIFIED_FLAG=()
  if [[ "$FORCE_LOAD_THIS_CYCLE" == "1" ]]; then
    FORCE_LOAD_FROM_VERIFIED_FLAG=(--force-load-from-verified)
    log_keep "first_pass_force_load cycle=1 marker_missing=$FIRST_PASS_FORCE_LOAD_MARKER"
  fi

  HOLD_PROMOTE_ROWS="$HOLD_PROMOTE_PER_CYCLE"
  if [[ "$ROTATE_BOOST" == "1" ]]; then
    HOLD_PROMOTE_ROWS="$((HOLD_PROMOTE_PER_CYCLE + HOLD_PROMOTE_ON_ROTATE))"
  fi
  HOLD_PROMOTE_STATS="$(promote_drive_hold_rows "$HOLD_PROMOTE_ROWS")"
  PROMOTED_HOLD_ROWS="$(json_field "$HOLD_PROMOTE_STATS" "promoted")"
  REMAIN_HOLD_STATE="$(json_field "$HOLD_PROMOTE_STATS" "remaining_hold_state")"
  REMAIN_HOLD_VERIFIED="$(json_field "$HOLD_PROMOTE_STATS" "remaining_hold_verified")"
  log_keep "hold_promote rotate_boost=$ROTATE_BOOST stats=$HOLD_PROMOTE_STATS"

  # Preserve resume semantics while still writing into *.next outputs.
  if [[ -s "$STATE_FILE" ]]; then
    cp -f "$STATE_FILE" "$STATE_NEXT"
  fi

  PREV_STATE_ROWS="$(csv_rows "$STATE_FILE")"
  PREV_USABLE_ROWS="$(csv_rows "$USABLE_FILE")"

  cd "$APP"
  if [[ "$PARALLEL_SHARDS" -gt 1 ]]; then
    .venv/bin/python -u -m waterfall_pipeline.sharded_reverify_cycle \
      "$RUN/provider_loop/provider_candidates_verified.csv" \
      "$RUN/waterfall_unknown_undeliverable.csv" \
      "$STATE_NEXT" \
      "$USABLE_NEXT" \
      "$SUMMARY_NEXT" \
      "$API_KEY" \
      --api-url http://127.0.0.1:8025 \
      --resume-state "$STATE_FILE" \
      --batch-size "$BATCH_SIZE" \
      --concurrency "$CONCURRENCY" \
      --request-timeout-seconds "$REQUEST_TIMEOUT_SECONDS" \
      --batch-max-attempts "$BATCH_MAX_ATTEMPTS" \
      --max-pending-per-iter "$MAX_PENDING_PER_ITER" \
      --max-iters "$MAX_ITERS" \
      --cooldown-seconds "$COOLDOWN_SECONDS" \
      --gain-stop-abs "$GAIN_STOP_ABS" \
      --gain-stop-rate "$GAIN_STOP_RATE" \
      --gain-stop-streak "$GAIN_STOP_STREAK" \
      --min-pending-for-stop "$MIN_PENDING_FOR_STOP" \
      --unknown-streak-lock "$UNKNOWN_STREAK_LOCK" \
      --unknown-retry-gap-iters "$UNKNOWN_RETRY_GAP_ITERS" \
      --good-results "$GOOD_RESULTS" \
      --hot-source-prefixes "$HOT_SOURCE_PREFIXES" \
      --hot-priority-quota "$HOT_PRIORITY_QUOTA" \
      --fresh-unknown-streak-max "$FRESH_UNKNOWN_STREAK_MAX" \
      --qa-report "$QA_NEXT" \
      --shard-count "$PARALLEL_SHARDS" \
      "${FORCE_LOAD_FROM_VERIFIED_FLAG[@]}" \
      >> "$REVERIFY_LOG" 2>&1 &
  else
    .venv/bin/python -u -m waterfall_pipeline.reverify_loop \
      "$RUN/provider_loop/provider_candidates_verified.csv" \
      "$RUN/waterfall_unknown_undeliverable.csv" \
      "$STATE_NEXT" \
      "$USABLE_NEXT" \
      "$SUMMARY_NEXT" \
      "$API_KEY" \
      --api-url http://127.0.0.1:8025 \
      --resume-state "$STATE_FILE" \
      --batch-size "$BATCH_SIZE" \
      --concurrency "$CONCURRENCY" \
      --request-timeout-seconds "$REQUEST_TIMEOUT_SECONDS" \
      --batch-max-attempts "$BATCH_MAX_ATTEMPTS" \
      --max-pending-per-iter "$MAX_PENDING_PER_ITER" \
      --max-iters "$MAX_ITERS" \
      --cooldown-seconds "$COOLDOWN_SECONDS" \
      --gain-stop-abs "$GAIN_STOP_ABS" \
      --gain-stop-rate "$GAIN_STOP_RATE" \
      --gain-stop-streak "$GAIN_STOP_STREAK" \
      --min-pending-for-stop "$MIN_PENDING_FOR_STOP" \
      --unknown-streak-lock "$UNKNOWN_STREAK_LOCK" \
      --unknown-retry-gap-iters "$UNKNOWN_RETRY_GAP_ITERS" \
      --good-results "$GOOD_RESULTS" \
      --hot-source-prefixes "$HOT_SOURCE_PREFIXES" \
      --hot-priority-quota "$HOT_PRIORITY_QUOTA" \
      --fresh-unknown-streak-max "$FRESH_UNKNOWN_STREAK_MAX" \
      --qa-report "$QA_NEXT" \
      "${FORCE_LOAD_FROM_VERIFIED_FLAG[@]}" \
      >> "$REVERIFY_LOG" 2>&1 &
  fi

  RPID=$!
  echo "$RPID" > "$RUN/provider_reverify.pid"
  echo "$RPID" > "$RUN/run_reverify.pid"
  log_keep "reverify_pid=$RPID shards=$PARALLEL_SHARDS prev_state_rows=$PREV_STATE_ROWS prev_usable_rows=$PREV_USABLE_ROWS"

  set +e
  wait "$RPID"
  REVERIFY_EXIT="$?"
  set -e

  echo "0" > "$RUN/provider_reverify.pid"
  echo "0" > "$RUN/run_reverify.pid"

  CYCLE_METRICS_JSON="$(parse_cycle_metrics "$SUMMARY_NEXT" "$QA_NEXT")"
  CYCLE_GAINS="$(json_field "$CYCLE_METRICS_JSON" "gains")"
  CYCLE_GAIN_RATE="$(json_field "$CYCLE_METRICS_JSON" "gain_rate")"
  CYCLE_STOP_REASON="$(json_field "$CYCLE_METRICS_JSON" "stop_reason")"

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
    if [[ "$FORCE_LOAD_THIS_CYCLE" == "1" ]]; then
      date -u +%Y-%m-%dT%H:%M:%SZ > "$FIRST_PASS_FORCE_LOAD_MARKER"
      log_keep "first_pass_force_load marker_written=$FIRST_PASS_FORCE_LOAD_MARKER"
    fi
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

  LOW_GAIN_HIT="$(python3 - "$CYCLE_GAINS" "$CYCLE_GAIN_RATE" "$LOW_GAIN_ROTATE_ABS" "$LOW_GAIN_ROTATE_RATE" <<'PY'
import sys
gains = float(sys.argv[1] or 0.0)
gain_rate = float(sys.argv[2] or 0.0)
abs_thr = float(sys.argv[3] or 0.0)
rate_thr = float(sys.argv[4] or 0.0)
print(1 if (gains <= abs_thr or gain_rate < rate_thr) else 0)
PY
)"

  LOW_GAIN_STREAK=0
  if [[ -f "$LOW_GAIN_STREAK_FILE" ]]; then
    LOW_GAIN_STREAK="$(cat "$LOW_GAIN_STREAK_FILE" 2>/dev/null || echo 0)"
  fi
  if [[ "$LOW_GAIN_HIT" == "1" ]]; then
    LOW_GAIN_STREAK="$((LOW_GAIN_STREAK + 1))"
  else
    LOW_GAIN_STREAK=0
  fi

  ROTATE_NEXT=0
  if [[ "$LOW_GAIN_STREAK" -ge "$LOW_GAIN_ROTATE_STREAK" ]]; then
    ROTATE_NEXT=1
    LOW_GAIN_STREAK=0
    date -u +%Y-%m-%dT%H:%M:%SZ > "$ROTATE_NEXT_FILE"
  fi
  echo "$LOW_GAIN_STREAK" > "$LOW_GAIN_STREAK_FILE"
  log_keep "low_gain_gate gains=$CYCLE_GAINS gain_rate=$CYCLE_GAIN_RATE stop_reason=$CYCLE_STOP_REASON streak=$LOW_GAIN_STREAK rotate_next=$ROTATE_NEXT"

  CYCLE_END_EPOCH="$(date +%s)"
  CYCLE_SECS="$((CYCLE_END_EPOCH - CYCLE_START_EPOCH))"
  KPI_HOURLY_JSON="$(append_cycle_kpis "$CYCLE_METRICS_JSON" "$CYCLE_SECS" "$PROMOTED_HOLD_ROWS" "$ROTATE_BOOST")"
  log_keep "kpi cycle_secs=$CYCLE_SECS promoted_hold_rows=$PROMOTED_HOLD_ROWS hold_remaining_state=$REMAIN_HOLD_STATE hold_remaining_verified=$REMAIN_HOLD_VERIFIED cycle_metrics=$CYCLE_METRICS_JSON hourly=$KPI_HOURLY_JSON"

  log_keep "cycle_done $(utc_now)"
  sleep 2
done
