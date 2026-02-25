# Handoff: Big-List Verification Ownership

**Prepared:** 2026-02-23 (UTC)  
**Purpose:** transfer full operational ownership of big-list verification to teammate

## 0) Quick Start (new owner)
1. Connect to server:
   - `ssh mundi`
   - fallback: `ssh root@149.28.37.34`
2. Check processes:
   - `ps -axo pid,etime,%cpu,%mem,command | grep -E "waterfall_pipeline.reverify_loop|provider_keepalive_loop" | grep -v grep`
3. Check progress:
   - `LOG=/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211/provider_reverify.log; echo ts=$(date -u +%Y-%m-%dT%H:%M:%SZ); grep -E "\[reverify\] iter=" "$LOG" | tail -n 1; grep -E "\[reverify\] complete usable_total=" "$LOG" | tail -n 1`
4. Check API health:
   - `curl -sS http://127.0.0.1:8025/health`
5. If stalled/down, follow restart in Section 8.

## 1) Objective
Own and operate the big-list verification loop end-to-end:
- keep reverify loop + keepalive healthy
- track deliverable/catch-all gains and unresolved remaining
- run sheet backfill only when true net-new exists
- send periodic checkpoint updates with exact counts

## 2) Access + Environment Requirements
- SSH access to `mundi` with read/write on run directory (currently root-run)
- Google Sheet access to workbook listed below
- App path on server:
  - `/opt/mundi-princeps/apps/email-verifier`
- Preferred interpreter for operational scripts:
  - `/opt/mundi-princeps/apps/email-verifier/.venv/bin/python`
- Google token file:
  - `/opt/mundi-princeps/config/token.json`
- Verifier API endpoint:
  - `http://127.0.0.1:8025`

## 3) Data Location Map (Drive + Server + Scripts)

### Google Drive / Sheet
- Workbook URL:
  - `https://docs.google.com/spreadsheets/d/1I7B2Dg4XcZRHrJ_zowH1gxtFXYIm2_LtQXLgSA4yXVw/edit`
- Unmigrated data archive folder:
  - `https://drive.google.com/drive/folders/1LMsC6GCXl0PlWkqImvew1EpTb4ROkNPU?usp=drive_link`
  - Folder ID: `1LMsC6GCXl0PlWkqImvew1EpTb4ROkNPU`
- Workbook title:
  - `quick_wins_deliverable_20260211`
- Relevant tabs:
  - `✅quick_wins_old_2026-02-11`
  - `✅C-Suite Tier One First Batch (For Paradigm Feb 13)`
  - `✅quick_wins_net_new_valid_catchall_2026-02-16`
  - `✅quick_wins_net_new_valid_catchall_2026-02-17`
  - `✅quick_wins_net_new_valid_catchall_2026-02-18`
  - `✅quick_wins_net_new_valid_catchall_2026-02-19`
  - `✅quick_wins_net_new_valid_catchall_2026-02-20`
  - `quick_wins_net_new_valid_catchall_2026-02-23`

### Server run directory (authoritative artifacts)
- Root:
  - `/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211`
- Main logs:
  - `provider_reverify.log`
  - `provider_keepalive.log`
  - `queue_after_reverify.log`
  - `orchestrator.log`
- Provider loop artifacts:
  - `provider_loop/provider_reverify_summary.txt`
  - `provider_loop/provider_reverify_qa.json`
  - `provider_loop/provider_reverify_state.csv`
  - `provider_loop/provider_reverify_additional_usable.csv`
- Core CSV inputs/outputs:
  - `state.csv`
  - `waterfall_unknown_undeliverable.csv`
  - `quick_wins_plus_catchall_fullloop.csv`
  - `bulk_net_new_consolidated_sendable_2026-02-18.csv`
  - `quick_wins_deliverable.csv`

### Runtime scripts used
- Keepalive loop script:
  - `/tmp/provider_keepalive_loop.sh`
- Sheet upload/backfill script:
  - `/tmp/add_feb19_net_new.py`
- Older uploader reference:
  - `/tmp/bulk_net_new_consolidated_to_sheet.py`
- Persistent backup copies (preferred):
  - `/opt/mundi-princeps/apps/email-verifier/scripts/add_feb19_net_new.py`
  - `/opt/mundi-princeps/apps/email-verifier/scripts/bulk_net_new_consolidated_to_sheet.py`
  - `/opt/mundi-princeps/apps/email-verifier/scripts/check_needed_net_new.py`

Note: `/tmp` scripts are ephemeral and can disappear on reboot.

## 4) Current Live State (validated)
**Validation timestamp:** `2026-02-23T22:10:20Z`

### Process status
- Reverify active PID: `3691746`
- Keepalive active PID: `4124281`
- API health response:
  - `{"status":"ok","service":"kadenverify","version":"0.1.0"}`

### Latest reverify progress observed
- Iter lines:
  - `iter=826 pending=28379 ... gains=3 remaining=28376`
  - `iter=827 pending=28376 ... gains=3 remaining=28373`
- Latest complete lines:
  - `complete usable_total=23149 deliverable=18328 catch_all=4821 remaining=89497`
  - `complete usable_total=23179 deliverable=18342 catch_all=4837 remaining=89467`
- Recent retry pressure signal:
  - `status=500` count in last 2000 reverify-log lines: `1059`

### Keepalive cadence signal
`provider_keepalive.log` shows repeated cycle pattern:
- `[keeper] cycle_start ...`
- `[keeper] reverify_pid=...`
- `[keeper] cycle_done ...`

## 5) Monitoring Commands (copy/paste)

### A) Process health
```bash
ssh mundi 'ps -axo pid,etime,%cpu,%mem,command | grep -E "waterfall_pipeline.reverify_loop|provider_keepalive_loop" | grep -v grep'
```

### B) Fast checkpoint
```bash
ssh mundi '
RUN=/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211
LOG="$RUN/provider_reverify.log"

echo ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "latest_iter:"; grep -E "\[reverify\] iter=" "$LOG" | tail -n 1
echo "latest_complete:"; grep -E "\[reverify\] complete usable_total=" "$LOG" | tail -n 1
'
```

### C) Tail logs
```bash
ssh mundi '
RUN=/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211
echo "---reverify---"; tail -n 40 "$RUN/provider_reverify.log"
echo "---keepalive---"; tail -n 40 "$RUN/provider_keepalive.log"
'
```

### D) API health
```bash
ssh mundi 'curl -sS http://127.0.0.1:8025/health'
```

### E) Summary staleness check
```bash
ssh mundi '
RUN=/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211
stat -c "%y %n" "$RUN/provider_loop/provider_reverify_summary.txt" "$RUN/provider_reverify.log"
'
```

### F) Recent API 500 pressure
```bash
ssh mundi '
RUN=/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211
tail -n 2000 "$RUN/provider_reverify.log" | grep -c "status=500"
'
```

## 6) Baseline Data Volumes (validated)
- `state.csv`: `664,982` rows
- `waterfall_unknown_undeliverable.csv`: `612,810` rows
- `quick_wins_plus_catchall_fullloop.csv`: `51,371` rows
- `bulk_net_new_consolidated_sendable_2026-02-18.csv`: `39,233` rows
- `provider_loop/provider_reverify_state.csv`: `35,902` rows
- `provider_loop/provider_reverify_additional_usable.csv`: `23,179` rows

Recompute command:
```bash
ssh mundi 'python3 - <<"PY"
import csv
from pathlib import Path
run=Path("/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211")
for rel in [
    "state.csv",
    "waterfall_unknown_undeliverable.csv",
    "quick_wins_plus_catchall_fullloop.csv",
    "bulk_net_new_consolidated_sendable_2026-02-18.csv",
    "provider_loop/provider_reverify_state.csv",
    "provider_loop/provider_reverify_additional_usable.csv",
]:
    p=run/rel
    with p.open("r",encoding="utf-8-sig",newline="") as f:
        r=csv.reader(f); h=next(r,None); n=sum(1 for _ in r)
    print(f"{rel}\trows={n}\theader_cols={len(h or [])}")
PY'
```

## 7) Net-New Sheet Status + Backfill
Current checked status:
- sendable set size: `39,233`
- existing emails across workbook tabs: `305,740`
- needed net new: `0`
- Feb 23 tab exists: `quick_wins_net_new_valid_catchall_2026-02-23`
- Feb 23 tab current rows: `0`

Source files used:
- `/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211/bulk_net_new_consolidated_sendable_2026-02-18.csv`
- `/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211/quick_wins_plus_catchall_fullloop.csv`

Read-only recompute of `needed_net_new` (no writes):
```bash
ssh mundi '/opt/mundi-princeps/apps/email-verifier/.venv/bin/python /opt/mundi-princeps/apps/email-verifier/scripts/check_needed_net_new.py \
  --sheet-id 1I7B2Dg4XcZRHrJ_zowH1gxtFXYIm2_LtQXLgSA4yXVw \
  --sendable-csv /data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211/bulk_net_new_consolidated_sendable_2026-02-18.csv'
```

If net-new becomes positive, run (preferred path):
```bash
ssh mundi '/opt/mundi-princeps/apps/email-verifier/.venv/bin/python /opt/mundi-princeps/apps/email-verifier/scripts/add_feb19_net_new.py \
  --sheet-id 1I7B2Dg4XcZRHrJ_zowH1gxtFXYIm2_LtQXLgSA4yXVw \
  --template-tab "✅quick_wins_net_new_valid_catchall_2026-02-18" \
  --target-tab "quick_wins_net_new_valid_catchall_YYYY-MM-DD" \
  --sendable-csv /data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211/bulk_net_new_consolidated_sendable_2026-02-18.csv \
  --master-csv /data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211/quick_wins_plus_catchall_fullloop.csv \
  --chunk 1000'
```

## 8) Control Commands (start/stop/restart)

### Check PID files
```bash
ssh mundi '
RUN=/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211
for f in provider_keepalive.pid run_keepalive.pid provider_reverify.pid run_reverify.pid queue_after_reverify.pid run_provider_round2.pid; do
  printf "%s=" "$f"; cat "$RUN/$f" 2>/dev/null || echo missing
done
'
```

### Safe restart keepalive + reverify
```bash
ssh mundi '
set -e
RUN=/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211

for p in $(cat "$RUN/provider_keepalive.pid" 2>/dev/null || true) $(cat "$RUN/run_keepalive.pid" 2>/dev/null || true) $(cat "$RUN/provider_reverify.pid" 2>/dev/null || true) $(cat "$RUN/run_reverify.pid" 2>/dev/null || true); do
  if [ -n "${p:-}" ] && [ "$p" != "0" ]; then kill "$p" 2>/dev/null || true; fi
done

echo 0 > "$RUN/provider_keepalive.pid"
echo 0 > "$RUN/run_keepalive.pid"
echo 0 > "$RUN/provider_reverify.pid"
echo 0 > "$RUN/run_reverify.pid"
echo 0 > "$RUN/queue_after_reverify.pid"
echo 0 > "$RUN/run_provider_round2.pid"

nohup bash /tmp/provider_keepalive_loop.sh >> "$RUN/provider_keepalive.nohup.log" 2>&1 &
NEWPID=$!
echo "$NEWPID" > "$RUN/provider_keepalive.pid"
echo "$NEWPID" > "$RUN/run_keepalive.pid"
echo "restarted keepalive pid=$NEWPID"
'
```

### Verify restart succeeded
```bash
ssh mundi 'ps -axo pid,etime,%cpu,%mem,command | grep -E "waterfall_pipeline.reverify_loop|provider_keepalive_loop" | grep -v grep'
```

## 9) Failure Playbook
1. If no new `iter=` line for >30 minutes:
   - check Section 5 A/B/D
   - inspect keepalive tail (Section 5 C)
   - restart loop (Section 8)
2. If Sheets API returns `429`:
   - wait 60-180 seconds
   - rerun once
   - avoid tight retry loops
3. If Sheets range parse fails:
   - verify exact tab title (`✅` prefix may differ by tab)
4. If `/tmp` script missing:
   - use persistent copy under `/opt/mundi-princeps/apps/email-verifier/scripts/`
   - optional restore: `cp /opt/mundi-princeps/apps/email-verifier/scripts/add_feb19_net_new.py /tmp/add_feb19_net_new.py`
5. If repeated process exits:
   - capture last 100 lines from `provider_reverify.log` and `provider_keepalive.log`
   - escalate with timestamped evidence

## 10) Known Gotchas
- `provider_reverify_summary.txt` can lag behind live `provider_reverify.log`.
- Feb 23 tab currently has no `✅` prefix.
- Frequent batch `status=500` can appear under load; key signal is whether `iter`/`complete` lines keep moving.
- `provider_reverify_state.csv` has very wide headers (hundreds of columns).

## 11) Definition of Done (each checkpoint report)
Include all six:
1. UTC timestamp
2. active PID(s) + elapsed runtime
3. latest `iter=...` line
4. latest `complete usable_total=...` line
5. anomalies observed (`429`, `500`, stalls, restarts)
6. net-new needed count (when sheet task is in scope)

## 12) Minimal Operating Cadence
- Every 30-60 minutes while active:
  - Section 5 A + B + D
- If cycle boundary suspected:
  - Section 5 C and capture new `complete` line
- Run sheet upload only when `needed_net_new > 0`

## 13) Diagnostic Coverage Completed
Validated against live environment:
- processes active
- key paths exist
- API healthy
- workbook ID/title/tabs verified
- baseline row counts captured
- persistent script backups created under `/opt/mundi-princeps/apps/email-verifier/scripts/`
- restart procedure documented (not executed during diagnostic to avoid interrupting live run)
- failure playbook documented
