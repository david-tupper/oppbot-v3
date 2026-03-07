# oppbot-v3

A local tool for syncing Gong call transcripts from BigQuery and triaging unmatched calls into customer folders.

---

## What it does

1. **Syncs Gong calls from BigQuery** (`gong_fetch.py`) — pulls call transcripts for a defined set of AEs by owner ID, auto-routes them into customer directories, and drops anything it can't match into `_unmatched/unprocessed/`.

2. **Runs a triage UI** (`triage_server.py` + `triage.html`) — a local web app for reviewing unmatched calls, skipping irrelevant ones, and routing them to the right customer folder.

---

## Directory structure

```
/Users/davidtupper/customers/
  <customer-name>/
    gong/
      manifest.json         # index of all calls for this customer
      YYYY-MM-DD_<title>.md # transcript files
    gong_routing.json       # optional: routing aliases for this customer
  _unmatched/
    unprocessed/gong/       # calls with no customer match (pending triage)
    processed/gong/         # calls that were skipped in triage
```

---

## Usage

### Triage UI (recommended)
```bash
./triage.sh
```
Opens `http://gong-triage.local` in your browser. Prompts for `sudo` once per boot to set up port forwarding (80 → 5555). Press Ctrl+C to stop.

**One-time setup** (adds the local hostname):
```bash
echo "127.0.0.1 gong-triage.local" | sudo tee -a /etc/hosts
```

### Daily sync (runs automatically at 12pm PT via launchd)
```bash
python3 gong_fetch.py --sync
python3 gong_fetch.py --sync --since 2025-01-01   # backfill from a date
python3 gong_fetch.py --sync --dry-run             # preview without writing
```

Defaults to the last 30 days when `--since` is omitted. Warns if the result count hits `--limit` (default 200), which may indicate truncation.

### Manual fetch for a specific account
```bash
python3 gong_fetch.py --account "<customer-name>"
python3 gong_fetch.py --account "<customer-name>" --title-pattern "<title-of-call>"
```

### Routing config management
```bash
python3 gong_fetch.py --init-routing                        # scaffold gong_routing.json for all customer dirs
python3 gong_fetch.py --add-alias grafana-labs "Grafana"   # add a routing alias
python3 gong_fetch.py --show-routing                        # print the full routing table
```

---

## Triage UI keyboard shortcuts

| Key | Action |
|-----|--------|
| `↑` / `↓` | Navigate up/down |
| `Enter` | Open routing overlay |
| `s` | Skip (moves to processed) |
| `u` | Switch to Unprocessed tab |
| `p` | Switch to Processed tab |
| `Esc` | Close overlay |

In the routing overlay, type to filter customer folders. If no match exists, a **+ Create folder** option appears for valid kebab-case names (`^[a-z0-9][a-z0-9-]*$`). Selecting it creates the folder and routes the call in one step.

The **Processed tab** shows calls that were skipped. You can re-route them to a customer folder from there if you skipped something by mistake.

---

## Auto-routing logic

When syncing, `gong_fetch.py` tries to match each call title to a customer directory using three strategies in order:

1. **Routing aliases** — word-boundary match against patterns in `gong_routing.json`
2. **Auto-derived pattern** — converts dir name to title case (e.g. `ventura-foods` → `Ventura Foods`)
3. **Raw dir name** — word-boundary match on the directory name itself

Calls that don't match any strategy go to `_unmatched/unprocessed/`.

---

## Automatic sync

A launchd agent runs the sync daily at **12pm PT**:

```
~/Library/LaunchAgents/com.davidtupper.gongsync.plist
```

Logs: `~/Library/Logs/gongsync.log`

```bash
# Manually trigger a run
launchctl start com.davidtupper.gongsync

# Check logs
tail -f ~/Library/Logs/gongsync.log

# Stop/start the agent
launchctl unload ~/Library/LaunchAgents/com.davidtupper.gongsync.plist
launchctl load ~/Library/LaunchAgents/com.davidtupper.gongsync.plist
```

---

## Setup: finding your Gong owner IDs

Before running a sync, populate `OWNER_IDS` in `gong_fetch.py` with the people whose calls you want to pull.

1. **Find a call you know they organised.** Any call where they were the host works — you just need the exact title and date.

2. **Run this query in BigQuery** (substituting the title and date):

```sql
SELECT owner_id, call_title, call_started_at
FROM `grafanalabs-data-marts.mrt_core.brk_gong_calls`
WHERE DATE(call_started_at) = '2025-06-15'
  AND call_title = 'Grafana <> Acme Corp'
LIMIT 10
```

3. **Copy the `owner_id`** from the result and add it to the dict:

```python
OWNER_IDS = {
    "Jane Smith": "1234567890123456789",
}
```

The key is just a display label used in transcript filenames — it doesn't need to match anything in the system.

---

## Requirements

- Python 3.11+
- `google-cloud-bigquery` — BigQuery client
- `flask` — triage server
- GCP credentials: `gcloud auth application-default login`
