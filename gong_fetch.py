"""
gong_fetch.py — Daily sync of Gong call transcripts by owner, auto-routed to customer directories.

Usage:
    # Auto-sync by owner (daily use)
    python gong_fetch.py --sync
    python gong_fetch.py --sync --dry-run
    python gong_fetch.py --sync --since 2025-01-01
    python gong_fetch.py --sync --since 2025-01-01 --until 2025-03-31

    # Manual fetch by title pattern (targeted use)
    python gong_fetch.py --account "Applied Research Associates"
    python gong_fetch.py --account "Applied Research Associates" --title-pattern "Applied Research Associates,ARA"
    python gong_fetch.py --account "Applied Research Associates" --title-pattern "Applied Research Associates,ARA" --dry-run

    # Schema inspection
    python gong_fetch.py --schema

    # Routing config management
    python gong_fetch.py --init-routing
    python gong_fetch.py --add-alias ventura-foods "Ventura Foods"
    python gong_fetch.py --add-alias ventura-foods "VF"
    python gong_fetch.py --show-routing

    # Owner config
    python gong_fetch.py --init-owners   # scaffold gong_owners.json
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Load .env from repo root if present
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        if _line.strip() and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

try:
    from tech_stack_update import update_tech_stack
    TECH_STACK_AVAILABLE = True
except ImportError:
    TECH_STACK_AVAILABLE = False

try:
    from three_whys_update import update_3_whys
    THREE_WHYS_AVAILABLE = True
except ImportError:
    THREE_WHYS_AVAILABLE = False

TABLE = "grafanalabs-data-marts.mrt_core.brk_gong_calls"

OWNERS_FILE = Path(__file__).parent / "gong_owners.json"


def _load_owner_ids() -> dict:
    """Load owner name→id mapping from gong_owners.json, or return empty dict."""
    if not OWNERS_FILE.exists():
        return {}
    try:
        return json.loads(OWNERS_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"WARNING: Could not load {OWNERS_FILE}: {e}", file=sys.stderr)
        return {}


OWNER_IDS = _load_owner_ids()
OWNER_ID_TO_NAME = {v: k for k, v in OWNER_IDS.items()}


def is_phone_call(title: str) -> bool:
    """Return True for auto-titled phone calls like 'Call with John Smith'."""
    return bool(re.match(r"(?i)^call with\b", title or ""))


def slugify(title: str) -> str:
    """Convert a call title to a dash-separated filename-safe string."""
    slug = re.sub(r"[^\w\s-]", "", title)   # strip non-word chars (keep letters, digits, spaces, dashes)
    slug = re.sub(r"\s+", "-", slug.strip()) # spaces → dashes
    slug = re.sub(r"-{2,}", "-", slug)       # collapse multiple dashes
    return slug[:100]                         # cap length


def unique_filename(gong_dir: Path, filename: str) -> str:
    """Return filename unchanged if it doesn't exist, otherwise append _2, _3, etc."""
    if not (gong_dir / filename).exists():
        return filename
    stem = filename[:-3] if filename.endswith(".md") else filename
    i = 2
    while True:
        candidate = f"{stem}_{i}.md"
        if not (gong_dir / candidate).exists():
            return candidate
        i += 1


# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------

def get_bq_client():
    try:
        from google.cloud import bigquery
        return bigquery.Client(project="grafanalabs-global")
    except Exception as e:
        print(f"ERROR: Failed to initialize BigQuery client: {e}")
        print("Hint: run `gcloud auth application-default login` to authenticate.")
        sys.exit(1)


def cmd_schema(client):
    sql = """
    SELECT column_name, data_type, ordinal_position
    FROM `grafanalabs-data-marts.mrt_core.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'brk_gong_calls'
    ORDER BY ordinal_position
    """
    try:
        rows = list(client.query(sql).result())
    except Exception as e:
        print(f"ERROR: Schema query failed: {e}")
        sys.exit(1)

    print(f"{'#':<5} {'Column':<40} {'Type'}")
    print("-" * 70)
    for row in rows:
        print(f"{row.ordinal_position:<5} {row.column_name:<40} {row.data_type}")


def fetch_calls_by_owners(client, owner_ids, since, until, limit):
    from google.cloud import bigquery

    since_clause = "AND call_ended_at >= @since" if since else ""
    until_clause = "AND call_ended_at <= @until" if until else ""

    sql = f"""
    SELECT DISTINCT
        pkey_id,
        call_title,
        call_ended_at,
        gong_call_url,
        owner_id,
        call_spotlight_brief,
        TO_JSON_STRING(transcript_text) AS transcript_json
    FROM `{TABLE}`
    WHERE owner_id IN UNNEST(@owner_ids)
      {since_clause}
      {until_clause}
    ORDER BY call_ended_at DESC
    LIMIT @limit
    """

    params = [
        bigquery.ArrayQueryParameter("owner_ids", "STRING", owner_ids),
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
    ]
    if since:
        params.append(bigquery.ScalarQueryParameter("since", "TIMESTAMP", since))
    if until:
        params.append(bigquery.ScalarQueryParameter("until", "TIMESTAMP", until))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    try:
        return list(client.query(sql, job_config=job_config).result())
    except Exception as e:
        print(f"ERROR: Fetch query failed: {e}")
        sys.exit(1)


def fetch_calls_by_title(client, patterns, since, until, limit):
    from google.cloud import bigquery

    pattern_conditions = " OR ".join(
        f"REGEXP_CONTAINS(call_title, @p{i})" for i in range(len(patterns))
    )
    since_clause = "AND call_ended_at >= @since" if since else ""
    until_clause = "AND call_ended_at <= @until" if until else ""

    sql = f"""
    SELECT DISTINCT
        pkey_id,
        call_title,
        call_ended_at,
        gong_call_url,
        owner_id,
        call_spotlight_brief,
        TO_JSON_STRING(transcript_text) AS transcript_json
    FROM `{TABLE}`
    WHERE ({pattern_conditions})
      {since_clause}
      {until_clause}
    ORDER BY call_ended_at DESC
    LIMIT @limit
    """

    params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    for i, pattern in enumerate(patterns):
        params.append(bigquery.ScalarQueryParameter(f"p{i}", "STRING", f"(?i)\\b{re.escape(pattern)}\\b"))
    if since:
        params.append(bigquery.ScalarQueryParameter("since", "TIMESTAMP", since))
    if until:
        params.append(bigquery.ScalarQueryParameter("until", "TIMESTAMP", until))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    try:
        return list(client.query(sql, job_config=job_config).result())
    except Exception as e:
        print(f"ERROR: Fetch query failed: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Customer detection
# ---------------------------------------------------------------------------

def list_customer_dirs(customers_dir: Path) -> list[Path]:
    if not customers_dir.exists():
        return []
    return [d for d in customers_dir.iterdir() if d.is_dir() and not d.name.startswith(("_", "."))]



def load_routing(customer_dirs: list[Path]) -> dict[str, list[str]]:
    """
    For each customer dir, load aliases from gong_routing.json if present.
    Returns dict mapping dir name → list of alias patterns.
    """
    routing: dict[str, list[str]] = {}
    for d in customer_dirs:
        config_path = d / "gong_routing.json"
        if config_path.exists():
            try:
                data = json.loads(config_path.read_text())
                aliases = data.get("aliases", [])
                if isinstance(aliases, list):
                    routing[d.name] = [str(a) for a in aliases]
            except Exception:
                pass
    return routing


def detect_customer(call_title: str, customer_dirs: list[Path], routing: dict[str, list[str]] | None = None) -> tuple[str | None, str]:
    """
    Returns (customer_dir_name, strategy_description).
    Returns (None, reason) if no match found.

    Priority:
    1. Routing config aliases (gong_routing.json word-boundary match)
    2. Auto-derived pattern from dir name (hyphen→space, title-case)
    3. Dir name scan (raw dir name word-boundary match)
    4. No match → None
    """
    if routing is None:
        routing = {}

    dir_names_with_routing = set(routing.keys())

    # Strategy 1: routing config aliases
    for dir_name, aliases in routing.items():
        for alias in aliases:
            if re.search(rf"(?i)\b{re.escape(alias)}\b", call_title):
                return dir_name, f"routing alias '{alias}' matched"

    # Strategy 2: auto-derived pattern (hyphen→space, title-case) for dirs WITHOUT routing config
    for d in customer_dirs:
        if d.name in dir_names_with_routing:
            continue
        derived = d.name.replace("-", " ").title()
        if re.search(rf"(?i)\b{re.escape(derived)}\b", call_title):
            return d.name, f"auto-derived pattern '{derived}' matched"

    # Strategy 3: raw dir name word-boundary scan
    for d in customer_dirs:
        if d.name in dir_names_with_routing:
            continue
        if re.search(rf"\b{re.escape(d.name)}\b", call_title, re.IGNORECASE):
            return d.name, f"dir name '{d.name}' found in title"

    return None, "no customer pattern matched title"


# ---------------------------------------------------------------------------
# Transcript rendering
# ---------------------------------------------------------------------------

def render_transcript(transcript_json: str) -> str:
    try:
        data = json.loads(transcript_json)
    except (json.JSONDecodeError, TypeError):
        return f"[Could not parse transcript JSON]\n\n```\n{transcript_json[:2000]}\n```"

    if not isinstance(data, list):
        return f"[Unexpected transcript shape — raw dump]\n\n```json\n{json.dumps(data, indent=2)[:3000]}\n```"

    lines = []
    for entry in data:
        if not isinstance(entry, dict):
            continue

        # Shape A: {speaker, sentences: [{text}]}
        if "speaker" in entry and "sentences" in entry:
            speaker = entry["speaker"] or "Unknown"
            text = " ".join(s.get("text", "") for s in entry["sentences"] if isinstance(s, dict))
            if text.strip():
                lines.append(f"**{speaker}:** {text.strip()}")

        # Shape B: {speakerName, words: [{text}]}
        elif "speakerName" in entry and "words" in entry:
            speaker = entry["speakerName"] or "Unknown"
            text = " ".join(w.get("text", "") for w in entry["words"] if isinstance(w, dict))
            if text.strip():
                lines.append(f"**{speaker}:** {text.strip()}")

        # Shape C: {role, content}
        elif "role" in entry and "content" in entry:
            role = entry["role"] or "Unknown"
            content = entry["content"] or ""
            if content.strip():
                lines.append(f"**{role}:** {content.strip()}")

        else:
            lines.append(f"[Unknown entry shape]\n```json\n{json.dumps(entry)[:500]}\n```")

    if not lines:
        return f"[No renderable content found — raw dump]\n\n```json\n{json.dumps(data, indent=2)[:3000]}\n```"

    return "\n\n".join(lines)


def format_call_md(row, owner_name: str, fetched_at: str) -> str:
    call_date = ""
    if row.call_ended_at:
        if hasattr(row.call_ended_at, "strftime"):
            call_date = row.call_ended_at.strftime("%Y-%m-%d")
        else:
            call_date = str(row.call_ended_at)[:10]

    url = row.gong_call_url or ""
    url_cell = f"[{url}]({url})" if url else "N/A"

    try:
        transcript_md = render_transcript(row.transcript_json)
    except Exception as e:
        transcript_md = f"[Error rendering transcript: {e}]\n\nRaw:\n```\n{str(row.transcript_json)[:1000]}\n```"

    return f"""# {row.call_title or 'Untitled Call'}

| Field | Value |
|---|---|
| **Call ID** | `{row.pkey_id}` |
| **Date** | {call_date} |
| **Owner** | {owner_name} |
| **Gong URL** | {url_cell} |
| **Fetched** | {fetched_at} |

---

## Transcript

{transcript_md}
"""


# ---------------------------------------------------------------------------
# Manifest / sync state
# ---------------------------------------------------------------------------

def load_manifest(gong_dir: Path) -> dict:
    manifest_path = gong_dir / "manifest.json"
    if manifest_path.exists():
        try:
            return json.loads(manifest_path.read_text())
        except Exception:
            return {}
    return {}


def save_manifest(gong_dir: Path, manifest: dict):
    tmp = gong_dir / ".manifest.json.tmp"
    final = gong_dir / "manifest.json"
    tmp.write_text(json.dumps(manifest, indent=2, default=str))
    tmp.rename(final)


def load_sync_state(customers_dir: Path) -> dict:
    path = customers_dir / ".gong_sync.json"
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}
    return {}


def save_sync_state(customers_dir: Path, state: dict):
    tmp = customers_dir / ".gong_sync.json.tmp"
    final = customers_dir / ".gong_sync.json"
    tmp.write_text(json.dumps(state, indent=2, default=str))
    tmp.rename(final)


def append_fetch_log(customers_dir: Path, entry: dict):
    log_path = customers_dir / ".fetch_log.jsonl"
    try:
        with log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except Exception as e:
        print(f"WARNING: Could not write fetch log: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Manual fetch (by title pattern → specific account dir)
# ---------------------------------------------------------------------------

def run_fetch(args):
    client = get_bq_client()
    customers_dir = Path(args.customers_dir)
    job_id = f"account_{args.account}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    patterns = [p.strip() for p in args.title_pattern.split(",")] if args.title_pattern else [args.account]
    print(f"Searching call titles for: {patterns}")

    rows = fetch_calls_by_title(client, patterns=patterns, since=args.since, until=args.until, limit=args.limit)

    if not rows:
        print(f"\nNo calls found. Try adjusting --title-pattern.")
        return

    if len(rows) == args.limit:
        print(f"WARNING: Fetched exactly {args.limit} calls (the limit). Older calls may have been omitted. Use --since or --limit to adjust.\n")
    rows = [r for r in rows if not is_phone_call(r.call_title)]
    print(f"Found {len(rows)} call(s) (phone calls excluded).\n")

    if args.dry_run:
        print(f"[DRY RUN] Would write to: {customers_dir / args.account / 'gong'}/")
        for row in rows:
            date_str = row.call_ended_at.strftime("%Y-%m-%d") if row.call_ended_at and hasattr(row.call_ended_at, "strftime") else ""
            owner_name = OWNER_ID_TO_NAME.get(str(row.owner_id), row.owner_id or "Unknown")
            filename = f"{date_str}_{slugify(row.call_title or 'untitled')}.md"
            print(f"  {filename}  (owner: {owner_name})")
        return

    account_dir = customers_dir / args.account
    if not account_dir.exists():
        print(f"WARNING: {account_dir} does not exist — creating it.")
    gong_dir = account_dir / "gong"
    gong_dir.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest(gong_dir)
    calls_index = {c["pkey_id"]: c for c in manifest.get("calls", [])}
    known_ids = set(calls_index.keys())

    job_started_at = datetime.now(timezone.utc).isoformat()
    new_count = 0
    skipped_count = 0

    for row in rows:
        pkey_id = str(row.pkey_id)
        if pkey_id in known_ids and not args.force:
            skipped_count += 1
            continue

        owner_name = OWNER_ID_TO_NAME.get(str(row.owner_id), row.owner_id or "Unknown")
        date_str = row.call_ended_at.strftime("%Y-%m-%d") if row.call_ended_at and hasattr(row.call_ended_at, "strftime") else ""
        filename = f"{date_str}_{slugify(row.call_title or 'untitled')}.md"
        fetched_at = datetime.now(timezone.utc).isoformat()

        try:
            md_content = format_call_md(row, owner_name, fetched_at)
        except Exception as e:
            print(f"WARNING: Failed to format call {pkey_id}: {e}")
            md_content = f"# Error rendering call {pkey_id}\n\nRaw:\n```\n{str(row.transcript_json)[:2000]}\n```"

        filename = unique_filename(gong_dir, filename)
        filepath = gong_dir / filename
        filepath.write_text(md_content, encoding="utf-8")
        calls_index[pkey_id] = {
            "pkey_id": pkey_id,
            "call_title": row.call_title or "",
            "call_ended_at": date_str,
            "owner": owner_name,
            "file": filename,
            "transcript_chars": len(md_content),
            "call_spotlight_brief": (row.call_spotlight_brief or "")[:300],
            "fetched_at": fetched_at,
            "fetch_mode": "account",
            "job_id": job_id,
        }
        append_fetch_log(customers_dir, {
            "job_id": job_id,
            "ts": fetched_at,
            "fetch_mode": "account",
            "pkey_id": pkey_id,
            "call_title": row.call_title or "",
            "call_ended_at": date_str,
            "customer": args.account,
            "filename": filename,
            "transcript_chars": len(md_content),
        })
        new_count += 1
        print(f"  Wrote: {filename}")
        if TECH_STACK_AVAILABLE:
            update_tech_stack(filepath, account_dir)
        if THREE_WHYS_AVAILABLE:
            update_3_whys(filepath, account_dir)

    manifest = {
        "account": args.account,
        "last_fetched": job_started_at,
        "fetch_params": {"patterns": patterns, "since": args.since, "limit": args.limit},
        "total_calls": len(calls_index),
        "calls": list(calls_index.values()),
    }
    save_manifest(gong_dir, manifest)
    print(f"\nDone. {new_count} new, {skipped_count} skipped.")


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

def run_sync(args):
    if not OWNER_IDS:
        print(f"ERROR: No owners configured. Run `python3 gong_fetch.py --init-owners` to create {OWNERS_FILE}, then add your name and Gong owner ID.")
        sys.exit(1)

    client = get_bq_client()
    customers_dir = Path(args.customers_dir)
    job_id = f"sync_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    owner_ids = list(OWNER_IDS.values())

    since = args.since
    if not since:
        since = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        print(f"No --since provided; defaulting to 30 days ago ({since})")

    print(f"Fetching calls for: {', '.join(OWNER_IDS.keys())}")
    rows = fetch_calls_by_owners(client, owner_ids, since=since, until=args.until, limit=args.limit)

    if not rows:
        print("No calls found.")
        return

    if len(rows) == args.limit:
        print(f"WARNING: Fetched exactly {args.limit} calls (the limit). Older calls in this window may have been omitted. Use --since to narrow the window or --limit to raise the cap.\n")

    print(f"Found {len(rows)} call(s) total.\n")

    # Load global sync state to skip already-processed calls
    sync_state = load_sync_state(customers_dir)
    raw_ids = sync_state.get("processed_ids", {})
    # Migrate legacy list format → dict {pkey_id: date_str}
    if isinstance(raw_ids, list):
        today = datetime.now(timezone.utc).date().isoformat()
        raw_ids = {pid: today for pid in raw_ids}
    # Prune IDs older than 90 days (sync window is 30 days, 90 gives comfortable buffer)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
    processed_ids: dict = {pid: dt for pid, dt in raw_ids.items() if dt >= cutoff}

    customer_dirs = list_customer_dirs(customers_dir)
    routing = load_routing(customer_dirs)

    job_started_at = datetime.now(timezone.utc).isoformat()
    new_count = 0
    skipped_count = 0
    unmatched_count = 0

    # Accumulate writes per customer: {customer_name: [(row, pkey_id, date_str, filename, md_content, fetched_at)]}
    writes: dict[str, list] = {}

    for row in rows:
        pkey_id = str(row.pkey_id)

        if pkey_id in processed_ids and not args.force:
            skipped_count += 1
            continue

        if is_phone_call(row.call_title):
            skipped_count += 1
            continue

        owner_name = OWNER_ID_TO_NAME.get(str(row.owner_id), row.owner_id)
        customer, strategy = detect_customer(row.call_title or "", customer_dirs, routing)
        dest = customer if customer else "_unmatched/unprocessed"

        date_str = ""
        if row.call_ended_at and hasattr(row.call_ended_at, "strftime"):
            date_str = row.call_ended_at.strftime("%Y-%m-%d")
        filename = f"{date_str}_{slugify(row.call_title or 'untitled')}.md"

        if args.dry_run:
            status = f"→ {dest}/" if customer else f"→ _unmatched/  [{strategy}]"
            print(f"  {filename}  {status}")
            print(f"    {row.call_title or 'Untitled'}  (owner: {owner_name})")
        else:
            fetched_at = datetime.now(timezone.utc).isoformat()
            try:
                md_content = format_call_md(row, owner_name, fetched_at)
            except Exception as e:
                print(f"WARNING: Failed to format call {pkey_id}: {e}")
                md_content = f"# Error rendering call {pkey_id}\n\nRaw:\n```\n{str(row.transcript_json)[:2000]}\n```"

            if dest not in writes:
                writes[dest] = []
            writes[dest].append((row, pkey_id, date_str, filename, md_content, fetched_at))

        if not customer:
            unmatched_count += 1
        new_count += 1

    if args.dry_run:
        print(f"\n[DRY RUN] {new_count} new, {skipped_count} skipped, {unmatched_count} unmatched.")
        return

    # Write files and update manifests
    for dest, call_list in writes.items():
        gong_dir = customers_dir / dest / "gong"
        gong_dir.mkdir(parents=True, exist_ok=True)

        manifest = load_manifest(gong_dir)
        calls_index = {c["pkey_id"]: c for c in manifest.get("calls", [])}

        for row, pkey_id, date_str, filename, md_content, fetched_at in call_list:
            filename = unique_filename(gong_dir, filename)
            filepath = gong_dir / filename
            filepath.write_text(md_content, encoding="utf-8")
            calls_index[pkey_id] = {
                "pkey_id": pkey_id,
                "call_title": row.call_title or "",
                "call_ended_at": date_str,
                "owner": OWNER_ID_TO_NAME.get(str(row.owner_id), row.owner_id),
                "file": filename,
                "transcript_chars": len(md_content),
                "call_spotlight_brief": (row.call_spotlight_brief or "")[:300],
                "fetched_at": fetched_at,
                "fetch_mode": "sync",
                "job_id": job_id,
            }
            append_fetch_log(customers_dir, {
                "job_id": job_id,
                "ts": fetched_at,
                "fetch_mode": "sync",
                "pkey_id": pkey_id,
                "call_title": row.call_title or "",
                "call_ended_at": date_str,
                "customer": dest,
                "filename": filename,
                "transcript_chars": len(md_content),
            })
            processed_ids[pkey_id] = date_str
            print(f"  [{dest}] {filename}  —  {row.call_title or 'Untitled'}")
            is_matched = not dest.startswith("_unmatched")
            if TECH_STACK_AVAILABLE and is_matched:
                update_tech_stack(filepath, customers_dir / dest)
            if THREE_WHYS_AVAILABLE and is_matched:
                update_3_whys(filepath, customers_dir / dest)

        manifest = {
            "account": dest,
            "last_fetched": job_started_at,
            "total_calls": len(calls_index),
            "calls": list(calls_index.values()),
        }
        save_manifest(gong_dir, manifest)

    # Save updated global sync state
    sync_state["last_sync"] = job_started_at
    sync_state["processed_ids"] = processed_ids
    save_sync_state(customers_dir, sync_state)

    print(f"\nDone. {new_count} new, {skipped_count} skipped, {unmatched_count} → _unmatched.")


# ---------------------------------------------------------------------------
# Nuke command
# ---------------------------------------------------------------------------

def cmd_nuke(customers_dir: Path):
    """Delete all fetched data, leaving directories and config intact."""
    print("This will permanently delete:")
    print(f"  - All transcript .md files in */gong/")
    print(f"  - All manifest.json files in */gong/")
    print(f"  - tech_stack.md, 3_whys_summary.md, 3_whys.json per customer")
    print(f"  - .gong_sync.json (sync state)")
    print()
    confirm = input("Type 'nuke' to confirm: ").strip()
    if confirm != "nuke":
        print("Aborted.")
        return

    deleted = 0

    # Sync state
    sync_state_path = customers_dir / ".gong_sync.json"
    if sync_state_path.exists():
        sync_state_path.unlink()
        print(f"  deleted  .gong_sync.json")
        deleted += 1

    # Fetch log
    fetch_log_path = customers_dir / ".fetch_log.jsonl"
    if fetch_log_path.exists():
        fetch_log_path.unlink()
        print(f"  deleted  .fetch_log.jsonl")
        deleted += 1

    # All dirs including _unmatched subdirs
    all_gong_dirs = list(customers_dir.rglob("gong"))
    for gong_dir in all_gong_dirs:
        if not gong_dir.is_dir():
            continue
        for f in gong_dir.iterdir():
            if f.suffix == ".md" or f.name == "manifest.json":
                f.unlink()
                print(f"  deleted  {f.relative_to(customers_dir)}")
                deleted += 1

    # Per-customer enrichment files
    for customer_dir in list_customer_dirs(customers_dir):
        for fname in ("tech_stack.md", "3_whys_summary.md", "3_whys.json"):
            f = customer_dir / fname
            if f.exists():
                f.unlink()
                print(f"  deleted  {f.relative_to(customers_dir)}")
                deleted += 1

    print()
    print(f"Done. {deleted} files deleted. Directories and gong_routing.json preserved.")


# ---------------------------------------------------------------------------
# Owner management commands
# ---------------------------------------------------------------------------

def cmd_init_owners():
    """Scaffold gong_owners.json if it doesn't already exist."""
    if OWNERS_FILE.exists():
        print(f"{OWNERS_FILE} already exists.")
        print("Edit it directly to add or remove owners.")
        return
    template = {"Your Name": "your-gong-owner-id"}
    OWNERS_FILE.write_text(json.dumps(template, indent=2), encoding="utf-8")
    print(f"Created {OWNERS_FILE}")
    print("Edit it to add your name and Gong owner ID.")
    print("See README.md for how to find your owner ID in BigQuery.")


# ---------------------------------------------------------------------------
# Routing management commands
# ---------------------------------------------------------------------------

def cmd_init_routing(customers_dir: Path):
    """Auto-generate gong_routing.json for every customer dir that doesn't have one."""
    dirs = list_customer_dirs(customers_dir)
    if not dirs:
        print(f"No customer directories found in {customers_dir}")
        return
    created = 0
    skipped = 0
    for d in sorted(dirs, key=lambda x: x.name):
        config_path = d / "gong_routing.json"
        if config_path.exists():
            print(f"  SKIP  {d.name}  (already has gong_routing.json)")
            skipped += 1
        else:
            derived = d.name.replace("-", " ").title()
            config = {"aliases": [derived]}
            config_path.write_text(json.dumps(config, indent=2))
            print(f"  CREATE {d.name}  → {config_path}  aliases: {config['aliases']}")
            created += 1
    print(f"\nDone. {created} created, {skipped} skipped.")


def cmd_add_alias(customers_dir: Path, dir_name: str, alias: str):
    """Append an alias to a customer's gong_routing.json, creating it if needed."""
    target = customers_dir / dir_name
    if not target.exists():
        print(f"ERROR: Directory not found: {target}")
        sys.exit(1)
    config_path = target / "gong_routing.json"
    if config_path.exists():
        try:
            data = json.loads(config_path.read_text())
        except Exception:
            data = {}
    else:
        data = {}
    aliases = data.get("aliases", [])
    if alias in aliases:
        print(f"Alias '{alias}' already exists for '{dir_name}' — no change.")
        return
    aliases.append(alias)
    data["aliases"] = aliases
    config_path.write_text(json.dumps(data, indent=2))
    print(f"Added alias '{alias}' to {dir_name}/gong_routing.json  →  aliases: {aliases}")


def cmd_show_routing(customers_dir: Path):
    """Print the current routing table for all customer dirs."""
    dirs = list_customer_dirs(customers_dir)
    if not dirs:
        print(f"No customer directories found in {customers_dir}")
        return
    routing = load_routing(dirs)
    for d in sorted(dirs, key=lambda x: x.name):
        if d.name in routing:
            aliases = routing[d.name]
            print(f"  {d.name:<30} → {aliases}")
        else:
            derived = d.name.replace("-", " ").title()
            print(f"  {d.name:<30} → [auto: \"{derived}\"]")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Sync Gong call transcripts by owner, auto-routed to customer directories."
    )
    parser.add_argument("--schema", action="store_true", help="Print table columns and exit")
    parser.add_argument("--sync", action="store_true", help="Fetch new calls by owner and auto-route to customer dirs")
    parser.add_argument("--account", default=None, help="Manually fetch calls for a specific customer dir by title pattern")
    parser.add_argument("--title-pattern", default=None, help="Comma-separated title patterns for --account mode (e.g. 'Applied Research Associates,ARA')")
    parser.add_argument("--since", default=None, help="ISO date (YYYY-MM-DD) — lower bound on call_ended_at")
    parser.add_argument("--until", default=None, help="ISO date (YYYY-MM-DD) — upper bound on call_ended_at")
    parser.add_argument("--limit", type=int, default=1000, help="Max calls to fetch (default: 1000)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be written without writing")
    parser.add_argument("--force", action="store_true", help="Re-process already-synced calls")
    parser.add_argument(
        "--customers-dir",
        default=str(Path.home() / "customers"),
        help="Base path for customer directories",
    )
    parser.add_argument("--init-routing", action="store_true", help="Auto-generate gong_routing.json for all customer dirs")
    parser.add_argument("--add-alias", nargs=2, metavar=("DIR", "PATTERN"), help="Append a routing alias to a customer dir")
    parser.add_argument("--show-routing", action="store_true", help="Print the current routing table for all customer dirs")
    parser.add_argument("--init-owners", action="store_true", help=f"Scaffold {OWNERS_FILE.name} with a template entry")
    parser.add_argument("--nuke", action="store_true", help="Delete all fetched transcripts, manifests, enrichment files, and sync state (keeps dirs and config)")

    args = parser.parse_args()

    customers_dir = Path(args.customers_dir)

    if args.nuke:
        cmd_nuke(customers_dir)
        return

    if args.init_owners:
        cmd_init_owners()
        return

    if args.init_routing:
        cmd_init_routing(customers_dir)
        return

    if args.add_alias:
        cmd_add_alias(customers_dir, args.add_alias[0], args.add_alias[1])
        return

    if args.show_routing:
        cmd_show_routing(customers_dir)
        return

    if args.schema:
        client = get_bq_client()
        cmd_schema(client)
        return

    if args.since:
        try:
            datetime.strptime(args.since, "%Y-%m-%d")
        except ValueError:
            parser.error(f"--since must be in YYYY-MM-DD format, got: {args.since}")
    if args.until:
        try:
            datetime.strptime(args.until, "%Y-%m-%d")
        except ValueError:
            parser.error(f"--until must be in YYYY-MM-DD format, got: {args.until}")

    if args.account or args.sync:
        if not os.environ.get("ANTHROPIC_API_KEY"):
            print("ERROR: ANTHROPIC_API_KEY is not set. Enrichment requires this key.")
            print("Set it with: export ANTHROPIC_API_KEY=your-key-here")
            sys.exit(1)

    if args.account:
        run_fetch(args)
    elif args.sync:
        run_sync(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
