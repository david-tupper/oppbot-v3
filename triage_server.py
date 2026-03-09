"""
triage_server.py — Flask backend for Gong triage UI.

Usage:
    python3 triage_server.py
"""

import json
import os
import shutil
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

# Load .env from repo root if present
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        if _line.strip() and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

try:
    from flask import Flask, jsonify, request, send_file
except ImportError:
    print("Flask is not installed. Run: pip install flask")
    sys.exit(1)

# In-memory job progress tracking: manual_id → {steps, done, error}
_job_status: dict = {}

try:
    from tech_stack_update import update_tech_stack, delete_entry as _ts_delete_entry
    TECH_STACK_AVAILABLE = True
except ImportError:
    TECH_STACK_AVAILABLE = False

try:
    from three_whys_update import update_3_whys, delete_entry as _3w_delete_entry
    THREE_WHYS_AVAILABLE = True
except ImportError:
    THREE_WHYS_AVAILABLE = False

CUSTOMERS_DIR = Path.home() / "customers"
UNMATCHED_GONG_DIR = CUSTOMERS_DIR / "_unmatched" / "unprocessed" / "gong"
PROCESSED_GONG_DIR = CUSTOMERS_DIR / "_unmatched" / "processed" / "gong"

app = Flask(__name__)


# ---------------------------------------------------------------------------
# Helpers (mirrored from gong_fetch.py)
# ---------------------------------------------------------------------------

def list_customer_dirs(customers_dir: Path) -> list[Path]:
    if not customers_dir.exists():
        return []
    return [d for d in customers_dir.iterdir() if d.is_dir() and not d.name.startswith(("_", "."))]


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



# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    html_path = Path(__file__).parent / "triage.html"
    return send_file(html_path)


@app.route("/api/calls")
def api_calls():
    manifest = load_manifest(UNMATCHED_GONG_DIR)
    calls = manifest.get("calls", [])
    calls.sort(key=lambda c: c.get("call_ended_at", ""), reverse=True)
    return jsonify(calls)


@app.route("/api/processed")
def api_processed():
    manifest = load_manifest(PROCESSED_GONG_DIR)
    calls = manifest.get("calls", [])
    calls.sort(key=lambda c: c.get("call_ended_at", ""), reverse=True)
    return jsonify(calls)


@app.route("/api/customers")
def api_customers():
    dirs = list_customer_dirs(CUSTOMERS_DIR)
    return jsonify(sorted(d.name for d in dirs))


def _load_manual_manifest(customer_dir: Path) -> list:
    """Load ~/customers/<customer>/manual/manifest.json, return list (empty if missing)."""
    manifest_path = customer_dir / "manual" / "manifest.json"
    if manifest_path.exists():
        try:
            return json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def _save_manual_manifest(customer_dir: Path, entries: list):
    manual_dir = customer_dir / "manual"
    manual_dir.mkdir(parents=True, exist_ok=True)
    tmp = manual_dir / ".manifest.json.tmp"
    final = manual_dir / "manifest.json"
    tmp.write_text(json.dumps(entries, indent=2), encoding="utf-8")
    tmp.rename(final)


@app.route('/api/markdown/tech-stack/<customer>')
def api_tech_stack(customer):
    path = CUSTOMERS_DIR / customer / 'tech_stack.md'
    if not path.exists():
        return jsonify({'error': 'not found', 'dir': str(CUSTOMERS_DIR / customer)}), 404
    entries = _load_manual_manifest(CUSTOMERS_DIR / customer)
    manual_entries = [
        {'manual_id': e['manual_id'], 'call_title': e['call_title']}
        for e in entries
        if 'tech-stack' in e.get('targets', [])
    ]
    return jsonify({
        'content': path.read_text(encoding='utf-8'),
        'path': str(path),
        'dir': str(path.parent),
        'manual_entries': manual_entries,
    })


@app.route('/api/markdown/3-whys/<customer>')
def api_3_whys(customer):
    path = CUSTOMERS_DIR / customer / '3_whys_summary.md'
    if not path.exists():
        return jsonify({'error': 'not found', 'dir': str(CUSTOMERS_DIR / customer)}), 404
    entries = _load_manual_manifest(CUSTOMERS_DIR / customer)
    manual_entries = [
        {'manual_id': e['manual_id'], 'call_title': e['call_title']}
        for e in entries
        if '3-whys' in e.get('targets', [])
    ]
    return jsonify({
        'content': path.read_text(encoding='utf-8'),
        'path': str(path),
        'dir': str(path.parent),
        'manual_entries': manual_entries,
    })


@app.route('/api/add-alias', methods=['POST'])
def api_add_alias():
    data = request.get_json(force=True)
    customer = data.get('customer')
    alias = data.get('alias', '').strip()
    if not customer or not alias:
        return jsonify({'error': 'customer and alias required'}), 400
    target = CUSTOMERS_DIR / customer
    if not target.exists():
        return jsonify({'error': f'Unknown customer: {customer}'}), 404
    config_path = target / 'gong_routing.json'
    if config_path.exists():
        try:
            cfg = json.loads(config_path.read_text())
        except Exception:
            cfg = {}
    else:
        cfg = {}
    aliases = cfg.get('aliases', [])
    if alias not in aliases:
        aliases.append(alias)
    cfg['aliases'] = aliases
    config_path.write_text(json.dumps(cfg, indent=2))
    return jsonify({'ok': True, 'aliases': aliases})


@app.route('/api/open-editor/<customer>')
def api_open_editor(customer):
    view = request.args.get('view', 'tech-stack')
    filename = 'tech_stack.md' if view == 'tech-stack' else '3_whys_summary.md'
    path = CUSTOMERS_DIR / customer / filename
    if not path.exists():
        return jsonify({'error': 'not found'}), 404
    subprocess.run(['open', str(path)], check=False)
    return jsonify({'ok': True})


@app.route("/api/route", methods=["POST"])
def api_route():
    import re as _re
    data = request.get_json(force=True)
    pkey_id = data.get("pkey_id")
    dest_dir = data.get("dest_dir")
    create = data.get("create", False)
    source = data.get("source", "unprocessed")

    if not pkey_id or not dest_dir:
        return jsonify({"error": "pkey_id and dest_dir required"}), 400

    # Validate dest_dir exists or create it
    customer_dirs = list_customer_dirs(CUSTOMERS_DIR)
    valid_names = {d.name for d in customer_dirs}
    if dest_dir not in valid_names:
        if not create:
            return jsonify({"error": f"Unknown dest_dir: {dest_dir}"}), 400
        if not _re.match(r'^[a-z0-9][a-z0-9-]*$', dest_dir):
            return jsonify({"error": "dest_dir must match ^[a-z0-9][a-z0-9-]*$"}), 400
        (CUSTOMERS_DIR / dest_dir).mkdir()

    # Resolve source dir
    src_gong_dir = PROCESSED_GONG_DIR if source == "processed" else UNMATCHED_GONG_DIR

    # Find call in source manifest
    src_manifest = load_manifest(src_gong_dir)
    src_calls = src_manifest.get("calls", [])
    call = next((c for c in src_calls if c["pkey_id"] == pkey_id), None)
    if call is None:
        return jsonify({"error": f"pkey_id not found: {pkey_id}"}), 404

    # Move file
    dest_gong_dir = CUSTOMERS_DIR / dest_dir / "gong"
    dest_gong_dir.mkdir(parents=True, exist_ok=True)

    src_file = src_gong_dir / call["file"]
    dst_file = dest_gong_dir / call["file"]

    if src_file.exists():
        shutil.move(str(src_file), str(dst_file))
    # If already moved, skip shutil.move but still update manifests

    # Remove from source manifest
    src_calls = [c for c in src_calls if c["pkey_id"] != pkey_id]
    src_manifest["calls"] = src_calls
    src_manifest["total_calls"] = len(src_calls)
    save_manifest(src_gong_dir, src_manifest)

    # Add to dest manifest
    dest_manifest = load_manifest(dest_gong_dir)
    dest_calls = dest_manifest.get("calls", [])
    dest_index = {c["pkey_id"]: c for c in dest_calls}
    dest_index[pkey_id] = call
    dest_manifest["account"] = dest_dir
    dest_manifest["calls"] = list(dest_index.values())
    dest_manifest["total_calls"] = len(dest_manifest["calls"])
    save_manifest(dest_gong_dir, dest_manifest)

    # Run enrichment in background so the UI doesn't hang on Claude API calls
    customer_dir = CUSTOMERS_DIR / dest_dir
    if TECH_STACK_AVAILABLE or THREE_WHYS_AVAILABLE:
        def enrich():
            if TECH_STACK_AVAILABLE:
                update_tech_stack(dst_file, customer_dir)
            if THREE_WHYS_AVAILABLE:
                update_3_whys(dst_file, customer_dir)
        threading.Thread(target=enrich, daemon=True).start()

    return jsonify({"ok": True})


@app.route("/api/skip", methods=["POST"])
def api_skip():
    data = request.get_json(force=True)
    pkey_id = data.get("pkey_id")
    if not pkey_id:
        return jsonify({"error": "pkey_id required"}), 400

    # Find call in unmatched manifest
    src_manifest = load_manifest(UNMATCHED_GONG_DIR)
    src_calls = src_manifest.get("calls", [])
    call = next((c for c in src_calls if c["pkey_id"] == pkey_id), None)
    if call is None:
        return jsonify({"error": f"pkey_id not found: {pkey_id}"}), 404

    # Move file to processed
    PROCESSED_GONG_DIR.mkdir(parents=True, exist_ok=True)
    src_file = UNMATCHED_GONG_DIR / call["file"]
    dst_file = PROCESSED_GONG_DIR / call["file"]
    if src_file.exists():
        shutil.move(str(src_file), str(dst_file))

    # Remove from unmatched manifest
    src_calls = [c for c in src_calls if c["pkey_id"] != pkey_id]
    src_manifest["calls"] = src_calls
    src_manifest["total_calls"] = len(src_calls)
    save_manifest(UNMATCHED_GONG_DIR, src_manifest)

    # Add to processed manifest
    proc_manifest = load_manifest(PROCESSED_GONG_DIR)
    proc_calls = proc_manifest.get("calls", [])
    proc_index = {c["pkey_id"]: c for c in proc_calls}
    proc_index[pkey_id] = call
    proc_manifest["account"] = "_unmatched/processed"
    proc_manifest["calls"] = list(proc_index.values())
    proc_manifest["total_calls"] = len(proc_manifest["calls"])
    save_manifest(PROCESSED_GONG_DIR, proc_manifest)

    return jsonify({"ok": True})


@app.route('/api/add-context-status/<manual_id>', methods=['GET'])
def api_add_context_status(manual_id):
    return jsonify(_job_status.get(manual_id, {"done": True, "steps": [], "error": "not found"}))


@app.route('/api/add-context/<customer>', methods=['POST'])
def api_add_context(customer):
    customer_dir = CUSTOMERS_DIR / customer
    if not customer_dir.exists():
        return jsonify({'error': f'Unknown customer: {customer}'}), 404

    data = request.get_json(force=True)
    title = (data.get('title') or '').strip()
    text = (data.get('text') or '').strip()
    targets = data.get('targets', ['3-whys', 'tech-stack'])

    if not text:
        return jsonify({'error': 'text is required'}), 400

    now = datetime.now(timezone.utc)
    manual_id = now.strftime('%Y%m%d_%H%M%S')
    date_str = now.strftime('%Y-%m-%d')
    datetime_str = now.strftime('%Y-%m-%d %H:%M')
    created_at = now.isoformat()

    display_title = title if title else 'Manual Entry'
    call_title = f"[Manual] {display_title} — {datetime_str}"

    manual_content = f"""# {call_title}

| Field | Value |
|---|---|
| **Date** | {date_str} |
| **Added** | {created_at} |
| **Source** | Manual Entry |

---

## Context

{text}
"""

    manual_dir = customer_dir / "manual"
    manual_dir.mkdir(parents=True, exist_ok=True)
    manual_file = manual_dir / f"{manual_id}.md"
    try:
        manual_file.write_text(manual_content, encoding="utf-8")
    except Exception as e:
        return jsonify({'error': f'Could not write file: {e}'}), 500

    # Update manifest
    entries = _load_manual_manifest(customer_dir)
    entries.append({
        'manual_id': manual_id,
        'call_title': call_title,
        'targets': targets,
        'created_at': created_at,
    })
    _save_manual_manifest(customer_dir, entries)

    # Build step list for progress tracking
    steps = [{"label": "Writing context file", "state": "done"}]
    if '3-whys' in targets and THREE_WHYS_AVAILABLE:
        steps.append({"label": "Extracting 3 Whys evidence", "state": "pending"})
        steps.append({"label": "Updating 3 Whys synthesis", "state": "pending"})
    if 'tech-stack' in targets and TECH_STACK_AVAILABLE:
        steps.append({"label": "Extracting tech stack facts", "state": "pending"})
        steps.append({"label": "Updating tech stack summary", "state": "pending"})
    _job_status[manual_id] = {"steps": steps, "done": False, "error": None}

    def make_callback(mid):
        def callback(label):
            job = _job_status.get(mid)
            if not job:
                return
            s = job["steps"]
            # Mark the currently active step done
            for step in s:
                if step["state"] == "active":
                    step["state"] = "done"
                    break
            # Mark the matching pending step active
            for step in s:
                if step["label"] == label and step["state"] == "pending":
                    step["state"] = "active"
                    break
        return callback

    # Spawn background enrichment
    def enrich():
        cb = make_callback(manual_id)
        try:
            if '3-whys' in targets and THREE_WHYS_AVAILABLE:
                update_3_whys(manual_file, customer_dir, progress_callback=cb)
            if 'tech-stack' in targets and TECH_STACK_AVAILABLE:
                update_tech_stack(manual_file, customer_dir, progress_callback=cb)
        except Exception as e:
            if manual_id in _job_status:
                _job_status[manual_id]["error"] = str(e)
        finally:
            if manual_id in _job_status:
                # Mark any remaining active/pending steps done
                for step in _job_status[manual_id]["steps"]:
                    if step["state"] in ("active", "pending"):
                        step["state"] = "done"
                _job_status[manual_id]["done"] = True

    threading.Thread(target=enrich, daemon=True).start()

    return jsonify({'status': 'processing', 'manual_id': manual_id})


@app.route('/api/delete-manual/<customer>/<manual_id>', methods=['DELETE'])
def api_delete_manual(customer, manual_id):
    customer_dir = CUSTOMERS_DIR / customer
    if not customer_dir.exists():
        return jsonify({'error': f'Unknown customer: {customer}'}), 404

    entries = _load_manual_manifest(customer_dir)
    entry = next((e for e in entries if e.get('manual_id') == manual_id), None)
    if entry is None:
        return jsonify({'error': f'manual_id not found: {manual_id}'}), 404

    call_title = entry['call_title']
    targets = entry.get('targets', [])

    # Remove from markdown files
    if '3-whys' in targets and THREE_WHYS_AVAILABLE:
        _3w_delete_entry(call_title, customer_dir)
    if 'tech-stack' in targets and TECH_STACK_AVAILABLE:
        _ts_delete_entry(call_title, customer_dir)

    # Remove the manual file
    manual_file = customer_dir / "manual" / f"{manual_id}.md"
    try:
        manual_file.unlink(missing_ok=True)
    except Exception as e:
        print(f"WARNING: Could not remove {manual_file}: {e}", file=sys.stderr)

    # Remove from manifest
    entries = [e for e in entries if e.get('manual_id') != manual_id]
    _save_manual_manifest(customer_dir, entries)

    return jsonify({'status': 'ok'})


if __name__ == "__main__":
    print("Starting Gong Triage server on http://localhost:5555")
    app.run(host="127.0.0.1", port=5555, debug=False)
