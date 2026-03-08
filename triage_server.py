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


@app.route('/api/markdown/tech-stack/<customer>')
def api_tech_stack(customer):
    path = CUSTOMERS_DIR / customer / 'tech_stack.md'
    if not path.exists():
        return jsonify({'error': 'not found', 'dir': str(CUSTOMERS_DIR / customer)}), 404
    return jsonify({'content': path.read_text(encoding='utf-8'), 'path': str(path), 'dir': str(path.parent)})


@app.route('/api/markdown/3-whys/<customer>')
def api_3_whys(customer):
    path = CUSTOMERS_DIR / customer / '3_whys_summary.md'
    if not path.exists():
        return jsonify({'error': 'not found', 'dir': str(CUSTOMERS_DIR / customer)}), 404
    return jsonify({'content': path.read_text(encoding='utf-8'), 'path': str(path), 'dir': str(path.parent)})


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


if __name__ == "__main__":
    print("Starting Gong Triage server on http://localhost:5555")
    app.run(host="127.0.0.1", port=5555, debug=False)
