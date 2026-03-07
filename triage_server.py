"""
triage_server.py — Flask backend for Gong triage UI.

Usage:
    python3 triage_server.py
"""

import json
import shutil
import sys
from pathlib import Path

try:
    from flask import Flask, jsonify, request, send_file
except ImportError:
    print("Flask is not installed. Run: pip install flask")
    sys.exit(1)

CUSTOMERS_DIR = Path("/Users/davidtupper/customers")
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
