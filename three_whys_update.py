"""
three_whys_update.py — Maintain a 3_whys_summary.md and 3_whys.json for a customer from Gong transcripts.

Usage:
    python3 three_whys_update.py --transcript path/to/call.md --customer-dir path/to/customer/
    python3 three_whys_update.py --transcript path/to/call.md --customer-dir path/to/customer/ --dry-run
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

SECTION_KEYS = ["Why Grafana?", "Why Now?", "Why Anything?"]

SECTION_DESCRIPTIONS = {
    "Why Grafana?": "*Why would this customer want Grafana over competitive or incumbent solutions?*",
    "Why Now?": "*What compelling events make this a now decision rather than a later decision?*",
    "Why Anything?": "*What pain makes the status quo untenable?*",
}

# Maps markdown section key → JSON field name
JSON_KEY_MAP = {
    "Why Grafana?": "why_grafana",
    "Why Now?": "why_now",
    "Why Anything?": "why_anything",
}

SYSTEM_PROMPT = """\
You are a sales intelligence extractor. Extract evidence for three sales qualification
questions from a Gong call transcript. Return only the structured markdown block described
— no prose, no code fences.

IMPORTANT: Only include evidence and quotes from the customer's side. Do NOT include
quotes or paraphrased statements from Grafana Labs employees (AEs, SEs, or anyone
representing Grafana). If a Grafana employee says something that prompts a customer
response, attribute the insight to the customer's response only."""

USER_PROMPT_TEMPLATE = """\
## Transcript

{transcript}

## Call metadata

Title: {call_title}
Date: {call_date}
Gong URL: {gong_url}

## Instructions

Extract evidence from this transcript for each of the three Whys below.

The bar for inclusion is HIGH. Default to omitting a section. Only include a bullet or quote
if the evidence is clear and direct — not inferred, not borderline, not "could be interpreted
as." If you are uncertain whether something qualifies, leave it out.

For each Why where compelling evidence exists, return:
- Up to 5 bullet summary points (only bullets with strong, unambiguous evidence)
- Up to 5 verbatim quotes with speaker attribution (only quotes that directly support the Why)
- If there is no compelling evidence for a Why in this call, omit that section entirely

DEFINITIONS — apply strictly:

**Why Grafana?** — The customer expressed a clear positive signal toward Grafana specifically:
  - They said they want a specific Grafana capability or feature
  - They expressed that Grafana is better suited to their needs than alternatives
  - They proactively sought out or prioritized Grafana
  EXCLUDE: competitor pain points, multi-cloud requirements, evaluation framing ("we're
  comparing vendors"), or interest in Oracle/AWS/Azure support that does not explicitly
  credit Grafana. If the customer has not yet formed a preference for Grafana, omit this
  section entirely — an early-stage evaluation with no stated preference is not evidence.

**Why Now?** — A specific event, deadline, or trigger is forcing a decision now rather than
  later: contract renewals, migration deadlines, fiscal year timing, incidents, leadership
  mandates. EXCLUDE: general interest or vague future intent with no concrete timeline.

**Why Anything?** — The customer described pain or a gap in their current state that makes
  doing nothing untenable: operational failures, scale problems, tool limitations, business
  risk, incumbent tool gaps. Competitor weaknesses that reveal what the customer has struggled
  with belong here, not in Why Grafana?.

Return exactly this markdown structure (only include Whys where evidence exists):

#### {call_title} — {call_date} ([Gong]({gong_url}))

##### Why Grafana?
- bullet summary 1 ([{call_title}]({gong_url}))
- bullet summary 2 ([{call_title}]({gong_url}))

> "{{quote}}" — {{Speaker}} ([{call_title}]({gong_url}))
> "{{quote}}" — {{Speaker}} ([{call_title}]({gong_url}))

##### Why Now?
- ... ([{call_title}]({gong_url}))

> "..." — {{Speaker}} ([{call_title}]({gong_url}))

##### Why Anything?
- ... ([{call_title}]({gong_url}))

> "..." — {{Speaker}} ([{call_title}]({gong_url}))"""

SYNTHESIS_PROMPT_TEMPLATE = """\
Based on the customer evidence below, write a 2-3 sentence synthesis for each Why section
that summarizes the cumulative signal across all calls. Be direct and specific — write as
if briefing a salesperson before a call. Use present tense.

If a section has no evidence (empty calls list), write exactly: No signal captured yet.

Return only valid JSON with this exact structure (no code fences, no extra keys):
{{"why_grafana": "...", "why_now": "...", "why_anything": "..."}}

Evidence:
{evidence}"""


# ---------------------------------------------------------------------------
# File initialization
# ---------------------------------------------------------------------------

def _init_3_whys(name: str) -> str:
    """Return the initial content for a new 3_whys_summary.md."""
    sections = []
    for key in SECTION_KEYS:
        desc = SECTION_DESCRIPTIONS[key]
        sections.append(f"## {key}\n{desc}")
    return f"# 3 Whys — {name}\n\n" + "\n\n---\n\n".join(sections) + "\n"


def _init_3_whys_json(name: str) -> dict:
    """Return the initial structure for a new 3_whys.json."""
    return {
        "customer": name,
        "last_updated": "",
        "why_grafana": {"synthesis": "", "calls": []},
        "why_now": {"synthesis": "", "calls": []},
        "why_anything": {"synthesis": "", "calls": []},
    }


# ---------------------------------------------------------------------------
# Transcript parsing
# ---------------------------------------------------------------------------

def _parse_transcript_metadata(transcript: str) -> tuple[str, str, str]:
    """Parse call title, date, and Gong URL from a transcript markdown file."""
    title_match = re.search(r"^#\s+(.+)$", transcript, re.MULTILINE)
    call_title = title_match.group(1).strip() if title_match else "Unknown Call"

    date_match = re.search(r"\|\s*\*\*Date\*\*\s*\|\s*(\S+)\s*\|", transcript)
    call_date = date_match.group(1).strip() if date_match else ""

    # Match markdown link format: | **Gong URL** | [url](url) |
    url_match = re.search(r"\|\s*\*\*Gong URL\*\*\s*\|\s*\[([^\]]+)\]\(([^\)]+)\)", transcript)
    if url_match:
        gong_url = url_match.group(2).strip()
    else:
        # Fallback: bare URL
        url_match2 = re.search(r"\|\s*\*\*Gong URL\*\*\s*\|\s*(https?://\S+)\s*\|", transcript)
        gong_url = url_match2.group(1).strip() if url_match2 else ""

    return call_title, call_date, gong_url


# ---------------------------------------------------------------------------
# Claude response parsing
# ---------------------------------------------------------------------------

def _parse_claude_sections(response: str) -> dict[str, str]:
    """
    Parse Claude's response into per-Why content blocks.

    Returns {"Why Grafana?": "#### call header\n\n##### Why Grafana?\n...", ...}
    Each value is a self-contained block ready to append under its ## section.
    """
    h4_match = re.search(r"^(####\s+.+)$", response, re.MULTILINE)
    h4_header = h4_match.group(1).strip() if h4_match else "#### Unknown Call"

    result = {}
    # Split on ##### Why X? headers — alternating [preamble, header, content, ...]
    parts = re.split(r"^(#####\s+Why (?:Grafana|Now|Anything)\?)\s*$", response, flags=re.MULTILINE)

    for i in range(1, len(parts), 2):
        h5_header = parts[i].strip()          # "##### Why Grafana?"
        content = parts[i + 1].strip() if i + 1 < len(parts) else ""
        key = h5_header.lstrip("#").strip()   # "Why Grafana?"
        if content:
            result[key] = f"{h4_header}\n\n{content}"

    return result


def _extract_structured_data(
    call_blocks: dict[str, str],
    call_title: str,
    call_date: str,
    gong_url: str,
) -> dict:
    """
    Parse bullets and quotes out of call_blocks for JSON storage.

    Returns {"Why Grafana?": {"call_title": ..., "bullets": [...], "quotes": [...]}, ...}
    """
    result = {}
    for why_key, block in call_blocks.items():
        bullets = []
        for m in re.finditer(r"^- (.+)$", block, re.MULTILINE):
            text = m.group(1)
            # Strip trailing ([Call Title](url))
            text = re.sub(r"\s+\(\[[^\]]*\]\([^\)]*\)\)\s*$", "", text).strip()
            if text:
                bullets.append(text)

        quotes = []
        for m in re.finditer(r'^> "(.+?)" — (.+?)\s+\(\[', block, re.MULTILINE):
            quotes.append({"text": m.group(1), "speaker": m.group(2).strip()})

        result[why_key] = {
            "call_title": call_title,
            "call_date": call_date,
            "gong_url": gong_url,
            "bullets": bullets,
            "quotes": quotes,
        }
    return result


# ---------------------------------------------------------------------------
# JSON sidecar
# ---------------------------------------------------------------------------

def load_3_whys_json(customer_dir: Path) -> dict:
    json_path = customer_dir / "3_whys.json"
    if json_path.exists():
        try:
            return json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return _init_3_whys_json(customer_dir.name)


def save_3_whys_json(customer_dir: Path, data: dict):
    json_path = customer_dir / "3_whys.json"
    tmp = customer_dir / ".3_whys.json.tmp"
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.rename(json_path)


# ---------------------------------------------------------------------------
# Synthesis
# ---------------------------------------------------------------------------

def _generate_syntheses(client, json_data: dict) -> dict[str, str]:
    """
    Make a second Claude call to generate/update a 2-3 sentence synthesis per Why section,
    based on all accumulated call evidence in json_data.
    """
    evidence = {
        key: json_data[key]["calls"]
        for key in ["why_grafana", "why_now", "why_anything"]
    }
    prompt = SYNTHESIS_PROMPT_TEMPLATE.format(evidence=json.dumps(evidence, indent=2))

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        # Strip code fences if the model wraps in them anyway
        text = re.sub(r"^```(?:json)?\s*", "", text).strip()
        text = re.sub(r"\s*```$", "", text).strip()
        return json.loads(text)
    except Exception as e:
        print(f"WARNING: Synthesis generation failed: {e}", file=sys.stderr)
        return {}


def _update_syntheses_in_content(content: str, syntheses: dict[str, str], date_str: str) -> str:
    """
    Insert or replace the synthesis line in each ## Why section of the markdown.
    Synthesis appears immediately after the section description line.
    """
    parts = re.split(r"\n---\n", content)
    result_parts = []

    for part in parts:
        key = next((k for k in SECTION_KEYS if f"## {k}" in part), None)
        if key:
            json_key = JSON_KEY_MAP[key]
            synthesis_text = syntheses.get(json_key, "").strip()
            if synthesis_text:
                synthesis_line = f"> **Synthesis ({date_str}):** {synthesis_text}"
                if re.search(r"^> \*\*Synthesis", part, re.MULTILINE):
                    # Replace existing synthesis line
                    part = re.sub(
                        r"^> \*\*Synthesis[^\n]*$",
                        synthesis_line,
                        part,
                        flags=re.MULTILINE,
                    )
                else:
                    # Insert after the italicised description line
                    part = re.sub(
                        r"(\*[^\n]+\*\n)",
                        f"\\1\n{synthesis_line}\n",
                        part,
                        count=1,
                    )
        result_parts.append(part)

    return "\n---\n".join(result_parts)


# ---------------------------------------------------------------------------
# Markdown content update
# ---------------------------------------------------------------------------

def _append_blocks_to_content(current_content: str, call_blocks: dict[str, str]) -> str:
    """Append call blocks into their respective ## Why sections."""
    if not call_blocks:
        return current_content

    parts = re.split(r"\n---\n", current_content)

    part_keys: list[str | None] = []
    for part in parts:
        matched = False
        for key in SECTION_KEYS:
            if f"## {key}" in part:
                part_keys.append(key)
                matched = True
                break
        if not matched:
            part_keys.append(None)

    result_parts = []
    for part, key in zip(parts, part_keys):
        if key and key in call_blocks:
            part = part.rstrip() + "\n\n" + call_blocks[key] + "\n"
        result_parts.append(part)

    return "\n---\n".join(result_parts)


# ---------------------------------------------------------------------------
# Anthropic client
# ---------------------------------------------------------------------------

def get_anthropic_client():
    try:
        import anthropic
    except ImportError:
        print(
            "WARNING: anthropic package not installed. "
            "Run `pip install anthropic` to enable 3 Whys enrichment.",
            file=sys.stderr,
        )
        return None
    import os
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print(
            "WARNING: ANTHROPIC_API_KEY not set. 3 Whys enrichment skipped.",
            file=sys.stderr,
        )
        return None
    try:
        return anthropic.Anthropic()
    except Exception as e:
        print(f"WARNING: Failed to initialize Anthropic client: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def update_3_whys(transcript_path: Path, customer_dir: Path, dry_run: bool = False) -> bool:
    """
    Extract 3 Whys evidence from transcript_path, append to 3_whys_summary.md,
    update 3_whys.json, and regenerate per-section synthesis.

    Returns True if files were written (or would be in dry-run), False on skip/error.
    """
    client = get_anthropic_client()
    if client is None:
        return False

    try:
        transcript = transcript_path.read_text(encoding="utf-8")
    except Exception as e:
        print(f"WARNING: Could not read transcript {transcript_path}: {e}", file=sys.stderr)
        return False

    call_title, call_date, gong_url = _parse_transcript_metadata(transcript)

    summary_path = customer_dir / "3_whys_summary.md"
    if summary_path.exists():
        try:
            current_content = summary_path.read_text(encoding="utf-8")
        except Exception as e:
            print(f"WARNING: Could not read {summary_path}: {e}", file=sys.stderr)
            return False
    else:
        current_content = _init_3_whys(customer_dir.name)

    # Duplicate guard — skip if this transcript is already present
    if call_title in current_content or transcript_path.name in current_content:
        print(f"  [3-whys] Skipping {transcript_path.name} — already present in 3_whys_summary.md")
        return False

    # --- Call 1: extract per-call evidence ---
    user_prompt = USER_PROMPT_TEMPLATE.format(
        transcript=transcript,
        call_title=call_title,
        call_date=call_date,
        gong_url=gong_url,
    )
    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        claude_response = response.content[0].text.strip()
    except Exception as e:
        print(f"WARNING: Claude API call failed for {transcript_path.name}: {e}", file=sys.stderr)
        return False

    call_blocks = _parse_claude_sections(claude_response)

    if not call_blocks:
        print(f"  [3-whys] No evidence found in {transcript_path.name}")
        return False

    found_whys = list(call_blocks.keys())
    missing_whys = [k for k in SECTION_KEYS if k not in call_blocks]

    if dry_run:
        print(f"  [3-whys] DRY RUN — {transcript_path.name}")
        print(f"    Evidence for: {', '.join(found_whys)}")
        if missing_whys:
            print(f"    No evidence for: {', '.join(missing_whys)}")
        print()
        print(claude_response)
        return True

    # Update JSON sidecar with new call data
    json_data = load_3_whys_json(customer_dir)
    call_structured = _extract_structured_data(call_blocks, call_title, call_date, gong_url)
    for why_key, data in call_structured.items():
        json_key = JSON_KEY_MAP[why_key]
        json_data[json_key]["calls"].append(data)
    json_data["last_updated"] = datetime.now(timezone.utc).isoformat()

    # --- Call 2: regenerate synthesis across all calls ---
    syntheses = _generate_syntheses(client, json_data)
    for why_key in ["why_grafana", "why_now", "why_anything"]:
        if why_key in syntheses:
            json_data[why_key]["synthesis"] = syntheses[why_key]

    # Update markdown: append call blocks then refresh synthesis lines
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    updated_content = _append_blocks_to_content(current_content, call_blocks)
    updated_content = _update_syntheses_in_content(updated_content, syntheses, date_str)

    try:
        summary_path.write_text(updated_content, encoding="utf-8")
        save_3_whys_json(customer_dir, json_data)
        print(f"  [3-whys] Updated {summary_path} and 3_whys.json from {transcript_path.name}")
        print(f"    Evidence for: {', '.join(found_whys)}")
        if missing_whys:
            print(f"    No evidence for: {', '.join(missing_whys)}")
        return True
    except Exception as e:
        print(f"WARNING: Could not write outputs: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Maintain 3_whys_summary.md and 3_whys.json for a customer from a Gong transcript."
    )
    parser.add_argument("--transcript", required=True, help="Path to the transcript .md file")
    parser.add_argument("--customer-dir", required=True, help="Path to the customer directory")
    parser.add_argument("--dry-run", action="store_true", help="Print what would change without writing")
    args = parser.parse_args()

    transcript_path = Path(args.transcript).expanduser()
    customer_dir = Path(args.customer_dir).expanduser()

    if not transcript_path.exists():
        print(f"ERROR: Transcript not found: {transcript_path}")
        sys.exit(1)
    if not customer_dir.exists():
        print(f"ERROR: Customer directory not found: {customer_dir}")
        sys.exit(1)

    success = update_3_whys(transcript_path, customer_dir, dry_run=args.dry_run)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
