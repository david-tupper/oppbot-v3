"""
tech_stack_update.py — Enrich a customer's tech_stack.md from a Gong transcript.

Usage:
    python tech_stack_update.py --transcript path/to/call.md --customer-dir path/to/customer/
    python tech_stack_update.py --transcript path/to/call.md --customer-dir path/to/customer/ --dry-run
"""

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

EXTRACTION_SYSTEM_PROMPT = """\
You are a technical intelligence extractor. Extract tech stack observations from a Gong
call transcript. Return only a structured markdown block — no prose preamble, no code fences."""

MANUAL_EXTRACTION_SYSTEM_PROMPT = """\
You are a technical intelligence extractor. Extract tech stack observations from a manual
context note written by a Grafana AE. Return only a structured markdown block — no prose
preamble, no code fences.

IMPORTANT: This note was written by the Grafana AE directly — it is NOT a customer
transcript. Do NOT attribute observations to "Customer". Write bullets as third-person
factual statements about the customer's environment (e.g. "Customer uses X for Y")."""

EXTRACTION_USER_PROMPT_TEMPLATE = """\
## Transcript
{transcript}

## Call metadata
Title: {call_title}
Date: {call_date}
Gong URL: {gong_url}

## Instructions
Extract all tech stack facts mentioned or clearly implied in this transcript.
Organize them into whatever categories best fit what you found (e.g. Hosting, Databases,
Languages/Frameworks, Observability, Applications, Security, Networking, etc.).
Only include categories where you have something concrete to say.
For each category, write 1-3 specific fact bullets.

Also include an "Open questions" category for:
- \u2753 Anything unclear that warrants a follow-up question
- \u26a0\ufe0f Any apparent conflict with what you know

If there are no open questions, omit that category.
If no tech stack information is present in this transcript, return exactly: NO_TECH_FACTS

Return exactly this structure:

{header_line}

**{{Category}}**
- fact

**Open questions**
- \u2753 ..."""

SYNTHESIS_PROMPT_TEMPLATE = """\
Based on the tech stack observations below, write a 2-3 sentence factual inventory of the
technologies in use in this customer's environment and what they are used for. Focus on what
is present and how it is used — not on trials, evaluations, timelines, or business context.
Only include transitional details if a technology is actively being replaced or migrated away
from. Use present tense. Return ONLY the paragraph — no headers, no code fences.

Observations:
{all_call_blocks}"""


def get_anthropic_client():
    try:
        import anthropic
    except ImportError:
        print(
            "WARNING: anthropic package not installed. "
            "Run `pip install anthropic` to enable tech stack enrichment.",
            file=sys.stderr,
        )
        return None
    import os
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print(
            "WARNING: ANTHROPIC_API_KEY not set. Tech stack enrichment skipped.",
            file=sys.stderr,
        )
        return None
    try:
        return anthropic.Anthropic()
    except Exception as e:
        print(f"WARNING: Failed to initialize Anthropic client: {e}", file=sys.stderr)
        return None


def _init_tech_stack(name: str) -> str:
    return f"# Tech Stack \u2014 {name}\n"


def _parse_transcript_metadata(transcript: str) -> tuple[str, str, str]:
    """Parse call title, date, and Gong URL from a transcript markdown file.

    gong_url is optional — returns "" if the Gong URL row is absent.
    """
    title_match = re.search(r"^#\s+(.+)$", transcript, re.MULTILINE)
    call_title = title_match.group(1).strip() if title_match else "Unknown Call"

    date_match = re.search(r"\|\s*\*\*Date\*\*\s*\|\s*(\S+)\s*\|", transcript)
    call_date = date_match.group(1).strip() if date_match else ""

    url_match = re.search(r"\|\s*\*\*Gong URL\*\*\s*\|\s*\[([^\]]+)\]\(([^\)]+)\)", transcript)
    if url_match:
        gong_url = url_match.group(2).strip()
    else:
        url_match2 = re.search(r"\|\s*\*\*Gong URL\*\*\s*\|\s*(https?://\S+)\s*\|", transcript)
        gong_url = url_match2.group(1).strip() if url_match2 else ""

    return call_title, call_date, gong_url


def _extract_call_blocks(content: str) -> str:
    """Extract all #### call blocks from the file for synthesis input."""
    blocks = re.findall(r"(?:^|\n)(###.+?)(?=\n---|\Z)", content, re.DOTALL)
    return "\n\n---\n\n".join(b.strip() for b in blocks)


def _update_summary_line(content: str, summary_text: str, date_str: str) -> str:
    """Insert or replace the > **Summary** line near the top of the file."""
    summary_line = f"\n> **Summary ({date_str}):** {summary_text}\n"
    if re.search(r"^> \*\*Summary", content, re.MULTILINE):
        return re.sub(
            r"^> \*\*Summary[^\n]*$",
            summary_line.strip(),
            content,
            flags=re.MULTILINE,
        )
    # Insert after the first heading line
    return re.sub(r"(^# .+\n)", f"\\1{summary_line}", content, count=1)


def update_tech_stack(transcript_path: Path, customer_dir: Path, dry_run: bool = False, progress_callback=None) -> bool:
    """
    Extract tech stack facts from transcript_path and append a per-call block to
    customer_dir/tech_stack.md. Regenerates the summary line on each new call.

    Returns True if the file was written (or would be in dry-run), False on skip/error.
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

    tech_stack_path = customer_dir / "tech_stack.md"
    if tech_stack_path.exists():
        try:
            current_content = tech_stack_path.read_text(encoding="utf-8")
        except Exception as e:
            print(f"WARNING: Could not read {tech_stack_path}: {e}", file=sys.stderr)
            return False
    else:
        current_content = _init_tech_stack(customer_dir.name)

    # Duplicate guard
    if call_title in current_content or transcript_path.name in current_content:
        print(f"  [tech_stack] Skipping {transcript_path.name} — already present in tech_stack.md")
        return False

    # --- Call 1: extract per-call block ---
    # When gong_url is absent (manual entries), render header without a link
    if gong_url:
        header_line = f"### {call_title} \u2014 {call_date} ([Gong]({gong_url}))"
    else:
        header_line = f"### {call_title} \u2014 {call_date}"
    user_prompt = EXTRACTION_USER_PROMPT_TEMPLATE.format(
        transcript=transcript,
        call_title=call_title,
        call_date=call_date,
        gong_url=gong_url,
        header_line=header_line,
    )
    if progress_callback:
        progress_callback("Extracting tech stack facts")
    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=MANUAL_EXTRACTION_SYSTEM_PROMPT if not gong_url else EXTRACTION_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        extracted_block = response.content[0].text.strip()
    except Exception as e:
        print(f"WARNING: Claude API call failed for {transcript_path.name}: {e}", file=sys.stderr)
        return False

    if extracted_block == "NO_TECH_FACTS":
        print(f"  [tech_stack] No tech facts found in {transcript_path.name} — skipping")
        return False

    if dry_run:
        print(f"  [tech_stack] DRY RUN — extracted block from {transcript_path.name}:")
        print()
        print(extracted_block)
        return True

    # Insert block after header/summary section so newest appears first
    h3_match = re.search(r"^###", current_content, re.MULTILINE)
    if h3_match:
        updated_content = (
            current_content[: h3_match.start()]
            + extracted_block
            + "\n\n---\n\n"
            + current_content[h3_match.start():]
        )
    else:
        updated_content = current_content.rstrip() + "\n\n---\n\n" + extracted_block + "\n"

    # --- Call 2: regenerate summary ---
    if progress_callback:
        progress_callback("Updating tech stack summary")
    all_blocks = _extract_call_blocks(updated_content)
    synthesis_prompt = SYNTHESIS_PROMPT_TEMPLATE.format(all_call_blocks=all_blocks)
    try:
        synthesis_response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            messages=[{"role": "user", "content": synthesis_prompt}],
        )
        summary_text = synthesis_response.content[0].text.strip()
    except Exception as e:
        print(f"WARNING: Synthesis generation failed: {e}", file=sys.stderr)
        summary_text = ""

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if summary_text:
        updated_content = _update_summary_line(updated_content, summary_text, date_str)

    try:
        tech_stack_path.write_text(updated_content, encoding="utf-8")
        print(f"  [tech_stack] Updated {tech_stack_path} from {transcript_path.name}")
        return True
    except Exception as e:
        print(f"WARNING: Could not write {tech_stack_path}: {e}", file=sys.stderr)
        return False


def delete_entry(call_title: str, customer_dir: Path) -> bool:
    """
    Remove a call block from tech_stack.md and re-run synthesis on the remaining data.

    Finds the ### block for call_title and removes it, then regenerates the summary line.
    Returns True if a change was made, False if the entry was not found.
    """
    tech_stack_path = customer_dir / "tech_stack.md"
    if not tech_stack_path.exists():
        print(f"  [tech_stack] delete_entry: tech_stack.md not found")
        return False

    try:
        content = tech_stack_path.read_text(encoding="utf-8")
    except Exception as e:
        print(f"WARNING: Could not read {tech_stack_path}: {e}", file=sys.stderr)
        return False

    # Remove the ### block for this call_title (down to but not including next ### or ---)
    escaped = re.escape(call_title)
    new_content = re.sub(
        rf"(?m)^###\s+{escaped}[^\n]*\n(?:(?!^###(?!#)|^---).*\n)*",
        "",
        content,
    )
    if new_content == content:
        print(f"  [tech_stack] delete_entry: '{call_title}' not found — nothing to remove")
        return False

    # Clean up extra blank lines
    new_content = re.sub(r"\n{3,}", "\n\n", new_content)

    # Also strip the --- separator that preceded this block if it's now dangling.
    # Pattern: ---\n\n immediately followed by --- or end of content
    new_content = re.sub(r"---\n\n(?=---|\Z)", "", new_content)

    try:
        tech_stack_path.write_text(new_content, encoding="utf-8")
    except Exception as e:
        print(f"WARNING: Could not write {tech_stack_path}: {e}", file=sys.stderr)
        return False

    # Regenerate summary line if client available
    client = get_anthropic_client()
    if client is not None:
        all_blocks = _extract_call_blocks(new_content)
        if all_blocks.strip():
            synthesis_prompt = SYNTHESIS_PROMPT_TEMPLATE.format(all_call_blocks=all_blocks)
            try:
                synthesis_response = client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=512,
                    messages=[{"role": "user", "content": synthesis_prompt}],
                )
                summary_text = synthesis_response.content[0].text.strip()
                if summary_text:
                    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    new_content = _update_summary_line(new_content, summary_text, date_str)
                    tech_stack_path.write_text(new_content, encoding="utf-8")
            except Exception as e:
                print(f"WARNING: Synthesis regeneration failed: {e}", file=sys.stderr)

    print(f"  [tech_stack] Removed '{call_title}' from tech_stack.md")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Enrich a customer's tech_stack.md from a Gong transcript using Claude."
    )
    parser.add_argument("--transcript", required=True, help="Path to the transcript .md file")
    parser.add_argument("--customer-dir", required=True, help="Path to the customer directory (where tech_stack.md lives)")
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

    success = update_tech_stack(transcript_path, customer_dir, dry_run=args.dry_run)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
