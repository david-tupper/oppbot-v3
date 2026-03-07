"""
tech_stack_update.py — Enrich a customer's tech_stack.yaml from a Gong transcript.

Usage:
    python tech_stack_update.py --transcript path/to/call.md --customer-dir path/to/customer/
    python tech_stack_update.py --transcript path/to/call.md --customer-dir path/to/customer/ --dry-run
"""

import argparse
import sys
from pathlib import Path

TECH_STACK_TEMPLATE = """\
customer:
environment_profile:
  hosting:
    primary: # e.g. VMware vSphere, AWS EC2, bare metal
    secondary: # e.g. Kubernetes (~10% of workloads)
    also: # e.g. a few remaining physical servers
  platform_bias: # e.g. Microsoft-heavy enterprise stack, Java/Linux shop
  databases:
    primary: # e.g. Microsoft SQL Server, PostgreSQL, Oracle
    secondary:
      - # e.g. MySQL
      - # e.g. Redis
    analytics_platforms:
      - # e.g. Snowflake
      - # e.g. Databricks
  applications:
    nature: # e.g. Mostly COTS vendor apps with light customization, or custom microservices
    examples_named:
      - # App 1
      - # App 2
    access_constraints:
      - # e.g. Many apps are web-based; deep code-level instrumentation not possible
      - # e.g. Prefer visibility without needing application source code
  batch_and_workflows:
    critical_process: # e.g. Nightly end-of-day batch chain that closes daily trades
    implementation: # e.g. In-house Python scripts with cron orchestration
    monitoring_need: # e.g. Track step completion times, detect SLA breach, cascade impact
  observability_current_state:
    monitoring_tool_outgrown: # e.g. ManageEngine, Nagios, Datadog — what they're replacing/augmenting
    logging_security:
      primary: # e.g. Splunk for security
      secondary: # e.g. Elasticsearch, lightly used
  observability_goal_state:
    priorities:
      - # Priority 1 — e.g. Correlate end-user experience → app → DB → infra
      - # Priority 2 — e.g. Fast onboarding across acquisitions
      - # Priority 3
demo_app_requirements:
  must_show:
    - # e.g. Web UI user journey + login + key transaction
    - # e.g. Backend API with distributed traces
    - # e.g. Database dependency with SQL Server
    - # e.g. Nightly batch workflow with multi-step spans
  nice_to_show:
    - # Nice-to-have 1
    - # Nice-to-have 2
  avoid_overemphasis:
    - # e.g. Deep Kubernetes-first demo
    - # e.g. AI-first pitch if customer cares more about correlation
"""

SYSTEM_PROMPT = """\
You are a technical intelligence extractor. Your job is to update a customer tech_stack.yaml \
by extracting facts from a Gong call transcript and merging them according to strict rules.

You must return ONLY the complete updated YAML — no prose, no markdown fences, no explanation.
"""

USER_PROMPT_TEMPLATE = """\
## Current tech_stack.yaml

{current_yaml}

## Transcript

{transcript}

## Instructions

Extract tech stack facts from the transcript and merge them into the YAML above using these rules:

- **Blank field** (value is a `# e.g. ...` comment or empty): populate with the extracted value
- **Field has a value**: enrich if the transcript adds specificity (e.g. "SQL Server" → "SQL Server 2019"); \
do NOT overwrite with a less specific value
- **Contradiction** (transcript says something different than what's recorded): keep existing value AND add \
`# CONFLICT: transcript says "[new value]" — verify` as an inline comment on the same line
- **Ambiguous info** (transcript hints at something but is unclear): add \
`# UNCLEAR — ask: [specific follow-up question]` as an inline comment
- **New list items** (secondary databases, analytics platforms, must_show, etc.): append to the list if not \
already present
- If the transcript contains no relevant tech stack information, return the YAML unchanged

Return ONLY the complete updated YAML. No prose. No markdown code fences.
"""


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


def _diff_summary(old_yaml: str, new_yaml: str) -> list[str]:
    """Return a list of human-readable change descriptions."""
    old_lines = set(old_yaml.splitlines())
    new_lines = set(new_yaml.splitlines())
    added = [l.strip() for l in (new_lines - old_lines) if l.strip() and not l.strip().startswith("#")]
    removed = [l.strip() for l in (old_lines - new_lines) if l.strip() and not l.strip().startswith("#")]
    changes = []
    for line in added:
        changes.append(f"  + {line}")
    for line in removed:
        changes.append(f"  - {line}")
    return changes


def update_tech_stack(transcript_path: Path, customer_dir: Path, dry_run: bool = False) -> bool:
    """
    Enrich customer_dir/tech_stack.yaml from transcript_path using Claude.

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

    tech_stack_path = customer_dir / "tech_stack.yaml"
    if tech_stack_path.exists():
        try:
            current_yaml = tech_stack_path.read_text(encoding="utf-8")
        except Exception as e:
            print(f"WARNING: Could not read {tech_stack_path}: {e}", file=sys.stderr)
            return False
    else:
        current_yaml = TECH_STACK_TEMPLATE

    user_prompt = USER_PROMPT_TEMPLATE.format(
        current_yaml=current_yaml,
        transcript=transcript,
    )

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        updated_yaml = response.content[0].text.strip()
    except Exception as e:
        print(f"WARNING: Claude API call failed for {transcript_path.name}: {e}", file=sys.stderr)
        return False

    changes = _diff_summary(current_yaml, updated_yaml)

    if not changes:
        print(f"  [tech_stack] No changes from {transcript_path.name}")
        return False

    if dry_run:
        print(f"  [tech_stack] DRY RUN — changes from {transcript_path.name}:")
        for line in changes:
            print(f"    {line}")
        return True

    try:
        tech_stack_path.write_text(updated_yaml, encoding="utf-8")
        print(f"  [tech_stack] Updated {tech_stack_path} from {transcript_path.name} ({len(changes)} change(s))")
        for line in changes:
            print(f"    {line}")
        return True
    except Exception as e:
        print(f"WARNING: Could not write {tech_stack_path}: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Enrich a customer's tech_stack.yaml from a Gong transcript using Claude."
    )
    parser.add_argument("--transcript", required=True, help="Path to the transcript .md file")
    parser.add_argument("--customer-dir", required=True, help="Path to the customer directory (where tech_stack.yaml lives)")
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
