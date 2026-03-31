#!/usr/bin/env python3
"""Generate a compact markdown summary from the latest weak sports analysis report.

Writes ``data/reports/weak_sports_summary_<date>.md``.
Designed to be posted as a PR comment artifact or attached to CI runs.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any


_SEVERITY_ICON = {
    "critical": "🔴",
    "warning": "🟡",
    "info": "🟢",
}


def _latest_report(reports_dir: Path) -> Path | None:
    files = _report_candidates(reports_dir)
    return files[-1] if files else None


def _report_candidates(reports_dir: Path) -> list[Path]:
    history_files = sorted((reports_dir / "history").glob("weak_sports_feature_analysis_*.json"))
    if history_files:
        return history_files
    return sorted(reports_dir.glob("weak_sports_feature_analysis_*.json"))


def _previous_report(reports_dir: Path) -> Path | None:
    files = _report_candidates(reports_dir)
    return files[-2] if len(files) >= 2 else None


def _critical_sports(payload: dict[str, Any]) -> set[str]:
    return {
        str(f.get("sport", ""))
        for f in payload.get("findings", [])
        if f.get("severity") == "critical"
    }


def _trend_summary(payload: dict[str, Any], previous_payload: dict[str, Any] | None) -> dict[str, Any]:
    current_count = int(payload.get("critical_count", 0))
    if previous_payload is None:
        return {
            "label": "n/a",
            "detail": "First available snapshot",
            "new_critical": [],
            "resolved_critical": [],
        }

    previous_count = int(previous_payload.get("critical_count", 0))
    delta = current_count - previous_count
    new_critical = sorted(_critical_sports(payload) - _critical_sports(previous_payload))
    resolved_critical = sorted(_critical_sports(previous_payload) - _critical_sports(payload))

    if delta < 0:
        label = "↓ improved"
    elif delta > 0:
        label = "↑ regressed"
    else:
        label = "= stable"

    detail = f"Critical count {previous_count} → {current_count}"
    return {
        "label": label,
        "detail": detail,
        "new_critical": new_critical,
        "resolved_critical": resolved_critical,
    }


def _render(payload: dict[str, Any], previous_payload: dict[str, Any] | None = None) -> str:
    report_date = payload.get("date", date.today().isoformat())
    generated_at = str(payload.get("generated_at", report_date))
    critical_count = int(payload.get("critical_count", 0))
    warning_count = int(payload.get("warning_count", 0))
    findings = payload.get("findings", [])
    trend = _trend_summary(payload, previous_payload)

    lines: list[str] = [
        f"## Weak Sports Feature Analysis — {report_date}",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Generated At | {generated_at} |",
        f"| 🔴 Critical | {critical_count} |",
        f"| 🟡 Warning | {warning_count} |",
        f"| Trend | {trend['label']} ({trend['detail']}) |",
        "",
    ]

    if trend["new_critical"]:
        lines.append(f"New critical sports: {', '.join(s.upper() for s in trend['new_critical'])}")
        lines.append("")
    if trend["resolved_critical"]:
        lines.append(f"Resolved critical sports: {', '.join(s.upper() for s in trend['resolved_critical'])}")
        lines.append("")

    if not findings:
        lines.append("_No findings._")
        return "\n".join(lines)

    # Sort: criticals first, then by sport name
    findings_sorted = sorted(
        findings,
        key=lambda f: (0 if f.get("severity") == "critical" else 1, f.get("sport", "")),
    )

    lines += [
        "### Per-Sport Findings",
        "",
        "| Sport | Severity | Rows | High Missing | Near Constant | Leakage | Top Fix |",
        "|-------|----------|-----:|-------------:|--------------:|--------:|---------|",
    ]

    for f in findings_sorted:
        sport = str(f.get("sport", "")).upper()
        sev = str(f.get("severity", "info"))
        icon = _SEVERITY_ICON.get(sev, "⚪")
        rows = f.get("rows", 0)
        hm = len(f.get("high_missing_columns", []))
        nc = len(f.get("near_constant_columns", []))
        lk = len(f.get("leakage_like_columns", []))
        top_fix = ""
        candidates = f.get("top_fix_candidates", [])
        if candidates:
            c = candidates[0]
            top_fix = f"`{c.get('column', '')}` ({c.get('issue', '')})"
        lines.append(
            f"| {sport} | {icon} {sev} | {rows:,} | {hm} | {nc} | {lk} | {top_fix} |"
        )

    lines += [""]

    # Expanded details for critical/warning sports only
    detail_sports = [f for f in findings_sorted if f.get("severity") in ("critical", "warning")]
    if detail_sports:
        lines += ["### Remediation Details", ""]
        for f in detail_sports:
            sport = str(f.get("sport", "")).upper()
            sev = str(f.get("severity", "info"))
            icon = _SEVERITY_ICON.get(sev, "⚪")
            lines.append(f"#### {icon} {sport}")

            recs = f.get("recommendations", [])
            hints = f.get("remediation_hints", [])
            candidates = f.get("top_fix_candidates", [])

            if recs:
                lines.append("")
                lines.append("**Issues detected:**")
                for r in recs:
                    lines.append(f"- {r}")

            if hints:
                lines.append("")
                lines.append("**Remediation hints:**")
                for h in hints:
                    lines.append(f"- {h}")

            if candidates:
                lines.append("")
                lines.append("**Top fix candidates:**")
                lines.append("")
                lines.append("| Column | Family | Issue | Score | Action |")
                lines.append("|--------|--------|-------|------:|--------|")
                for c in candidates:
                    col = c.get("column", "")
                    fam = c.get("family", "")
                    issue = c.get("issue", "")
                    score = c.get("score", 0)
                    action = c.get("recommended_action", "")
                    lines.append(f"| `{col}` | {fam} | {issue} | {score:.1f} | {action} |")

            lines.append("")

    lines.append(
        f"_Generated {date.today().isoformat()} by `generate_weak_sports_summary.py`_"
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate markdown PR summary from weak sports report")
    parser.add_argument("--data-dir", default="data", help="Path to data directory")
    parser.add_argument("--output", default="", help="Override output path")
    args = parser.parse_args()

    reports_dir = Path(args.data_dir) / "reports"
    report_path = _latest_report(reports_dir)
    if report_path is None:
        print("No weak sports report found; run weak_sports_feature_analysis.py first.")
        return 1

    payload = json.loads(report_path.read_text())
    previous_path = _previous_report(reports_dir)
    previous_payload = json.loads(previous_path.read_text()) if previous_path else None
    md = _render(payload, previous_payload)

    out_path = Path(args.output) if args.output else (
        reports_dir / f"weak_sports_summary_{date.today().isoformat()}.md"
    )
    out_path.write_text(md)
    print(f"Summary written: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
