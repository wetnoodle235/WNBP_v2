#!/usr/bin/env python3
"""Validate weak sports analysis report schema for CI stability."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


REQUIRED_TOP_LEVEL_KEYS = {
    "date",
    "sports_analyzed",
    "critical_count",
    "warning_count",
    "findings",
}

REQUIRED_FINDING_KEYS = {
    "sport",
    "rows",
    "cols",
    "severity",
    "high_missing_columns",
    "near_constant_columns",
    "leakage_like_columns",
    "target_balance",
    "recommendations",
    "remediation_hints",
    "top_fix_candidates",
}


def _latest_report(reports_dir: Path) -> Path | None:
    files = sorted(reports_dir.glob("weak_sports_feature_analysis_*.json"))
    return files[-1] if files else None


def _latest_csv(reports_dir: Path) -> Path | None:
    files = sorted(reports_dir.glob("weak_sports_feature_analysis_*.csv"))
    return files[-1] if files else None


def _validate_json(report_path: Path) -> None:
    payload = json.loads(report_path.read_text())

    missing_top = REQUIRED_TOP_LEVEL_KEYS - set(payload)
    if missing_top:
        raise AssertionError(f"Missing top-level keys: {sorted(missing_top)}")

    findings = payload.get("findings")
    if not isinstance(findings, list):
        raise AssertionError("findings must be a list")

    for idx, finding in enumerate(findings):
        if not isinstance(finding, dict):
            raise AssertionError(f"finding[{idx}] must be an object")

        missing = REQUIRED_FINDING_KEYS - set(finding)
        if missing:
            raise AssertionError(f"finding[{idx}] missing keys: {sorted(missing)}")

        if not isinstance(finding["remediation_hints"], list):
            raise AssertionError(f"finding[{idx}] remediation_hints must be a list")
        if not isinstance(finding["top_fix_candidates"], list):
            raise AssertionError(f"finding[{idx}] top_fix_candidates must be a list")
        _validate_top_fix_candidates(idx, finding["top_fix_candidates"])


_REQUIRED_CANDIDATE_KEYS = {"column", "family", "issue", "score", "recommended_action"}
_VALID_ISSUES = {
    "high_missingness",
    "near_constant",
    "high_missingness_and_near_constant",
}
_VALID_FAMILIES = {
    "odds_market",
    "roster_availability",
    "advanced_attack",
    "player_team_form",
    "matchup_context",
    "event_split",
    "efficiency_rate",
    "general",
}


def _validate_top_fix_candidates(finding_idx: int, candidates: list) -> None:
    for cidx, c in enumerate(candidates):
        if not isinstance(c, dict):
            raise AssertionError(
                f"finding[{finding_idx}].top_fix_candidates[{cidx}] must be an object"
            )
        missing = _REQUIRED_CANDIDATE_KEYS - set(c)
        if missing:
            raise AssertionError(
                f"finding[{finding_idx}].top_fix_candidates[{cidx}] missing keys: {sorted(missing)}"
            )
        issue = c.get("issue", "")
        if issue not in _VALID_ISSUES:
            raise AssertionError(
                f"finding[{finding_idx}].top_fix_candidates[{cidx}] unknown issue '{issue}'"
            )
        family = c.get("family", "")
        if family not in _VALID_FAMILIES:
            raise AssertionError(
                f"finding[{finding_idx}].top_fix_candidates[{cidx}] unknown family '{family}'"
            )
        score = c.get("score")
        if not isinstance(score, (int, float)):
            raise AssertionError(
                f"finding[{finding_idx}].top_fix_candidates[{cidx}] score must be numeric"
            )
        if not isinstance(c.get("column"), str) or not c["column"]:
            raise AssertionError(
                f"finding[{finding_idx}].top_fix_candidates[{cidx}] column must be a non-empty string"
            )


def _validate_csv(csv_path: Path) -> None:
    with open(csv_path, newline="") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames or []
        required_columns = {"date", "sport", "severity", "recommendation_1", "top_fix_1"}
        missing = required_columns - set(fieldnames)
        if missing:
            raise AssertionError(f"CSV missing columns: {sorted(missing)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate weak sports report schema")
    parser.add_argument("--data-dir", default="data", help="Path to data directory")
    args = parser.parse_args()

    reports_dir = Path(args.data_dir) / "reports"
    report_path = _latest_report(reports_dir)
    csv_path = _latest_csv(reports_dir)

    if report_path is None:
        raise SystemExit("No weak sports JSON report found")
    if csv_path is None:
        raise SystemExit("No weak sports CSV report found")

    _validate_json(report_path)
    _validate_csv(csv_path)
    print(f"weak_sports_report_validation: OK ({report_path.name}, {csv_path.name})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
