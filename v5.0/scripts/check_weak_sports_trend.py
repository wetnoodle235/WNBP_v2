#!/usr/bin/env python3
"""CI regression guard: fail if weak sports critical count worsened vs. last run.

Compares the two most recent weak_sports_feature_analysis_*.json reports.
Exits 0 (pass) when:
  - fewer than 2 reports exist (first run — nothing to compare)
  - critical_count stayed the same or improved
  - only warnings added (no new criticals)
Exits 3 (fail) when critical_count increased vs. the previous report.
Exits 4 (fail) when a sport that was not critical before is now critical.

Exit codes chosen to not collide with the analyzer's exit 2.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _load_report(path: Path) -> dict:
    return json.loads(path.read_text())


def _critical_sports(report: dict) -> set[str]:
    return {
        f["sport"]
        for f in report.get("findings", [])
        if f.get("severity") == "critical"
    }


def _report_candidates(reports_dir: Path) -> tuple[list[Path], str]:
    history_candidates = sorted((reports_dir / "history").glob("weak_sports_feature_analysis_*.json"))
    if len(history_candidates) >= 2:
        return history_candidates, "history"

    daily_candidates = sorted(reports_dir.glob("weak_sports_feature_analysis_*.json"))
    return daily_candidates, "daily"


def main() -> int:
    parser = argparse.ArgumentParser(description="Check weak sports trend for CI regression")
    parser.add_argument("--data-dir", default="data", help="Path to data directory")
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="Print regression info but exit 0 (non-blocking mode)",
    )
    args = parser.parse_args()

    reports_dir = Path(args.data_dir) / "reports"
    candidates, source = _report_candidates(reports_dir)

    if len(candidates) < 2:
        print(f"check_weak_sports_trend: only one {source} report found — skipping regression check.")
        return 0

    prev_path, curr_path = candidates[-2], candidates[-1]
    prev = _load_report(prev_path)
    curr = _load_report(curr_path)

    prev_count = int(prev.get("critical_count", 0))
    curr_count = int(curr.get("critical_count", 0))
    prev_critical = _critical_sports(prev)
    curr_critical = _critical_sports(curr)
    new_critical = curr_critical - prev_critical

    print(f"check_weak_sports_trend: source={source} {prev_path.name} → {curr_path.name}")
    print(f"  critical_count: {prev_count} → {curr_count}")

    if curr_critical:
        print(f"  currently critical: {sorted(curr_critical)}")
    if new_critical:
        print(f"  newly critical (regression): {sorted(new_critical)}")

    regressions: list[str] = []
    if curr_count > prev_count:
        regressions.append(
            f"critical_count increased: {prev_count} → {curr_count}"
        )
    if new_critical:
        regressions.append(
            f"new critical sports vs. previous run: {sorted(new_critical)}"
        )

    if not regressions:
        delta = curr_count - prev_count
        symbol = "↓" if delta < 0 else ("=" if delta == 0 else "↑")
        print(f"  trend: {symbol}  OK (no regression)")
        return 0

    for r in regressions:
        print(f"  REGRESSION: {r}", file=sys.stderr)

    if args.warn_only:
        print("check_weak_sports_trend: regression detected but --warn-only; continuing.")
        return 0

    return 3


if __name__ == "__main__":
    raise SystemExit(main())
