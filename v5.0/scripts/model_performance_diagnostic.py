#!/usr/bin/env python3
"""
Model Performance Diagnostic Tool

Analyzes backtest vs live accuracy gaps and identifies sports needing attention.
Generates structured reports highlighting:
  - Sports with low absolute accuracy (<65%)
  - Sports with large live/backtest gaps (>10%)
  - Sample size warnings (insufficient data)
  - Trend indicators (improving/declining)
"""

import json
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Any


@dataclass
class SportMetrics:
    sport: str
    backtest_acc: float
    backtest_brier: float
    backtest_n: int
    live_acc: float | None
    live_brier: float | None
    live_n: int | None
    live_date: str | None
    
    @property
    def gap(self) -> float | None:
        """Live vs Backtest accuracy gap (negative = worse performance)."""
        if self.live_acc is None:
            return None
        return self.live_acc - self.backtest_acc
    
    @property
    def is_weak(self) -> bool:
        """True if backtest accuracy is below 65% or sample is too small."""
        return self.backtest_acc < 0.65 or self.backtest_n < 50
    
    @property
    def has_large_gap(self) -> bool:
        """True if live accuracy dropped >10% from backtest."""
        if self.gap is None:
            return False
        return self.gap < -0.10
    
    @property
    def priority(self) -> int:
        """Score for priority (higher = more urgent): 0-100."""
        score = 0
        
        # Penalize low accuracy
        if self.backtest_acc < 0.55:
            score += 30
        elif self.backtest_acc < 0.65:
            score += 20
        
        # Penalize large accuracy gaps
        if self.gap is not None:
            if self.gap < -0.15:
                score += 30
            elif self.gap < -0.10:
                score += 20
            elif self.gap < -0.05:
                score += 10
        
        # Penalize small sample sizes
        if self.backtest_n < 50:
            score += 25
        elif self.backtest_n < 100:
            score += 10
        
        # Bonus for live data (shows model is in production)
        if self.live_acc is not None:
            score += 5
        
        return min(score, 100)


def load_backtest(path: Path) -> dict[str, Any]:
    """Load backtest report from JSON."""
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def load_accuracy(path: Path) -> dict[str, Any]:
    """Load accuracy report from JSON."""
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def analyze(backtest_path: Path = None, accuracy_path: Path = None) -> list[SportMetrics]:
    """Analyze models and return list of metrics."""
    
    if backtest_path is None:
        reports_dir = Path(__file__).parent.parent / "data" / "reports"
        # Use most recent backtest report
        backtest_files = sorted(reports_dir.glob("backtest_*.json"))
        backtest_path = backtest_files[-1] if backtest_files else None
    
    if accuracy_path is None:
        reports_dir = Path(__file__).parent.parent / "data" / "reports"
        # Use most recent accuracy report
        accuracy_files = sorted(reports_dir.glob("accuracy_*.json"))
        accuracy_path = accuracy_files[-1] if accuracy_files else None
    
    backtest = load_backtest(backtest_path) if backtest_path else {}
    accuracy = load_accuracy(accuracy_path) if accuracy_path else {}
    
    metrics = []
    
    # Backtest data (source of truth for baseline)
    backtest_sports = backtest.get("by_sport", {})
    for sport, bt_data in backtest_sports.items():
        # Live data (if available)
        live_sport_data = accuracy.get("by_sport", {}).get(sport)
        live_acc = live_sport_data["correct"] / live_sport_data["evaluated"] if live_sport_data and live_sport_data.get("evaluated") else None
        live_brier = accuracy.get("brier_score") if live_sport_data and len(accuracy.get("by_sport", {})) == 1 else None
        live_n = live_sport_data["evaluated"] if live_sport_data else None
        live_date = accuracy.get("date") if live_sport_data else None
        
        metrics.append(SportMetrics(
            sport=sport,
            backtest_acc=bt_data["accuracy"],
            backtest_brier=bt_data["avg_brier"],
            backtest_n=bt_data["total"],
            live_acc=live_acc,
            live_brier=live_brier,
            live_n=live_n,
            live_date=live_date,
        ))
    
    return sorted(metrics, key=lambda m: m.priority, reverse=True)


def format_report(metrics: list[SportMetrics]) -> str:
    """Generate formatted diagnostic report."""
    lines = [
        "╔════════════════════════════════════════════════════════════════════╗",
        "║  MODEL PERFORMANCE DIAGNOSTIC REPORT                              ║",
        "╚════════════════════════════════════════════════════════════════════╝",
        "",
        "🔴 CRITICAL ISSUES (Action Required):",
        "",
    ]
    
    critical = [m for m in metrics if m.priority >= 50]
    if not critical:
        lines.append("  ✓ No critical issues detected")
    else:
        for m in critical:
            status = "📉" if m.has_large_gap else "⚠️ "
            lines.append(f"  {status} {m.sport.upper():12} backtest:{m.backtest_acc*100:5.1f}%  " +
                        f"live:{m.live_acc*100:5.1f}%  gap:{m.gap*100 if m.gap else 0:+6.1f}%  " +
                        f"n={m.backtest_n}" + 
                        (f"  [Only {m.live_n} live samples]" if m.live_n and m.live_n < 10 else ""))
    
    lines += [
        "",
        "⚠️  WEAK PERFORMERS (Backtest Accuracy < 65%):",
        "",
    ]
    
    weak = [m for m in metrics if m.backtest_acc < 0.65]
    if not weak:
        lines.append("  ✓ All sports above 65% baseline")
    else:
        for m in weak:
            reason = []
            if m.backtest_acc < 0.55:
                reason.append("very_low_acc")
            if m.backtest_n < 50:
                reason.append("small_sample")
            reason_str = f"  ({', '.join(reason)})" if reason else ""
            lines.append(f"  • {m.sport.upper():12} acc={m.backtest_acc*100:5.1f}%  " +
                        f"brier={m.backtest_brier:.3f}  n={m.backtest_n}{reason_str}")
    
    lines += [
        "",
        "📊 LIVE DATA STATUS:",
        "",
    ]
    
    live_sports = [m for m in metrics if m.live_acc is not None]
    if not live_sports:
        lines.append("  ⓘ No live predictions evaluated yet")
    else:
        lines.append(f"  Evaluated: {', '.join([m.sport.upper() for m in live_sports])}")
        for m in live_sports:
            lines.append(f"    • {m.sport.upper():12} {m.live_n} samples  acc={m.live_acc*100:5.1f}%  " +
                        f"gap={m.gap*100:+6.1f}%  date={m.live_date}")
    
    lines += [
        "",
        "💡 RECOMMENDATIONS:",
        "",
    ]
    
    recs = set()
    for m in metrics:
        if m.sport in ["dota2", "lol"]:
            recs.add("• Data quality: Dota2 has ID mismatches (fixing schema), LoL has only 1 sample")
        if m.sport == "mlb" and m.gap and m.gap < -0.10:
            recs.add("• MLB: Investigate live/backtest gap (→18%); may indicate data distribution shift")
        if m.sport in ["csgo", "ufc"] and m.backtest_n < 100:
            recs.add("• Small field sports: Collect more samples (CSGO=15, UFC=103) before tuning")
        if m.backtest_acc < 0.55:
            recs.add(f"• {m.sport.upper()}: Add feature engineering or review training data")
    
    if not recs:
        lines.append("  ✓ All metrics within expected ranges")
    else:
        for rec in sorted(recs):
            lines.append(rec)
    
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    t0 = time.monotonic()
    metrics = analyze()
    report = format_report(metrics)
    print(report)
    
    # Save structured report
    reports_dir = Path(__file__).parent.parent / "data" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    report_file = reports_dir / f"model_perf_diagnostic_{date.today().isoformat()}.json"
    with open(report_file, "w") as f:
        json.dump({
            "date": date.today().isoformat(),
            "generated_at": datetime.now(tz=timezone.utc).isoformat(timespec="microseconds"),
            "analysis_duration_s": round(time.monotonic() - t0, 3),
            "critical_count": len([m for m in metrics if m.priority >= 50]),
            "weak_count": len([m for m in metrics if m.backtest_acc < 0.65]),
            "metrics": [
                {
                    "sport": m.sport,
                    "backtest_accuracy": round(m.backtest_acc, 4),
                    "backtest_brier": round(m.backtest_brier, 4),
                    "backtest_samples": m.backtest_n,
                    "live_accuracy": round(m.live_acc, 4) if m.live_acc else None,
                    "live_gap": round(m.gap, 4) if m.gap else None,
                    "live_samples": m.live_n,
                    "priority": m.priority,
                    "is_weak": m.is_weak,
                    "has_large_gap": m.has_large_gap,
                }
                for m in metrics
            ],
        }, f, indent=2)
    
    print(f"\n📄 Report saved: {report_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
