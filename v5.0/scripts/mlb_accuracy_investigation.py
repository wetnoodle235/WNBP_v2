#!/usr/bin/env python3
"""
MLB Model Accuracy Gap Investigation

Analyzes why live MLB predictions (41.7%) are significantly worse than 
backtest accuracy (57.4%). Investigates:
  - Feature availability and freshness
  - Data quality issues (missing player stats, injuries, etc.)
  - Schedule/game type effects (home/away, opponent strength)
  - Temporal patterns (improving/declining performance)
"""

import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd


@dataclass
class InvestigationResult:
    category: str
    finding: str
    severity: str  # critical, warning, info
    recommendation: str


def investigate_feature_coverage() -> list[InvestigationResult]:
    """Check if required MLfeatures are available and fresh."""
    results = []
    
    data_dir = Path(__file__).parent.parent / "data"
    mlb_features_dir = data_dir / "features"
    
    # Check for MLB feature files
    mlb_feature_files = list(mlb_features_dir.glob("mlb_*.parquet"))
    
    if not mlb_feature_files:
        results.append(InvestigationResult(
            category="Feature Availability",
            finding="No MLB feature parquets found",
            severity="critical",
            recommendation="Verify consolidator is parsing MLB data correctly"
        ))
        return results
    
    for fpath in sorted(mlb_feature_files):
        try:
            df = pd.read_parquet(fpath)
            
            # Check for nulls in key features
            null_pct = (df.isnull().sum() / len(df) * 100).sort_values(ascending=False)
            high_null_cols = null_pct[null_pct > 20]
            
            if len(high_null_cols) > 0:
                cols_str = ", ".join([f"{c}({pct:.0f}%)" for c, pct in high_null_cols.head(5).items()])
                results.append(InvestigationResult(
                    category="Feature Quality",
                    finding=f"High null rates in {fpath.name}: {cols_str}",
                    severity="warning",
                    recommendation="Check if historical data is missing for recent games"
                ))
            
            # Check data freshness
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                latest_date = df['date'].max()
                days_old = (date.today() - latest_date.date()).days if pd.notna(latest_date) else None
                
                if days_old is None or days_old > 7:
                    results.append(InvestigationResult(
                        category="Data Freshness",
                        finding=f"MLB features file {fpath.name} is {days_old or '?'} days old",
                        severity="warning" if days_old and days_old > 3 else "info",
                        recommendation="Verify daily pipeline is updating features regularly"
                    ))
            
            # Check coverage vs live predictions
            sample_size = len(df)
            if sample_size < 100:
                results.append(InvestigationResult(
                    category="Sample Size",
                    finding=f"Only {sample_size} samples in {fpath.name} - insufficient for training",
                    severity="critical",
                    recommendation="Backfill historical MLB data or increase look-back window"
                ))
        
        except Exception as e:
            results.append(InvestigationResult(
                category="File Error",
                finding=f"Error reading {fpath.name}: {str(e)[:80]}",
                severity="critical",
                recommendation="Check parquet file integrity and format"
            ))
    
    return results


def investigate_player_stats() -> list[InvestigationResult]:
    """Check MLB player-level stats availability."""
    results = []
    
    data_dir = Path(__file__).parent.parent / "data"
    player_stats_path = data_dir / "normalized" / "mlb" / "player_stats_2026.parquet"
    
    if not player_stats_path.exists():
        results.append(InvestigationResult(
            category="Player Stats",
            finding="MLB player_stats_2026.parquet not found",
            severity="critical",
            recommendation="Check if player stats are being imported and normalized"
        ))
        return results
    
    try:
        df = pd.read_parquet(player_stats_path)
        
        # Check column coverage
        expected_cols = ['player_id', 'game_id', 'date', 'team_id', 'kills', 'deaths']
        missing_cols = [c for c in expected_cols if c not in df.columns]
        if missing_cols:
            results.append(InvestigationResult(
                category="Player Stats Schema",
                finding=f"Missing columns: {', '.join(missing_cols)}",
                severity="warning",
                recommendation="Verify schema matches expected format for feature extraction"
            ))
        
        # Check date coverage
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            date_range = (df['date'].min(), df['date'].max())
            coverage_days = (date_range[1] - date_range[0]).days if pd.notna(date_range[0]) else None
            
            results.append(InvestigationResult(
                category="Data Coverage",
                finding=f"Player stats cover {coverage_days} days ({date_range[0].date()} to {date_range[1].date()})",
                severity="info",
                recommendation="Ensure recent 2026 games are included in feature extraction"
            ))
        
        # Check unique games/players
        unique_games = df['game_id'].nunique() if 'game_id' in df.columns else 0
        unique_players = df['player_id'].nunique() if 'player_id' in df.columns else 0
        
        results.append(InvestigationResult(
            category="Data Volume",
            finding=f"{len(df)} player-game records ({unique_players} unique players, {unique_games} unique games)",
            severity="info",
            recommendation="Verify game count matches expected MLB schedule (2016 games in 6-month period)"
        ))
    
    except Exception as e:
        results.append(InvestigationResult(
            category="File Error",
            finding=f"Error reading player_stats: {str(e)[:80]}",
            severity="critical",
            recommendation="Check data integrity and schema"
        ))
    
    return results


def analyze_accuracy_trend() -> list[InvestigationResult]:
    """Check if MLB accuracy has been trending downward."""
    results = []
    
    reports_dir = Path(__file__).parent.parent / "data" / "reports"
    
    # Load recent accuracy reports
    accuracy_reports = sorted(reports_dir.glob("accuracy_*.json"))[-10:]  # Last 10 days
    
    mlb_accs = []
    for fpath in accuracy_reports:
        try:
            with open(fpath) as f:
                data = json.load(f)
                if 'by_sport' in data and 'mlb' in data['by_sport']:
                    mlb_data = data['by_sport']['mlb']
                    if mlb_data.get('evaluated', 0) > 0:
                        acc = mlb_data['correct'] / mlb_data['evaluated']
                        date_str = data.get('date', fpath.stem.split('_')[-1])
                        mlb_accs.append((date_str, acc, mlb_data['evaluated']))
        except:
            pass
    
    if mlb_accs:
        # Trend analysis
        if len(mlb_accs) >= 3:
            recent_accs = [acc for _, acc, _ in mlb_accs[-5:]]
            older_accs = [acc for _, acc, _ in mlb_accs[:5]]
            
            recent_avg = sum(recent_accs) / len(recent_accs) if recent_accs else 0
            older_avg = sum(older_accs) / len(older_accs) if older_accs else 0
            
            trend = recent_avg - older_avg
            
            if trend < -0.10:
                results.append(InvestigationResult(
                    category="Accuracy Trend",
                    finding=f"MLB accuracy declining: {older_avg*100:.1f}% → {recent_avg*100:.1f}% (drop: {trend*100:.1f}%)",
                    severity="critical",
                    recommendation="Investigate recent changes to features, data, or opponent strength"
                ))
            elif trend > 0.05:
                results.append(InvestigationResult(
                    category="Accuracy Trend",
                    finding=f"MLB accuracy improving: {older_avg*100:.1f}% → {recent_avg*100:.1f}% (gain: {trend*100:.1f}%)",
                    severity="info",
                    recommendation="Continue monitoring; improvements may indicate model stabilization"
                ))
        
        results.append(InvestigationResult(
            category="Recent Accuracy",
            finding=f"Latest: {mlb_accs[-1][0]} → {mlb_accs[-1][1]*100:.1f}% ({mlb_accs[-1][2]} samples)",
            severity="info",
            recommendation="Track daily for patterns"
        ))
    else:
        results.append(InvestigationResult(
            category="Historical Data",
            finding="No historical accuracy reports found for MLB",
            severity="warning",
            recommendation="Start collecting daily accuracy metrics"
        ))
    
    return results


def main(argv: list[str] | None = None) -> int:
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║  MLB ACCURACY GAP INVESTIGATION                                   ║")
    print("║  Backtest: 57.4%  vs  Live: 41.7%  (Gap: -15.7%)                ║")
    print("╚════════════════════════════════════════════════════════════════════╝\n")
    
    all_results = []
    
    print("🔍 Investigating feature coverage...")
    all_results.extend(investigate_feature_coverage())
    
    print("🔍 Investigating player stats...")
    all_results.extend(investigate_player_stats())
    
    print("🔍 Analyzing accuracy trend...")
    all_results.extend(analyze_accuracy_trend())
    
    # Group by severity
    by_severity = {}
    for result in all_results:
        if result.severity not in by_severity:
            by_severity[result.severity] = []
        by_severity[result.severity].append(result)
    
    # Display results
    for severity in ['critical', 'warning', 'info']:
        results = by_severity.get(severity, [])
        if not results:
            continue
        
        icon = {"critical": "🔴", "warning": "⚠️ ", "info": "ℹ️ "}[severity]
        print(f"\n{icon} {severity.upper()}:")
        print("─" * 70)
        
        for result in results:
            print(f"\n  [{result.category}]")
            print(f"  Finding:      {result.finding}")
            print(f"  Recommend:    {result.recommendation}")
    
    # Save structured report
    reports_dir = Path(__file__).parent.parent / "data" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    report_file = reports_dir / f"mlb_accuracy_investigation_{date.today().isoformat()}.json"
    with open(report_file, "w") as f:
        json.dump({
            "date": date.today().isoformat(),
            "backtest_accuracy": 0.574,
            "live_accuracy": 0.417,
            "gap": -0.157,
            "critical_count": len(by_severity.get('critical', [])),
            "warning_count": len(by_severity.get('warning', [])),
            "findings": [
                {
                    "category": r.category,
                    "finding": r.finding,
                    "severity": r.severity,
                    "recommendation": r.recommendation,
                }
                for r in all_results
            ],
        }, f, indent=2)
    
    print(f"\n\n📄 Report saved: {report_file}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
