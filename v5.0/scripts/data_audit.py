#!/usr/bin/env python3
"""V5.0 Data Platform — End-to-End Data Audit

Traces data flow through the entire pipeline:
  Raw JSON/CSV → Normalized Parquets → DataService API → Features → ML

Usage:
  python3 scripts/data_audit.py              # full audit
  python3 scripts/data_audit.py --raw-only   # raw data inventory only
  python3 scripts/data_audit.py --norm-only  # normalized parquet audit only
  python3 scripts/data_audit.py --api-only   # DataService verification only
  python3 scripts/data_audit.py --gaps-only  # gap analysis report only
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

# ── Path setup ────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
BACKEND_DIR = PROJECT_ROOT / "backend"
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
NORM_DIR = DATA_DIR / "normalized"
AUDIT_DIR = DATA_DIR / "audit"

sys.path.insert(0, str(BACKEND_DIR))

from config import SPORT_DEFINITIONS, ALL_SPORTS  # noqa: E402
from normalization.provider_map import (  # noqa: E402
    PROVIDER_PRIORITY,
    ALL_DATA_TYPES,
    providers_for,
)

# ── Constants ─────────────────────────────────────────────
EXPECTED_YEAR_MIN = 2023
EXPECTED_YEAR_MAX = 2026
CORE_DATA_TYPES = ["games", "teams", "players", "standings", "player_stats"]
HTML_ERROR_SIGS = [b"<!DOCTYPE", b"<html", b"<!doctype", b"<HTML", b"403 Forbidden",
                   b"Access Denied", b"Rate limit"]
MIN_FILE_SIZE = 10  # bytes — anything smaller is effectively empty

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
logger = logging.getLogger("data_audit")

# ═══════════════════════════════════════════════════════════
#  Formatting helpers
# ═══════════════════════════════════════════════════════════

W = 80  # report width

def _header(title: str) -> str:
    return f"\n{'═' * W}\n  {title}\n{'═' * W}"

def _subheader(title: str) -> str:
    return f"\n{'─' * W}\n  {title}\n{'─' * W}"

def _fmt_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"

def _pct(num: int, denom: int) -> str:
    if denom == 0:
        return "N/A"
    return f"{num / denom * 100:.0f}%"

def _bar(ratio: float, width: int = 30) -> str:
    filled = int(ratio * width)
    return f"[{'█' * filled}{'░' * (width - filled)}]"

# ═══════════════════════════════════════════════════════════
#  1. RAW DATA INVENTORY
# ═══════════════════════════════════════════════════════════

def _is_html_error(path: Path) -> bool:
    """Quick heuristic: check first 512 bytes for HTML error signatures."""
    try:
        with open(path, "rb") as f:
            head = f.read(512)
        return any(sig in head for sig in HTML_ERROR_SIGS)
    except Exception:
        return False


def audit_raw_data() -> dict[str, Any]:
    """Inventory every file under data/raw/."""
    print(_header("1. RAW DATA INVENTORY"))

    if not RAW_DIR.exists():
        print("  ⚠  data/raw/ does not exist — skipping")
        return {"status": "missing"}

    results: dict[str, Any] = {}

    for provider_dir in sorted(RAW_DIR.iterdir()):
        if not provider_dir.is_dir():
            continue
        provider = provider_dir.name
        prov_info: dict[str, Any] = {
            "sports": {},
            "total_files": 0,
            "total_size": 0,
            "empty_files": 0,
            "html_errors": 0,
        }

        for sport_dir in sorted(provider_dir.iterdir()):
            if not sport_dir.is_dir():
                continue
            sport = sport_dir.name
            sport_info: dict[str, Any] = {"seasons": {}, "files": 0, "size": 0}

            for fpath in sport_dir.rglob("*"):
                if not fpath.is_file():
                    continue
                sport_info["files"] += 1
                sz = fpath.stat().st_size
                sport_info["size"] += sz

                # Determine season bucket from path components
                parts = fpath.relative_to(sport_dir).parts
                season = parts[0] if parts and parts[0].isdigit() else "_unversioned"
                endpoint = fpath.stem
                if season not in sport_info["seasons"]:
                    sport_info["seasons"][season] = {"files": 0, "size": 0, "endpoints": set()}
                sport_info["seasons"][season]["files"] += 1
                sport_info["seasons"][season]["size"] += sz
                sport_info["seasons"][season]["endpoints"].add(endpoint)

                # Quality checks
                if sz < MIN_FILE_SIZE:
                    prov_info["empty_files"] += 1
                elif _is_html_error(fpath):
                    prov_info["html_errors"] += 1

            prov_info["sports"][sport] = sport_info
            prov_info["total_files"] += sport_info["files"]
            prov_info["total_size"] += sport_info["size"]

        results[provider] = prov_info

        # Print summary
        print(f"\n  📦 {provider:20s}  {prov_info['total_files']:>6,} files  "
              f"{_fmt_size(prov_info['total_size']):>10s}")
        if prov_info["empty_files"] or prov_info["html_errors"]:
            flags = []
            if prov_info["empty_files"]:
                flags.append(f"⚠ {prov_info['empty_files']} empty")
            if prov_info["html_errors"]:
                flags.append(f"⚠ {prov_info['html_errors']} HTML errors")
            print(f"     {'  '.join(flags)}")
        for sport, si in sorted(prov_info["sports"].items()):
            seasons = sorted(s for s in si["seasons"] if s != "_unversioned")
            print(f"     {sport:12s}  {si['files']:>5,} files  "
                  f"seasons: {', '.join(seasons) if seasons else 'none'}")

    # Grand totals
    grand_files = sum(p["total_files"] for p in results.values())
    grand_size = sum(p["total_size"] for p in results.values())
    grand_empty = sum(p["empty_files"] for p in results.values())
    grand_html = sum(p["html_errors"] for p in results.values())
    print(f"\n  TOTAL: {grand_files:,} files  {_fmt_size(grand_size)}")
    if grand_empty or grand_html:
        print(f"  FLAGS: {grand_empty} empty, {grand_html} HTML error pages")

    # Serialize (convert sets to lists for JSON)
    for prov in results.values():
        for sport_info in prov["sports"].values():
            for season_info in sport_info["seasons"].values():
                season_info["endpoints"] = sorted(season_info["endpoints"])

    return results


# ═══════════════════════════════════════════════════════════
#  2. NORMALIZATION COVERAGE
# ═══════════════════════════════════════════════════════════

def audit_normalized_data() -> dict[str, Any]:
    """Audit every parquet file under data/normalized/."""
    print(_header("2. NORMALIZATION COVERAGE"))

    if not NORM_DIR.exists():
        print("  ⚠  data/normalized/ does not exist — skipping")
        return {"status": "missing"}

    results: dict[str, Any] = {}

    for sport_dir in sorted(NORM_DIR.iterdir()):
        if not sport_dir.is_dir():
            continue
        sport = sport_dir.name
        defn = SPORT_DEFINITIONS.get(sport, {})
        label = defn.get("label", sport.upper())
        category = defn.get("category", "unknown")

        sport_result: dict[str, Any] = {
            "label": label,
            "category": category,
            "data_types": {},
            "total_rows": 0,
            "total_size": 0,
        }

        parquets = sorted(sport_dir.glob("*.parquet"))
        if not parquets:
            results[sport] = sport_result
            continue

        kind_groups: dict[str, list[Path]] = defaultdict(list)
        for pf in parquets:
            stem = pf.stem
            # Parse kind from filename: <kind>_<season>.parquet
            for k in CORE_DATA_TYPES + ["odds", "predictions", "injuries", "news",
                                        "player_props", "weather"]:
                if stem.startswith(k + "_") or stem == k:
                    kind_groups[k].append(pf)
                    break

        print(f"\n  🏟  {label} ({sport})")

        for kind, files in sorted(kind_groups.items()):
            kind_info: dict[str, Any] = {
                "files": [],
                "total_rows": 0,
                "total_size": 0,
                "columns": {},
            }
            all_columns: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "non_null": 0})

            for fp in files:
                try:
                    pf = pq.ParquetFile(fp)
                    meta = pf.metadata
                    nrows = meta.num_rows
                    sz = fp.stat().st_size
                    season = fp.stem.replace(f"{kind}_", "")

                    # Read the dataframe to check column completeness
                    df = pd.read_parquet(fp, engine="pyarrow")
                    for col in df.columns:
                        all_columns[col]["total"] += len(df)
                        all_columns[col]["non_null"] += int(df[col].notna().sum())

                    kind_info["files"].append({
                        "name": fp.name,
                        "season": season,
                        "rows": nrows,
                        "size": sz,
                    })
                    kind_info["total_rows"] += nrows
                    kind_info["total_size"] += sz
                except Exception as exc:
                    kind_info["files"].append({
                        "name": fp.name,
                        "error": str(exc),
                    })

            # Column completeness
            col_summary: dict[str, str] = {}
            for col, counts in all_columns.items():
                pct = counts["non_null"] / counts["total"] * 100 if counts["total"] > 0 else 0
                col_summary[col] = f"{pct:.0f}%"
            kind_info["columns"] = col_summary

            sport_result["data_types"][kind] = kind_info
            sport_result["total_rows"] += kind_info["total_rows"]
            sport_result["total_size"] += kind_info["total_size"]

            # Print per-kind summary
            seasons_str = ", ".join(
                fi.get("season", "?") for fi in kind_info["files"]
                if "error" not in fi
            )
            print(f"     {kind:15s}  {kind_info['total_rows']:>8,} rows  "
                  f"{_fmt_size(kind_info['total_size']):>10s}  "
                  f"seasons: [{seasons_str}]")

            # Flag mostly-null columns
            weak_cols = [c for c, v in col_summary.items()
                         if int(v.replace("%", "")) < 20 and c not in ("id", "sport")]
            if weak_cols:
                print(f"       ⚠ mostly null: {', '.join(weak_cols[:5])}"
                      f"{'...' if len(weak_cols) > 5 else ''}")

        results[sport] = sport_result

    # Summary table
    print(_subheader("Normalization Summary"))
    print(f"  {'Sport':12s} {'Rows':>10s} {'Size':>10s} {'Data Types':>12s}")
    print(f"  {'─' * 48}")
    for sport, info in sorted(results.items(), key=lambda x: -x[1]["total_rows"]):
        dt_count = len(info["data_types"])
        if dt_count == 0:
            continue
        print(f"  {info['label']:12s} {info['total_rows']:>10,} "
              f"{_fmt_size(info['total_size']):>10s} {dt_count:>12}")

    # Coverage: raw vs normalized
    print(_subheader("Raw → Normalized Coverage"))
    for sport in ALL_SPORTS:
        expected = PROVIDER_PRIORITY.get(sport, {})
        actual_kinds = set(results.get(sport, {}).get("data_types", {}).keys())
        expected_kinds = {k for k, providers in expected.items()
                         if providers}  # only kinds that have configured providers
        missing = expected_kinds - actual_kinds - {"player_props", "weather"}
        if missing:
            label = SPORT_DEFINITIONS.get(sport, {}).get("label", sport)
            print(f"  ⚠ {label:12s}  missing normalized data for: "
                  f"{', '.join(sorted(missing))}")

    return results


# ═══════════════════════════════════════════════════════════
#  3. API VERIFICATION (via DataService directly)
# ═══════════════════════════════════════════════════════════

def audit_api() -> dict[str, Any]:
    """Import DataService and verify it returns real data."""
    print(_header("3. API / DATASERVICE VERIFICATION"))

    try:
        from services.data_service import DataService
    except ImportError as e:
        print(f"  ⚠  Cannot import DataService: {e}")
        return {"status": "import_error", "error": str(e)}

    ds = DataService()
    results: dict[str, Any] = {}

    # List available sports first
    available = ds.list_available_sports()
    sport_keys = [s["key"] for s in available]
    print(f"\n  DataService sees {len(available)} sports with data")

    for sport_info in available:
        sport = sport_info["key"]
        label = sport_info.get("label", sport)
        sr: dict[str, Any] = {"label": label, "checks": {}}

        print(f"\n  🔌 {label} ({sport})")

        # Test get_teams
        try:
            teams = ds.get_teams(sport)
            n = len(teams)
            populated = _count_populated_fields(teams[:10]) if teams else {}
            sr["checks"]["teams"] = {
                "count": n,
                "status": "ok" if n > 0 else "empty",
                "sample_quality": populated,
            }
            status = "✅" if n > 0 else "⚠ "
            print(f"     teams:    {status} {n:>6,} records")
        except Exception as e:
            sr["checks"]["teams"] = {"error": str(e)}
            print(f"     teams:    ❌ {e}")

        # Test get_games
        try:
            games = ds.get_games(sport)
            n = len(games)
            populated = _count_populated_fields(games[:10]) if games else {}
            sr["checks"]["games"] = {
                "count": n,
                "status": "ok" if n > 0 else "empty",
                "sample_quality": populated,
            }
            status = "✅" if n > 0 else "⚠ "
            print(f"     games:    {status} {n:>6,} records")
        except Exception as e:
            sr["checks"]["games"] = {"error": str(e)}
            print(f"     games:    ❌ {e}")

        # Test get_players
        try:
            players = ds.get_players(sport)
            n = len(players)
            sr["checks"]["players"] = {
                "count": n,
                "status": "ok" if n > 0 else "empty",
            }
            status = "✅" if n > 0 else "⚠ "
            print(f"     players:  {status} {n:>6,} records")
        except Exception as e:
            sr["checks"]["players"] = {"error": str(e)}
            print(f"     players:  ❌ {e}")

        # Test get_standings
        try:
            standings = ds.get_standings(sport)
            n = len(standings)
            sr["checks"]["standings"] = {
                "count": n,
                "status": "ok" if n > 0 else "empty",
            }
            status = "✅" if n > 0 else "⚠ "
            print(f"     standings: {status} {n:>5,} records")
        except Exception as e:
            sr["checks"]["standings"] = {"error": str(e)}
            print(f"     standings: ❌ {e}")

        results[sport] = sr

    # Cache stats
    print(f"\n  Cache: {ds.cache_stats}")
    results["_cache_stats"] = ds.cache_stats
    return results


def _count_populated_fields(records: list[dict]) -> dict[str, str]:
    """For a sample of records, report % of fields that are non-null."""
    if not records:
        return {}
    all_keys = set()
    for r in records:
        all_keys.update(r.keys())
    result = {}
    for key in sorted(all_keys):
        non_null = sum(1 for r in records if r.get(key) is not None)
        result[key] = f"{non_null}/{len(records)}"
    return result


# ═══════════════════════════════════════════════════════════
#  4. DATA QUALITY CHECKS
# ═══════════════════════════════════════════════════════════

def audit_quality() -> dict[str, Any]:
    """Run quality checks on normalized data."""
    print(_header("4. DATA QUALITY CHECKS"))

    if not NORM_DIR.exists():
        print("  ⚠  data/normalized/ does not exist — skipping")
        return {"status": "missing"}

    results: dict[str, Any] = {}

    for sport_dir in sorted(NORM_DIR.iterdir()):
        if not sport_dir.is_dir():
            continue
        sport = sport_dir.name
        label = SPORT_DEFINITIONS.get(sport, {}).get("label", sport.upper())
        issues: list[str] = []
        sr: dict[str, Any] = {"issues": issues, "details": {}}

        # ── Games quality ─────────────────────────────────
        game_files = sorted(sport_dir.glob("games_*.parquet"))
        if game_files:
            dfs = [pd.read_parquet(f, engine="pyarrow") for f in game_files]
            df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            gq = _check_games_quality(df, sport, label)
            sr["details"]["games"] = gq
            issues.extend(gq.get("issues", []))

        # ── Teams quality ─────────────────────────────────
        team_files = sorted(sport_dir.glob("teams_*.parquet"))
        if team_files:
            dfs = [pd.read_parquet(f, engine="pyarrow") for f in team_files]
            df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            tq = _check_teams_quality(df, sport, label)
            sr["details"]["teams"] = tq
            issues.extend(tq.get("issues", []))

        # ── Player stats quality ──────────────────────────
        stat_files = sorted(sport_dir.glob("player_stats_*.parquet"))
        if stat_files:
            dfs = [pd.read_parquet(f, engine="pyarrow") for f in stat_files]
            df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            sq = _check_stats_quality(df, sport, label)
            sr["details"]["player_stats"] = sq
            issues.extend(sq.get("issues", []))

        # ── Cross-reference: game team_ids → teams ────────
        if game_files and team_files:
            xr = _check_cross_refs(sport_dir, sport, label)
            sr["details"]["cross_ref"] = xr
            issues.extend(xr.get("issues", []))

        results[sport] = sr

        # Print issues
        if issues:
            print(f"\n  🔍 {label} ({sport})")
            for iss in issues:
                print(f"     ⚠ {iss}")

    # Summary
    total_issues = sum(len(r["issues"]) for r in results.values())
    clean = sum(1 for r in results.values() if not r["issues"])
    print(f"\n  Quality summary: {total_issues} issues across "
          f"{len(results)} sports ({clean} clean)")

    return results


def _check_games_quality(df: pd.DataFrame, sport: str, label: str) -> dict:
    """Check game records for duplicates, date ranges, score sanity."""
    info: dict[str, Any] = {"issues": []}
    if df.empty:
        return info

    # Duplicate detection
    if "id" in df.columns:
        dupes = df["id"].duplicated().sum()
        if dupes > 0:
            info["issues"].append(f"games: {dupes} duplicate IDs")
        info["duplicate_ids"] = int(dupes)

    # Date range
    if "date" in df.columns:
        dates = pd.to_datetime(df["date"], errors="coerce").dropna()
        if len(dates) > 0:
            min_year = dates.dt.year.min()
            max_year = dates.dt.year.max()
            info["date_range"] = f"{min_year}-{max_year}"
            if min_year < EXPECTED_YEAR_MIN - 1:
                info["issues"].append(
                    f"games: dates go back to {min_year} (expected ≥{EXPECTED_YEAR_MIN})")
            if max_year > EXPECTED_YEAR_MAX + 1:
                info["issues"].append(
                    f"games: dates extend to {max_year} (expected ≤{EXPECTED_YEAR_MAX})")

    # Score sanity
    for col in ("home_score", "away_score"):
        if col in df.columns:
            scores = pd.to_numeric(df[col], errors="coerce").dropna()
            if len(scores) > 0:
                if (scores < 0).any():
                    info["issues"].append(f"games: negative values in {col}")
                # Sport-specific upper bounds
                cat = SPORT_DEFINITIONS.get(sport, {}).get("category", "")
                max_reasonable = {
                    "basketball": 200, "football": 80, "baseball": 40,
                    "hockey": 20, "soccer": 15, "mma": 5,
                }.get(cat, 500)
                over = (scores > max_reasonable).sum()
                if over > 0:
                    info["issues"].append(
                        f"games: {int(over)} {col} values > {max_reasonable}")

    info["row_count"] = len(df)
    return info


def _check_teams_quality(df: pd.DataFrame, sport: str, label: str) -> dict:
    info: dict[str, Any] = {"issues": []}
    if df.empty:
        return info

    if "id" in df.columns:
        dupes = df["id"].duplicated().sum()
        if dupes > 0:
            info["issues"].append(f"teams: {dupes} duplicate IDs")

    if "name" in df.columns:
        empty_names = df["name"].isna().sum() + (df["name"] == "").sum()
        if empty_names > 0:
            info["issues"].append(f"teams: {int(empty_names)} records with empty name")

    info["row_count"] = len(df)
    return info


def _check_stats_quality(df: pd.DataFrame, sport: str, label: str) -> dict:
    info: dict[str, Any] = {"issues": []}
    if df.empty:
        return info

    # Check for negative stats in numeric columns
    numeric_cols = df.select_dtypes(include=["number"]).columns
    for col in numeric_cols:
        neg_count = (df[col] < 0).sum()
        # Allow plus_minus and similar signed stats
        if neg_count > 0 and col not in ("plus_minus", "point_diff", "margin",
                                          "spread", "predicted_spread"):
            # Only flag if > 5% negative (avoid false positives for signed stats)
            if neg_count / len(df) > 0.05:
                info["issues"].append(
                    f"player_stats: {int(neg_count)} negative values in '{col}'")

    info["row_count"] = len(df)
    info["numeric_columns"] = len(numeric_cols)
    return info


def _check_cross_refs(sport_dir: Path, sport: str, label: str) -> dict:
    """Check if game team references match actual team records."""
    info: dict[str, Any] = {"issues": []}

    try:
        team_dfs = [pd.read_parquet(f) for f in sorted(sport_dir.glob("teams_*.parquet"))]
        game_dfs = [pd.read_parquet(f) for f in sorted(sport_dir.glob("games_*.parquet"))]
        if not team_dfs or not game_dfs:
            return info

        teams_df = pd.concat(team_dfs, ignore_index=True)
        games_df = pd.concat(game_dfs, ignore_index=True)

        if "id" not in teams_df.columns:
            return info

        team_ids = set(teams_df["id"].dropna().astype(str))

        for col in ("home_team_id", "away_team_id"):
            if col in games_df.columns:
                game_team_ids = set(games_df[col].dropna().astype(str))
                orphaned = game_team_ids - team_ids
                if orphaned:
                    n = len(orphaned)
                    sample = sorted(orphaned)[:3]
                    info["issues"].append(
                        f"cross-ref: {n} {col} values not in teams table "
                        f"(e.g. {', '.join(sample)})")
                    info[f"orphaned_{col}"] = n
    except Exception as exc:
        info["issues"].append(f"cross-ref check failed: {exc}")

    return info


# ═══════════════════════════════════════════════════════════
#  5. GAP ANALYSIS & RECOMMENDATIONS
# ═══════════════════════════════════════════════════════════

def audit_gaps(raw_results: dict, norm_results: dict) -> dict[str, Any]:
    """Produce a gap analysis comparing raw data availability, normalization
    status, and expected data types per sport."""
    print(_header("5. GAP ANALYSIS & RECOMMENDATIONS"))

    results: dict[str, Any] = {"sports": {}, "provider_analysis": {}}

    # ── Per-sport completeness score ──────────────────────
    print(f"\n  {'Sport':12s} {'Score':>6s} {'Bar':32s} {'Available':>10s} {'Missing'}")
    print(f"  {'─' * 78}")

    for sport in ALL_SPORTS:
        defn = SPORT_DEFINITIONS[sport]
        label = defn["label"]
        expected = PROVIDER_PRIORITY.get(sport, {})
        # Count kinds that have at least one configured provider
        expected_kinds = {k for k, prov in expected.items()
                         if prov and k not in ("player_props", "weather")}
        actual_kinds = set()
        if norm_results and sport in norm_results and isinstance(norm_results[sport], dict):
            actual_kinds = set(norm_results[sport].get("data_types", {}).keys())

        have = actual_kinds & expected_kinds
        missing = expected_kinds - actual_kinds

        score = len(have) / len(expected_kinds) if expected_kinds else 0
        bar = _bar(score)

        missing_str = ", ".join(sorted(missing)) if missing else "—"
        print(f"  {label:12s} {score:>5.0%}  {bar}  {len(have):>4}/{len(expected_kinds):<4}  "
              f"{missing_str}")

        results["sports"][sport] = {
            "label": label,
            "category": defn["category"],
            "score": round(score, 2),
            "expected": sorted(expected_kinds),
            "available": sorted(have),
            "missing": sorted(missing),
        }

    # ── Provider contribution analysis ────────────────────
    print(_subheader("Provider Contribution"))

    provider_contribution: dict[str, dict[str, int]] = defaultdict(lambda: {"sports": set(), "files": 0})
    if raw_results and isinstance(raw_results, dict):
        for provider, pinfo in raw_results.items():
            if not isinstance(pinfo, dict):
                continue
            sports_data = pinfo.get("sports", {})
            provider_contribution[provider]["sports"] = set(sports_data.keys())
            provider_contribution[provider]["files"] = pinfo.get("total_files", 0)
            provider_contribution[provider]["size"] = pinfo.get("total_size", 0)

    # Check which providers are the SOLE source for any sport/kind
    sole_source: dict[str, list[str]] = defaultdict(list)
    for sport in ALL_SPORTS:
        expected = PROVIDER_PRIORITY.get(sport, {})
        for kind, providers in expected.items():
            if len(providers) == 1 and providers[0]:
                sole_source[providers[0]].append(f"{sport}/{kind}")

    print(f"\n  {'Provider':18s} {'Sports':>7s} {'Files':>8s} {'Size':>10s} {'Sole Source For'}")
    print(f"  {'─' * 72}")
    for provider in sorted(provider_contribution.keys()):
        pc = provider_contribution[provider]
        n_sports = len(pc["sports"])
        n_files = pc["files"]
        sz = _fmt_size(pc.get("size", 0))
        sole = sole_source.get(provider, [])
        sole_str = f"{len(sole)} kind(s)" if sole else "—"
        print(f"  {provider:18s} {n_sports:>7} {n_files:>8,} {sz:>10s} {sole_str}")

    # ── Recommendations ───────────────────────────────────
    print(_subheader("Recommendations"))
    recs: list[str] = []

    # Sports with teams-only data that should have games
    for sport, info in results["sports"].items():
        avail = info["available"]
        missing = info["missing"]
        if avail == ["teams"] and "games" in missing:
            recs.append(f"🔴 {info['label']}: Only teams normalized — "
                        f"needs games, standings, players (high priority)")
        elif info["score"] < 0.3 and info["score"] > 0:
            recs.append(f"🟡 {info['label']}: Very low coverage ({info['score']:.0%}) — "
                        f"missing: {', '.join(missing)}")
        elif info["score"] == 0:
            recs.append(f"⚫ {info['label']}: No normalized data at all")

    # Most complete sports (good examples)
    top_sports = sorted(results["sports"].items(), key=lambda x: -x[1]["score"])[:5]
    recs.append("")
    recs.append("Most complete sports (best data pipelines):")
    for sport, info in top_sports:
        recs.append(f"  ✅ {info['label']} — {info['score']:.0%} coverage")

    # Provider recommendations
    recs.append("")
    recs.append("Provider notes:")
    for provider, sole_list in sorted(sole_source.items()):
        if len(sole_list) >= 3:
            recs.append(f"  📌 {provider} is sole source for {len(sole_list)} data types — "
                        f"cannot be removed without data loss")

    for rec in recs:
        print(f"  {rec}")

    results["recommendations"] = recs
    results["provider_analysis"] = {
        p: {
            "sports": sorted(pc["sports"]),
            "files": pc["files"],
            "sole_source_count": len(sole_source.get(p, [])),
        }
        for p, pc in provider_contribution.items()
    }

    return results


# ═══════════════════════════════════════════════════════════
#  Orchestration
# ═══════════════════════════════════════════════════════════

def run_full_audit(
    *,
    raw: bool = True,
    norm: bool = True,
    api: bool = True,
    quality: bool = True,
    gaps: bool = True,
) -> dict[str, Any]:
    """Run the full (or partial) audit and save results."""
    start = time.time()

    print(f"{'═' * W}")
    print(f"  V5.0 DATA PLATFORM — END-TO-END AUDIT")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Data dir: {DATA_DIR}")
    print(f"{'═' * W}")

    report: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data_dir": str(DATA_DIR),
    }

    raw_results = None
    norm_results = None

    if raw:
        raw_results = audit_raw_data()
        report["raw_data"] = raw_results

    if norm:
        norm_results = audit_normalized_data()
        report["normalized_data"] = norm_results

    if quality:
        report["quality"] = audit_quality()

    if api:
        report["api_verification"] = audit_api()

    if gaps:
        report["gap_analysis"] = audit_gaps(raw_results or {}, norm_results or {})

    elapsed = time.time() - start

    print(_header("AUDIT COMPLETE"))
    print(f"  Elapsed: {elapsed:.1f}s")

    # Save JSON report
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = AUDIT_DIR / "audit_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"  Report saved: {report_path}")
    print(f"{'═' * W}\n")

    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="V5.0 Data Platform — End-to-End Audit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--raw-only", action="store_true",
                        help="Run raw data inventory only")
    parser.add_argument("--norm-only", action="store_true",
                        help="Run normalization audit only")
    parser.add_argument("--api-only", action="store_true",
                        help="Run API verification only")
    parser.add_argument("--quality-only", action="store_true",
                        help="Run data quality checks only")
    parser.add_argument("--gaps-only", action="store_true",
                        help="Run gap analysis only")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # If any --*-only flag is set, run only those sections
    any_only = (args.raw_only or args.norm_only or args.api_only
                or args.quality_only or args.gaps_only)
    if any_only:
        run_full_audit(
            raw=args.raw_only or args.gaps_only,     # gaps needs raw data
            norm=args.norm_only or args.gaps_only,   # gaps needs norm data
            api=args.api_only,
            quality=args.quality_only,
            gaps=args.gaps_only,
        )
    else:
        run_full_audit()


if __name__ == "__main__":
    main()
