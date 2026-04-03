#!/usr/bin/env python3
"""
auto_curated_sync.py — Automatic incremental sync: raw → normalized → curated parquets → DuckDB.

Designed to be called after every import or on a short interval (e.g., every 5 minutes).
Only processes data that has actually changed, using content-hash fingerprints
stored alongside each curated parquet.

Architecture:
    1. Scan raw provider dirs for files newer than last sync
    2. Normalize only the affected sport/season/data_type combinations
    3. Diff normalized output against curated parquets (row-count + hash)
    4. Write updated curated parquets atomically (tmp → rename)
    5. Bulk-refresh affected DuckDB views

Usage:
    # Called automatically after imports:
    from normalization.auto_curated_sync import AutoCuratedSync
    sync = AutoCuratedSync()
    sync.run(sports=["ncaaf"])  # or sync.run() for all

    # CLI:
    python -m normalization.auto_curated_sync --sports ncaaf
    python -m normalization.auto_curated_sync --sports ncaaf --force
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import tempfile
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fingerprint helpers
# ---------------------------------------------------------------------------

FINGERPRINT_FILENAME = ".sync_fingerprint.json"


def _file_content_hash(path: Path, block_size: int = 1 << 20) -> str:
    """Fast xxhash-style hash of file contents (falls back to sha256)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(block_size):
            h.update(chunk)
    return h.hexdigest()[:16]


def _dir_fingerprint(directory: Path) -> dict[str, str]:
    """Build {relative_path: content_hash} for all parquets in a directory tree."""
    fingerprint: dict[str, str] = {}
    if not directory.exists():
        return fingerprint
    for pf in sorted(directory.rglob("*.parquet")):
        rel = str(pf.relative_to(directory))
        fingerprint[rel] = _file_content_hash(pf)
    return fingerprint


def _load_fingerprint(directory: Path) -> dict[str, str]:
    """Load saved fingerprint from a directory."""
    fp_file = directory / FINGERPRINT_FILENAME
    if not fp_file.exists():
        return {}
    try:
        return json.loads(fp_file.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_fingerprint(directory: Path, fingerprint: dict[str, str]) -> None:
    """Save fingerprint atomically."""
    fp_file = directory / FINGERPRINT_FILENAME
    fp_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = fp_file.with_suffix(".tmp")
    tmp.write_text(json.dumps(fingerprint, indent=2), encoding="utf-8")
    tmp.replace(fp_file)


# ---------------------------------------------------------------------------
# Raw change detection
# ---------------------------------------------------------------------------

def _scan_raw_changes(
    raw_dir: Path,
    sport: str,
    since: datetime | None,
) -> dict[str, set[str]]:
    """Scan raw provider dirs for a sport, return {provider: {season, ...}} with changes.

    If `since` is None, treat everything as changed.
    """
    from normalization.normalizer import SPORT_PROVIDER_DIR

    provider_dirs = SPORT_PROVIDER_DIR.get(sport, {})
    changes: dict[str, set[str]] = {}

    for provider, sport_dir in provider_dirs.items():
        root = raw_dir / provider / sport_dir
        if not root.is_dir():
            continue
        for season_dir in root.iterdir():
            if not season_dir.is_dir() or not season_dir.name.isdigit():
                continue
            season = season_dir.name
            if since is None:
                changes.setdefault(provider, set()).add(season)
                continue
            # Check if any file in this season dir is newer than `since`
            since_ts = since.timestamp()
            try:
                for f in season_dir.rglob("*"):
                    if f.is_file() and f.stat().st_mtime > since_ts:
                        changes.setdefault(provider, set()).add(season)
                        break
            except OSError:
                continue

    return changes


# ---------------------------------------------------------------------------
# Sync result
# ---------------------------------------------------------------------------

@dataclass
class SyncResult:
    sport: str
    seasons_processed: list[str] = field(default_factory=list)
    normalized_rows: int = 0
    curated_updated: int = 0
    curated_unchanged: int = 0
    duckdb_refreshed: bool = False
    duration_s: float = 0.0
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Core sync engine
# ---------------------------------------------------------------------------

class AutoCuratedSync:
    """Incremental sync engine: raw → normalized → curated → DuckDB."""

    STATE_FILENAME = "auto_sync_state.json"

    def __init__(self) -> None:
        from config import get_settings
        cfg = get_settings()
        self._raw_dir = cfg.raw_dir
        self._normalized_dir = cfg.normalized_dir
        self._curated_dir = cfg.normalized_curated_dir
        self._duckdb_path = cfg.duckdb_path
        self._state_file = cfg.project_root / "data" / "reports" / self.STATE_FILENAME

    # ── State persistence ────────────────────────────────────────────

    def _load_state(self) -> dict[str, Any]:
        if not self._state_file.exists():
            return {}
        try:
            return json.loads(self._state_file.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_state(self, state: dict[str, Any]) -> None:
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._state_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
        tmp.replace(self._state_file)

    def _last_sync_time(self, sport: str) -> datetime | None:
        state = self._load_state()
        ts = state.get("sports", {}).get(sport, {}).get("last_sync_utc")
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return None

    # ── Main entry point ─────────────────────────────────────────────

    def run(
        self,
        sports: Sequence[str] | None = None,
        force: bool = False,
        skip_duckdb: bool = False,
    ) -> list[SyncResult]:
        """Run incremental sync for the given sports.

        Args:
            sports: Sport keys to sync. None = auto-detect from raw changes.
            force: If True, skip change detection and rebuild everything.
            skip_duckdb: If True, skip DuckDB view refresh.

        Returns:
            List of SyncResult per sport processed.
        """
        from config import ALL_SPORTS

        if sports is None:
            sports = list(ALL_SPORTS)

        results: list[SyncResult] = []
        refreshed_sports: list[str] = []

        for sport in sports:
            t0 = time.monotonic()
            result = self._sync_sport(sport, force=force)
            result.duration_s = round(time.monotonic() - t0, 2)
            results.append(result)

            if result.curated_updated > 0:
                refreshed_sports.append(sport)

        # Bulk DuckDB refresh for all sports that had curated changes
        if refreshed_sports and not skip_duckdb:
            self._refresh_duckdb(refreshed_sports)
            for r in results:
                if r.sport in refreshed_sports:
                    r.duckdb_refreshed = True

        # Save state
        state = self._load_state()
        now_iso = datetime.now(timezone.utc).isoformat()
        sports_state = state.setdefault("sports", {})
        for r in results:
            if r.curated_updated > 0 or r.curated_unchanged > 0:
                sports_state.setdefault(r.sport, {})["last_sync_utc"] = now_iso
        state["last_run_utc"] = now_iso
        state["last_results"] = [asdict(r) for r in results]
        self._save_state(state)

        # Log summary
        total_updated = sum(r.curated_updated for r in results)
        total_unchanged = sum(r.curated_unchanged for r in results)
        total_time = sum(r.duration_s for r in results)
        logger.info(
            "auto_sync complete: %d sports, %d updated, %d unchanged, %.1fs",
            len(results), total_updated, total_unchanged, total_time,
        )

        return results

    # ── Per-sport sync ───────────────────────────────────────────────

    def _sync_sport(self, sport: str, force: bool = False) -> SyncResult:
        """Sync a single sport: detect changes → normalize → update curated."""
        result = SyncResult(sport=sport)

        # 1. Detect which seasons have new raw data
        since = None if force else self._last_sync_time(sport)
        raw_changes = _scan_raw_changes(self._raw_dir, sport, since)

        if not raw_changes and not force:
            logger.debug("No raw changes for %s since %s — skipping", sport, since)
            return result

        # Collect all affected seasons
        affected_seasons = set()
        for seasons in raw_changes.values():
            affected_seasons.update(seasons)
        if not affected_seasons and force:
            # Discover all available
            from normalization.normalizer import Normalizer
            n = Normalizer()
            affected_seasons = set(n._discover_raw_seasons(sport))

        affected_seasons_sorted = sorted(affected_seasons)
        result.seasons_processed = affected_seasons_sorted
        logger.info(
            "Syncing %s: %d seasons with changes → %s",
            sport, len(affected_seasons_sorted), affected_seasons_sorted,
        )

        # 2. Run normalization for affected seasons only
        try:
            from normalization.normalizer import Normalizer
            normalizer = Normalizer()
            totals = normalizer.run_sport(sport, affected_seasons_sorted)
            result.normalized_rows = sum(totals.values())
            logger.info(
                "Normalized %s: %d rows across %d data types",
                sport, result.normalized_rows, len(totals),
            )
        except Exception as e:
            logger.exception("Normalization failed for %s", sport)
            result.errors.append(f"normalize: {e}")
            return result

        # 3. Diff curated output against saved fingerprints and count changes
        #    (CuratedParquetBuilder is already called inside run_sport)
        sport_curated = self._curated_dir / sport
        if sport_curated.exists():
            for entity_dir in sorted(sport_curated.iterdir()):
                if not entity_dir.is_dir() or entity_dir.name.startswith("."):
                    continue
                old_fp = _load_fingerprint(entity_dir)
                new_fp = _dir_fingerprint(entity_dir)
                if new_fp != old_fp:
                    result.curated_updated += 1
                    _save_fingerprint(entity_dir, new_fp)
                else:
                    result.curated_unchanged += 1

        return result

    # ── DuckDB bulk refresh ──────────────────────────────────────────

    def _refresh_duckdb(self, sports: list[str]) -> None:
        """Refresh DuckDB views for the given sports.

        The API server should use read-only mode, so the sync pipeline can
        acquire the write lock.  If the lock is still held (e.g. server hasn't
        restarted yet), fall back to writing a deferred refresh request.
        """
        try:
            from services.duckdb_catalog import DuckDBCatalog, create_duckdb_connection

            conn = create_duckdb_connection(self._duckdb_path)
            try:
                catalog = DuckDBCatalog(conn)
                t0 = time.monotonic()
                catalog.refresh_all(sports)
                elapsed = round(time.monotonic() - t0, 2)
                logger.info(
                    "DuckDB views refreshed for %d sports in %.1fs: %s",
                    len(sports), elapsed, sports,
                )
            finally:
                conn.close()

            # Signal the API server to reconnect and pick up the new views.
            self._write_deferred_refresh(sports)
        except Exception as exc:
            if "lock" in str(exc).lower() or "Conflicting" in str(exc):
                logger.info("DuckDB write lock held — writing deferred refresh")
                self._write_deferred_refresh(sports)
            else:
                logger.exception("DuckDB refresh failed")

    def _write_deferred_refresh(self, sports: list[str]) -> None:
        """Write a file that the running API server can pick up to refresh views."""
        refresh_file = self._curated_dir.parent / ".duckdb_refresh_pending.json"
        pending = {}
        if refresh_file.exists():
            try:
                pending = json.loads(refresh_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        existing = set(pending.get("sports", []))
        existing.update(sports)
        pending["sports"] = sorted(existing)
        pending["requested_utc"] = datetime.now(timezone.utc).isoformat()
        tmp = refresh_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(pending, indent=2), encoding="utf-8")
        tmp.replace(refresh_file)
        logger.info("Deferred DuckDB refresh written for: %s", sorted(existing))


# ---------------------------------------------------------------------------
# Hook for the Normalizer to call automatically after run_sport
# ---------------------------------------------------------------------------

def post_normalize_hook(sport: str, seasons: list[str]) -> None:
    """Called automatically after Normalizer.run_sport() to sync curated + DuckDB.

    This is the main integration point — import the hook and call it from
    the normalizer or daily pipeline.
    """
    try:
        sync = AutoCuratedSync()
        # Since normalization just ran, we only need to diff curated and refresh DuckDB.
        # The curated parquets were already built by run_sport() → CuratedParquetBuilder.
        from config import get_settings
        cfg = get_settings()
        sport_curated = cfg.normalized_curated_dir / sport

        updated = 0
        if sport_curated.exists():
            for entity_dir in sorted(sport_curated.iterdir()):
                if not entity_dir.is_dir() or entity_dir.name.startswith("."):
                    continue
                old_fp = _load_fingerprint(entity_dir)
                new_fp = _dir_fingerprint(entity_dir)
                if new_fp != old_fp:
                    updated += 1
                    _save_fingerprint(entity_dir, new_fp)

        if updated > 0:
            logger.info("post_normalize_hook: %s has %d changed entities, refreshing DuckDB", sport, updated)
            sync._refresh_duckdb([sport])

            # Update state
            state = sync._load_state()
            now_iso = datetime.now(timezone.utc).isoformat()
            state.setdefault("sports", {}).setdefault(sport, {})["last_sync_utc"] = now_iso
            state["last_run_utc"] = now_iso
            sync._save_state(state)
        else:
            logger.debug("post_normalize_hook: %s — no curated changes detected", sport)
    except Exception:
        logger.exception("post_normalize_hook failed for %s (non-fatal)", sport)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Auto-sync: raw → normalized → curated parquets → DuckDB",
    )
    parser.add_argument("--sports", default="", help="Comma-separated sports (empty = all with changes)")
    parser.add_argument("--force", action="store_true", help="Skip change detection, rebuild all")
    parser.add_argument("--skip-duckdb", action="store_true", help="Skip DuckDB refresh")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
    )

    sports = [s.strip().lower() for s in args.sports.split(",") if s.strip()] or None

    sync = AutoCuratedSync()
    results = sync.run(sports=sports, force=args.force, skip_duckdb=args.skip_duckdb)

    # Print summary
    print(f"\n{'='*70}")
    print("AUTO-SYNC RESULTS")
    print(f"{'='*70}")
    print(f"{'Sport':<12} {'Seasons':>8} {'Rows':>10} {'Updated':>8} {'Same':>6} {'DuckDB':>7} {'Time':>7}")
    print(f"{'-'*70}")
    for r in results:
        duck = "✓" if r.duckdb_refreshed else "-"
        print(
            f"{r.sport:<12} {len(r.seasons_processed):>8} {r.normalized_rows:>10} "
            f"{r.curated_updated:>8} {r.curated_unchanged:>6} {duck:>7} {r.duration_s:>6.1f}s"
        )
        if r.errors:
            for err in r.errors:
                print(f"  ⚠ {err}")
    total_time = sum(r.duration_s for r in results)
    print(f"{'-'*70}")
    print(f"Total: {sum(r.normalized_rows for r in results)} rows, {total_time:.1f}s")
    print(f"{'='*70}\n")

    return 1 if any(r.errors for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
