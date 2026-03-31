#!/usr/bin/env python3
# ──────────────────────────────────────────────────────────
# V5.0 — Live Data Importer Service
# ──────────────────────────────────────────────────────────
#
# Background service that continuously imports live data
# from ESPN, NHL, and other public APIs.
#
# Architecture:
#   - Async polling with configurable intervals per endpoint
#   - Smart polling: only polls scoreboards during game hours
#   - Writes raw JSON to data/raw/{provider}/{sport}/live/
#   - Incremental parquet updates
#   - SSE output for real-time frontend updates
#   - Graceful shutdown on SIGTERM/SIGINT
#
# Usage:
#   python3 scripts/live_importer.py                    # all sports
#   python3 scripts/live_importer.py --sports nba,nhl   # specific
#   python3 scripts/live_importer.py --dry-run           # show config
#   python3 scripts/live_importer.py --interval 60       # override
# ──────────────────────────────────────────────────────────
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
BACKEND_DIR = PROJECT_ROOT / "backend"
DATA_DIR = PROJECT_ROOT / "data"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(BACKEND_DIR))

logger = logging.getLogger("live_importer")

EST = timezone(timedelta(hours=-5))

# ── Endpoint Configuration ───────────────────────────────

LIVE_ENDPOINTS: dict[str, dict[str, Any]] = {
    "espn_scoreboard": {
        "url": "https://site.api.espn.com/apis/site/v2/sports/{group}/{slug}/scoreboard",
        "interval": 30,
        "sports": {
            "nba": ("basketball", "nba"),
            "nfl": ("football", "nfl"),
            "nhl": ("hockey", "nhl"),
            "mlb": ("baseball", "mlb"),
            "ncaab": ("basketball", "mens-college-basketball"),
            "ncaaf": ("football", "college-football"),
            "epl": ("soccer", "eng.1"),
            "mls": ("soccer", "usa.1"),
        },
        "active_only": True,
    },
    "espn_news": {
        "url": "https://site.api.espn.com/apis/site/v2/sports/{group}/{slug}/news",
        "interval": 300,
        "sports": {
            "nba": ("basketball", "nba"),
            "nfl": ("football", "nfl"),
            "nhl": ("hockey", "nhl"),
            "mlb": ("baseball", "mlb"),
            "ncaab": ("basketball", "mens-college-basketball"),
            "ncaaf": ("football", "college-football"),
            "epl": ("soccer", "eng.1"),
            "mls": ("soccer", "usa.1"),
        },
        "active_only": False,
    },
    "nhl_live": {
        "url": "https://api-web.nhle.com/v1/score/now",
        "interval": 30,
        "sports": {"nhl": None},
        "active_only": True,
    },
}

# ── SSE Event Queue ──────────────────────────────────────

_sse_subscribers: list[asyncio.Queue[dict[str, Any]]] = []


def _broadcast_sse(event: dict[str, Any]) -> None:
    """Push an event to all SSE subscribers."""
    dead: list[asyncio.Queue[dict[str, Any]]] = []
    for q in _sse_subscribers:
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        _sse_subscribers.remove(q)


# ── Logging Setup ────────────────────────────────────────


def _setup_logging(level: str = "info") -> None:
    log_level = getattr(logging, level.upper(), logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger.setLevel(log_level)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    log_file = LOG_DIR / f"live_importer_{date.today().isoformat()}.log"
    fh = logging.FileHandler(log_file)
    fh.setFormatter(fmt)
    logger.addHandler(fh)


# ── Live Importer ────────────────────────────────────────


class LiveImporter:
    """Continuously imports live data from multiple sources."""

    def __init__(
        self,
        sports: list[str] | None = None,
        interval_override: int | None = None,
        dry_run: bool = False,
    ) -> None:
        self.sports = sports or list(
            LIVE_ENDPOINTS["espn_scoreboard"]["sports"].keys()
        )
        self.interval_override = interval_override
        self.dry_run = dry_run
        self._shutdown = asyncio.Event()
        self._session: Any = None  # aiohttp.ClientSession
        self._poll_count = 0
        self._error_count = 0
        self._last_data: dict[str, dict[str, Any]] = {}
        self._start_time = time.monotonic()

    # ── Smart polling: check if games are live ───────────

    def _has_active_games(self, sport: str) -> bool:
        """Check if a sport has games live or starting soon.

        Uses the game_schedule module if available, otherwise falls back
        to a time-of-day heuristic.
        """
        try:
            from game_schedule import sports_with_live_games
            return sport in sports_with_live_games([sport])
        except ImportError:
            pass

        # Heuristic: most US games are 12pm-11:59pm EST
        now_est = datetime.now(EST)
        hour = now_est.hour

        # Sport-specific windows (EST)
        windows: dict[str, tuple[int, int]] = {
            "nba": (11, 23),
            "nfl": (12, 23),
            "nhl": (11, 23),
            "mlb": (11, 23),
            "ncaab": (11, 23),
            "ncaaf": (11, 23),
            "epl": (6, 13),   # UK morning matches
            "mls": (12, 22),
        }
        low, high = windows.get(sport, (10, 23))
        return low <= hour <= high

    # ── Data output ──────────────────────────────────────

    def _write_raw_json(
        self,
        provider: str,
        sport: str,
        data: Any,
    ) -> Path:
        """Write raw API response to timestamped JSON file."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_dir = DATA_DIR / "raw" / provider / sport / "live"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{ts}.json"
        out_file.write_text(json.dumps(data, indent=2, default=str))
        return out_file

    def _update_parquet(self, sport: str, data: dict[str, Any]) -> None:
        """Incrementally append live data to normalized parquet."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            return

        events = data.get("events", [])
        if not events:
            return

        records: list[dict[str, Any]] = []
        ts = datetime.now(timezone.utc).isoformat()
        for ev in events:
            competitions = ev.get("competitions", [{}])
            comp = competitions[0] if competitions else {}
            competitors = comp.get("competitors", [])
            home = next((c for c in competitors if c.get("homeAway") == "home"), {})
            away = next((c for c in competitors if c.get("homeAway") == "away"), {})

            records.append({
                "snapshot_time": ts,
                "game_id": str(ev.get("id", "")),
                "sport": sport,
                "status": ev.get("status", {}).get("type", {}).get("name", ""),
                "home_team": home.get("team", {}).get("displayName", ""),
                "away_team": away.get("team", {}).get("displayName", ""),
                "home_score": int(home.get("score", 0) or 0),
                "away_score": int(away.get("score", 0) or 0),
                "period": ev.get("status", {}).get("period", 0),
                "clock": ev.get("status", {}).get("displayClock", ""),
            })

        if not records:
            return

        schema = pa.schema([
            ("snapshot_time", pa.string()),
            ("game_id", pa.string()),
            ("sport", pa.string()),
            ("status", pa.string()),
            ("home_team", pa.string()),
            ("away_team", pa.string()),
            ("home_score", pa.int32()),
            ("away_score", pa.int32()),
            ("period", pa.int32()),
            ("clock", pa.string()),
        ])

        out_dir = DATA_DIR / "normalized" / sport
        out_dir.mkdir(parents=True, exist_ok=True)
        live_parquet = out_dir / "live_snapshots.parquet"

        columns = {k: [r[k] for r in records] for k in records[0]}
        new_table = pa.table(columns, schema=schema)

        if live_parquet.exists():
            try:
                existing = pq.read_table(live_parquet, schema=schema)
                combined = pa.concat_tables([existing, new_table])
            except Exception:
                combined = new_table
        else:
            combined = new_table

        pq.write_table(combined, live_parquet)

    # ── Polling ──────────────────────────────────────────

    async def _poll_endpoint(
        self,
        endpoint_name: str,
        config: dict[str, Any],
    ) -> None:
        """Poll a single endpoint type for all its sports."""
        interval = self.interval_override or config["interval"]
        active_only = config.get("active_only", False)
        url_template = config["url"]
        sport_map = config["sports"]

        logger.info(
            "Poller [%s] started — interval=%ds, sports=%s",
            endpoint_name,
            interval,
            [s for s in sport_map if s in self.sports],
        )

        while not self._shutdown.is_set():
            for sport, params in sport_map.items():
                if sport not in self.sports:
                    continue

                if active_only and not self._has_active_games(sport):
                    logger.debug(
                        "[%s/%s] No active games, skipping poll",
                        endpoint_name, sport,
                    )
                    continue

                # Build URL
                if params is not None:
                    group, slug = params
                    url = url_template.format(group=group, slug=slug)
                else:
                    url = url_template

                try:
                    async with self._session.get(
                        url, timeout=__import__("aiohttp").ClientTimeout(total=15)
                    ) as resp:
                        if resp.status != 200:
                            logger.warning(
                                "[%s/%s] HTTP %d from %s",
                                endpoint_name, sport, resp.status, url,
                            )
                            self._error_count += 1
                            continue

                        data = await resp.json(content_type=None)
                        self._poll_count += 1

                        # Deduplicate: skip if data hasn't changed
                        cache_key = f"{endpoint_name}:{sport}"
                        data_hash = hash(json.dumps(data, sort_keys=True, default=str))
                        prev_hash = self._last_data.get(cache_key, {}).get("hash")

                        if data_hash == prev_hash:
                            logger.debug(
                                "[%s/%s] No changes",
                                endpoint_name, sport,
                            )
                            continue

                        self._last_data[cache_key] = {
                            "hash": data_hash,
                            "time": datetime.now(timezone.utc).isoformat(),
                        }

                        # Write raw JSON
                        out = self._write_raw_json(
                            endpoint_name.split("_")[0], sport, data
                        )
                        logger.info(
                            "[%s/%s] ✓ polled, wrote %s",
                            endpoint_name, sport, out.name,
                        )

                        # Update parquet for scoreboard data
                        if "scoreboard" in endpoint_name:
                            self._update_parquet(sport, data)

                        # Broadcast SSE event
                        _broadcast_sse({
                            "endpoint": endpoint_name,
                            "sport": sport,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "events_count": len(data.get("events", [])),
                        })

                except asyncio.CancelledError:
                    return
                except Exception as exc:
                    logger.error(
                        "[%s/%s] Poll error: %s",
                        endpoint_name, sport, exc,
                    )
                    self._error_count += 1

            # Wait for interval or shutdown
            try:
                await asyncio.wait_for(
                    self._shutdown.wait(), timeout=interval
                )
                break  # shutdown signaled
            except asyncio.TimeoutError:
                pass  # normal: interval elapsed, poll again

    # ── SSE server ───────────────────────────────────────

    async def _sse_handler(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle an SSE client connection."""
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=100)
        _sse_subscribers.append(q)

        try:
            # Send HTTP headers
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/event-stream\r\n"
                b"Cache-Control: no-cache\r\n"
                b"Connection: keep-alive\r\n"
                b"X-Accel-Buffering: no\r\n"
                b"\r\n"
            )
            await writer.drain()

            # Connected event
            writer.write(
                b"event: system\n"
                b'data: {"type": "connected"}\n\n'
            )
            await writer.drain()

            seq = 0
            while not self._shutdown.is_set():
                try:
                    event = await asyncio.wait_for(q.get(), timeout=30.0)
                    seq += 1
                    payload = json.dumps(event, default=str)
                    msg = f"id: {seq}\nevent: live_update\ndata: {payload}\n\n"
                    writer.write(msg.encode())
                    await writer.drain()
                except asyncio.TimeoutError:
                    # Heartbeat
                    writer.write(b": heartbeat\n\n")
                    await writer.drain()
                except (ConnectionResetError, BrokenPipeError):
                    break
        finally:
            if q in _sse_subscribers:
                _sse_subscribers.remove(q)
            writer.close()

    async def start_sse_server(self, port: int = 8001) -> asyncio.Server | None:
        """Start a lightweight SSE server for real-time updates."""
        try:
            server = await asyncio.start_server(
                self._sse_handler, "127.0.0.1", port,
            )
            logger.info("SSE server listening on http://127.0.0.1:%d", port)
            return server
        except OSError as e:
            logger.warning("Could not start SSE server on port %d: %s", port, e)
            return None

    # ── Main run loop ────────────────────────────────────

    async def run(self) -> None:
        """Start all pollers and run until shutdown."""
        import aiohttp

        logger.info(
            "Live importer starting — sports=%s, dry_run=%s",
            self.sports, self.dry_run,
        )

        if self.dry_run:
            self._print_config()
            return

        self._session = aiohttp.ClientSession(
            headers={"User-Agent": "SportStock/5.0 LiveImporter"},
        )
        self._start_time = time.monotonic()

        # Start SSE server
        sse_server = await self.start_sse_server()

        # Start pollers
        tasks: list[asyncio.Task[None]] = []
        for name, config in LIVE_ENDPOINTS.items():
            # Only start poller if it has sports we care about
            endpoint_sports = set(config["sports"].keys()) & set(self.sports)
            if not endpoint_sports:
                continue
            task = asyncio.create_task(
                self._poll_endpoint(name, config),
                name=f"poller:{name}",
            )
            tasks.append(task)

        if not tasks:
            logger.warning("No pollers started — no matching sports")
            await self._session.close()
            return

        logger.info("Started %d pollers", len(tasks))

        # Wait for shutdown
        await self._shutdown.wait()

        logger.info("Shutdown signal received, stopping pollers …")

        # Cancel all tasks
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        # Stop SSE server
        if sse_server:
            sse_server.close()
            await sse_server.wait_closed()

        # Close HTTP session
        await self._session.close()

        elapsed = time.monotonic() - self._start_time
        logger.info(
            "Live importer stopped — ran %.0fs, %d polls, %d errors",
            elapsed, self._poll_count, self._error_count,
        )

    def shutdown(self) -> None:
        """Signal graceful shutdown."""
        self._shutdown.set()

    def _print_config(self) -> None:
        """Print what would be polled (dry-run mode)."""
        print("\n═══ Live Importer Configuration (dry-run) ═══\n")
        print(f"  Sports: {', '.join(self.sports)}")
        if self.interval_override:
            print(f"  Interval override: {self.interval_override}s")
        print()

        for name, config in LIVE_ENDPOINTS.items():
            endpoint_sports = set(config["sports"].keys()) & set(self.sports)
            if not endpoint_sports:
                continue

            interval = self.interval_override or config["interval"]
            active_only = config.get("active_only", False)
            print(f"  [{name}]")
            print(f"    URL pattern: {config['url']}")
            print(f"    Interval:    {interval}s")
            print(f"    Active only: {active_only}")
            print(f"    Sports:      {', '.join(sorted(endpoint_sports))}")

            for sport in sorted(endpoint_sports):
                params = config["sports"][sport]
                if params is not None:
                    group, slug = params
                    url = config["url"].format(group=group, slug=slug)
                else:
                    url = config["url"]
                print(f"      → {sport}: {url}")
            print()

        print("  Output directories:")
        for sport in self.sports:
            live_dir = DATA_DIR / "raw" / "espn" / sport / "live"
            print(f"    {sport}: {live_dir}")
        print()


# ── CLI ──────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Live data importer for SportStock V5.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  %(prog)s                          Run all pollers
  %(prog)s --sports nba,nhl         Only NBA and NHL
  %(prog)s --dry-run                Print configuration
  %(prog)s --interval 60            Override all intervals to 60s
  %(prog)s --sse-port 8001          SSE server on custom port
""",
    )
    p.add_argument(
        "--sports",
        type=str,
        default=None,
        help="Comma-separated sports to poll (default: all)",
    )
    p.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Override all poll intervals (seconds)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print configuration and exit without polling",
    )
    p.add_argument(
        "--sse-port",
        type=int,
        default=8001,
        help="Port for SSE output server (default: 8001)",
    )
    p.add_argument(
        "--log-level",
        choices=["debug", "info", "warning", "error"],
        default="info",
        help="Logging verbosity (default: info)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    _setup_logging(args.log_level)

    sports = args.sports.split(",") if args.sports else None

    importer = LiveImporter(
        sports=sports,
        interval_override=args.interval,
        dry_run=args.dry_run,
    )

    # Signal handlers for graceful shutdown
    loop = asyncio.new_event_loop()

    def _handle_signal(sig: int, frame: Any) -> None:
        signame = signal.Signals(sig).name
        logger.info("Received %s, shutting down …", signame)
        importer.shutdown()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        loop.run_until_complete(importer.run())
    except KeyboardInterrupt:
        importer.shutdown()
        loop.run_until_complete(asyncio.sleep(0.5))
    finally:
        loop.close()


if __name__ == "__main__":
    main()
