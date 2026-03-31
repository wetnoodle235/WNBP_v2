"""AutoBet scheduler — daemon that runs betting and grading cycles.

Usage::

    python -m autobet.scheduler              # continuous polling (5-min bet, 30-min grade)
    python -m autobet.scheduler --once       # single betting cycle then exit
    python -m autobet.scheduler --grade-only # grade pending bets then exit
    python -m autobet.scheduler --stats      # print performance stats then exit
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from pathlib import Path

from .config import AutobetConfig
from .engine import BettingEngine
from .grader import Grader
from .ledger import Ledger

logger = logging.getLogger("autobet.scheduler")


class AutobetScheduler:
    """Runs betting and grading cycles on schedule."""

    def __init__(self, config: AutobetConfig | None = None):
        self.config = config or AutobetConfig.from_env()
        self.ledger = Ledger(self._default_db_path())
        self.engine = BettingEngine(self.config, self.ledger)
        self.grader = Grader(self.config, self.ledger)
        self._running = False
        self._last_grade_time: float = 0.0

    def _default_db_path(self) -> Path:
        return Path(__file__).resolve().parent / "data" / "paper_bets.db"

    # ── main loop ───────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the continuous scheduler loop."""
        self._running = True
        logging.basicConfig(
            level=getattr(logging, self.config.log_level.upper(), logging.INFO),
            format="%(asctime)s  %(name)-22s  %(levelname)-7s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        logger.info(
            "AutoBet scheduler starting  [cycle=%ds  grade=%ds  sports=%s]",
            self.config.betting_cycle_seconds,
            self.config.grading_cycle_seconds,
            ",".join(self.config.sports),
        )

        while self._running:
            cycle_start = time.monotonic()

            # ── Betting cycle ───────────────────────────────────────────
            try:
                placed = await self.engine.run_cycle()
                if placed:
                    await self._notify_placed(placed)
            except Exception as exc:
                logger.error("Betting cycle error: %s", exc, exc_info=True)

            # ── Grading cycle (if due) ──────────────────────────────────
            if self._should_grade():
                try:
                    summary = await self.grader.run_grading()
                    if summary.get("graded", 0) > 0:
                        await self._notify_graded(summary)
                    self._last_grade_time = time.monotonic()
                except Exception as exc:
                    logger.error("Grading cycle error: %s", exc, exc_info=True)

            # ── Bankroll snapshot ───────────────────────────────────────
            try:
                balance = self.ledger.get_bankroll(self.config.bankroll_dollars)
                pending = self.ledger.get_pending_exposure()
                peak = max(balance, self.config.bankroll_dollars)
                self.ledger.snapshot_bankroll(balance, peak, pending, "cycle")
            except Exception:
                pass  # non-critical

            # ── Sleep ───────────────────────────────────────────────────
            elapsed = time.monotonic() - cycle_start
            sleep_time = max(1.0, self.config.betting_cycle_seconds - elapsed)
            logger.debug("Sleeping %.0fs until next cycle", sleep_time)
            await asyncio.sleep(sleep_time)

    def stop(self) -> None:
        """Signal the scheduler to stop after the current cycle."""
        self._running = False
        logger.info("AutoBet scheduler stopping…")

    def _should_grade(self) -> bool:
        """Check if enough time has elapsed since the last grading run."""
        if self._last_grade_time == 0.0:
            return True  # first run
        return (time.monotonic() - self._last_grade_time) >= self.config.grading_cycle_seconds

    # ── one-shot commands ───────────────────────────────────────────────

    async def run_once(self) -> None:
        """Execute a single betting cycle then return."""
        logger.info("Running single betting cycle…")
        placed = await self.engine.run_cycle()
        logger.info("Single cycle complete: %d bets placed", len(placed))
        for bet in placed:
            _print_bet(bet)

    async def grade_only(self) -> None:
        """Grade pending bets then return."""
        logger.info("Running grading pass…")
        summary = await self.grader.run_grading()
        logger.info("Grading complete: %s", json.dumps(summary))

    def print_stats(self) -> None:
        """Print performance statistics."""
        stats = self.ledger.get_stats(days=30)
        bankroll = self.ledger.get_bankroll(self.config.bankroll_dollars)
        pending = self.ledger.get_pending_exposure()

        print("\n══════════════════════════════════════════════")
        print("  AutoBet Paper Trading — 30-Day Performance")
        print("══════════════════════════════════════════════")
        print(f"  Bankroll:   ${bankroll:,.2f}  (started ${self.config.bankroll_dollars:,.2f})")
        print(f"  Pending:    ${pending:,.2f}")
        print(f"  Total P&L:  ${stats['total_pnl']:+,.2f}")
        print(f"  ROI:        {stats['roi']:.1%}")
        print(f"  Win Rate:   {stats['win_rate']:.1%}  ({stats['won']}W / {stats['lost']}L / {stats['push']}P)")
        print(f"  Bets Graded:{stats['total_graded']}")

        if stats["by_sport"]:
            print("\n  By Sport:")
            for sport, data in sorted(stats["by_sport"].items()):
                total = data["won"] + data["lost"] + data["push"]
                wr = data["won"] / total if total > 0 else 0.0
                print(
                    f"    {sport:<6s}  {data['won']}W/{data['lost']}L  "
                    f"WR={wr:.0%}  P&L=${data['pnl']:+.2f}"
                )

        if stats["by_bet_type"]:
            print("\n  By Bet Type:")
            for bt, data in sorted(stats["by_bet_type"].items()):
                total = data["won"] + data["lost"] + data["push"]
                wr = data["won"] / total if total > 0 else 0.0
                print(
                    f"    {bt:<8s}  {data['won']}W/{data['lost']}L  "
                    f"WR={wr:.0%}  P&L=${data['pnl']:+.2f}"
                )

        today_bets = self.ledger.get_today_bets()
        if today_bets:
            print(f"\n  Today: {len(today_bets)} bets")
            for bet in today_bets[:10]:
                _print_bet(bet, indent=4)

        print("══════════════════════════════════════════════\n")

    # ── notifications ───────────────────────────────────────────────────

    async def _notify_placed(self, bets: list) -> None:
        """Send Discord alert for newly placed bets (if configured)."""
        if not self.config.discord_enabled or not self.config.discord_webhook_url:
            return
        try:
            import httpx

            content = f"🎯 **AutoBet placed {len(bets)} bet(s)**\n"
            for bet in bets[:5]:
                picks = ", ".join(lg.selection for lg in bet.legs)
                content += (
                    f"• {bet.strategy.upper()} {bet.bet_type} "
                    f"**{picks}** — {bet.stake_units:.2f}u "
                    f"@ {bet.implied_odds:.2f} "
                    f"(conf={bet.model_confidence:.0%} edge={bet.model_edge:.0%})\n"
                )

            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    self.config.discord_webhook_url,
                    json={"content": content[:2000]},
                )
        except Exception as exc:
            logger.debug("Discord notify failed: %s", exc)

    async def _notify_graded(self, summary: dict) -> None:
        """Send Discord alert for grading results."""
        if not self.config.discord_enabled or not self.config.discord_webhook_url:
            return
        try:
            import httpx

            content = (
                f"📊 **Grading complete**: "
                f"{summary.get('won', 0)}W / {summary.get('lost', 0)}L / "
                f"{summary.get('push', 0)}P / {summary.get('void', 0)}V"
            )
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    self.config.discord_webhook_url,
                    json={"content": content},
                )
        except Exception as exc:
            logger.debug("Discord notify failed: %s", exc)


# ── helpers ─────────────────────────────────────────────────────────────────


def _print_bet(bet, indent: int = 2) -> None:
    prefix = " " * indent
    picks = ", ".join(lg.selection for lg in bet.legs)
    status = bet.status.upper()
    pnl = f"P&L={bet.pnl_units:+.2f}" if bet.status != "pending" else ""
    print(
        f"{prefix}[{status:7s}] {bet.strategy:<14s} {bet.bet_type:<8s} "
        f"{picks}  {bet.stake_units:.2f}u @ {bet.implied_odds:.2f}  "
        f"conf={bet.model_confidence:.0%} edge={bet.model_edge:.0%} {pnl}"
    )


# ── CLI entry point ─────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="autobet",
        description="AutoBet paper-trading bot — autonomous ML-driven betting",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single betting cycle then exit",
    )
    parser.add_argument(
        "--grade-only",
        action="store_true",
        help="Grade pending bets then exit",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show performance statistics then exit",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Override log level",
    )
    args = parser.parse_args()

    config = AutobetConfig.from_env()
    if args.log_level:
        config.log_level = args.log_level

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s  %(name)-22s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    scheduler = AutobetScheduler(config)

    if args.stats:
        scheduler.print_stats()
        return

    if args.once:
        asyncio.run(scheduler.run_once())
        return

    if args.grade_only:
        asyncio.run(scheduler.grade_only())
        return

    # Default: continuous mode
    try:
        asyncio.run(scheduler.start())
    except KeyboardInterrupt:
        scheduler.stop()
        logger.info("AutoBet stopped by user")


if __name__ == "__main__":
    main()
