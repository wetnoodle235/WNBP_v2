"""AutoBet — autonomous paper-trading bot for the v5.0 sports platform.

Quick start::

    python -m autobet.scheduler              # continuous mode
    python -m autobet.scheduler --once       # single cycle
    python -m autobet.scheduler --stats      # view performance
    python -m autobet.scheduler --grade-only # grade pending bets
"""

from .config import AutobetConfig
from .engine import BettingEngine
from .grader import Grader
from .ledger import Ledger
from .meta_evaluator import MetaEvaluator
from .models import BetCandidate, BetLeg, PaperBet
from .scheduler import AutobetScheduler

__all__ = [
    "AutobetConfig",
    "AutobetScheduler",
    "BetCandidate",
    "BetLeg",
    "BettingEngine",
    "Grader",
    "Ledger",
    "MetaEvaluator",
    "PaperBet",
]
