"""
Shared pytest configuration for the backend test suite.

- Adds the backend root to sys.path so tests can be run from any working
  directory (e.g. `pytest backend/tests/` from the project root).
- Disables the IP-based rate-limit middleware so the token bucket does not
  drain across the full test suite and cause spurious 429 responses.
- Silences the PytestDeprecationWarning about asyncio_default_fixture_loop_scope
  by declaring the scope explicitly (see pytest.ini).
"""

import os
import sys
from pathlib import Path

# Must be set BEFORE any test module imports `main` so that the middleware's
# per-request env-var check sees the correct value.
os.environ.setdefault("RATE_LIMIT_ENABLED", "false")

# Ensure `from main import app`, `from config import ...` etc. work regardless
# of the cwd from which pytest is invoked.
_backend_root = Path(__file__).resolve().parent.parent
if str(_backend_root) not in sys.path:
    sys.path.insert(0, str(_backend_root))
