#!/usr/bin/env python3
"""Simple in-process API performance sanity checks.

This is not a benchmark; it only guards against severe regressions.
"""

from __future__ import annotations

import statistics
import sys
import time
from pathlib import Path

from fastapi.testclient import TestClient


PROJECT_ROOT = Path(__file__).resolve().parent.parent
BACKEND_DIR = PROJECT_ROOT / "backend"
sys.path.insert(0, str(BACKEND_DIR))

from main import app  # noqa: E402


CHECKS = [
    ("/health", 1.0),
    ("/v1/sports", 3.0),
    ("/v1/predictions/nba/history?limit=100", 4.0),
    ("/v1/predictions/nba/metrics/calibration?days=90&bins=10", 5.0),
]


def timed_get(client: TestClient, path: str, runs: int = 3) -> tuple[int, float]:
    times = []
    status = 0
    for _ in range(runs):
        t0 = time.perf_counter()
        resp = client.get(path)
        dt = time.perf_counter() - t0
        status = resp.status_code
        times.append(dt)
    return status, statistics.median(times)


def main() -> int:
    with TestClient(app) as client:
        failures: list[str] = []
        for path, max_seconds in CHECKS:
            status, median_s = timed_get(client, path)
            print(f"{path} -> status={status}, median={median_s:.3f}s, limit={max_seconds:.3f}s")
            if status != 200:
                failures.append(f"{path}: status {status}")
                continue
            if median_s > max_seconds:
                failures.append(f"{path}: median {median_s:.3f}s exceeded {max_seconds:.3f}s")

    if failures:
        print("backend_perf_sanity: FAIL")
        for f in failures:
            print(f"  - {f}")
        return 1

    print("backend_perf_sanity: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
