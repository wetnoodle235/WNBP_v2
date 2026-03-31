#!/usr/bin/env python3
"""Lightweight backend smoke checks for CI.

Runs fast in-process checks without requiring live external providers.
"""

from __future__ import annotations

import sys
from pathlib import Path

from fastapi.testclient import TestClient


PROJECT_ROOT = Path(__file__).resolve().parent.parent
BACKEND_DIR = PROJECT_ROOT / "backend"
sys.path.insert(0, str(BACKEND_DIR))

from main import app  # noqa: E402


REQUIRED_PATHS = {
    "/health",
    "/v1/sports",
    "/v1/meta/sports",
    "/v1/predictions/{sport}",
    "/v1/predictions/{sport}/history",
    "/v1/predictions/{sport}/metrics/calibration",
}


def assert_routes_present() -> None:
    seen = {route.path for route in app.routes}
    missing = sorted(REQUIRED_PATHS - seen)
    if missing:
        raise AssertionError(f"Missing expected API routes: {missing}")


def run_http_smoke() -> None:
    with TestClient(app) as client:
        resp = client.get("/health")
        if resp.status_code != 200:
            raise AssertionError(f"/health failed: {resp.status_code} {resp.text}")

        sports_resp = client.get("/v1/sports")
        if sports_resp.status_code != 200:
            raise AssertionError(f"/v1/sports failed: {sports_resp.status_code} {sports_resp.text}")

        hist_resp = client.get("/v1/predictions/nba/history", params={"limit": 1})
        if hist_resp.status_code != 200:
            raise AssertionError(
                f"/v1/predictions/nba/history failed: {hist_resp.status_code} {hist_resp.text}"
            )

        cal_resp = client.get("/v1/predictions/nba/metrics/calibration", params={"days": 30, "bins": 5})
        if cal_resp.status_code != 200:
            raise AssertionError(
                f"/v1/predictions/nba/metrics/calibration failed: {cal_resp.status_code} {cal_resp.text}"
            )


if __name__ == "__main__":
    assert_routes_present()
    run_http_smoke()
    print("backend_smoke: OK")
