#!/usr/bin/env python3
from __future__ import annotations

import csv
import importlib
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BACKEND_DIR = PROJECT_ROOT / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

provider_map = importlib.import_module("normalization.provider_map")

ROUTING_CSV = PROJECT_ROOT / "config" / "normalized_blended_routing_registry.csv"
FIELD_PRIORITY_CSV = PROJECT_ROOT / "config" / "field_vendor_priority_registry.csv"


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"missing required file: {path}")
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _valid_provider_names(order: str) -> list[str]:
    return [p.strip() for p in order.split(">") if p.strip()]


def main() -> int:
    routing_rows = _read_csv(ROUTING_CSV)
    field_rows = _read_csv(FIELD_PRIORITY_CSV)

    errors: list[str] = []
    warnings: list[str] = []

    # Build quick lookup maps
    routing_by_sport_dtype: dict[tuple[str, str], dict[str, str]] = {}
    for row in routing_rows:
        sport = (row.get("sport") or "").strip().lower()
        dtype = (row.get("source_data_type") or "").strip()
        key = (sport, dtype)
        if not sport or not dtype:
            errors.append(f"routing row missing sport/source_data_type: {row}")
            continue
        if key in routing_by_sport_dtype:
            errors.append(f"duplicate routing row for {sport}/{dtype}")
            continue
        routing_by_sport_dtype[key] = row

    field_defaults: dict[tuple[str, str], bool] = {}
    field_pairs: dict[tuple[str, str], list[dict[str, str]]] = {}
    for row in field_rows:
        sport = (row.get("sport") or "").strip().lower()
        dtype = (row.get("data_type") or "").strip()
        label_pattern = (row.get("label_pattern") or "").strip()
        priority = (row.get("provider_priority") or "").strip()
        if not sport or not dtype or not label_pattern or not priority:
            errors.append(f"field-priority row missing required value: {row}")
            continue

        providers = _valid_provider_names(priority)
        if not providers:
            errors.append(f"invalid provider_priority for {sport}/{dtype}/{label_pattern}: {priority}")
        field_pairs.setdefault((sport, dtype), []).append(row)

        if label_pattern == "*":
            field_defaults[(sport, dtype)] = True

    # Validate routing references provider_map + has field default
    provider_priority = provider_map.PROVIDER_PRIORITY
    for (sport, dtype), _ in sorted(routing_by_sport_dtype.items()):
        sport_map = provider_priority.get(sport)
        if sport_map is None:
            errors.append(f"routing references unknown sport in PROVIDER_PRIORITY: {sport}")
            continue
        if dtype not in sport_map:
            errors.append(f"routing references missing data_type in PROVIDER_PRIORITY: {sport}/{dtype}")

        if (sport, dtype) not in field_defaults:
            warnings.append(f"missing field priority '*' default for {sport}/{dtype}")

    # Validate providers in field_priority rows are known for that sport.
    for (sport, dtype), rows in field_pairs.items():
        known = set(provider_map.all_providers(sport))
        for row in rows:
            label_pattern = row["label_pattern"]
            providers = _valid_provider_names(row["provider_priority"])
            unknown = [p for p in providers if p not in known]
            if unknown:
                errors.append(
                    f"unknown providers in field priority {sport}/{dtype}/{label_pattern}: {','.join(unknown)}"
                )

    if errors:
        print("FAILED: blended registry validation")
        for e in errors:
            print(f" - {e}")
        return 1

    print("OK: blended registry validation passed")
    if warnings:
        print(f"Warnings: {len(warnings)}")
        for w in warnings:
            print(f" - {w}")
    print(f"Routing rows: {len(routing_rows)}")
    print(f"Field-priority rows: {len(field_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
