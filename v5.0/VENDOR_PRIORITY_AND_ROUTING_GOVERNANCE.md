# Vendor Priority and Routing Governance

This file defines how to keep blended routing and field-level provider priority safe as loaders grow.

## Files of Record

- Routing ledger: `config/normalized_blended_routing_registry.csv`
- Field-level priority ledger: `config/field_vendor_priority_registry.csv`
- Runtime defaults: `backend/normalization/provider_map.py`

## Required Workflow for Every New Loader

1. Add provider order in `PROVIDER_PRIORITY`.
2. Add destination mapping row in `config/normalized_blended_routing_registry.csv`.
3. Add default `*` label priority row in `config/field_vendor_priority_registry.csv`.
4. Add explicit label overrides for any known quality-sensitive fields (for example logo, weather, spread, advanced metrics).
5. Run a focused normalize command for one season and verify row counts are non-zero.

## Merge Rules

1. Data-type provider order is the baseline.
2. Label-level overrides are allowed and should be applied where available.
3. If a label has no override, fall back to data-type provider order.
4. Thin snapshots are allowed for model-critical scalar fields only.
5. Canonical full-detail rows should exist in only one blended destination.

## Suggested Ownership Fields

Each CSV row should maintain:
- `owner`: person/team accountable
- `confidence`: low/medium/high
- `last_reviewed`: date of latest confidence review
- `notes`: why this priority exists

## Review Cadence

- Weekly while adding new loaders.
- Monthly after ingestion stabilizes.
- Immediate review when a new vendor is introduced or quality shifts.
