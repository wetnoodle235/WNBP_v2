# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — MMA Normalized-Curated PyArrow Schemas (Consolidated)
# ──────────────────────────────────────────────────────────────────────
#
# 10-entity consolidated design.  Raw data from BDL MMA API and
# odds providers are merged into 10 wide schemas that use
# discriminator columns where needed to distinguish record subtypes
# within a single table.
#
# Entity overview
# ───────────────
#  1. leagues         — static reference, no partitioning
#  2. weight_classes  — static reference, no partitioning
#  3. venues          — static reference, no partitioning
#  4. fighters        — partition: season=
#  5. events          — partition: season=
#  6. fights          — partition: season=
#  7. rankings        — partition: season=
#  8. fight_stats     — partition: season=
#  9. odds            — partition: season=
# 10. player_props    — partition: season=
#
# MMA does NOT use week-based partitioning — events are date-based.
# Season is the calendar year of the event (e.g. 2024).
#
# Every schema carries a mandatory ``source`` field for vendor provenance.
# ──────────────────────────────────────────────────────────────────────

from __future__ import annotations

import pyarrow as pa


# ═══════════════════════════════════════════════════════════════════════
# Helper — shorthand for pa.field with metadata description
# ═══════════════════════════════════════════════════════════════════════

def _f(name: str, dtype: pa.DataType, description: str, nullable: bool = True) -> pa.Field:
    """Create a pa.field with an embedded description in its metadata."""
    return pa.field(
        name,
        dtype,
        nullable=nullable,
        metadata={"description": description},
    )


# ═══════════════════════════════════════════════════════════════════════
# 1. leagues — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

MMA_LEAGUES_SCHEMA = pa.schema([
    # Core identity
    _f("league_id",      pa.int32(),  "Unique league identifier",                    nullable=False),
    _f("name",           pa.string(), "League name (e.g. Ultimate Fighting Championship)", nullable=False),
    _f("abbreviation",   pa.string(), "League abbreviation (e.g. UFC)"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                      nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. weight_classes — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

MMA_WEIGHT_CLASSES_SCHEMA = pa.schema([
    # Core identity
    _f("weight_class_id",    pa.int32(),  "Unique weight class identifier",           nullable=False),
    _f("name",               pa.string(), "Weight class name (e.g. Lightweight)",     nullable=False),
    _f("abbreviation",       pa.string(), "Weight class abbreviation (e.g. LW)"),
    _f("weight_limit_lbs",   pa.float64(),"Upper weight limit in pounds"),
    _f("gender",             pa.string(), "Gender category (male, female)"),
    # Provenance
    _f("source",             pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. venues — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

MMA_VENUES_SCHEMA = pa.schema([
    # Core identity
    _f("venue_id",   pa.string(), "Unique venue identifier (derived from events)",    nullable=False),
    _f("name",       pa.string(), "Venue name",                                       nullable=False),
    _f("city",       pa.string(), "Venue city"),
    _f("state",      pa.string(), "Venue state / province"),
    _f("country",    pa.string(), "Venue country"),
    # Provenance
    _f("source",     pa.string(), "Data vendor provenance",                           nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 4. fighters — partition: season=
# ═══════════════════════════════════════════════════════════════════════

MMA_FIGHTERS_SCHEMA = pa.schema([
    # Core identity
    _f("fighter_id",         pa.int32(),  "Unique fighter identifier",                nullable=False),
    _f("name",               pa.string(), "Fighter full name",                        nullable=False),
    _f("first_name",         pa.string(), "First name"),
    _f("last_name",          pa.string(), "Last name"),
    _f("nickname",           pa.string(), "Fighter nickname"),
    _f("season",             pa.int32(),  "Snapshot season year",                     nullable=False),
    # Bio
    _f("date_of_birth",      pa.string(), "Date of birth (YYYY-MM-DD)"),
    _f("birth_place",        pa.string(), "Birth place"),
    _f("nationality",        pa.string(), "Nationality / country"),
    # Physical
    _f("height_inches",      pa.float64(),"Height in inches"),
    _f("reach_inches",       pa.float64(),"Reach in inches"),
    _f("weight_lbs",         pa.float64(),"Weight in pounds"),
    _f("stance",             pa.string(), "Fighting stance (Orthodox, Southpaw, Switch)"),
    # Record
    _f("record_wins",        pa.int32(),  "Career wins"),
    _f("record_losses",      pa.int32(),  "Career losses"),
    _f("record_draws",       pa.int32(),  "Career draws"),
    _f("record_no_contests", pa.int32(),  "Career no-contests"),
    _f("active",             pa.bool_(),  "Whether the fighter is currently active"),
    # Weight class (flattened from nested)
    _f("weight_class_id",    pa.int32(),  "Primary weight class identifier"),
    _f("weight_class_name",  pa.string(), "Primary weight class name"),
    # Provenance
    _f("source",             pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. events — partition: season=
# ═══════════════════════════════════════════════════════════════════════

MMA_EVENTS_SCHEMA = pa.schema([
    # Core identity
    _f("event_id",                  pa.int32(),  "Unique event identifier",            nullable=False),
    _f("name",                      pa.string(), "Event name (e.g. UFC 300)",          nullable=False),
    _f("short_name",                pa.string(), "Short event name"),
    _f("season",                    pa.int32(),  "Season / calendar year",             nullable=False),
    _f("date",                      pa.string(), "Event date (YYYY-MM-DD)"),
    # Venue (flattened)
    _f("venue_name",                pa.string(), "Venue name"),
    _f("venue_city",                pa.string(), "Venue city"),
    _f("venue_state",               pa.string(), "Venue state / province"),
    _f("venue_country",             pa.string(), "Venue country"),
    # Status / timing
    _f("status",                    pa.string(), "Event status (scheduled, completed, cancelled)"),
    _f("main_card_start_time",      pa.string(), "Main card start time (ISO-8601)"),
    _f("prelims_start_time",        pa.string(), "Prelims start time (ISO-8601)"),
    _f("early_prelims_start_time",  pa.string(), "Early prelims start time (ISO-8601)"),
    # League (flattened from nested)
    _f("league_id",                 pa.int32(),  "League identifier"),
    _f("league_name",               pa.string(), "League name"),
    # Provenance
    _f("source",                    pa.string(), "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. fights — partition: season=
# ═══════════════════════════════════════════════════════════════════════

MMA_FIGHTS_SCHEMA = pa.schema([
    # Core identity
    _f("fight_id",              pa.int32(),  "Unique fight identifier",                nullable=False),
    _f("season",                pa.int32(),  "Season / calendar year",                 nullable=False),
    # Event (flattened from nested)
    _f("event_id",              pa.int32(),  "Event identifier"),
    _f("event_name",            pa.string(), "Event name"),
    _f("event_date",            pa.string(), "Event date (YYYY-MM-DD)"),
    # Fighters (flattened from nested)
    _f("fighter1_id",           pa.int32(),  "Fighter 1 identifier"),
    _f("fighter1_name",         pa.string(), "Fighter 1 full name"),
    _f("fighter2_id",           pa.int32(),  "Fighter 2 identifier"),
    _f("fighter2_name",         pa.string(), "Fighter 2 full name"),
    _f("winner_id",             pa.int32(),  "Winner fighter identifier (null if draw/NC)"),
    _f("winner_name",           pa.string(), "Winner fighter name"),
    # Weight class (flattened from nested)
    _f("weight_class_id",       pa.int32(),  "Weight class identifier"),
    _f("weight_class_name",     pa.string(), "Weight class name"),
    # Fight metadata
    _f("is_main_event",         pa.bool_(),  "Whether this is the main event"),
    _f("is_title_fight",        pa.bool_(),  "Whether this is a title fight"),
    _f("card_segment",          pa.string(), "Card segment (main, prelims, early_prelims)"),
    _f("fight_order",           pa.int32(),  "Fight order on the card"),
    _f("scheduled_rounds",      pa.int32(),  "Number of scheduled rounds (3 or 5)"),
    # Result
    _f("result_method",         pa.string(), "Result method (KO/TKO, Submission, Decision)"),
    _f("result_method_detail",  pa.string(), "Detailed result (e.g. Rear-Naked Choke, Unanimous)"),
    _f("result_round",          pa.int32(),  "Round in which fight ended"),
    _f("result_time",           pa.string(), "Time in round when fight ended (MM:SS)"),
    _f("status",                pa.string(), "Fight status (scheduled, completed, cancelled)"),
    # Provenance
    _f("source",                pa.string(), "Data vendor provenance",                 nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 7. rankings — partition: season=
# ═══════════════════════════════════════════════════════════════════════

MMA_RANKINGS_SCHEMA = pa.schema([
    # Keys
    _f("season",              pa.int32(),  "Season / calendar year",                   nullable=False),
    # Weight class (flattened from nested)
    _f("weight_class_id",     pa.int32(),  "Weight class identifier"),
    _f("weight_class_name",   pa.string(), "Weight class name"),
    # League (flattened from nested)
    _f("league_id",           pa.int32(),  "League identifier"),
    _f("league_name",         pa.string(), "League name"),
    # Ranking entry (flattened from rankings array)
    _f("rank",                pa.int32(),  "Numerical rank position"),
    _f("is_champion",         pa.bool_(),  "Whether fighter is the current champion"),
    _f("is_interim_champion", pa.bool_(),  "Whether fighter is the interim champion"),
    # Fighter (flattened from nested)
    _f("fighter_id",          pa.int32(),  "Fighter identifier"),
    _f("fighter_name",        pa.string(), "Fighter full name"),
    # Provenance
    _f("source",              pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 8. fight_stats — partition: season=
# ═══════════════════════════════════════════════════════════════════════

MMA_FIGHT_STATS_SCHEMA = pa.schema([
    # Keys
    _f("fight_id",                      pa.int32(),  "Fight identifier",                nullable=False),
    _f("fighter_id",                    pa.int32(),  "Fighter identifier",              nullable=False),
    _f("fighter_name",                  pa.string(), "Fighter full name"),
    _f("season",                        pa.int32(),  "Season / calendar year",          nullable=False),
    _f("round_number",                  pa.int32(),  "Round number (0 = totals)"),
    # Knockdowns
    _f("knockdowns",                    pa.int32(),  "Knockdowns landed"),
    # Significant strikes
    _f("sig_strikes_landed",            pa.int32(),  "Significant strikes landed"),
    _f("sig_strikes_attempted",         pa.int32(),  "Significant strikes attempted"),
    _f("sig_strike_pct",                pa.float64(),"Significant strike accuracy (0-1)"),
    # Total strikes
    _f("total_strikes_landed",          pa.int32(),  "Total strikes landed"),
    _f("total_strikes_attempted",       pa.int32(),  "Total strikes attempted"),
    # Takedowns
    _f("takedowns_landed",              pa.int32(),  "Takedowns landed"),
    _f("takedowns_attempted",           pa.int32(),  "Takedowns attempted"),
    _f("takedown_pct",                  pa.float64(),"Takedown accuracy (0-1)"),
    # Grappling
    _f("sub_attempts",                  pa.int32(),  "Submission attempts"),
    _f("reversals",                     pa.int32(),  "Reversals"),
    _f("control_time_seconds",          pa.int32(),  "Control time in seconds"),
    # Strike targets
    _f("head_strikes_landed",           pa.int32(),  "Head strikes landed"),
    _f("head_strikes_attempted",        pa.int32(),  "Head strikes attempted"),
    _f("body_strikes_landed",           pa.int32(),  "Body strikes landed"),
    _f("body_strikes_attempted",        pa.int32(),  "Body strikes attempted"),
    _f("leg_strikes_landed",            pa.int32(),  "Leg strikes landed"),
    _f("leg_strikes_attempted",         pa.int32(),  "Leg strikes attempted"),
    # Strike positions
    _f("distance_strikes_landed",       pa.int32(),  "Distance strikes landed"),
    _f("distance_strikes_attempted",    pa.int32(),  "Distance strikes attempted"),
    _f("clinch_strikes_landed",         pa.int32(),  "Clinch strikes landed"),
    _f("clinch_strikes_attempted",      pa.int32(),  "Clinch strikes attempted"),
    _f("ground_strikes_landed",         pa.int32(),  "Ground strikes landed"),
    _f("ground_strikes_attempted",      pa.int32(),  "Ground strikes attempted"),
    # Provenance
    _f("source",                        pa.string(), "Data vendor provenance",          nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 9. odds — partition: season=
# ═══════════════════════════════════════════════════════════════════════

MMA_ODDS_SCHEMA = pa.schema([
    # Keys
    _f("fight_id",            pa.int32(),  "Fight identifier",                          nullable=False),
    _f("event_id",            pa.int32(),  "Event identifier"),
    _f("season",              pa.int32(),  "Season / calendar year",                    nullable=False),
    _f("date",                pa.string(), "Event date (YYYY-MM-DD)"),
    _f("sportsbook",          pa.string(), "Sportsbook name"),
    # Fighters
    _f("fighter1_name",       pa.string(), "Fighter 1 display name"),
    _f("fighter2_name",       pa.string(), "Fighter 2 display name"),
    # Moneyline
    _f("moneyline_fighter1",  pa.int32(),  "Fighter 1 moneyline odds (American)"),
    _f("moneyline_fighter2",  pa.int32(),  "Fighter 2 moneyline odds (American)"),
    # Totals (over/under rounds)
    _f("total_over",          pa.float64(),"Over line (total rounds)"),
    _f("total_under",         pa.float64(),"Under line (total rounds)"),
    _f("total_over_odds",     pa.int32(),  "Over odds (American)"),
    _f("total_under_odds",    pa.int32(),  "Under odds (American)"),
    # Method / round props
    _f("method",              pa.string(), "Win method market (KO, Sub, Decision)"),
    _f("method_odds",         pa.int32(),  "Win method odds (American)"),
    # Discriminator
    _f("line_type",           pa.string(), "open, current, closing, etc."),
    _f("timestamp",           pa.string(), "Timestamp when line was captured (ISO-8601)"),
    # Provenance
    _f("source",              pa.string(), "Data vendor provenance",                    nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 10. player_props — partition: season=
# ═══════════════════════════════════════════════════════════════════════

MMA_PLAYER_PROPS_SCHEMA = pa.schema([
    # Keys
    _f("fight_id",       pa.int32(),  "Fight identifier",                               nullable=False),
    _f("fighter_id",     pa.int32(),  "Fighter identifier",                             nullable=False),
    _f("fighter_name",   pa.string(), "Fighter display name"),
    _f("season",         pa.int32(),  "Season / calendar year",                         nullable=False),
    _f("date",           pa.string(), "Event date (YYYY-MM-DD)"),
    _f("event_name",     pa.string(), "Event name"),
    # Prop details
    _f("prop_type",      pa.string(), "Prop market type (e.g. sig_strikes, takedowns, method)"),
    _f("line",           pa.float64(),"Prop line value"),
    _f("over_odds",      pa.int32(),  "Over odds (American)"),
    _f("under_odds",     pa.int32(),  "Under odds (American)"),
    _f("sportsbook",     pa.string(), "Sportsbook name"),
    # Provenance
    _f("source",         pa.string(), "Data vendor provenance",                         nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# Registry — schema, partition key, and path look-ups
# ═══════════════════════════════════════════════════════════════════════

MMA_SCHEMAS: dict[str, pa.Schema] = {
    "leagues":        MMA_LEAGUES_SCHEMA,
    "weight_classes": MMA_WEIGHT_CLASSES_SCHEMA,
    "venues":         MMA_VENUES_SCHEMA,
    "fighters":       MMA_FIGHTERS_SCHEMA,
    "events":         MMA_EVENTS_SCHEMA,
    "fights":         MMA_FIGHTS_SCHEMA,
    "rankings":       MMA_RANKINGS_SCHEMA,
    "fight_stats":    MMA_FIGHT_STATS_SCHEMA,
    "odds":           MMA_ODDS_SCHEMA,
    "player_props":   MMA_PLAYER_PROPS_SCHEMA,
}

MMA_PARTITION_KEYS: dict[str, list[str]] = {
    # Static reference — no partitioning
    "leagues":        [],
    "weight_classes": [],
    "venues":         [],
    # Season only — MMA uses dates, not weeks
    "fighters":       ["season"],
    "events":         ["season"],
    "fights":         ["season"],
    "rankings":       ["season"],
    "fight_stats":    ["season"],
    "odds":           ["season"],
    "player_props":   ["season"],
}

MMA_ENTITY_PATHS: dict[str, str] = {
    "leagues":        "leagues",
    "weight_classes": "weight_classes",
    "venues":         "venues",
    "fighters":       "fighters",
    "events":         "events",
    "fights":         "fights",
    "rankings":       "rankings",
    "fight_stats":    "fight_stats",
    "odds":           "odds",
    "player_props":   "player_props",
}


# ── Normalizer data-type → entity routing ─────────────────────────────
# Maps the normalizer's data-type names (the prefix of {type}_{season}.parquet)
# to the flat entity directory name under normalized_curated/mma/.
# Types mapping to None are intentionally skipped (non-entity artefacts).
MMA_TYPE_TO_ENTITY: dict[str, str | None] = {
    "teams":            None,           # MMA doesn't have teams
    "players":          "fighters",
    "schedule":         "events",
    "scores":           "fights",
    "standings":        "rankings",
    "odds":             "odds",
    "player_props":     "player_props",
    "player_stats":     "fight_stats",
    "roster":           None,
    "lineups":          None,
    "plays":            None,
    "news":             None,
    "weather":          None,
    "market_signals":   None,
    "injuries":         None,
    "depth_charts":     None,
    "rankings":         "rankings",
    "conferences":      "leagues",
    "venues":           "venues",
}

# ── Entity allow-list and static entities ─────────────────────────────
MMA_ENTITY_ALLOWLIST: set[str] = {
    "leagues",
    "weight_classes",
    "venues",
    "fighters",
    "events",
    "fights",
    "rankings",
    "fight_stats",
    "odds",
    "player_props",
}

MMA_STATIC_ENTITIES: set[str] = {"leagues", "weight_classes", "venues"}
