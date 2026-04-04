# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — Golf (PGA) Normalized-Curated PyArrow Schemas (Consolidated)
# ──────────────────────────────────────────────────────────────────────
#
# 7-entity consolidated design.  Raw data from BDL PGA API and ESPN
# providers are merged into 7 wide schemas.
#
# Entity overview
# ───────────────
#  1. players            — static reference, no partitioning
#  2. courses            — static reference, no partitioning
#  3. tournaments        — partition: season=
#  4. tournament_results — partition: season=
#  5. tournament_fields  — partition: season=
#  6. round_results      — partition: season=
#  7. round_stats        — partition: season=
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
# 1. players — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

GOLF_PLAYERS_SCHEMA = pa.schema([
    _f("id",                   pa.int32(),   "Unique player identifier",            nullable=False),
    _f("first_name",           pa.string(),  "First name"),
    _f("last_name",            pa.string(),  "Last name"),
    _f("display_name",         pa.string(),  "Display name"),
    _f("country",              pa.string(),  "Country of origin"),
    _f("country_code",         pa.string(),  "ISO country code"),
    _f("height",               pa.string(),  "Height"),
    _f("weight",               pa.string(),  "Weight"),
    _f("birth_date",           pa.string(),  "Date of birth (YYYY-MM-DD)"),
    _f("birthplace_city",      pa.string(),  "Birthplace city"),
    _f("birthplace_state",     pa.string(),  "Birthplace state/province"),
    _f("birthplace_country",   pa.string(),  "Birthplace country"),
    _f("turned_pro",           pa.int32(),   "Year turned professional"),
    _f("school",               pa.string(),  "College / university attended"),
    _f("residence_city",       pa.string(),  "Current residence city"),
    _f("residence_state",      pa.string(),  "Current residence state"),
    _f("owgr",                 pa.int32(),   "Official World Golf Ranking"),
    _f("active",               pa.string(),  "Whether the player is active"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. courses — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

GOLF_COURSES_SCHEMA = pa.schema([
    _f("id",                   pa.int32(),   "Unique course identifier",            nullable=False),
    _f("name",                 pa.string(),  "Course name",                         nullable=False),
    _f("city",                 pa.string(),  "City"),
    _f("state",                pa.string(),  "State / province"),
    _f("country",              pa.string(),  "Country"),
    _f("par",                  pa.int32(),   "Course par"),
    _f("yardage",              pa.int32(),   "Total course yardage"),
    _f("established",          pa.int32(),   "Year established"),
    _f("architect",            pa.string(),  "Course architect / designer"),
    _f("fairway_grass",        pa.string(),  "Fairway grass type"),
    _f("rough_grass",          pa.string(),  "Rough grass type"),
    _f("green_grass",          pa.string(),  "Green grass type"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. tournaments — partition: season=
# ═══════════════════════════════════════════════════════════════════════

GOLF_TOURNAMENTS_SCHEMA = pa.schema([
    _f("id",                   pa.int32(),   "Unique tournament identifier",        nullable=False),
    _f("season",               pa.string(),  "Season identifier"),
    _f("name",                 pa.string(),  "Tournament name",                     nullable=False),
    _f("start_date",           pa.string(),  "Start date (YYYY-MM-DD)"),
    _f("end_date",             pa.string(),  "End date (YYYY-MM-DD)"),
    _f("city",                 pa.string(),  "Host city"),
    _f("state",                pa.string(),  "Host state / province"),
    _f("country",              pa.string(),  "Host country"),
    _f("course_name",          pa.string(),  "Host course name"),
    _f("purse",                pa.float64(), "Total purse amount (USD)"),
    _f("status",               pa.string(),  "Tournament status"),
    _f("champion_id",          pa.int32(),   "Champion player identifier"),
    _f("champion_name",        pa.string(),  "Champion player name"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. tournament_results — partition: season=
# ═══════════════════════════════════════════════════════════════════════

GOLF_TOURNAMENT_RESULTS_SCHEMA = pa.schema([
    _f("tournament_id",        pa.int32(),   "Tournament identifier",               nullable=False),
    _f("tournament_name",      pa.string(),  "Tournament name"),
    _f("player_id",            pa.int32(),   "Player identifier",                   nullable=False),
    _f("player_name",          pa.string(),  "Player name"),
    _f("position",             pa.string(),  "Finishing position (e.g. T3, CUT)"),
    _f("total_score",          pa.int32(),   "Total score"),
    _f("total_strokes",        pa.int32(),   "Total strokes"),
    _f("par_relative",         pa.int32(),   "Score relative to par"),
    _f("rounds_played",        pa.int32(),   "Number of rounds played"),
    _f("money",                pa.float64(), "Prize money earned (USD)"),
    _f("fedex_points",         pa.float64(), "FedEx Cup points earned"),
    # Partition key
    _f("season",               pa.string(),  "Season identifier for partitioning"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. tournament_fields — partition: season=
# ═══════════════════════════════════════════════════════════════════════

GOLF_TOURNAMENT_FIELDS_SCHEMA = pa.schema([
    _f("id",                   pa.int32(),   "Unique field entry identifier",       nullable=False),
    _f("tournament_id",        pa.int32(),   "Tournament identifier",               nullable=False),
    _f("player_id",            pa.int32(),   "Player identifier",                   nullable=False),
    _f("player_name",          pa.string(),  "Player name"),
    _f("entry_status",         pa.string(),  "Entry status (e.g. exempt, alternate)"),
    _f("qualifier",            pa.string(),  "Qualifier type"),
    _f("owgr",                 pa.int32(),   "OWGR at time of entry"),
    _f("is_amateur",           pa.string(),  "Whether the player is an amateur"),
    # Partition key
    _f("season",               pa.string(),  "Season identifier for partitioning"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 7. round_results — partition: season=
# ═══════════════════════════════════════════════════════════════════════

GOLF_ROUND_RESULTS_SCHEMA = pa.schema([
    _f("tournament_id",        pa.int32(),   "Tournament identifier",               nullable=False),
    _f("player_id",            pa.int32(),   "Player identifier",                   nullable=False),
    _f("player_name",          pa.string(),  "Player name"),
    _f("round_number",         pa.int32(),   "Round number (1-4)",                  nullable=False),
    _f("score",                pa.int32(),   "Round score (strokes)"),
    _f("par_relative_score",   pa.int32(),   "Round score relative to par"),
    # Partition key
    _f("season",               pa.string(),  "Season identifier for partitioning"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 8. round_stats — partition: season=
# ═══════════════════════════════════════════════════════════════════════

GOLF_ROUND_STATS_SCHEMA = pa.schema([
    _f("tournament_id",        pa.int32(),   "Tournament identifier",               nullable=False),
    _f("player_id",            pa.int32(),   "Player identifier",                   nullable=False),
    _f("player_name",          pa.string(),  "Player name"),
    _f("round_number",         pa.int32(),   "Round number (1-4)",                  nullable=False),
    # Strokes gained
    _f("sg_off_tee",           pa.float64(), "Strokes gained off the tee"),
    _f("sg_approach",          pa.float64(), "Strokes gained approach the green"),
    _f("sg_around_green",      pa.float64(), "Strokes gained around the green"),
    _f("sg_putting",           pa.float64(), "Strokes gained putting"),
    _f("sg_total",             pa.float64(), "Strokes gained total"),
    # Traditional stats
    _f("driving_accuracy",     pa.float64(), "Driving accuracy percentage"),
    _f("driving_distance",     pa.float64(), "Driving distance (yards)"),
    _f("greens_in_regulation", pa.float64(), "Greens in regulation percentage"),
    _f("sand_saves",           pa.float64(), "Sand save percentage"),
    _f("scrambling",           pa.float64(), "Scrambling percentage"),
    _f("putts_per_gir",        pa.float64(), "Putts per green in regulation"),
    # Scoring breakdown
    _f("eagles",               pa.int32(),   "Number of eagles"),
    _f("birdies",              pa.int32(),   "Number of birdies"),
    _f("pars",                 pa.int32(),   "Number of pars"),
    _f("bogeys",               pa.int32(),   "Number of bogeys"),
    _f("double_bogeys",        pa.int32(),   "Number of double bogeys or worse"),
    # Partition key
    _f("season",               pa.string(),  "Season identifier for partitioning"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",             nullable=False),
])



# ═══════════════════════════════════════════════════════════════════════
# Consolidated lookups
# ═══════════════════════════════════════════════════════════════════════

GOLF_SCHEMAS: dict[str, pa.Schema] = {
    "players":            GOLF_PLAYERS_SCHEMA,
    "courses":            GOLF_COURSES_SCHEMA,
    "tournaments":        GOLF_TOURNAMENTS_SCHEMA,
    "tournament_results": GOLF_TOURNAMENT_RESULTS_SCHEMA,
    "tournament_fields":  GOLF_TOURNAMENT_FIELDS_SCHEMA,
    "round_results":      GOLF_ROUND_RESULTS_SCHEMA,
    "round_stats":        GOLF_ROUND_STATS_SCHEMA,
}

GOLF_PARTITION_KEYS: dict[str, list[str]] = {
    # Static reference — no partitioning
    "players":            [],
    "courses":            [],
    # Season-partitioned
    "tournaments":        ["season"],
    "tournament_results": ["season"],
    "tournament_fields":  ["season"],
    "round_results":      ["season"],
    "round_stats":        ["season"],
}

GOLF_ENTITY_PATHS: dict[str, str] = {
    "players":            "players",
    "courses":            "courses",
    "tournaments":        "tournaments",
    "tournament_results": "tournament_results",
    "tournament_fields":  "tournament_fields",
    "round_results":      "round_results",
    "round_stats":        "round_stats",
}


# ── Normalizer data-type → entity routing ─────────────────────────────
GOLF_TYPE_TO_ENTITY: dict[str, str | None] = {
    "players":            "players",
    "courses":            "courses",
    "tournaments":        "tournaments",
    "tournament_results": "tournament_results",
    "tournament_fields":  "tournament_fields",
    "round_results":      "round_results",
    "round_stats":        "round_stats",
    # Removed entities — no raw data available
    "odds":               None,
    "course_holes":       None,
    "scorecards":         None,
    "season_stats":       None,
    "tee_times":          None,
    "course_stats":       None,
}


# ── Entity allow-list and static entities ─────────────────────────────
GOLF_ENTITY_ALLOWLIST: set[str] = {
    "players",
    "courses",
    "tournaments",
    "tournament_results",
    "tournament_fields",
    "round_results",
    "round_stats",
}

GOLF_STATIC_ENTITIES: set[str] = {"players", "courses"}
