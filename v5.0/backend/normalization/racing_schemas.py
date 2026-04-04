# ──────────────────────────────────────────────────────────────────────
# V5.0 Backend — Racing Normalized-Curated PyArrow Schemas (Consolidated)
# ──────────────────────────────────────────────────────────────────────
#
# 14-entity consolidated design.  Raw data from BDL F1 API + OpenF1 /
# Ergast / ESPN providers are merged into 14 wide schemas.
#
# Entity overview
# ───────────────
#  1. drivers            — static reference, no partitioning
#  2. teams              — static reference, no partitioning
#  3. circuits           — static reference, no partitioning
#  4. events             — partition: season=
#  5. sessions           — partition: season=
#  6. session_results    — partition: season=
#  7. qualifying         — partition: season=
#  8. lap_times          — partition: season=
#  9. pit_stops          — partition: season=
# 10. position_history   — partition: season=
# 11. weather            — partition: season=
# 12. race_control       — partition: season=
# 13. driver_standings   — partition: season=
# 14. team_standings     — partition: season=
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
# 1. drivers — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

RACING_DRIVERS_SCHEMA = pa.schema([
    _f("id",              pa.string(), "Unique driver identifier",                 nullable=False),
    _f("first_name",      pa.string(), "First name"),
    _f("last_name",       pa.string(), "Last name"),
    _f("display_name",    pa.string(), "Full display name"),
    _f("short_name",      pa.string(), "Three-letter abbreviation (e.g. VER)"),
    _f("country_code",    pa.string(), "ISO country code"),
    _f("country_name",    pa.string(), "Country name"),
    _f("racing_number",   pa.string(), "Permanent racing number"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 2. teams — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

RACING_TEAMS_SCHEMA = pa.schema([
    _f("id",              pa.string(), "Unique team identifier",                   nullable=False),
    _f("name",            pa.string(), "Team name",                                nullable=False),
    _f("display_name",    pa.string(), "Full display name"),
    _f("color",           pa.string(), "Team colour hex code"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 3. circuits — static reference (no partitioning)
# ═══════════════════════════════════════════════════════════════════════

RACING_CIRCUITS_SCHEMA = pa.schema([
    _f("id",              pa.string(), "Unique circuit identifier",                nullable=False),
    _f("name",            pa.string(), "Circuit name",                             nullable=False),
    _f("short_name",      pa.string(), "Short circuit name"),
    _f("country_code",    pa.string(), "ISO country code"),
    _f("country_name",    pa.string(), "Country name"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 4. events — partition: season=
# ═══════════════════════════════════════════════════════════════════════

RACING_EVENTS_SCHEMA = pa.schema([
    _f("id",              pa.string(), "Unique event identifier",                  nullable=False),
    _f("name",            pa.string(), "Event name",                               nullable=False),
    _f("short_name",      pa.string(), "Short event name"),
    _f("season",          pa.int32(),  "Season year"),
    _f("start_date",      pa.string(), "Start date (YYYY-MM-DD)"),
    _f("end_date",        pa.string(), "End date (YYYY-MM-DD)"),
    _f("status",          pa.string(), "Event status"),
    _f("circuit_id",      pa.string(), "Circuit identifier"),
    _f("circuit_name",    pa.string(), "Circuit name"),
    _f("location",        pa.string(), "City / venue location"),
    _f("country_code",    pa.string(), "ISO country code"),
    _f("country_name",    pa.string(), "Country name"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 5. sessions — partition: season=
# ═══════════════════════════════════════════════════════════════════════

RACING_SESSIONS_SCHEMA = pa.schema([
    _f("id",              pa.string(), "Unique session identifier",                nullable=False),
    _f("event_id",        pa.string(), "Parent event identifier"),
    _f("event_name",      pa.string(), "Parent event name"),
    _f("type",            pa.string(), "Session type (practice, qualifying, race, sprint)"),
    _f("name",            pa.string(), "Session name"),
    _f("date",            pa.string(), "Session date (YYYY-MM-DD)"),
    _f("status",          pa.string(), "Session status"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 6. session_results — partition: season=
# ═══════════════════════════════════════════════════════════════════════

RACING_SESSION_RESULTS_SCHEMA = pa.schema([
    _f("id",              pa.string(), "Unique result record identifier",          nullable=False),
    _f("session_id",      pa.string(), "Parent session identifier"),
    _f("driver_id",       pa.string(), "Driver identifier"),
    _f("driver_name",     pa.string(), "Driver display name"),
    _f("team_id",         pa.string(), "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("position",        pa.int32(),  "Finishing position"),
    _f("is_winner",       pa.bool_(),  "Whether the driver won the session"),
    _f("laps_completed",  pa.int32(),  "Number of laps completed"),
    _f("pit_stops",       pa.int32(),  "Number of pit stops made"),
    _f("retired",         pa.bool_(),  "Whether the driver retired"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 7. qualifying — partition: season=
# ═══════════════════════════════════════════════════════════════════════

RACING_QUALIFYING_SCHEMA = pa.schema([
    _f("id",              pa.string(), "Unique qualifying record identifier",      nullable=False),
    _f("session_id",      pa.string(), "Parent session identifier"),
    _f("driver_id",       pa.string(), "Driver identifier"),
    _f("driver_name",     pa.string(), "Driver display name"),
    _f("team_id",         pa.string(), "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("q1_time",         pa.string(), "Q1 lap time string"),
    _f("q1_time_ms",      pa.int32(),  "Q1 lap time in milliseconds"),
    _f("q2_time",         pa.string(), "Q2 lap time string"),
    _f("q2_time_ms",      pa.int32(),  "Q2 lap time in milliseconds"),
    _f("q3_time",         pa.string(), "Q3 lap time string"),
    _f("q3_time_ms",      pa.int32(),  "Q3 lap time in milliseconds"),
    _f("final_position",  pa.int32(),  "Final qualifying position"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 8. lap_times — partition: season=
# ═══════════════════════════════════════════════════════════════════════

RACING_LAP_TIMES_SCHEMA = pa.schema([
    _f("id",              pa.string(), "Unique lap time record identifier",        nullable=False),
    _f("session_id",      pa.string(), "Parent session identifier"),
    _f("driver_id",       pa.string(), "Driver identifier"),
    _f("driver_name",     pa.string(), "Driver display name"),
    _f("team_id",         pa.string(), "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("lap_number",      pa.int32(),  "Lap number"),
    _f("lap_time",        pa.string(), "Lap time string (M:SS.mmm)"),
    _f("lap_time_ms",     pa.int32(),  "Lap time in milliseconds"),
    _f("is_pit_out_lap",  pa.bool_(),  "Whether this was a pit-out lap"),
    _f("is_pit_in_lap",   pa.bool_(),  "Whether this was a pit-in lap"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 9. pit_stops — partition: season=
# ═══════════════════════════════════════════════════════════════════════

RACING_PIT_STOPS_SCHEMA = pa.schema([
    _f("id",                   pa.string(),  "Unique pit stop record identifier",  nullable=False),
    _f("session_id",           pa.string(),  "Parent session identifier"),
    _f("driver_id",            pa.string(),  "Driver identifier"),
    _f("driver_name",          pa.string(),  "Driver display name"),
    _f("team_id",              pa.string(),  "Team identifier"),
    _f("team_name",            pa.string(),  "Team name"),
    _f("stop_number",          pa.int32(),   "Pit stop number within the session"),
    _f("lap",                  pa.int32(),   "Lap number of pit stop"),
    _f("time_of_day",          pa.string(),  "Local time of pit stop"),
    _f("duration_seconds",     pa.float64(), "Pit stop duration in seconds"),
    _f("total_time_seconds",   pa.float64(), "Total time including pit lane travel"),
    # Provenance
    _f("source",               pa.string(),  "Data vendor provenance",             nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 10. position_history — partition: season=
# ═══════════════════════════════════════════════════════════════════════

RACING_POSITION_HISTORY_SCHEMA = pa.schema([
    _f("id",              pa.string(), "Unique position record identifier",        nullable=False),
    _f("session_id",      pa.string(), "Parent session identifier"),
    _f("driver_id",       pa.string(), "Driver identifier"),
    _f("driver_name",     pa.string(), "Driver display name"),
    _f("team_id",         pa.string(), "Team identifier"),
    _f("team_name",       pa.string(), "Team name"),
    _f("lap_number",      pa.int32(),  "Lap number"),
    _f("position",        pa.int32(),  "Position at end of lap"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 11. weather — partition: season=
# ═══════════════════════════════════════════════════════════════════════

RACING_WEATHER_SCHEMA = pa.schema([
    _f("id",              pa.string(),  "Unique weather record identifier",        nullable=False),
    _f("session_id",      pa.string(),  "Parent session identifier"),
    _f("air_temp",        pa.float64(), "Air temperature (°C)"),
    _f("track_temp",      pa.float64(), "Track temperature (°C)"),
    _f("humidity",        pa.float64(), "Relative humidity (%)"),
    _f("pressure",        pa.float64(), "Atmospheric pressure (mbar)"),
    _f("wind_speed",      pa.float64(), "Wind speed (m/s)"),
    _f("wind_direction",  pa.int32(),   "Wind direction (degrees 0-360)"),
    _f("rainfall",        pa.float64(), "Rainfall amount (mm)"),
    # Provenance
    _f("source",          pa.string(),  "Data vendor provenance",                  nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 13. race_control — partition: season=
# ═══════════════════════════════════════════════════════════════════════

RACING_RACE_CONTROL_SCHEMA = pa.schema([
    _f("id",              pa.string(), "Unique race control record identifier",    nullable=False),
    _f("session_id",      pa.string(), "Parent session identifier"),
    _f("utc_time",        pa.string(), "UTC timestamp (ISO-8601)"),
    _f("lap",             pa.int32(),  "Lap number"),
    _f("category",        pa.string(), "Message category (Flag, SafetyCar, DRS, etc.)"),
    _f("flag",            pa.string(), "Flag type (GREEN, YELLOW, RED, etc.)"),
    _f("scope",           pa.string(), "Scope (Track, Sector, Driver)"),
    _f("sector",          pa.int32(),  "Sector number (if applicable)"),
    _f("driver_number",   pa.string(), "Driver number (if applicable)"),
    _f("message",         pa.string(), "Race control message text"),
    # Provenance
    _f("source",          pa.string(), "Data vendor provenance",                   nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 13. driver_standings — partition: season=
# ═══════════════════════════════════════════════════════════════════════

RACING_DRIVER_STANDINGS_SCHEMA = pa.schema([
    _f("id",              pa.string(),  "Unique standing record identifier",       nullable=False),
    _f("season",          pa.int32(),   "Season year"),
    _f("driver_id",       pa.string(),  "Driver identifier"),
    _f("driver_name",     pa.string(),  "Driver display name"),
    _f("team_id",         pa.string(),  "Team identifier"),
    _f("team_name",       pa.string(),  "Team name"),
    _f("position",        pa.int32(),   "Championship position"),
    _f("points",          pa.float64(), "Championship points"),
    # Provenance
    _f("source",          pa.string(),  "Data vendor provenance",                  nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# 14. team_standings — partition: season=
# ═══════════════════════════════════════════════════════════════════════

RACING_TEAM_STANDINGS_SCHEMA = pa.schema([
    _f("id",              pa.string(),  "Unique standing record identifier",       nullable=False),
    _f("season",          pa.int32(),   "Season year"),
    _f("team_id",         pa.string(),  "Team identifier"),
    _f("team_name",       pa.string(),  "Team name"),
    _f("position",        pa.int32(),   "Championship position"),
    _f("points",          pa.float64(), "Championship points"),
    # Provenance
    _f("source",          pa.string(),  "Data vendor provenance",                  nullable=False),
])


# ═══════════════════════════════════════════════════════════════════════
# Consolidated lookups
# ═══════════════════════════════════════════════════════════════════════

RACING_SCHEMAS: dict[str, pa.Schema] = {
    "drivers":           RACING_DRIVERS_SCHEMA,
    "teams":             RACING_TEAMS_SCHEMA,
    "circuits":          RACING_CIRCUITS_SCHEMA,
    "events":            RACING_EVENTS_SCHEMA,
    "sessions":          RACING_SESSIONS_SCHEMA,
    "session_results":   RACING_SESSION_RESULTS_SCHEMA,
    "qualifying":        RACING_QUALIFYING_SCHEMA,
    "lap_times":         RACING_LAP_TIMES_SCHEMA,
    "pit_stops":         RACING_PIT_STOPS_SCHEMA,
    "position_history":  RACING_POSITION_HISTORY_SCHEMA,
    "weather":           RACING_WEATHER_SCHEMA,
    "race_control":      RACING_RACE_CONTROL_SCHEMA,
    "driver_standings":  RACING_DRIVER_STANDINGS_SCHEMA,
    "team_standings":    RACING_TEAM_STANDINGS_SCHEMA,
}

RACING_PARTITION_KEYS: dict[str, list[str]] = {
    # Static reference — no partitioning
    "drivers":           [],
    "teams":             [],
    "circuits":          [],
    # Season-partitioned
    "events":            ["season"],
    "sessions":          ["season"],
    "session_results":   ["season"],
    "qualifying":        ["season"],
    "lap_times":         ["season"],
    "pit_stops":         ["season"],
    "position_history":  ["season"],
    "weather":           ["season"],
    "race_control":      ["season"],
    "driver_standings":  ["season"],
    "team_standings":    ["season"],
}

RACING_ENTITY_PATHS: dict[str, str] = {
    "drivers":           "drivers",
    "teams":             "teams",
    "circuits":          "circuits",
    "events":            "events",
    "sessions":          "sessions",
    "session_results":   "session_results",
    "qualifying":        "qualifying",
    "lap_times":         "lap_times",
    "pit_stops":         "pit_stops",
    "position_history":  "position_history",
    "weather":           "weather",
    "race_control":      "race_control",
    "driver_standings":  "driver_standings",
    "team_standings":    "team_standings",
}


# ── Normalizer data-type → entity routing ─────────────────────────────
# Maps the normalizer's data-type names to the flat entity directory
# name under normalized_curated/racing/.
# Types mapping to None are intentionally skipped (non-entity artefacts).
RACING_TYPE_TO_ENTITY: dict[str, str | None] = {
    # Direct 1:1 entity matches
    "drivers":           "drivers",
    "teams":             "teams",
    "circuits":          "circuits",
    "events":            "events",
    "sessions":          "sessions",
    "session_results":   "session_results",
    "qualifying":        "qualifying",
    "lap_times":         "lap_times",
    "pit_stops":         "pit_stops",
    "position_history":  "position_history",
    "weather":           "weather",
    "race_control":      "race_control",
    "driver_standings":  "driver_standings",
    "team_standings":    "team_standings",
    "standings":         "driver_standings",
    "rankings":          "driver_standings",
    "schedule":          "events",
    "scores":            "session_results",
    # Removed entities — no raw data available
    "timing_stats":      None,
    "tire_stints":       None,
    # Non-entity normalizer artefacts — skip
    "odds":              None,
    "player_props":      None,
    "lineups":           None,
    "plays":             None,
    "news":              None,
    "market_signals":    None,
    "injuries":          None,
    "depth_charts":      None,
    "conferences":       None,
    "venues":            None,
}


# ── Entity allow-list and static entities ─────────────────────────────
RACING_ENTITY_ALLOWLIST: set[str] = {
    "drivers",
    "teams",
    "circuits",
    "events",
    "sessions",
    "session_results",
    "qualifying",
    "lap_times",
    "pit_stops",
    "position_history",
    "weather",
    "race_control",
    "driver_standings",
    "team_standings",
}

RACING_STATIC_ENTITIES: set[str] = {"drivers", "teams", "circuits"}
