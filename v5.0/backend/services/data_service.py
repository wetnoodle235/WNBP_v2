# ──────────────────────────────────────────────────────────
# V5.0 Backend — Central Data Service
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd

from config import SPORT_DEFINITIONS, get_current_season, get_settings

logger = logging.getLogger(__name__)

_ALL_KINDS = (
    "games", "teams", "standings", "players", "player_stats",
    "odds", "predictions", "injuries", "news",
    "market_signals", "schedule_fatigue",
)


class DataService:
    """Loads normalized parquet data with TTL caching.

    Directory layout:
        <normalized_dir>/<sport>/<kind>_<season>.parquet
        e.g. data/normalized/nba/teams_2024.parquet
    """

    def __init__(self) -> None:
        self._settings = get_settings()
        self._cache: dict[str, tuple[float, Any]] = {}
        self._stats = {"hits": 0, "misses": 0, "errors": 0}

    # ── Public API ────────────────────────────────────────

    def get_games(
        self,
        sport: str,
        season: str | None = None,
        date: str | None = None,
        columns: list[str] | None = None,
    ) -> list[dict]:
        # Cache the full season dataset so that every date/filter variant shares
        # the same warm cache entry instead of triggering a fresh parquet load.
        key = f"games:{sport}:{season}"
        ttl = self._settings.cache_ttl_games

        def loader() -> list[dict]:
            df = self._load_kind(sport, "games", season=season, columns=columns)
            if df.empty:
                return []
            return self._df_to_records(df)

        records = self._get_cached(key, ttl, loader)
        # Apply date filter in-memory from the already-cached list.
        if date is not None:
            records = [r for r in records if str(r.get("date", "")).startswith(date)]
        return records

    def get_teams(self, sport: str, season: str | None = None) -> list[dict]:
        key = f"teams:{sport}:{season}"
        ttl = self._settings.cache_ttl_players

        def loader() -> list[dict]:
            df = self._load_kind(sport, "teams", season=season)
            return self._df_to_records(df)

        return self._get_cached(key, ttl, loader)

    def get_teams_index(self, sport: str, season: str | None = None) -> dict[str, dict]:
        """Return a dict of team records indexed by both team ID and lower-case abbreviation.

        Allows O(1) team lookup instead of a linear scan over the full roster.
        """
        key = f"teams_index:{sport}:{season}"
        ttl = self._settings.cache_ttl_players

        def loader() -> dict[str, dict]:
            index: dict[str, dict] = {}
            for t in self.get_teams(sport, season=season):
                tid = str(t.get("id", ""))
                abbr = str(t.get("abbreviation", "")).lower()
                if tid:
                    index[tid] = t
                if abbr:
                    index[abbr] = t
            return index

        return self._get_cached(key, ttl, loader)

    def get_standings(self, sport: str, season: str | None = None) -> list[dict]:
        key = f"standings:{sport}:{season}"
        ttl = self._settings.cache_ttl_standings

        def loader() -> list[dict]:
            df = self._load_kind(sport, "standings", season=season)
            if df.empty:
                return []
            # Resolve team_name from teams data when missing (fully or partially)
            needs_resolve = (
                "team_name" not in df.columns
                or df["team_name"].isna().any()
                or (df["team_name"].astype(str).str.strip() == "").any()
            )
            if needs_resolve:
                teams_df = self._load_kind(sport, "teams", season=season)
                if not teams_df.empty and "team_id" in teams_df.columns:
                    name_col = "name" if "name" in teams_df.columns else "team_name" if "team_name" in teams_df.columns else None
                    if name_col:
                        lookup = teams_df.drop_duplicates("team_id").set_index("team_id")[name_col].to_dict()
                        if "team_name" not in df.columns:
                            df["team_name"] = df["team_id"].map(lookup)
                        else:
                            mask = df["team_name"].isna() | (df["team_name"].astype(str).str.strip() == "")
                            df.loc[mask, "team_name"] = df.loc[mask, "team_id"].map(lookup)
            return self._df_to_records(df)

        return self._get_cached(key, ttl, loader)

    def get_players(
        self,
        sport: str,
        season: str | None = None,
        team_id: str | None = None,
        search: str | None = None,
    ) -> list[dict]:
        # Cache the full season roster so that team/search filters all share
        # one cache entry instead of each combination being a separate miss.
        key = f"players:{sport}:{season}"
        ttl = self._settings.cache_ttl_players

        def loader() -> list[dict]:
            df = self._load_kind(sport, "players", season=season)
            if df.empty:
                return []
            return self._df_to_records(df)

        records = self._get_cached(key, ttl, loader)
        # Apply filters in-memory from the already-cached list.
        if team_id:
            records = [r for r in records if str(r.get("team_id", "")) == team_id]
        if search:
            sl = search.lower()
            records = [r for r in records if sl in str(r.get("name", "")).lower()]
        return records

    def get_player_stats(
        self,
        sport: str,
        season: str | None = None,
        player_id: str | None = None,
        aggregate: bool = False,
    ) -> list[dict]:
        key = f"player_stats:{sport}:{season}:{player_id}:{aggregate}"
        ttl = self._settings.cache_ttl_stats

        def loader() -> list[dict]:
            df = self._load_kind(sport, "player_stats", season=season)
            if df.empty:
                return []
            if player_id and "player_id" in df.columns:
                df = df[df["player_id"].astype(str) == player_id]
            if aggregate:
                return self._aggregate_player_stats(df, sport, season=season)
            return self._df_to_records(df)

        return self._get_cached(key, ttl, loader)

    async def get_player_stats_async(
        self,
        sport: str,
        season: str | None = None,
        player_id: str | None = None,
        aggregate: bool = False,
    ) -> list[dict]:
        """Async wrapper that offloads heavy aggregation to a thread pool.

        Use this from async route handlers when aggregate=True to avoid
        blocking the event loop during O(n log n) pandas operations.
        """
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self.get_player_stats, sport, season, player_id, aggregate
        )

    # NBA Stats API team IDs → standard abbreviations
    _NBA_TEAM_MAP: dict[str, str] = {
        "1610612737": "ATL", "1610612738": "BOS", "1610612739": "CLE",
        "1610612740": "NOP", "1610612741": "CHI", "1610612742": "DAL",
        "1610612743": "DEN", "1610612744": "GSW", "1610612745": "HOU",
        "1610612746": "LAC", "1610612747": "LAL", "1610612748": "MIA",
        "1610612749": "MIL", "1610612750": "MIN", "1610612751": "BKN",
        "1610612752": "NYK", "1610612753": "ORL", "1610612754": "IND",
        "1610612755": "PHI", "1610612756": "PHX", "1610612757": "POR",
        "1610612758": "SAC", "1610612759": "SAS", "1610612760": "OKC",
        "1610612761": "TOR", "1610612762": "UTA", "1610612763": "MEM",
        "1610612764": "WAS", "1610612765": "DET", "1610612766": "CHA",
    }

    @classmethod
    def _normalize_team_id(cls, t: object) -> str:
        """Convert numeric team IDs to standard abbreviations."""
        if not isinstance(t, str) or not t:
            return ""
        # Already a short abbreviation
        if len(t) <= 5 and not t.isdigit():
            return t
        # NBA Stats API numeric ID
        mapped = cls._NBA_TEAM_MAP.get(t)
        if mapped:
            return mapped
        # Unknown numeric ID
        return ""

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Strip accents and lowercase for cross-source deduplication."""
        import unicodedata
        if not isinstance(name, str):
            return str(name)
        nfkd = unicodedata.normalize("NFKD", name)
        return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()

    def _aggregate_player_stats(
        self, df: pd.DataFrame, sport: str, *, season: str | None = None,
    ) -> list[dict]:
        """Aggregate per-game stats into per-player season stats.

        Deduplicates across sources (ESPN vs NBA Stats etc.) by normalizing
        player names. For each player, prefers the source with more detailed
        stats (e.g., shooting percentages). Uses the latest team_id.

        When *season* is provided, filters to that season first to avoid
        cross-season merging of the same player.

        Basketball: per-game averages (PPG, RPG, APG)
        Hockey/Football: season totals (goals, yards, TDs)
        """
        if df.empty:
            return []

        # Filter to requested season to prevent cross-season dedup issues
        if season and "season" in df.columns:
            df = df[df["season"].astype(str) == str(season)]
            if df.empty:
                return []

        cat = SPORT_DEFINITIONS.get(sport, {}).get("category", "")

        # -- Step 1: Cross-source dedup by choosing best source per player --
        if "source" in df.columns and df["source"].nunique() > 1:
            df = self._pick_best_source_per_player(df)

        # -- Step 2: Normalize team_id (prefer short abbreviations over numeric IDs) --
        if "team_id" in df.columns:
            df["team_id"] = df["team_id"].apply(self._normalize_team_id)

        # -- Step 3: Normalize player names so variants group together --
        if "player_name" not in df.columns:
            return self._df_to_records(df)

        df = df.copy()
        df["_norm_name"] = df["player_name"].apply(self._normalize_name)

        # Build canonical name map: for each normalized name, prefer the
        # longest (most accented/complete) spelling.
        canon_map: dict[str, str] = {}
        for name in df["player_name"].unique():
            norm = self._normalize_name(name)
            existing = canon_map.get(norm, "")
            if len(name) >= len(existing):
                canon_map[norm] = name
        df["player_name"] = df["_norm_name"].map(canon_map).fillna(df["player_name"])

        group_col = "player_name"

        exclude = {"game_id", "player_id", "team_id", "season", "date",
                    "sport", "source", "category", "player_name", "position",
                    "_norm_name"}
        num_cols = [c for c in df.columns
                    if c not in exclude and pd.api.types.is_numeric_dtype(df[c])]

        avg_cols = {"fg_pct", "ft_pct", "three_pct", "save_pct"}
        basketball_avg = cat == "basketball"

        grouped = df.groupby(group_col, dropna=False)
        agg_dict: dict[str, Any] = {}

        if "game_id" in df.columns:
            agg_dict["game_id"] = "nunique"

        for col in num_cols:
            if col in avg_cols:
                agg_dict[col] = "mean"
            elif basketball_avg:
                agg_dict[col] = "mean"
            else:
                agg_dict[col] = "sum"

        # Pick latest team_id per player
        if "team_id" in df.columns:
            agg_dict["team_id"] = "last"
        if "player_id" in df.columns:
            agg_dict["player_id"] = "first"

        result = grouped.agg(agg_dict).reset_index()
        result = result.drop(columns=["_norm_name"], errors="ignore")

        if "game_id" in result.columns:
            result = result.rename(columns={"game_id": "gp"})

        if "position" in df.columns:
            pos = df.groupby("player_name", dropna=False)["position"].first().reset_index()
            result = result.merge(pos, on="player_name", how="left")

        # -- Final safety dedup: if any player_name still appears more than
        #    once (e.g. cross-season overlap), keep the row with the most GP.
        if "gp" in result.columns:
            result = (
                result.sort_values("gp", ascending=False)
                .drop_duplicates(subset=["player_name"], keep="first")
            )

        primary_sort = {
            "basketball": "pts",
            "hockey": "points",
            "football": "passing_yards",
            "baseball": "hits",
            "soccer": "goals",
        }
        sort_col = primary_sort.get(cat, "gp")
        if sort_col not in result.columns:
            sort_col = "gp" if "gp" in result.columns else (num_cols[0] if num_cols else None)
        if sort_col:
            result = result.sort_values(sort_col, ascending=False)

        pct_cols = {"fg_pct", "ft_pct", "three_pct", "save_pct"}
        for col in result.select_dtypes(include="number").columns:
            if col == "gp":
                result[col] = result[col].astype(int)
            elif col in pct_cols:
                result[col] = result[col].round(3)
            else:
                result[col] = result[col].round(1)

        # Add human-friendly aliases alongside short column names
        _STAT_ALIASES = {
            "pts": "points", "reb": "rebounds", "ast": "assists",
            "stl": "steals", "blk": "blocks", "to": "turnovers", "tov": "turnovers",
            "dreb": "defensive_rebounds", "oreb": "offensive_rebounds",
            "fgm": "field_goals_made", "fga": "field_goals_attempted",
            "three_m": "three_pointers_made", "three_a": "three_pointers_attempted",
            "ftm": "free_throws_made", "fta": "free_throws_attempted",
            "gp": "games_played", "min": "minutes", "pf": "personal_fouls",
            "plus_minus": "plus_minus", "fg_pct": "field_goal_pct",
            "ft_pct": "free_throw_pct", "three_pct": "three_point_pct",
        }
        for short, long in _STAT_ALIASES.items():
            if short in result.columns and long not in result.columns:
                result[long] = result[short]

        result["sport"] = sport
        return self._df_to_records(result)

    def _pick_best_source_per_player(self, df: pd.DataFrame) -> pd.DataFrame:
        """For each player, keep only the source with the most complete data.

        Uses normalized name matching to merge 'Luka Dončić' and 'Luka Doncic'.
        Picks the source with more non-null stat columns per player.
        """
        df = df.copy()
        df["_norm_name"] = df["player_name"].apply(self._normalize_name)

        exclude = {"game_id", "player_id", "team_id", "season", "date",
                    "sport", "source", "category", "player_name", "position",
                    "_norm_name", "_completeness"}
        stat_cols = [c for c in df.columns if c not in exclude and pd.api.types.is_numeric_dtype(df[c])]

        # Score each source per player: count of non-null stat values
        df["_completeness"] = df[stat_cols].notna().sum(axis=1)
        source_quality = df.groupby(["_norm_name", "source"])["_completeness"].mean()

        # For each normalized name, pick the source with highest completeness
        best_source_map: dict[str, str] = {}
        for norm_name in df["_norm_name"].unique():
            try:
                sub = source_quality.loc[norm_name]
                best_source_map[norm_name] = sub.idxmax()
            except (KeyError, ValueError):
                pass

        # Keep only the best source per player
        mask = df.apply(
            lambda row: row["source"] == best_source_map.get(row["_norm_name"], row["source"]),
            axis=1,
        )
        result = df[mask].drop(columns=["_completeness", "_norm_name"], errors="ignore")

        # Use the canonical name from the best source (prefer accented names)
        name_map: dict[str, str] = {}
        for _, row in result.iterrows():
            norm = self._normalize_name(row["player_name"])
            existing = name_map.get(norm, "")
            if len(row["player_name"]) >= len(existing):
                name_map[norm] = row["player_name"]
        result["player_name"] = result["player_name"].apply(
            lambda n: name_map.get(self._normalize_name(n), n)
        )
        return result

    def get_odds(
        self,
        sport: str,
        game_id: str | None = None,
        season: str | None = None,
    ) -> list[dict]:
        key = f"odds:{sport}:{game_id}:{season}"
        ttl = self._settings.cache_ttl_odds

        def loader() -> list[dict]:
            df = self._load_kind(sport, "odds", season=season)
            if df.empty:
                return []
            if game_id and "game_id" in df.columns:
                df = df[df["game_id"].astype(str) == game_id]
            return self._df_to_records(df)

        return self._get_cached(key, ttl, loader)

    def get_predictions(
        self,
        sport: str,
        date: str | None = None,
    ) -> list[dict]:
        key = f"predictions:{sport}:{date}"
        ttl = self._settings.cache_ttl_predictions

        def loader() -> list[dict]:
            # Predictions are stored as JSON in data/predictions/{date}.json
            pred_dir = self._settings.data_dir / "predictions"
            if not pred_dir.exists():
                return []

            if date:
                pred_file = pred_dir / f"{date}.json"
                if not pred_file.exists():
                    return []
                try:
                    with open(pred_file) as f:
                        data = json.load(f)
                    preds = data.get("predictions", data if isinstance(data, list) else [])
                    file_date = data.get("date", date)
                    return [self._enrich_prediction(p, file_date) for p in preds if p.get("sport") == sport]
                except Exception:
                    return []
            else:
                # Load most recent prediction file
                files = sorted(pred_dir.glob("*.json"), reverse=True)
                for pred_file in files[:3]:
                    try:
                        with open(pred_file) as f:
                            data = json.load(f)
                        preds = data.get("predictions", data if isinstance(data, list) else [])
                        file_date = data.get("date", pred_file.stem)
                        sport_preds = [self._enrich_prediction(p, file_date) for p in preds if p.get("sport") == sport]
                        if sport_preds:
                            return sport_preds
                    except Exception:
                        continue
                return []

        return self._get_cached(key, ttl, loader)

    def get_injuries(self, sport: str) -> list[dict]:
        key = f"injuries:{sport}"
        ttl = self._settings.cache_ttl_games

        def loader() -> list[dict]:
            df = self._load_kind(sport, "injuries")
            return self._df_to_records(df)

        return self._get_cached(key, ttl, loader)

    def get_news(self, sport: str, limit: int = 50) -> list[dict]:
        key = f"news:{sport}:{limit}"
        ttl = self._settings.cache_ttl_games

        def loader() -> list[dict]:
            df = self._load_kind(sport, "news")
            if df.empty:
                return []
            if "published_at" in df.columns:
                df = df.sort_values("published_at", ascending=False)
            # Deduplicate by headline (case-insensitive)
            if "headline" in df.columns:
                df["_hl_key"] = df["headline"].str.lower().str.strip()
                df = df.drop_duplicates(subset=["_hl_key"], keep="first").drop(columns=["_hl_key"])
            elif "id" in df.columns:
                df = df.drop_duplicates(subset=["id"], keep="first")
            return self._df_to_records(df.head(limit))

        return self._get_cached(key, ttl, loader)

    # ── Inventory helpers (for meta endpoints) ────────────

    def list_available_sports(self) -> list[dict]:
        """Return sports that have at least one parquet file on disk.

        Result is cached for 5 minutes to avoid repeated directory scans
        on every request to /v1/sports or /v1/meta/providers.
        """
        key = "meta:available_sports"
        ttl = 300  # 5 minutes

        def loader() -> list[dict]:
            norm = self._settings.normalized_dir
            if not norm.exists():
                return []
            results = []
            for sport_dir in sorted(norm.iterdir()):
                if not sport_dir.is_dir():
                    continue
                sport = sport_dir.name
                kinds: dict[str, int] = {}
                for f in sport_dir.glob("*.parquet"):
                    kind = self._kind_from_filename(f.name)
                    if kind:
                        kinds[kind] = kinds.get(kind, 0) + 1
                if kinds:
                    defn = SPORT_DEFINITIONS.get(sport, {})
                    results.append({
                        "key": sport,
                        **defn,
                        "data_types": kinds,
                        "file_count": sum(kinds.values()),
                    })
            return results

        return self._get_cached(key, ttl, loader)

    def get_data_freshness(self) -> dict[str, dict]:
        """Per-sport, per-kind file modification timestamps.

        Cached for 60 seconds — calling stat() on every parquet file on
        every request can mean 1000+ syscalls per call.
        """
        key = "meta:data_freshness"
        ttl = 60  # 1 minute

        def loader() -> dict[str, dict]:
            norm = self._settings.normalized_dir
            if not norm.exists():
                return {}
            result: dict[str, dict] = {}
            for sport_dir in sorted(norm.iterdir()):
                if not sport_dir.is_dir():
                    continue
                sport = sport_dir.name
                kinds_info: dict[str, str] = {}
                for f in sorted(sport_dir.glob("*.parquet")):
                    kind = self._kind_from_filename(f.name)
                    if kind:
                        mtime = f.stat().st_mtime
                        ts = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
                        kinds_info[f.name] = ts
                if kinds_info:
                    result[sport] = kinds_info
            return result

        return self._get_cached(key, ttl, loader)

    def get_seasons(self, sport: str, kind: str = "games") -> list[str]:
        """Return available seasons for a sport/kind combo.

        Scans ``<kind>_<year>.parquet`` filenames on disk.
        """
        norm = self._settings.normalized_dir / sport
        if not norm.is_dir():
            return []

        seasons: set[str] = set()
        for f in norm.glob(f"{kind}_*.parquet"):
            s = f.stem.replace(f"{kind}_", "")
            if s.isdigit():
                seasons.add(s)
        return sorted(seasons)

    # ── Cache bookkeeping ─────────────────────────────────

    @property
    def cache_stats(self) -> dict[str, Any]:
        return {
            **self._stats,
            "cached_keys": len(self._cache),
        }

    def clear_cache(self) -> None:
        self._cache.clear()
        self._stats = {"hits": 0, "misses": 0, "errors": 0}

    def warm_cache(self, sports: list[str]) -> None:
        """Pre-load commonly accessed data for the given sports."""
        # Pre-populate the sports inventory cache (used by /v1/sports)
        self.list_available_sports()
        for sport in sports:
            for kind in ("games", "teams", "standings", "players", "odds", "injuries"):
                try:
                    self._load_kind(sport, kind)
                except Exception:
                    logger.debug("warm_cache: no %s data for %s", kind, sport)
            # Build teams index while we're at it
            try:
                self.get_teams_index(sport)
            except Exception:
                logger.debug("warm_cache: could not build teams index for %s", sport)

    # ── Internal helpers ──────────────────────────────────

    def _get_cached(self, key: str, ttl: int, loader: Callable[[], Any]) -> Any:
        now = time.time()
        entry = self._cache.get(key)
        if entry and entry[0] > now:
            self._stats["hits"] += 1
            return entry[1]

        self._stats["misses"] += 1
        try:
            data = loader()
            self._cache[key] = (now + ttl, data)
            return data
        except Exception:
            self._stats["errors"] += 1
            logger.exception("Error loading data for cache key: %s", key)
            if entry:
                return entry[1]
            return []

    def _load_kind(
        self, sport: str, kind: str, season: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load parquet files matching <normalized_dir>/<sport>/<kind>_*.parquet.

        If *season* is ``"all"``, concatenate every season file for that kind.
        If *season* is a specific year, load only ``<kind>_<season>.parquet``.
        If *season* is ``None``, default to the current season for the sport
        (falling back to loading all files if no current-season file exists).
        """
        sport_dir = self._settings.normalized_dir / sport
        if not sport_dir.is_dir():
            return pd.DataFrame()

        # Explicit "all" → load every season file
        if season == "all":
            return self._load_all_season_files(sport_dir, kind, columns=columns)

        # Specific season requested (or smart default)
        target_season = season if season else get_current_season(sport)
        target = sport_dir / f"{kind}_{target_season}.parquet"
        if target.exists():
            return self._load_parquet(target, columns=columns)

        # If no file for the default season, fall back to all files
        if not season:
            return self._load_all_season_files(sport_dir, kind, columns=columns)

        return pd.DataFrame()

    def _load_all_season_files(
        self, sport_dir: Path, kind: str,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Concatenate all per-season files for a given kind."""
        files = sorted(sport_dir.glob(f"{kind}_*.parquet"))
        if not files:
            return pd.DataFrame()
        frames = [self._load_parquet(f, columns=columns) for f in files]
        non_empty = [f for f in frames if not f.empty]
        if not non_empty:
            return pd.DataFrame()
        return pd.concat(non_empty, ignore_index=True)

    def _load_parquet(self, path: Path, columns: list[str] | None = None) -> pd.DataFrame:
        logger.debug("Loading parquet: %s", path)
        if columns:
            import pyarrow.parquet as pq
            schema = pq.read_schema(path)
            available = set(schema.names)
            columns = [c for c in columns if c in available]
        return pd.read_parquet(path, engine="pyarrow", columns=columns or None)

    @staticmethod
    def _kind_from_filename(filename: str) -> str | None:
        """Extract the data kind from a filename like 'teams_2024.parquet'."""
        stem = filename.replace(".parquet", "")
        for kind in _ALL_KINDS:
            if stem.startswith(kind + "_") or stem == kind:
                return kind
        return None

    @staticmethod
    def _enrich_prediction(p: dict, pred_date: str | None = None) -> dict:
        """Add derived fields to a prediction record."""
        import math
        # Scrub NaN/Infinity values
        for k, v in list(p.items()):
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                p[k] = None
        if pred_date and "date" not in p:
            p["date"] = pred_date
        if "predicted_winner" not in p and "home_win_prob" in p:
            hwp = float(p.get("home_win_prob", 0.5))
            if hwp > 0.5:
                p["predicted_winner"] = p.get("home_team", "home")
            elif hwp < 0.5:
                p["predicted_winner"] = p.get("away_team", "away")
            else:
                p["predicted_winner"] = "toss-up"
        return p

    @staticmethod
    def _df_to_records(df: pd.DataFrame) -> list[dict]:
        if df.empty:
            return []
        import datetime as _dtmod
        import math

        # Convert date/datetime columns to ISO strings for JSON serialization
        df = df.copy()  # avoid SettingWithCopyWarning on caller-owned slices
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S").where(df[col].notna(), None)
            elif df[col].dtype == "object":
                sample = df[col].dropna().head(1)
                if len(sample) > 0:
                    val = sample.iloc[0]
                    if isinstance(val, _dtmod.date):
                        df[col] = df[col].apply(
                            lambda x: x.isoformat() if isinstance(x, (_dtmod.date, _dtmod.datetime)) else x
                        )

        records = df.where(df.notna(), None).to_dict(orient="records")
        import numpy as _np
        # Scrub non-JSON-serializable values (NaN, Infinity, numpy arrays)
        for rec in records:
            for k, v in rec.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    rec[k] = None
                elif isinstance(v, _np.ndarray):
                    rec[k] = v.tolist()
                elif isinstance(v, (_np.integer,)):
                    rec[k] = int(v)
                elif isinstance(v, (_np.floating,)):
                    f = float(v)
                    rec[k] = None if math.isnan(f) or math.isinf(f) else f
        return records


# ── Singleton dependency ──────────────────────────────────

_instance: DataService | None = None


def get_data_service() -> DataService:
    global _instance
    if _instance is None:
        _instance = DataService()
    return _instance
