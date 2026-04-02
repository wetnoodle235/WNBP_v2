# ──────────────────────────────────────────────────────────
# V5.0 Backend — Curated Normalized Parquet Builder
# ──────────────────────────────────────────────────────────

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from config import get_settings

logger = logging.getLogger(__name__)

WEEKLY_PARTITION_SPORTS = {"nfl", "ncaaf"}

# Core categories requested for the new normalized design.
BASE_CATEGORIES = (
    "teams",
    "players",
    "conferences",
    "games",
    "stats",
    "season_averages",
    "player_season_averages",
    "team_season_averages",
    "standings_season_averages",
    "coaches",
    "referees",
)

# Additional category sets per sport. These are emitted only if matching
# source files exist in normalized v1 for that sport/season.
SPORT_CATEGORY_OVERRIDES: dict[str, tuple[str, ...]] = {
    "nba": ("team_game_stats", "player_stats", "team_stats", "standings", "odds", "injuries", "news"),
    "wnba": ("team_game_stats", "player_stats", "team_stats", "standings", "odds", "injuries", "news"),
    "ncaab": ("team_game_stats", "player_stats", "team_stats", "standings", "odds", "injuries", "news"),
    "ncaaw": ("team_game_stats", "player_stats", "team_stats", "standings", "odds", "injuries", "news"),
    "nfl": ("team_game_stats", "player_stats", "team_stats", "standings", "odds", "injuries", "news"),
    "ncaaf": ("team_game_stats", "player_stats", "team_stats", "standings", "odds", "injuries", "news"),
    "mlb": ("batter_game_stats", "pitcher_game_stats", "team_stats", "standings", "odds", "injuries", "news"),
    "nhl": ("team_game_stats", "player_stats", "team_stats", "standings", "odds", "injuries", "news"),
    "epl": ("team_stats", "player_stats", "standings", "odds", "injuries", "news"),
    "laliga": ("team_stats", "player_stats", "standings", "odds", "injuries", "news"),
    "bundesliga": ("team_stats", "player_stats", "standings", "odds", "injuries", "news"),
    "seriea": ("team_stats", "player_stats", "standings", "odds", "injuries", "news"),
    "ligue1": ("team_stats", "player_stats", "standings", "odds", "injuries", "news"),
}

# Candidate source kinds from v1 normalized files.
CATEGORY_SOURCE_KINDS: dict[str, tuple[str, ...]] = {
    "teams": ("teams",),
    "players": ("players",),
    "games": ("games",),
    "stats": (
        "player_stats",
        "racer_stats",
        "driver_stats",
        "fighter_stats",
        "goalie_stats",
        "skater_stats",
        "play_stats",
        "plays",
        "drives",
        "circuits",
        "constructor_stats",
        "pitcher_vs_batter",
        "batter_vs_pitcher",
        "team_game_stats",
        "batter_game_stats",
        "pitcher_game_stats",
        "advanced_stats",
    ),
    "season_averages": ("player_stats", "racer_stats", "driver_stats", "fighter_stats", "team_stats", "standings"),
    "player_season_averages": ("player_stats", "racer_stats", "driver_stats", "fighter_stats"),
    "team_season_averages": ("team_stats",),
    "standings_season_averages": ("standings",),
}

SPORT_PARTICIPANT_KINDS: dict[str, tuple[str, ...]] = {
    "f1": ("racers", "drivers", "participants", "players"),
    "indycar": ("racers", "drivers", "participants", "players"),
    "ufc": ("fighters", "participants", "players"),
}

SPORT_PARTICIPANT_STATS_KINDS: dict[str, tuple[str, ...]] = {
    "f1": ("racer_stats", "driver_stats", "player_stats"),
    "indycar": ("racer_stats", "driver_stats", "player_stats"),
    "ufc": ("fighter_stats", "player_stats"),
}


@dataclass
class BuildResult:
    sport: str
    season: str
    category: str
    rows: int
    partitions: int
    output_root: str


class CuratedParquetBuilder:
    """Builds small, query-friendly category parquets from monolithic normalized files.

    Input:  data/normalized/<sport>/<kind>_<season>.parquet
    Output: data/normalized_curated/<sport>/<category>/season=<season>/.../*.parquet
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._normalized_dir = settings.normalized_dir
        self._curated_dir = settings.normalized_curated_dir

    def discover_seasons(self, sport: str) -> list[str]:
        sport_dir = self._normalized_dir / sport
        if not sport_dir.exists():
            return []
        seasons: set[str] = set()
        for file in sport_dir.glob("games_*.parquet"):
            season = file.stem.replace("games_", "")
            if season.isdigit():
                seasons.add(season)
        return sorted(seasons)

    def build_sport(
        self,
        sport: str,
        seasons: Iterable[str] | None = None,
        categories: Iterable[str] | None = None,
    ) -> list[BuildResult]:
        selected_seasons = list(seasons) if seasons else self.discover_seasons(sport)
        results: list[BuildResult] = []

        for season in selected_seasons:
            selected_categories = list(categories) if categories else self.categories_for_sport(sport, season)
            for category in selected_categories:
                try:
                    result = self._build_category(sport=sport, season=season, category=category)
                    if result is not None:
                        results.append(result)
                except Exception:
                    logger.exception(
                        "Curated build failed for sport=%s season=%s category=%s",
                        sport,
                        season,
                        category,
                    )

        # Some normalized kinds are not season-suffixed (e.g. predictions,
        # player_props, odds_history). Persist them under season=all so they are
        # queryable in the curated layout and DuckDB catalog.
        global_kinds = self.discover_global_kinds(sport)
        if global_kinds:
            selected_categories = set(categories) if categories else set(global_kinds)
            for kind in sorted(global_kinds):
                if kind not in selected_categories:
                    continue
                try:
                    result = self._build_global_kind(sport=sport, kind=kind)
                    if result is not None:
                        results.append(result)
                except Exception:
                    logger.exception("Curated global build failed for sport=%s kind=%s", sport, kind)
        return results

    def discover_kinds(self, sport: str, season: str) -> list[str]:
        sport_dir = self._normalized_dir / sport
        if not sport_dir.exists():
            return []

        suffix = f"_{season}.parquet"
        kinds: set[str] = set()
        for file in sport_dir.glob(f"*{suffix}"):
            name = file.name
            if not name.endswith(suffix):
                continue
            kind = name[: -len(suffix)]
            if kind:
                kinds.add(kind)
        return sorted(kinds)

    def discover_global_kinds(self, sport: str) -> list[str]:
        sport_dir = self._normalized_dir / sport
        if not sport_dir.exists():
            return []

        kinds: set[str] = set()
        for file in sport_dir.glob("*.parquet"):
            stem = file.stem
            if "_" in stem:
                suffix = stem.rsplit("_", 1)[1]
                if suffix.isdigit():
                    continue
            kinds.add(stem)
        return sorted(kinds)

    def categories_for_sport(self, sport: str, season: str | None = None) -> list[str]:
        categories: list[str] = list(BASE_CATEGORIES)
        categories.extend(SPORT_CATEGORY_OVERRIDES.get(sport.lower(), ()))
        if season is not None:
            discovered = self.discover_kinds(sport, season)
            categories.extend(discovered)
            categories.extend(self._derived_season_average_categories(discovered))

        seen: set[str] = set()
        ordered: list[str] = []
        for category in categories:
            if category in seen:
                continue
            seen.add(category)
            ordered.append(category)
        return ordered

    def _build_category(self, sport: str, season: str, category: str) -> BuildResult | None:
        if category == "players":
            players_df = self._load_first_available_kind(
                sport,
                self._participant_kinds_for_sport(sport),
                season,
            )
            player_stats_df = self._load_first_available_kind(
                sport,
                self._participant_stats_kinds_for_sport(sport),
                season,
            )
            standings_df = self._load_kind(sport, "standings", season)
            df = self._build_players(players_df, player_stats_df, standings_df, sport=sport, season=season)
            return self._write_partitioned(sport, season, category, df, temporal=False)

        if category == "conferences":
            teams_df = self._load_kind(sport, "teams", season)
            standings_df = self._load_kind(sport, "standings", season)
            df = self._build_conferences(teams_df, standings_df, sport=sport, season=season)
            return self._write_partitioned(sport, season, category, df, temporal=False)

        if category == "coaches":
            teams_df = self._load_kind(sport, "teams", season)
            games_df = self._load_kind(sport, "games", season)
            df = self._build_coaches(teams_df, games_df, sport=sport, season=season)
            return self._write_partitioned(sport, season, category, df, temporal=False)

        if category == "referees":
            games_df = self._load_kind(sport, "games", season)
            df = self._build_referees(games_df, sport=sport, season=season)
            return self._write_partitioned(sport, season, category, df, temporal=True)

        if category == "season_averages":
            player_df = self._load_first_available_kind(
                sport,
                self._participant_stats_kinds_for_sport(sport),
                season,
            )
            team_df = self._load_kind(sport, "team_stats", season)
            standings_df = self._load_kind(sport, "standings", season)
            df = self._build_season_averages(player_df, team_df, standings_df, sport=sport, season=season)
            return self._write_partitioned(sport, season, category, df, temporal=False)

        if category == "player_season_averages":
            player_df = self._load_first_available_kind(
                sport,
                self._participant_stats_kinds_for_sport(sport),
                season,
            )
            df = self._build_single_season_averages(player_df, entity_type="player", sport=sport, season=season)
            return self._write_partitioned(sport, season, category, df, temporal=False)

        if category == "team_season_averages":
            team_df = self._load_kind(sport, "team_stats", season)
            df = self._build_single_season_averages(team_df, entity_type="team", sport=sport, season=season)
            return self._write_partitioned(sport, season, category, df, temporal=False)

        if category == "standings_season_averages":
            standings_df = self._load_kind(sport, "standings", season)
            df = self._build_single_season_averages(standings_df, entity_type="team_standings", sport=sport, season=season)
            return self._write_partitioned(sport, season, category, df, temporal=False)

        if category.startswith("season_averages/"):
            kind = category.split("/", 1)[1].strip()
            if not kind:
                return None
            source_df = self._load_kind(sport, kind, season)
            if source_df.empty:
                return None
            entity_type = self._infer_entity_type_from_kind(kind)
            df = self._build_single_season_averages(source_df, entity_type=entity_type, sport=sport, season=season)
            return self._write_partitioned(sport, season, category, df, temporal=False)

        source_kinds = CATEGORY_SOURCE_KINDS.get(category)
        if not source_kinds:
            direct = self._load_kind(sport, category, season)
            if direct.empty:
                return None
            return self._write_partitioned(
                sport,
                season,
                category,
                direct,
                temporal=("date" in direct.columns or "week" in direct.columns),
            )

        frames: list[pd.DataFrame] = []
        for kind in source_kinds:
            source_df = self._load_kind(sport, kind, season)
            if source_df.empty:
                continue
            if category == "stats":
                source_df = source_df.copy()
                source_df["stats_kind"] = kind
            frames.append(source_df)

        if not frames:
            return None

        merged = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        return self._write_partitioned(sport, season, category, merged, temporal=(category in {"games", "stats"}))

    def _build_global_kind(self, sport: str, kind: str) -> BuildResult | None:
        path = self._normalized_dir / sport / f"{kind}.parquet"
        if not path.exists():
            return None
        df = pd.read_parquet(path, engine="pyarrow")
        if df.empty:
            return None
        return self._write_partitioned(
            sport,
            "all",
            kind,
            df,
            temporal=("date" in df.columns or "week" in df.columns),
        )

    def _participant_kinds_for_sport(self, sport: str) -> tuple[str, ...]:
        aliases = SPORT_PARTICIPANT_KINDS.get(sport.lower(), tuple())
        return aliases + ("players",)

    def _participant_stats_kinds_for_sport(self, sport: str) -> tuple[str, ...]:
        aliases = SPORT_PARTICIPANT_STATS_KINDS.get(sport.lower(), tuple())
        return aliases + ("player_stats",)

    def _load_first_available_kind(self, sport: str, kinds: Iterable[str], season: str) -> pd.DataFrame:
        for kind in kinds:
            df = self._load_kind(sport, kind, season)
            if not df.empty:
                return df
        return pd.DataFrame()

    def _derived_season_average_categories(self, discovered_kinds: Iterable[str]) -> list[str]:
        categories: list[str] = []
        for kind in discovered_kinds:
            if kind in {"games", "odds", "news", "injuries", "market_signals", "schedule_fatigue"}:
                continue
            if kind.endswith("_stats") or kind.endswith("_metrics") or kind.endswith("_tracking"):
                categories.append(f"season_averages/{kind}")
            elif kind in {"standings", "team_stats", "player_stats", "racer_stats", "driver_stats", "fighter_stats"}:
                categories.append(f"season_averages/{kind}")
        return categories

    @staticmethod
    def _infer_entity_type_from_kind(kind: str) -> str:
        if "player" in kind or "batter" in kind or "pitcher" in kind or "racer" in kind or "driver" in kind or "fighter" in kind:
            return "player"
        if "team" in kind or kind == "standings":
            return "team"
        return "player"

    def _load_kind(self, sport: str, kind: str, season: str) -> pd.DataFrame:
        path = self._normalized_dir / sport / f"{kind}_{season}.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path, engine="pyarrow")

    def _build_conferences(
        self,
        teams_df: pd.DataFrame,
        standings_df: pd.DataFrame,
        *,
        sport: str,
        season: str,
    ) -> pd.DataFrame:
        rows: list[dict] = []
        seen: set[tuple[str, str]] = set()

        def _ingest(df: pd.DataFrame) -> None:
            if df.empty:
                return
            for _, row in df.iterrows():
                conference = str(row.get("conference", "") or "").strip()
                division = str(row.get("division", "") or "").strip()
                if not conference:
                    continue
                key = (conference.lower(), division.lower())
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    {
                        "sport": sport,
                        "season": str(season),
                        "conference": conference,
                        "division": division or None,
                    }
                )

        _ingest(teams_df)
        _ingest(standings_df)

        return pd.DataFrame(rows)

    def _build_players(
        self,
        players_df: pd.DataFrame,
        player_stats_df: pd.DataFrame,
        standings_df: pd.DataFrame,
        *,
        sport: str,
        season: str,
    ) -> pd.DataFrame:
        if not players_df.empty:
            out = players_df.copy()
            out["sport"] = sport
            out["season"] = str(season)
            return out.drop_duplicates()

        if player_stats_df.empty:
            player_stats_df = standings_df
        if player_stats_df.empty:
            return pd.DataFrame()

        cols = [
            c
            for c in [
                "player_id",
                "driver_id",
                "racer_id",
                "fighter_id",
                "participant_id",
                "player_name",
                "driver_name",
                "racer_name",
                "fighter_name",
                "name",
                "team_id",
                "position",
            ]
            if c in player_stats_df.columns
        ]
        if not cols:
            return pd.DataFrame()

        out = player_stats_df[cols].copy()
        if "player_name" not in out.columns and "name" in out.columns:
            out = out.rename(columns={"name": "player_name"})
        for source_name in ("driver_name", "racer_name", "fighter_name"):
            if source_name in out.columns and "player_name" not in out.columns:
                out = out.rename(columns={source_name: "player_name"})
        for source_id in ("driver_id", "racer_id", "fighter_id", "participant_id"):
            if source_id in out.columns and "player_id" not in out.columns:
                out = out.rename(columns={source_id: "player_id"})
        out["sport"] = sport
        out["season"] = str(season)
        return out.drop_duplicates()

    def _build_coaches(
        self,
        teams_df: pd.DataFrame,
        games_df: pd.DataFrame,
        *,
        sport: str,
        season: str,
    ) -> pd.DataFrame:
        rows: list[dict] = []
        candidate_cols = [
            "home_coach",
            "away_coach",
            "coach",
            "head_coach",
        ]

        if not games_df.empty:
            for _, row in games_df.iterrows():
                game_id = row.get("id") or row.get("game_id")
                date = row.get("date")
                for col in candidate_cols:
                    coach = row.get(col)
                    if not coach:
                        continue
                    side = "home" if col.startswith("home_") else "away" if col.startswith("away_") else "unknown"
                    team_id = row.get(f"{side}_team_id") if side in {"home", "away"} else None
                    rows.append(
                        {
                            "sport": sport,
                            "season": str(season),
                            "game_id": str(game_id) if game_id is not None else None,
                            "date": date,
                            "team_id": str(team_id) if team_id is not None else None,
                            "coach_name": str(coach),
                            "coach_role": "head_coach",
                        }
                    )

        if teams_df is not None and not teams_df.empty:
            team_name_col = "name" if "name" in teams_df.columns else "team_name" if "team_name" in teams_df.columns else None
            for _, row in teams_df.iterrows():
                coach = row.get("coach") or row.get("head_coach")
                if not coach:
                    continue
                rows.append(
                    {
                        "sport": sport,
                        "season": str(season),
                        "game_id": None,
                        "date": None,
                        "team_id": str(row.get("team_id") or row.get("id") or ""),
                        "team_name": str(row.get(team_name_col) or "") if team_name_col else None,
                        "coach_name": str(coach),
                        "coach_role": "head_coach",
                    }
                )

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).drop_duplicates()

    def _build_referees(self, games_df: pd.DataFrame, *, sport: str, season: str) -> pd.DataFrame:
        if games_df.empty:
            return pd.DataFrame()

        referee_cols = [
            "referee",
            "referees",
            "official",
            "officials",
            "crew",
            "umpire",
            "umpires",
        ]
        available_cols = [c for c in referee_cols if c in games_df.columns]
        if not available_cols:
            return pd.DataFrame()

        rows: list[dict] = []
        for _, row in games_df.iterrows():
            game_id = row.get("id") or row.get("game_id")
            date = row.get("date")
            for col in available_cols:
                raw = row.get(col)
                if raw is None or raw == "":
                    continue
                if isinstance(raw, list):
                    names = [str(n).strip() for n in raw if str(n).strip()]
                else:
                    names = [n.strip() for n in str(raw).replace(";", ",").split(",") if n.strip()]
                for name in names:
                    rows.append(
                        {
                            "sport": sport,
                            "season": str(season),
                            "game_id": str(game_id) if game_id is not None else None,
                            "date": date,
                            "official_name": name,
                            "official_role": "referee" if "ump" not in col else "umpire",
                            "source_column": col,
                        }
                    )

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows).drop_duplicates()

    def _build_season_averages(
        self,
        player_df: pd.DataFrame,
        team_df: pd.DataFrame,
        standings_df: pd.DataFrame,
        *,
        sport: str,
        season: str,
    ) -> pd.DataFrame:
        rows: list[pd.DataFrame] = []

        if not player_df.empty:
            p = player_df.copy()
            key_col = "player_id" if "player_id" in p.columns else "player_name" if "player_name" in p.columns else None
            if key_col:
                numeric_cols = [c for c in p.columns if pd.api.types.is_numeric_dtype(p[c])]
                if numeric_cols:
                    agg = p.groupby(key_col, dropna=False)[numeric_cols].mean(numeric_only=True).reset_index()
                    agg["entity_type"] = "player"
                    rows.append(agg)

        if not team_df.empty:
            t = team_df.copy()
            key_col = "team_id" if "team_id" in t.columns else "id" if "id" in t.columns else None
            if key_col:
                numeric_cols = [c for c in t.columns if pd.api.types.is_numeric_dtype(t[c])]
                if numeric_cols:
                    agg = t.groupby(key_col, dropna=False)[numeric_cols].mean(numeric_only=True).reset_index()
                    agg["entity_type"] = "team"
                    rows.append(agg)

        if not standings_df.empty:
            s = standings_df.copy()
            key_col = "team_id" if "team_id" in s.columns else "id" if "id" in s.columns else None
            if key_col:
                numeric_cols = [c for c in s.columns if pd.api.types.is_numeric_dtype(s[c])]
                if numeric_cols:
                    agg = s.groupby(key_col, dropna=False)[numeric_cols].mean(numeric_only=True).reset_index()
                    agg["entity_type"] = "team_standings"
                    rows.append(agg)

        if not rows:
            return pd.DataFrame()

        out = pd.concat(rows, ignore_index=True, sort=False)
        out["sport"] = sport
        out["season"] = str(season)
        return out

    def _build_single_season_averages(
        self,
        df: pd.DataFrame,
        *,
        entity_type: str,
        sport: str,
        season: str,
    ) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        key_candidates = {
            "player": ("player_id", "player_name", "name"),
            "team": ("team_id", "id", "team_name", "name"),
            "team_standings": ("team_id", "id", "team_name", "name"),
        }
        keys = key_candidates.get(entity_type, ())
        key_col = next((k for k in keys if k in df.columns), None)
        if key_col is None:
            return pd.DataFrame()

        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if not numeric_cols:
            return pd.DataFrame()

        out = df.groupby(key_col, dropna=False)[numeric_cols].mean(numeric_only=True).reset_index()
        out["entity_type"] = entity_type
        out["sport"] = sport
        out["season"] = str(season)
        return out

    def _write_partitioned(
        self,
        sport: str,
        season: str,
        category: str,
        df: pd.DataFrame,
        *,
        temporal: bool,
    ) -> BuildResult | None:
        if df.empty:
            return None

        category_path = Path(*[p for p in category.split("/") if p])
        root = self._curated_dir / sport / category_path / f"season={season}"
        root.mkdir(parents=True, exist_ok=True)

        if not temporal:
            out = root / "part.parquet"
            df.to_parquet(out, engine="pyarrow", index=False)
            return BuildResult(
                sport=sport,
                season=str(season),
                category=category,
                rows=int(len(df)),
                partitions=1,
                output_root=str(root),
            )

        partition_col, partition_values = self._resolve_temporal_partitions(df, sport)
        if partition_col is None:
            out = root / "part.parquet"
            df.to_parquet(out, engine="pyarrow", index=False)
            return BuildResult(
                sport=sport,
                season=str(season),
                category=category,
                rows=int(len(df)),
                partitions=1,
                output_root=str(root),
            )

        working = df.copy()
        working[partition_col] = partition_values
        working = working[working[partition_col].notna()]
        if working.empty:
            return None

        parts_written = 0
        for part_value, chunk in working.groupby(partition_col, dropna=True):
            part_dir = root / f"{partition_col}={part_value}"
            part_dir.mkdir(parents=True, exist_ok=True)
            out = part_dir / "part.parquet"
            chunk.to_parquet(out, engine="pyarrow", index=False)
            parts_written += 1

        return BuildResult(
            sport=sport,
            season=str(season),
            category=category,
            rows=int(len(working)),
            partitions=parts_written,
            output_root=str(root),
        )

    def _resolve_temporal_partitions(self, df: pd.DataFrame, sport: str) -> tuple[str | None, pd.Series | None]:
        if sport.lower() in WEEKLY_PARTITION_SPORTS:
            if "week" in df.columns:
                week = pd.to_numeric(df["week"], errors="coerce")
                vals = week.apply(lambda x: f"{int(x):02d}" if pd.notna(x) else None)
                return "week", vals
            if "date" in df.columns:
                dt = pd.to_datetime(df["date"], errors="coerce", utc=True)
                vals = dt.dt.isocalendar().week.astype("Int64").apply(lambda x: f"{int(x):02d}" if pd.notna(x) else None)
                return "week", vals
            return None, None

        if "date" in df.columns:
            dt = pd.to_datetime(df["date"], errors="coerce", utc=True)
            vals = dt.dt.strftime("%Y-%m-%d")
            vals = vals.where(dt.notna(), None)
            return "date", vals
        return None, None
