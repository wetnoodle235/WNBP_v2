# ──────────────────────────────────────────────────────────
# V5.0 Backend — ML Predictors: Game Predictor
# ──────────────────────────────────────────────────────────
"""
Generate game-level predictions using trained ensemble models.

Supports both *joint* mode (home_score + away_score → derived
winner / total / spread) and *separate* mode (independent winner,
total, spread models).
"""

from __future__ import annotations

import logging
import pickle
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from features.registry import get_extractor
from ml.models.base import PredictionResult
from ml.models.ensemble import EnsembleVoter

logger = logging.getLogger(__name__)

def _add_delta_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute home-minus-away delta features (mirrors train.py logic).

    Called at inference time so the model receives the same delta columns
    it was trained on.  Already-present delta columns are not recomputed.
    """
    home_cols = {c[5:]: c for c in df.columns if c.startswith("home_")}
    away_cols = {c[5:]: c for c in df.columns if c.startswith("away_")}
    shared = set(home_cols) & set(away_cols)
    deltas: dict[str, Any] = {}
    for key in sorted(shared):
        delta_name = f"delta_{key}"
        if delta_name not in df.columns:
            deltas[delta_name] = df[home_cols[key]].values - df[away_cols[key]].values
    if deltas:
        df = pd.concat([df, pd.DataFrame(deltas, index=df.index)], axis=1)
    return df


# Metadata columns that must not be fed to the model
_META_COLS = {
    "game_id",
    "date",
    "home_team_id",
    "away_team_id",
    "home_score",
    "away_score",
    "season",
    # quarter/period/inning target columns — never used as features
    "home_q1", "home_q2", "home_q3", "home_q4", "home_ot",
    "away_q1", "away_q2", "away_q3", "away_q4", "away_ot",
    # MLB inning-by-inning scores (outcome data — must not be input features)
    "home_i1", "home_i2", "home_i3", "home_i4", "home_i5",
    "home_i6", "home_i7", "home_i8", "home_i9", "home_extras",
    "away_i1", "away_i2", "away_i3", "away_i4", "away_i5",
    "away_i6", "away_i7", "away_i8", "away_i9", "away_extras",
    # Soccer half scores (outcome data)
    "home_h1", "home_h2", "away_h1", "away_h2",
    "home_h1_score", "home_h2_score", "away_h1_score", "away_h2_score",
    # Hockey period scores (outcome data)
    "home_p1", "home_p2", "home_p3", "away_p1", "away_p2", "away_p3",
    # Motorsport current-race outcome columns — NOT available pre-race
    "podium", "points_finish", "dnf", "fastest_lap",
    "laps_completed", "laps_completion_pct",
    "avg_speed_kph", "pit_stops", "avg_pit_time_s",
    "safety_car_count", "dnf_count", "red_flag_count", "race_pit_stops_total",
}


class GamePredictor:
    """Load trained models and generate game predictions."""

    def __init__(self, sport: str, models_dir: Path | str, data_dir: Path | str) -> None:
        self.sport = sport.lower()
        self.data_dir = Path(data_dir) if not isinstance(data_dir, Path) else data_dir
        self.models_dir = Path(models_dir) if not isinstance(models_dir, Path) else models_dir
        self.extractor = get_extractor(self.sport, self.data_dir)
        self._bundle: dict[str, Any] | None = None
        self._mode: str | None = None
        self._extra: dict[str, Any] | None = None
        self._extra_future: "concurrent.futures.Future | None" = None
        self._load_models(self.models_dir)

    # ── Model loading ────────────────────────────────────

    def _load_models(self, models_dir: Path) -> None:
        """Try joint first, fall back to separate. Auto-resolve sport subdir."""
        # If models_dir doesn't contain .pkl directly, try {sport}/ subdir
        joint_path = models_dir / "joint_models.pkl"
        sep_path = models_dir / "separate_models.pkl"
        if not joint_path.exists() and not sep_path.exists():
            sport_sub = models_dir / self.sport
            if sport_sub.is_dir():
                models_dir = sport_sub
                joint_path = models_dir / "joint_models.pkl"
                sep_path = models_dir / "separate_models.pkl"

        if joint_path.exists():
            self._bundle = self._unpickle(joint_path)
            self._mode = "joint"
            logger.info("Loaded joint models from %s", joint_path)
        elif sep_path.exists():
            self._bundle = self._unpickle(sep_path)
            self._mode = "separate"
            logger.info("Loaded separate models from %s", sep_path)
        else:
            logger.warning(
                "No trained models found in %s — predictions will fail until "
                "models are trained",
                models_dir,
            )

        # Load extra-market models in a background thread (can be large: 0.5-1.5GB)
        extra_path = models_dir / "extra_models.pkl"
        if extra_path.exists():
            from concurrent.futures import ThreadPoolExecutor
            self._extra_pool = ThreadPoolExecutor(max_workers=1)
            self._extra_future = self._extra_pool.submit(self._unpickle, extra_path)
        else:
            self._extra_future = None

    def _ensure_extra_loaded(self) -> None:
        """Block until extra models finish loading (called before extra inference)."""
        if self._extra_future is not None:
            try:
                self._extra = self._extra_future.result()
                n = len(self._extra.get("models", {}))
                logger.info("Loaded %d extra-market models", n)
            except Exception:
                logger.warning("Could not load extra_models.pkl", exc_info=True)
                self._extra = None
            finally:
                self._extra_future = None
                if hasattr(self, "_extra_pool"):
                    self._extra_pool.shutdown(wait=False)

    @staticmethod
    def _unpickle(path: Path) -> dict[str, Any]:
        with open(path, "rb") as fh:
            return pickle.load(fh)  # noqa: S301

    # ── Feature helpers ──────────────────────────────────

    def _game_to_features(self, game: dict[str, Any]) -> pd.DataFrame | None:
        """Extract features for a single game and return a 1-row DataFrame."""
        try:
            features = self.extractor.extract_game_features(game)
        except Exception:
            logger.error(
                "Feature extraction failed for game %s",
                game.get("game_id", "?"),
                exc_info=True,
            )
            return None

        if not features:
            return None

        row = {k: v for k, v in features.items() if k not in _META_COLS}
        df = pd.DataFrame([row]).fillna(0)
        df = _add_delta_features(df)  # compute delta_* columns before alignment

        # Align columns to training feature names
        if self._bundle and "feature_names" in self._bundle:
            expected = self._bundle["feature_names"]
            for col in expected:
                if col not in df.columns:
                    df[col] = 0.0
            df = df[[c for c in expected if c in df.columns]]

        return df

    def _features_from_row(self, feature_row: dict[str, Any]) -> pd.DataFrame | None:
        """Build feature DataFrame from pre-computed row (bypasses live extraction)."""
        row = {k: v for k, v in feature_row.items() if k not in _META_COLS}
        df = pd.DataFrame([row]).fillna(0)
        df = _add_delta_features(df)  # compute delta_* columns before alignment
        if self._bundle and "feature_names" in self._bundle:
            expected = self._bundle["feature_names"]
            for col in expected:
                if col not in df.columns:
                    df[col] = 0.0
            df = df[[c for c in expected if c in df.columns]]
        return df

    def predict_from_precomputed(
        self, feature_row: dict[str, Any], game: dict[str, Any]
    ) -> "PredictionResult":
        """Predict using pre-computed features (skips extract_game_features)."""
        if self._bundle is None:
            raise RuntimeError(f"No trained models for {self.sport}")
        X = self._features_from_row(feature_row)
        if X is None or X.empty:
            raise ValueError("Empty feature row")
        game_id = str(game.get("game_id", game.get("id", "unknown")))
        home_team = str(game.get("home_team", game.get("home_team_id", "")))
        away_team = str(game.get("away_team", game.get("away_team_id", "")))
        if self._mode == "joint":
            return self._predict_joint(X, game_id, home_team, away_team)
        return self._predict_separate(X, game_id, home_team, away_team)

    def predict_batch_precomputed(
        self,
        precomp_df: "pd.DataFrame",
        games_df: "pd.DataFrame",
    ) -> list["PredictionResult"]:
        """Batch-predict all games using pre-computed features.

        Parameters
        ----------
        precomp_df : DataFrame indexed by game_id, containing feature columns.
        games_df : DataFrame with columns id/game_id, home_team_id, away_team_id, date.

        Returns
        -------
        List of PredictionResult objects, one per matched game.
        """
        if self._bundle is None:
            raise RuntimeError(f"No trained models for {self.sport}")

        expected = self._bundle.get("feature_names", [])
        meta_skip = _META_COLS

        results: list["PredictionResult"] = []

        # Identify matched games
        game_id_col = "game_id" if "game_id" in games_df.columns else "id"
        gids = games_df[game_id_col].astype(str).tolist()
        matched_gids = [g for g in gids if g in precomp_df.index]
        if not matched_gids:
            return results

        # Build aligned batch feature matrix
        rows = precomp_df.loc[matched_gids]
        feat_cols = [c for c in rows.columns if c not in meta_skip]
        X_all = rows[feat_cols].fillna(0)
        X_all = _add_delta_features(X_all)  # compute delta_* before alignment

        # Align to expected feature names
        if expected:
            for col in expected:
                if col not in X_all.columns:
                    X_all[col] = 0.0
            X_all = X_all[[c for c in expected if c in X_all.columns]]

        X_np = X_all.values.astype(np.float64)

        # ── Main models (batch inference) ─────────────────
        winner_ens: "EnsembleVoter" = self._bundle["winner_ensemble"]
        probs_all, preds_all, vote_details_all = winner_ens.predict_class(
            pd.DataFrame(X_np, columns=X_all.columns)
        )

        if self._mode == "joint":
            home_ens: "EnsembleVoter" = self._bundle["home_ensemble"]
            away_ens: "EnsembleVoter" = self._bundle["away_ensemble"]
            home_scores = home_ens.predict_regression(
                pd.DataFrame(X_np, columns=X_all.columns)
            )
            away_scores = away_ens.predict_regression(
                pd.DataFrame(X_np, columns=X_all.columns)
            )
        else:
            total_ens: "EnsembleVoter" = self._bundle["total_ensemble"]
            spread_ens: "EnsembleVoter" = self._bundle["spread_ensemble"]
            totals = total_ens.predict_regression(
                pd.DataFrame(X_np, columns=X_all.columns)
            )
            spreads = spread_ens.predict_regression(
                pd.DataFrame(X_np, columns=X_all.columns)
            )

        # ── Extra models (batch inference) ────────────────
        extra_models: dict[str, Any] = {}
        extra_cls_probs: dict[str, "np.ndarray"] = {}
        extra_reg_vals: dict[str, "np.ndarray"] = {}
        self._ensure_extra_loaded()
        if self._extra:
            extra_models = self._extra.get("models", {})
            extra_feature_names = self._extra.get("feature_names", [])

            # Build aligned extra feature matrix once
            X_extra = rows[feat_cols].fillna(0)
            X_extra = _add_delta_features(X_extra)  # ensure delta cols are present
            for col in extra_feature_names:
                if col not in X_extra.columns:
                    X_extra[col] = 0.0
            if extra_feature_names:
                X_extra = X_extra[
                    [c for c in extra_feature_names if c in X_extra.columns]
                ]
            X_extra_np = X_extra.values.astype(np.float64)

            for mkey, m in extra_models.items():
                try:
                    Xs = m.scaler.transform(X_extra_np)
                    if hasattr(m, "_fitted_classifiers") and m._fitted_classifiers:
                        extra_cls_probs[mkey] = m.predict_proba_fast_batch(Xs)
                    elif hasattr(m, "_fitted_regressors") and m._fitted_regressors:
                        extra_reg_vals[mkey] = m.predict_regression_fast_batch(Xs)
                except Exception:
                    pass

        # ── Build game metadata index ─────────────────────
        game_meta: dict[str, dict[str, str]] = {}
        for _, grow in games_df.iterrows():
            gid = str(grow.get(game_id_col, ""))
            game_meta[gid] = {
                "home_team": str(grow.get("home_team", grow.get("home_team_id", ""))),
                "away_team": str(grow.get("away_team", grow.get("away_team_id", ""))),
            }

        # ── Assemble results ──────────────────────────────
        sport_meta: dict[str, Any] = (self._extra or {}).get("sport_meta", {})
        period_label = sport_meta.get("period_label", "period")
        available_periods = sport_meta.get("available_periods", [])

        for i, gid in enumerate(matched_gids):
            hw_prob = float(probs_all[i])
            vd = vote_details_all.get(str(i), {})

            meta = game_meta.get(gid, {})

            if self._mode == "joint":
                hs = float(home_scores[i])
                as_ = float(away_scores[i])
                total = hs + as_
                spread = hs - as_
            else:
                total = float(totals[i])
                spread = float(spreads[i])
                hs = (total + spread) / 2
                as_ = (total - spread) / 2

            result = PredictionResult(
                game_id=gid,
                sport=self.sport,
                home_team=meta.get("home_team", ""),
                away_team=meta.get("away_team", ""),
                home_win_prob=hw_prob,
                away_win_prob=round(1.0 - hw_prob, 4),
                predicted_home_score=round(hs, 1),
                predicted_away_score=round(as_, 1),
                predicted_total=round(total, 1),
                predicted_spread=round(spread, 1),
                confidence=vd.get("confidence", 0.50),
                n_models=vd.get("n_models", 0),
                consensus=vd.get("consensus", 0.0),
                model_votes=vd.get("votes", {}),
            )

            # Extra market fields from batch arrays
            def _cls(key: str) -> "float | None":
                arr = extra_cls_probs.get(key)
                return float(arr[i]) if arr is not None else None

            def _reg(key: str) -> "float | None":
                arr = extra_reg_vals.get(key)
                return float(arr[i]) if arr is not None else None

            draw_p = _cls("draw")
            if draw_p is not None:
                result.draw_prob = round(draw_p, 4)
                hw = hw_prob * (1 - draw_p)
                aw = result.away_win_prob * (1 - draw_p)
                t = hw + draw_p + aw
                if t > 0:
                    result.three_way = {
                        "home": round(hw / t, 4),
                        "draw": round(draw_p / t, 4),
                        "away": round(aw / t, 4),
                    }

            ot_p = _cls("overtime")
            ot_p2 = _cls("overtime_prob")
            if ot_p is not None:
                result.ot_prob = round(ot_p, 4)
            elif ot_p2 is not None:
                result.ot_prob = round(ot_p2, 4)
            elif draw_p is not None:
                result.ot_prob = result.draw_prob

            # ── Q1 Winner + Q1 Total (Basketball) ─────────────
            q1_win_p = _cls("q1_winner")
            if q1_win_p is not None:
                result.q1_home_win_prob = round(q1_win_p, 4)
            for qk, ql_attr, qv_attr in [
                ("q1_total_over_low", "q1_total_over_low_prob", "q1_total_over_low_line"),
                ("q1_total_over_mid", "q1_total_over_mid_prob", "q1_total_over_mid_line"),
                ("q1_total_over_high", "q1_total_over_high_prob", "q1_total_over_high_line"),
            ]:
                qp = _cls(qk)
                if qp is not None:
                    setattr(result, ql_attr, round(qp, 4))
                    qline = extra_models.get(f"{qk}_line")
                    if qline is not None:
                        setattr(result, qv_attr, float(qline))

            ht_home = _reg("halftime_home_score")
            ht_away = _reg("halftime_away_score")
            if ht_home is not None and ht_away is not None:
                result.halftime_home_score = round(ht_home, 1)
                result.halftime_away_score = round(ht_away, 1)
                result.halftime_spread = round(ht_home - ht_away, 1)
                result.halftime_total = round(ht_home + ht_away, 1)

            ht_win_p = _cls("halftime_winner")
            if ht_win_p is not None:
                result.halftime_home_win_prob = round(ht_win_p, 4)
                result.halftime_away_win_prob = round(1.0 - ht_win_p, 4)
                ht_draw = _cls("halftime_draw")
                result.halftime_draw_prob = round(ht_draw, 4) if ht_draw is not None else None

            ht_spread = _reg("halftime_spread")
            if ht_spread is not None and result.halftime_spread is None:
                result.halftime_spread = round(ht_spread, 1)
            ht_total = _reg("halftime_total")
            if ht_total is not None and result.halftime_total is None:
                result.halftime_total = round(ht_total, 1)

            result.period_label = period_label
            if available_periods:
                period_preds: list[dict] = []
                for pi in available_periods:
                    ph = _reg(f"period_{pi}_home")
                    pa = _reg(f"period_{pi}_away")
                    pt = _reg(f"period_{pi}_total")
                    pw_p = _cls(f"period_{pi}_winner")
                    entry: dict[str, Any] = {"period": pi}
                    if ph is not None:
                        entry["home"] = round(ph, 2)
                    if pa is not None:
                        entry["away"] = round(pa, 2)
                    if ph is not None and pa is not None:
                        entry["spread"] = round(ph - pa, 2)
                        entry.setdefault("total", round(ph + pa, 2))
                    if pt is not None:
                        entry["total"] = round(pt, 2)
                    if pw_p is not None:
                        entry["home_win_prob"] = round(pw_p, 4)
                        entry["away_win_prob"] = round(1.0 - pw_p, 4)
                    if len(entry) > 1:
                        period_preds.append(entry)
                if period_preds:
                    result.period_predictions = period_preds

            fs_p = _cls("first_score")
            if fs_p is not None:
                result.first_score_home_prob = round(fs_p, 4)
                result.first_score_team = "home" if fs_p >= 0.5 else "away"
            ls_p = _cls("last_score")
            if ls_p is not None:
                result.last_score_home_prob = round(ls_p, 4)
                result.last_score_team = "home" if ls_p >= 0.5 else "away"

            btts_p = _cls("btts")
            if btts_p is not None:
                result.btts_prob = round(btts_p, 4)
            btts_ov = _cls("btts_over2_5")
            if btts_ov is not None:
                result.btts_over2_5_prob = round(btts_ov, 4)
            h15 = _cls("home_over1_5")
            if h15 is not None:
                result.home_over1_5_prob = round(h15, 4)
            a15 = _cls("away_over1_5")
            if a15 is not None:
                result.away_over1_5_prob = round(a15, 4)
            hw2p = _cls("home_win_score2plus")
            if hw2p is not None:
                result.home_win_score2plus_prob = round(hw2p, 4)
            aw2p = _cls("away_win_score2plus")
            if aw2p is not None:
                result.away_win_score2plus_prob = round(aw2p, 4)

            cs_h = _cls("clean_sheet_home")
            if cs_h is not None:
                result.home_clean_sheet_prob = round(cs_h, 4)
            cs_a = _cls("clean_sheet_away")
            if cs_a is not None:
                result.away_clean_sheet_prob = round(cs_a, 4)

            # ── Double Chance ───────────────────────
            dc_1x = _cls("double_chance_1X")
            if dc_1x is not None:
                result.double_chance_1X_prob = round(dc_1x, 4)
            dc_x2 = _cls("double_chance_X2")
            if dc_x2 is not None:
                result.double_chance_X2_prob = round(dc_x2, 4)
            dc_12 = _cls("double_chance_12")
            if dc_12 is not None:
                result.double_chance_12_prob = round(dc_12, 4)

            # ── NRFI / YRFI ─────────────────────────
            nrfi_p2 = _cls("nrfi")
            if nrfi_p2 is not None:
                result.nrfi_prob = round(nrfi_p2, 4)
            yrfi_p2 = _cls("yrfi")
            if yrfi_p2 is not None:
                result.yrfi_prob = round(yrfi_p2, 4)
            h_i1_p2 = _cls("home_scores_i1")
            if h_i1_p2 is not None:
                result.home_scores_i1_prob = round(h_i1_p2, 4)
            a_i1_p2 = _cls("away_scores_i1")
            if a_i1_p2 is not None:
                result.away_scores_i1_prob = round(a_i1_p2, 4)

            # ── Asian Handicap ───────────────────────
            ah_m1h2 = _cls("ah_minus1_home")
            if ah_m1h2 is not None:
                result.ah_minus1_home_prob = round(ah_m1h2, 4)
            ah_m1a2 = _cls("ah_minus1_away")
            if ah_m1a2 is not None:
                result.ah_minus1_away_prob = round(ah_m1a2, 4)
            ah_p1h2 = _cls("ah_plus1_home")
            if ah_p1h2 is not None:
                result.ah_plus1_home_prob = round(ah_p1h2, 4)
            ah_p1a2 = _cls("ah_plus1_away")
            if ah_p1a2 is not None:
                result.ah_plus1_away_prob = round(ah_p1a2, 4)

            # ── Shutout ──────────────────────────────
            so_h2 = _cls("shutout_home")
            if so_h2 is not None:
                result.shutout_home_prob = round(so_h2, 4)
            so_a2 = _cls("shutout_away")
            if so_a2 is not None:
                result.shutout_away_prob = round(so_a2, 4)

            dec_p = _cls("ufc_decision")
            ko_p = _cls("ufc_ko_tko")
            sub_p = _cls("ufc_submission")
            ef_p  = _cls("ufc_early_finish")
            r1_p  = _cls("ufc_round1_finish")
            if any(v is not None for v in (dec_p, ko_p, sub_p, ef_p, r1_p)):
                result.decision_prob = round(dec_p, 4) if dec_p is not None else None
                result.ko_tko_prob = round(ko_p, 4) if ko_p is not None else None
                result.submission_prob = round(sub_p, 4) if sub_p is not None else None
                result.early_finish_prob = round(ef_p, 4) if ef_p is not None else None
                result.round1_finish_prob = round(r1_p, 4) if r1_p is not None else None
                mp: dict[str, float] = {}
                if dec_p is not None:
                    mp["decision"] = round(dec_p, 4)
                if ko_p is not None:
                    mp["ko_tko"] = round(ko_p, 4)
                if sub_p is not None:
                    mp["submission"] = round(sub_p, 4)
                if ef_p is not None:
                    mp["early_finish"] = round(ef_p, 4)
                if r1_p is not None:
                    mp["round1_finish"] = round(r1_p, 4)
                if mp:
                    result.method_probs = mp

            ss_p = _cls("straight_sets")
            if ss_p is not None:
                result.straight_sets_prob = round(ss_p, 4)

            # Esports extra markets (CSGO, Dota2, LoL, Valorant)
            es_cs = _cls("esports_clean_sweep")
            if es_cs is not None:
                result.esports_clean_sweep_prob = round(es_cs, 4)
            es_mt = _reg("esports_map_total")
            if es_mt is not None:
                result.esports_map_total = round(es_mt, 2)
            es_o2 = _cls("esports_map_total_over2")
            if es_o2 is not None:
                result.esports_map_total_over2_prob = round(es_o2, 4)
            es_dec = _cls("esports_decider_map")
            if es_dec is not None:
                result.esports_decider_map_prob = round(es_dec, 4)
            es_hdom = _cls("esports_home_dominant")
            if es_hdom is not None:
                result.esports_home_dominant_prob = round(es_hdom, 4)
            es_adom = _cls("esports_away_dominant")
            if es_adom is not None:
                result.esports_away_dominant_prob = round(es_adom, 4)

            band_keys = [
                "margin_1", "margin_2", "margin_3", "margin_4plus",
                "margin_1_2", "margin_3_4", "margin_5_7", "margin_8plus",
                "margin_1_7", "margin_8_14", "margin_15_21", "margin_22plus",
                "margin_1_5", "margin_6_10", "margin_11_17", "margin_18plus",
            ]
            margin_probs: dict[str, float] = {}
            for bk in band_keys:
                bp = _cls(bk)
                if bp is not None:
                    margin_probs[bk.replace("margin_", "")] = round(bp, 4)
            if margin_probs:
                result.margin_band_probs = margin_probs

            dom_p = _cls("dominant_win")
            if dom_p is not None:
                result.dominant_win_prob = round(dom_p, 4)
            lm_p = _cls("large_margin_win")
            if lm_p is not None:
                result.large_margin_prob = round(lm_p, 4)

            total_band_probs: dict[str, float] = {}
            for tbk in ["total_low", "total_mid", "total_high"]:
                tp = _cls(tbk)
                if tp is not None:
                    total_band_probs[tbk.replace("total_", "")] = round(tp, 4)
            if total_band_probs:
                result.total_band_probs = total_band_probs
                # Pass stored thresholds so consumers can classify actual totals correctly
                band_thresh = extra_models.get("_total_band_thresholds")
                if band_thresh:
                    result.total_band_thresholds = band_thresh
            over_med_p = _cls("total_over_median")
            if over_med_p is not None:
                result.total_over_median_prob = round(over_med_p, 4)
                # Expose the training median so backtest can compare against it (not predicted_total)
                band_thresh2 = extra_models.get("_total_band_thresholds", {})
                med = band_thresh2.get("median")
                if med is not None:
                    result.total_median_threshold = round(float(med), 2)

            sh_p = _cls("second_half_winner")
            if sh_p is not None:
                result.second_half_home_win_prob = round(sh_p, 4)
            sh_t = _reg("second_half_total")
            if sh_t is not None:
                result.second_half_total = round(sh_t, 2)

            # ── F5 Innings (MLB) ──────────────────────────────
            for fk, attr in [
                ("f5_home_win", "f5_home_win_prob"), ("f5_away_win", "f5_away_win_prob"),
                ("f5_tie", "f5_tie_prob"), ("f5_over4_5", "f5_over4_5_prob"),
                ("f5_under4_5", "f5_under4_5_prob"),
            ]:
                fp = _cls(fk)
                if fp is not None:
                    setattr(result, attr, round(fp, 4))

            # ── Correct Score Bands (Soccer) ─────────────────
            for sk, attr in [
                ("score_nil_nil", "score_nil_nil_prob"), ("score_1_0", "score_1_0_prob"),
                ("score_0_1", "score_0_1_prob"), ("score_1_1", "score_1_1_prob"),
                ("score_2plus_0", "score_2plus_0_prob"), ("score_0_2plus", "score_0_2plus_prob"),
                ("score_2_1", "score_2_1_prob"), ("score_1_2", "score_1_2_prob"),
                ("score_3plus_total", "score_3plus_total_prob"), ("score_low_total", "score_low_total_prob"),
            ]:
                sp = _cls(sk)
                if sp is not None:
                    setattr(result, attr, round(sp, 4))

            # ── First Half O/U + Win Both Halves (Soccer) ────
            for hk, attr in [
                ("h1_over0_5", "h1_over0_5_prob"), ("h1_over1_5", "h1_over1_5_prob"),
                ("win_both_halves_home", "win_both_halves_home_prob"),
                ("win_both_halves_away", "win_both_halves_away_prob"),
            ]:
                hp = _cls(hk)
                if hp is not None:
                    setattr(result, attr, round(hp, 4))

            # ── Period BTTS (hockey/basketball) ──────────────
            for pk, attr in [
                ("btts_period1", "btts_period1_prob"),
                ("btts_period2", "btts_period2_prob"),
                ("btts_period3", "btts_period3_prob"),
            ]:
                pp = _cls(pk)
                if pp is not None:
                    setattr(result, attr, round(pp, 4))

            # ── Corners Markets (Soccer) ──────────────────────
            for ck, attr in [
                ("corners_over9_5", "corners_over9_5_prob"),
                ("corners_over10_5", "corners_over10_5_prob"),
                ("corners_over11_5", "corners_over11_5_prob"),
            ]:
                cp = _cls(ck)
                if cp is not None:
                    setattr(result, attr, round(cp, 4))

            # ── Cards Markets (Soccer) ────────────────────────
            for kk, attr in [
                ("cards_over3_5", "cards_over3_5_prob"),
                ("cards_over4_5", "cards_over4_5_prob"),
                ("cards_over5_5", "cards_over5_5_prob"),
            ]:
                kp = _cls(kk)
                if kp is not None:
                    setattr(result, attr, round(kp, 4))

            # ── Soccer H2 Goals Markets ───────────────────────
            for mk, attr in [
                ("soccer_h2_over0_5", "soccer_h2_over0_5_prob"),
                ("soccer_h2_over1_5", "soccer_h2_over1_5_prob"),
                ("soccer_h2_over2_5", "soccer_h2_over2_5_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))

            # ── Three-Pointer Markets (NBA/WNBA/NCAAB) ───────
            for mk, attr in [
                ("threes_over_low", "threes_over_low_prob"),
                ("threes_over_mid", "threes_over_mid_prob"),
                ("threes_over_high", "threes_over_high_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))
            # Dynamic lines stored alongside models
            for lk, attr in [
                ("threes_over_low_line", "threes_low_line"),
                ("threes_over_mid_line", "threes_mid_line"),
                ("threes_over_high_line", "threes_high_line"),
            ]:
                if lk in extra_models:
                    setattr(result, attr, extra_models[lk])

            # ── NHL Shots Markets (legacy fixed + dynamic) ────
            for mk, attr in [
                ("shots_over55_5", "shots_over55_5_prob"),
                ("shots_over60_5", "shots_over60_5_prob"),
                ("shots_over65_5", "shots_over65_5_prob"),
                ("shots_over_low", "shots_over_low_prob"),
                ("shots_over_mid", "shots_over_mid_prob"),
                ("shots_over_high", "shots_over_high_prob"),
                ("home_shots_advantage", "home_shots_advantage_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))
            for mk, attr in [
                ("shots_over_low_line", "shots_over_low_line"),
                ("shots_over_mid_line", "shots_over_mid_line"),
                ("shots_over_high_line", "shots_over_high_line"),
            ]:
                v = extra_models.get(mk)
                if v is not None:
                    setattr(result, attr, float(v))

            # ── MLB Hits Markets ──────────────────────────────
            for mk, attr in [
                ("hits_over14_5", "hits_over14_5_prob"),
                ("hits_over16_5", "hits_over16_5_prob"),
                ("hits_over18_5", "hits_over18_5_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))

            # ── NBA/WNBA Rebounds Markets ─────────────────────
            for mk, attr in [
                ("rebounds_over_low", "rebounds_over_low_prob"),
                ("rebounds_over_mid", "rebounds_over_mid_prob"),
                ("rebounds_over_high", "rebounds_over_high_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))
            for mk, attr in [
                ("rebounds_over_low_line", "rebounds_low_line"),
                ("rebounds_over_mid_line", "rebounds_mid_line"),
                ("rebounds_over_high_line", "rebounds_high_line"),
            ]:
                v = extra_models.get(mk)
                if v is not None:
                    setattr(result, attr, float(v))

            # ── NBA/WNBA Turnovers Markets ────────────────────
            for mk, attr in [
                ("turnovers_over_low", "turnovers_over_low_prob"),
                ("turnovers_over_mid", "turnovers_over_mid_prob"),
                ("turnovers_over_high", "turnovers_over_high_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))
            for mk, attr in [
                ("turnovers_over_low_line", "turnovers_low_line"),
                ("turnovers_over_mid_line", "turnovers_mid_line"),
                ("turnovers_over_high_line", "turnovers_high_line"),
            ]:
                v = extra_models.get(mk)
                if v is not None:
                    setattr(result, attr, float(v))

            # ── NBA/WNBA Assists Markets ──────────────────────
            for mk, attr in [
                ("assists_over_low", "assists_over_low_prob"),
                ("assists_over_mid", "assists_over_mid_prob"),
                ("assists_over_high", "assists_over_high_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))
            for mk, attr in [
                ("assists_over_low_line", "assists_low_line"),
                ("assists_over_mid_line", "assists_mid_line"),
                ("assists_over_high_line", "assists_high_line"),
            ]:
                v = extra_models.get(mk)
                if v is not None:
                    setattr(result, attr, float(v))

            # ── NHL Period Goals Markets ──────────────────────
            for period in ("p1", "p2", "p3"):
                for line_str in ("0.5", "1.5", "2.5"):
                    mk = f"nhl_{period}_goals_over_{line_str}"
                    attr = f"nhl_{period}_goals_over_{line_str.replace('.', '_')}_prob"
                    p = _cls(mk)
                    if p is not None:
                        setattr(result, attr, round(p, 4))

            # ── Soccer Total Shots Markets ────────────────────
            for mk, attr in [
                ("shots_total_over_low", "shots_total_over_low_prob"),
                ("shots_total_over_mid", "shots_total_over_mid_prob"),
                ("shots_total_over_high", "shots_total_over_high_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))
            for mk, attr in [
                ("shots_total_over_low_line", "shots_total_low_line"),
                ("shots_total_over_mid_line", "shots_total_mid_line"),
                ("shots_total_over_high_line", "shots_total_high_line"),
            ]:
                v = extra_models.get(mk)
                if v is not None:
                    setattr(result, attr, float(v))

            # ── UFC Total Rounds Markets ──────────────────────
            for line_str in ("1.5", "2.5", "3.5"):
                mk = f"ufc_rounds_over_{line_str}"
                attr = f"ufc_rounds_over_{line_str.replace('.', '_')}_prob"
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))

            # ── NFL First Quarter Total Markets ───────────────
            for mk, attr in [
                ("q1_total_over_low", "q1_total_over_low_prob"),
                ("q1_total_over_mid", "q1_total_over_mid_prob"),
                ("q1_total_over_high", "q1_total_over_high_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))
            for mk, attr in [
                ("q1_total_over_low_line", "q1_total_low_line"),
                ("q1_total_over_mid_line", "q1_total_mid_line"),
                ("q1_total_over_high_line", "q1_total_high_line"),
            ]:
                v = extra_models.get(mk)
                if v is not None:
                    setattr(result, attr, float(v))

            # ── Q3/Q4 Period Total Markets ────────────────────
            for period in ("q3", "q4"):
                for tier in ("low", "mid", "high"):
                    mk = f"{period}_over_{tier}"
                    p = _cls(mk)
                    if p is not None:
                        setattr(result, f"{period}_over_{tier}_prob", round(p, 4))
                    line_val = extra_models.get(f"{mk}_line")
                    if line_val is not None:
                        setattr(result, f"{period}_over_{tier}_line", float(line_val))

            # ── Q1 Winner (Basketball) ────────────────────────
            q1w = _cls("q1_winner")
            if q1w is not None:
                result.q1_home_win_prob = round(q1w, 4)

            # ── NHL Period Goals Markets ──────────────────────
            for period in ("p1", "p2", "p3"):
                for tier in ("low", "mid", "high"):
                    mk = f"nhl_{period}_goals_over_{tier}"
                    p = _cls(mk)
                    if p is not None:
                        setattr(result, f"nhl_{period}_goals_over_{tier}_prob", round(p, 4))

            # ── Soccer Total Shots Markets ────────────────────
            for mk, attr in [
                ("shots_total_over_low", "shots_total_over_low_prob"),
                ("shots_total_over_mid", "shots_total_over_mid_prob"),
                ("shots_total_over_high", "shots_total_over_high_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))
            for mk, attr in [
                ("shots_total_over_low_line", "shots_total_low_line"),
                ("shots_total_over_mid_line", "shots_total_mid_line"),
                ("shots_total_over_high_line", "shots_total_high_line"),
            ]:
                v = extra_models.get(mk)
                if v is not None:
                    setattr(result, attr, float(v))


            for inn in range(1, 10):
                p = _cls(f"nrfi_i{inn}")
                if p is not None:
                    setattr(result, f"nrfi_i{inn}_prob", round(p, 4))
            for mk, attr in [
                ("f7_home_win", "f7_home_win_prob"),
                ("f7_over", "f7_over_prob"),
                ("late_inning_over", "late_inning_over_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))
            for mk, attr in [
                ("f7_over_line", "f7_over_line"),
                ("late_inning_line", "late_inning_line"),
            ]:
                v = extra_models.get(mk)
                if v is not None:
                    setattr(result, attr, float(v))

            # ── Close Game / Blowout Markets ──────────────────
            for mk, attr in [
                ("close_game", "close_game_prob"),
                ("blowout_win", "blowout_win_prob"),
                ("one_score_game", "one_score_game_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))
            cg_thresh = extra_models.get("close_game_thresh")
            if cg_thresh is not None:
                result.close_game_thresh = float(cg_thresh)

            # ── NFL Second-Half Total Markets ─────────────────
            for mk, attr in [
                ("nfl_2h_over_low", "nfl_2h_over_low_prob"),
                ("nfl_2h_over_mid", "nfl_2h_over_mid_prob"),
                ("nfl_2h_over_high", "nfl_2h_over_high_prob"),
                ("nfl_scoring_surge", "nfl_scoring_surge_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))
            for mk, attr in [
                ("nfl_2h_over_low_line", "nfl_2h_over_low_line"),
                ("nfl_2h_over_mid_line", "nfl_2h_over_mid_line"),
                ("nfl_2h_over_high_line", "nfl_2h_over_high_line"),
            ]:
                v = extra_models.get(mk)
                if v is not None:
                    setattr(result, attr, float(v))

            # ── High / Low Scoring Markets ────────────────────
            for mk, attr in [
                ("high_scoring", "high_scoring_prob"),
                ("low_scoring", "low_scoring_prob"),
                ("both_score_high", "both_score_high_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))
            hs_thresh = extra_models.get("high_scoring_thresh")
            if hs_thresh is not None:
                result.high_scoring_line = float(hs_thresh)
            ls_thresh = extra_models.get("low_scoring_thresh")
            if ls_thresh is not None:
                result.low_scoring_line = float(ls_thresh)

            # ── BTTS + Win Combo Markets ──────────────────────
            for mk, attr in [
                ("btts_home_win", "btts_home_win_prob"),
                ("btts_away_win", "btts_away_win_prob"),
                ("btts_draw", "btts_draw_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))

            # ── eSports Series Markets ────────────────────────
            for mk, attr in [
                ("home_2_0_win", "home_2_0_win_prob"),
                ("away_2_0_win", "away_2_0_win_prob"),
                ("esports_decider_map", "esports_decider_map_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))

            # ── Motorsport Extra Markets ──────────────────────
            for mk, attr in [
                ("motor_podium", "motor_podium_prob"),
                ("motor_points", "motor_points_prob"),
                ("motor_dnf", "motor_dnf_prob"),
                ("motor_fastest_lap", "motor_fastest_lap_prob"),
            ]:
                p = _cls(mk)
                if p is not None:
                    setattr(result, attr, round(p, 4))

            results.append(result)

        return results

    # ── Single game prediction ───────────────────────────

    def predict_game(self, game: dict[str, Any]) -> PredictionResult:
        """Generate a prediction for a single game.

        Parameters
        ----------
        game : dict
            Must contain at least ``game_id``, ``home_team_id``,
            ``away_team_id``, and ``date``.
        """
        if self._bundle is None:
            raise RuntimeError(
                f"No trained models for {self.sport} — run training first"
            )

        X = self._game_to_features(game)
        if X is None or X.empty:
            raise ValueError(f"Could not extract features for game {game.get('game_id')}")

        game_id = str(game.get("game_id", game.get("id", "unknown")))
        home_team = str(game.get("home_team", game.get("home_team_id", "")))
        away_team = str(game.get("away_team", game.get("away_team_id", "")))

        if self._mode == "joint":
            return self._predict_joint(X, game_id, home_team, away_team)
        return self._predict_separate(X, game_id, home_team, away_team)

    def _predict_joint(
        self,
        X: pd.DataFrame,
        game_id: str,
        home_team: str,
        away_team: str,
    ) -> PredictionResult:
        """Predict using joint home/away score models."""
        winner_ens: EnsembleVoter = self._bundle["winner_ensemble"]
        home_ens: EnsembleVoter = self._bundle["home_ensemble"]
        away_ens: EnsembleVoter = self._bundle["away_ensemble"]

        probs, preds, vote_details = winner_ens.predict_class(X)
        home_score_pred = float(home_ens.predict_regression(X)[0])
        away_score_pred = float(away_ens.predict_regression(X)[0])

        vd = vote_details.get("0", {})
        home_win_prob = float(probs[0])

        result = PredictionResult(
            game_id=game_id,
            sport=self.sport,
            home_team=home_team,
            away_team=away_team,
            home_win_prob=home_win_prob,
            away_win_prob=round(1.0 - home_win_prob, 4),
            predicted_home_score=round(home_score_pred, 1),
            predicted_away_score=round(away_score_pred, 1),
            predicted_total=round(home_score_pred + away_score_pred, 1),
            predicted_spread=round(home_score_pred - away_score_pred, 1),
            confidence=vd.get("confidence", 0.50),
            n_models=vd.get("n_models", 0),
            consensus=vd.get("consensus", 0.0),
            model_votes=vd.get("votes", {}),
        )

        # Enrich with extra-market predictions
        self._predict_extra_markets(X, result)
        return result

    def _predict_separate(
        self,
        X: pd.DataFrame,
        game_id: str,
        home_team: str,
        away_team: str,
    ) -> PredictionResult:
        """Predict using separate winner / total / spread models."""
        winner_ens: EnsembleVoter = self._bundle["winner_ensemble"]
        total_ens: EnsembleVoter = self._bundle["total_ensemble"]
        spread_ens: EnsembleVoter = self._bundle["spread_ensemble"]

        probs, preds, vote_details = winner_ens.predict_class(X)
        total_pred = float(total_ens.predict_regression(X)[0])
        spread_pred = float(spread_ens.predict_regression(X)[0])

        vd = vote_details.get("0", {})
        home_win_prob = float(probs[0])

        # Derive approximate scores from total and spread
        home_score_est = (total_pred + spread_pred) / 2
        away_score_est = (total_pred - spread_pred) / 2

        result = PredictionResult(
            game_id=game_id,
            sport=self.sport,
            home_team=home_team,
            away_team=away_team,
            home_win_prob=home_win_prob,
            away_win_prob=round(1.0 - home_win_prob, 4),
            predicted_home_score=round(home_score_est, 1),
            predicted_away_score=round(away_score_est, 1),
            predicted_total=round(total_pred, 1),
            predicted_spread=round(spread_pred, 1),
            confidence=vd.get("confidence", 0.50),
            n_models=vd.get("n_models", 0),
            consensus=vd.get("consensus", 0.0),
            model_votes=vd.get("votes", {}),
        )

        self._predict_extra_markets(X, result)
        return result

    # ── Extra-market predictions ─────────────────────────

    def _predict_extra_markets(self, X: "pd.DataFrame", result: "PredictionResult") -> None:
        """Populate extra-market fields on *result* using extra_models.pkl."""
        self._ensure_extra_loaded()
        if not self._extra:
            return

        extra_models: dict[str, "EnsembleVoter"] = self._extra.get("models", {})
        sport_meta: dict[str, Any] = self._extra.get("sport_meta", {})

        # Align X to extra model feature names
        extra_feature_names = self._extra.get("feature_names", [])
        X_extra = X.copy()
        for col in extra_feature_names:
            if col not in X_extra.columns:
                X_extra[col] = 0.0
        if extra_feature_names:
            X_extra = X_extra[[c for c in extra_feature_names if c in X_extra.columns]]

        # Pre-scale once per first available model — all extra models share the
        # same feature space so we pass the pre-scaled array to avoid 25 redundant
        # scaler.transform() calls.
        _X_scaled_cache: dict[str, Any] = {}

        def _get_scaled(m: "EnsembleVoter") -> "np.ndarray":
            key = id(m.scaler)
            if key not in _X_scaled_cache:
                _X_scaled_cache[key] = m.scaler.transform(X_extra)
            return _X_scaled_cache[key]

        def _cls_prob(model_key: str) -> "float | None":
            m = extra_models.get(model_key)
            if m is None:
                return None
            try:
                return m.predict_proba_fast(_get_scaled(m))
            except Exception:
                return None

        def _reg_val(model_key: str) -> "float | None":
            m = extra_models.get(model_key)
            if m is None:
                return None
            try:
                return m.predict_regression_fast(_get_scaled(m))
            except Exception:
                return None

        # ── Draw / OT ────────────────────────────────────
        draw_p = _cls_prob("draw")
        if draw_p is not None:
            result.draw_prob = round(draw_p, 4)
            # Re-normalise three-way (home / draw / away)
            hw = result.home_win_prob * (1 - draw_p)
            aw = result.away_win_prob * (1 - draw_p)
            total = hw + draw_p + aw
            if total > 0:
                result.three_way = {
                    "home": round(hw / total, 4),
                    "draw": round(draw_p / total, 4),
                    "away": round(aw / total, 4),
                }

        ot_p = _cls_prob("overtime")
        ot_p2 = _cls_prob("overtime_prob")
        if ot_p is not None:
            result.ot_prob = round(ot_p, 4)
        elif ot_p2 is not None:
            result.ot_prob = round(ot_p2, 4)
        elif draw_p is not None:
            # For sports without explicit OT data, OT ≈ draw prob
            result.ot_prob = result.draw_prob

        # ── Q1 Winner + Q1 Total (Basketball) ─────────────────
        q1_win_p2 = _cls_prob("q1_winner")
        if q1_win_p2 is not None:
            result.q1_home_win_prob = round(q1_win_p2, 4)

        # ── Halftime ─────────────────────────────────────
        ht_home = _reg_val("halftime_home_score")
        ht_away = _reg_val("halftime_away_score")
        if ht_home is not None and ht_away is not None:
            result.halftime_home_score = round(ht_home, 1)
            result.halftime_away_score = round(ht_away, 1)
            result.halftime_spread = round(ht_home - ht_away, 1)
            result.halftime_total = round(ht_home + ht_away, 1)

        ht_win_p = _cls_prob("halftime_winner")
        if ht_win_p is not None:
            result.halftime_home_win_prob = round(ht_win_p, 4)
            result.halftime_away_win_prob = round(1.0 - ht_win_p, 4)
            result.halftime_draw_prob = _cls_prob("halftime_draw")

        # ── Halftime spread / total (if trained separately) ──
        ht_spread = _reg_val("halftime_spread")
        if ht_spread is not None and result.halftime_spread is None:
            result.halftime_spread = round(ht_spread, 1)
        ht_total = _reg_val("halftime_total")
        if ht_total is not None and result.halftime_total is None:
            result.halftime_total = round(ht_total, 1)

        # ── Per-period ────────────────────────────────────
        period_label = sport_meta.get("period_label", "period")
        available_periods = sport_meta.get("available_periods", [])
        result.period_label = period_label
        if available_periods:
            preds: list[dict] = []
            for i in available_periods:
                ph = _reg_val(f"period_{i}_home")
                pa = _reg_val(f"period_{i}_away")
                pt = _reg_val(f"period_{i}_total")
                pw_p = _cls_prob(f"period_{i}_winner")
                entry: dict[str, Any] = {"period": i}
                if ph is not None:
                    entry["home"] = round(ph, 2)
                if pa is not None:
                    entry["away"] = round(pa, 2)
                if ph is not None and pa is not None:
                    entry["spread"] = round(ph - pa, 2)
                if pt is not None:
                    entry["total"] = round(pt, 2)
                elif ph is not None and pa is not None:
                    entry["total"] = round(ph + pa, 2)
                if pw_p is not None:
                    entry["home_win_prob"] = round(pw_p, 4)
                    entry["away_win_prob"] = round(1.0 - pw_p, 4)
                if len(entry) > 1:
                    preds.append(entry)
            if preds:
                result.period_predictions = preds

        # ── First / last score ────────────────────────────
        fs_p = _cls_prob("first_score")
        if fs_p is not None:
            result.first_score_home_prob = round(fs_p, 4)
            result.first_score_team = "home" if fs_p >= 0.5 else "away"
        ls_p = _cls_prob("last_score")
        if ls_p is not None:
            result.last_score_home_prob = round(ls_p, 4)
            result.last_score_team = "home" if ls_p >= 0.5 else "away"

        # ── BTTS / Clean Sheet ────────────────────────────
        btts_p = _cls_prob("btts")
        if btts_p is not None:
            result.btts_prob = round(btts_p, 4)

        btts_ov = _cls_prob("btts_over2_5")
        if btts_ov is not None:
            result.btts_over2_5_prob = round(btts_ov, 4)
        for fk2, attr2 in [
            ("home_over1_5", "home_over1_5_prob"), ("away_over1_5", "away_over1_5_prob"),
            ("home_win_score2plus", "home_win_score2plus_prob"),
            ("away_win_score2plus", "away_win_score2plus_prob"),
        ]:
            fv = _cls_prob(fk2)
            if fv is not None:
                setattr(result, attr2, round(fv, 4))

        cs_h_p = _cls_prob("clean_sheet_home")
        if cs_h_p is not None:
            result.home_clean_sheet_prob = round(cs_h_p, 4)

        cs_a_p = _cls_prob("clean_sheet_away")
        if cs_a_p is not None:
            result.away_clean_sheet_prob = round(cs_a_p, 4)

        # ── UFC Method of Victory ─────────────────────────
        dec_p = _cls_prob("ufc_decision")
        ko_p  = _cls_prob("ufc_ko_tko")
        sub_p = _cls_prob("ufc_submission")
        ef_p  = _cls_prob("ufc_early_finish")
        r1_p  = _cls_prob("ufc_round1_finish")
        if dec_p is not None or ko_p is not None or sub_p is not None or ef_p is not None or r1_p is not None:
            result.decision_prob = round(dec_p, 4) if dec_p is not None else None
            result.ko_tko_prob   = round(ko_p,  4) if ko_p  is not None else None
            result.submission_prob = round(sub_p, 4) if sub_p is not None else None
            result.early_finish_prob = round(ef_p, 4) if ef_p is not None else None
            result.round1_finish_prob = round(r1_p, 4) if r1_p is not None else None
            # Rebuild method_probs dict for convenience
            mp: dict[str, float] = {}
            if dec_p is not None: mp["decision"] = round(dec_p, 4)
            if ko_p  is not None: mp["ko_tko"]   = round(ko_p,  4)
            if sub_p is not None: mp["submission"] = round(sub_p, 4)
            if ef_p  is not None: mp["early_finish"] = round(ef_p, 4)
            if r1_p  is not None: mp["round1_finish"] = round(r1_p, 4)
            if mp:
                result.method_probs = mp

        # ── Tennis Straight Sets ──────────────────────────
        ss_p = _cls_prob("straight_sets")
        if ss_p is not None:
            result.straight_sets_prob = round(ss_p, 4)

        # ── Winning Margin Bands ──────────────────────────
        band_keys = [
            "margin_1", "margin_2", "margin_3", "margin_4plus",
            "margin_1_2", "margin_3_4", "margin_5_7", "margin_8plus",
            "margin_1_7", "margin_8_14", "margin_15_21", "margin_22plus",
            "margin_1_5", "margin_6_10", "margin_11_17", "margin_18plus",
        ]
        margin_probs: dict[str, float] = {}
        for bk in band_keys:
            bp = _cls_prob(bk)
            if bp is not None:
                # strip "margin_" prefix for display
                margin_probs[bk.replace("margin_", "")] = round(bp, 4)
        if margin_probs:
            result.margin_band_probs = margin_probs

        dom_p = _cls_prob("dominant_win")
        if dom_p is not None:
            result.dominant_win_prob = round(dom_p, 4)
        lm_p = _cls_prob("large_margin_win")
        if lm_p is not None:
            result.large_margin_prob = round(lm_p, 4)

        # ── Total Score Bands ─────────────────────────────
        total_band_probs: dict[str, float] = {}
        for tbk in ["total_low", "total_mid", "total_high"]:
            tp = _cls_prob(tbk)
            if tp is not None:
                total_band_probs[tbk.replace("total_", "")] = round(tp, 4)
        if total_band_probs:
            result.total_band_probs = total_band_probs
            band_thresh = extra_models.get("_total_band_thresholds")
            if band_thresh:
                result.total_band_thresholds = band_thresh
        over_med_p = _cls_prob("total_over_median")
        if over_med_p is not None:
            result.total_over_median_prob = round(over_med_p, 4)
            band_thresh2 = extra_models.get("_total_band_thresholds", {})
            med = band_thresh2.get("median")
            if med is not None:
                result.total_median_threshold = round(float(med), 2)

        # ── Second Half ───────────────────────────────────
        sh_p = _cls_prob("second_half_winner")
        if sh_p is not None:
            result.second_half_home_win_prob = round(sh_p, 4)
        if "second_half_total" in extra_models and X_extra is not None:
            try:
                sh_t = float(extra_models["second_half_total"].predict_regression_fast(
                    extra_models["second_half_total"].scaler.transform(X_extra)
                ))
                result.second_half_total = round(sh_t, 2)
            except Exception:
                pass

        # ── Regulation Result (pre-OT) ───────────────────
        reg_w = _cls_prob("regulation_winner")
        reg_d = _cls_prob("regulation_draw")
        if reg_w is not None:
            result.regulation_home_win_prob = round(reg_w, 4)
        if reg_d is not None:
            result.regulation_draw_prob = round(reg_d, 4)
            result.regulation_away_win_prob = round(max(0.0, 1.0 - (reg_w or 0.5) - reg_d), 4)

        # ── Team Totals ───────────────────────────────────
        ht_over = _cls_prob("home_team_total_over")
        at_over = _cls_prob("away_team_total_over")
        if ht_over is not None:
            result.home_team_total_over_prob = round(ht_over, 4)
        if at_over is not None:
            result.away_team_total_over_prob = round(at_over, 4)

        # ── Comeback ─────────────────────────────────────
        cb_h = _cls_prob("comeback_home")
        cb_a = _cls_prob("comeback_away")
        if cb_h is not None:
            result.comeback_home_prob = round(cb_h, 4)
        if cb_a is not None:
            result.comeback_away_prob = round(cb_a, 4)

        # ── Double Chance (model-backed) ─────────────────
        dc_1x = _cls_prob("double_chance_1X")
        if dc_1x is not None:
            result.double_chance_1X_prob = round(dc_1x, 4)
        dc_x2 = _cls_prob("double_chance_X2")
        if dc_x2 is not None:
            result.double_chance_X2_prob = round(dc_x2, 4)
        dc_12 = _cls_prob("double_chance_12")
        if dc_12 is not None:
            result.double_chance_12_prob = round(dc_12, 4)

        # ── NRFI / YRFI (MLB) ────────────────────────────
        nrfi_p = _cls_prob("nrfi")
        if nrfi_p is not None:
            result.nrfi_prob = round(nrfi_p, 4)
        yrfi_p = _cls_prob("yrfi")
        if yrfi_p is not None:
            result.yrfi_prob = round(yrfi_p, 4)
        h_i1_p = _cls_prob("home_scores_i1")
        if h_i1_p is not None:
            result.home_scores_i1_prob = round(h_i1_p, 4)
        a_i1_p = _cls_prob("away_scores_i1")
        if a_i1_p is not None:
            result.away_scores_i1_prob = round(a_i1_p, 4)

        # ── Asian Handicap ────────────────────────────────
        ah_m1h = _cls_prob("ah_minus1_home")
        if ah_m1h is not None:
            result.ah_minus1_home_prob = round(ah_m1h, 4)
        ah_m1a = _cls_prob("ah_minus1_away")
        if ah_m1a is not None:
            result.ah_minus1_away_prob = round(ah_m1a, 4)
        ah_p1h = _cls_prob("ah_plus1_home")
        if ah_p1h is not None:
            result.ah_plus1_home_prob = round(ah_p1h, 4)
        ah_p1a = _cls_prob("ah_plus1_away")
        if ah_p1a is not None:
            result.ah_plus1_away_prob = round(ah_p1a, 4)

        # ── Shutout / NHL clean sheet ─────────────────────
        so_h = _cls_prob("shutout_home")
        if so_h is not None:
            result.shutout_home_prob = round(so_h, 4)
        so_a = _cls_prob("shutout_away")
        if so_a is not None:
            result.shutout_away_prob = round(so_a, 4)

        # ── Esports extra markets ─────────────────────────
        # Available for CSGO, Dota2, LoL, Valorant
        cs_p = _cls_prob("esports_clean_sweep")
        if cs_p is not None:
            result.esports_clean_sweep_prob = round(cs_p, 4)
        mt = _reg_val("esports_map_total")
        if mt is not None:
            result.esports_map_total = round(mt, 2)
        mt_o2 = _cls_prob("esports_map_total_over2")
        if mt_o2 is not None:
            result.esports_map_total_over2_prob = round(mt_o2, 4)
        dec_p = _cls_prob("esports_decider_map")
        if dec_p is not None:
            result.esports_decider_map_prob = round(dec_p, 4)
        hdom_p = _cls_prob("esports_home_dominant")
        if hdom_p is not None:
            result.esports_home_dominant_prob = round(hdom_p, 4)
        adom_p = _cls_prob("esports_away_dominant")
        if adom_p is not None:
            result.esports_away_dominant_prob = round(adom_p, 4)

        # ── F5 Innings (MLB) ──────────────────────────────
        for fk, attr in [
            ("f5_home_win", "f5_home_win_prob"), ("f5_away_win", "f5_away_win_prob"),
            ("f5_tie", "f5_tie_prob"), ("f5_over4_5", "f5_over4_5_prob"),
            ("f5_under4_5", "f5_under4_5_prob"),
        ]:
            fp = _cls_prob(fk)
            if fp is not None:
                setattr(result, attr, round(fp, 4))

        # ── Correct Score Bands (Soccer) ─────────────────
        for sk, attr in [
            ("score_nil_nil", "score_nil_nil_prob"), ("score_1_0", "score_1_0_prob"),
            ("score_0_1", "score_0_1_prob"), ("score_1_1", "score_1_1_prob"),
            ("score_2plus_0", "score_2plus_0_prob"), ("score_0_2plus", "score_0_2plus_prob"),
            ("score_2_1", "score_2_1_prob"), ("score_1_2", "score_1_2_prob"),
            ("score_3plus_total", "score_3plus_total_prob"), ("score_low_total", "score_low_total_prob"),
        ]:
            sp = _cls_prob(sk)
            if sp is not None:
                setattr(result, attr, round(sp, 4))

        # ── First Half O/U + Win Both Halves (Soccer) ────
        for hk, attr in [
            ("h1_over0_5", "h1_over0_5_prob"), ("h1_over1_5", "h1_over1_5_prob"),
            ("win_both_halves_home", "win_both_halves_home_prob"),
            ("win_both_halves_away", "win_both_halves_away_prob"),
        ]:
            hp = _cls_prob(hk)
            if hp is not None:
                setattr(result, attr, round(hp, 4))

        # ── Period BTTS (hockey/basketball) ──────────────
        for pk, attr in [
            ("btts_period1", "btts_period1_prob"),
            ("btts_period2", "btts_period2_prob"),
            ("btts_period3", "btts_period3_prob"),
        ]:
            pp = _cls_prob(pk)
            if pp is not None:
                setattr(result, attr, round(pp, 4))

        # ── Corners Markets (Soccer) ──────────────────────
        for ck, attr in [
            ("corners_over9_5", "corners_over9_5_prob"),
            ("corners_over10_5", "corners_over10_5_prob"),
            ("corners_over11_5", "corners_over11_5_prob"),
        ]:
            cp = _cls_prob(ck)
            if cp is not None:
                setattr(result, attr, round(cp, 4))

        # ── Cards Markets (Soccer) ────────────────────────
        for kk, attr in [
            ("cards_over3_5", "cards_over3_5_prob"),
            ("cards_over4_5", "cards_over4_5_prob"),
            ("cards_over5_5", "cards_over5_5_prob"),
        ]:
            kp = _cls_prob(kk)
            if kp is not None:
                setattr(result, attr, round(kp, 4))

        # ── Soccer H2 Goals Markets ───────────────────────
        for mk, attr in [
            ("soccer_h2_over0_5", "soccer_h2_over0_5_prob"),
            ("soccer_h2_over1_5", "soccer_h2_over1_5_prob"),
            ("soccer_h2_over2_5", "soccer_h2_over2_5_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))

        # ── Three-Pointer Markets (NBA/WNBA/NCAAB) ───────
        for mk, attr in [
            ("threes_over_low", "threes_over_low_prob"),
            ("threes_over_mid", "threes_over_mid_prob"),
            ("threes_over_high", "threes_over_high_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))
        for lk, attr in [
            ("threes_over_low_line", "threes_low_line"),
            ("threes_over_mid_line", "threes_mid_line"),
            ("threes_over_high_line", "threes_high_line"),
        ]:
            if lk in extra_models:
                setattr(result, attr, extra_models[lk])

        # ── NHL Shots Markets (legacy fixed + dynamic) ────
        for mk, attr in [
            ("shots_over55_5", "shots_over55_5_prob"),
            ("shots_over60_5", "shots_over60_5_prob"),
            ("shots_over65_5", "shots_over65_5_prob"),
            ("shots_over_low", "shots_over_low_prob"),
            ("shots_over_mid", "shots_over_mid_prob"),
            ("shots_over_high", "shots_over_high_prob"),
            ("home_shots_advantage", "home_shots_advantage_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))
        for mk, attr in [
            ("shots_over_low_line", "shots_over_low_line"),
            ("shots_over_mid_line", "shots_over_mid_line"),
            ("shots_over_high_line", "shots_over_high_line"),
        ]:
            v = extra_models.get(mk)
            if v is not None:
                setattr(result, attr, float(v))

        # ── MLB Hits Markets ──────────────────────────────
        for mk, attr in [
            ("hits_over14_5", "hits_over14_5_prob"),
            ("hits_over16_5", "hits_over16_5_prob"),
            ("hits_over18_5", "hits_over18_5_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))

        # ── NBA/WNBA Rebounds Markets ─────────────────────
        for mk, attr in [
            ("rebounds_over_low", "rebounds_over_low_prob"),
            ("rebounds_over_mid", "rebounds_over_mid_prob"),
            ("rebounds_over_high", "rebounds_over_high_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))
        for lk, attr in [
            ("rebounds_over_low_line", "rebounds_low_line"),
            ("rebounds_over_mid_line", "rebounds_mid_line"),
            ("rebounds_over_high_line", "rebounds_high_line"),
        ]:
            if lk in extra_models:
                setattr(result, attr, extra_models[lk])

        # ── NBA/WNBA Turnovers Markets ────────────────────
        for mk, attr in [
            ("turnovers_over_low", "turnovers_over_low_prob"),
            ("turnovers_over_mid", "turnovers_over_mid_prob"),
            ("turnovers_over_high", "turnovers_over_high_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))
        for lk, attr in [
            ("turnovers_over_low_line", "turnovers_low_line"),
            ("turnovers_over_mid_line", "turnovers_mid_line"),
            ("turnovers_over_high_line", "turnovers_high_line"),
        ]:
            if lk in extra_models:
                setattr(result, attr, extra_models[lk])

        # ── NBA/WNBA Assists Markets ──────────────────────
        for mk, attr in [
            ("assists_over_low", "assists_over_low_prob"),
            ("assists_over_mid", "assists_over_mid_prob"),
            ("assists_over_high", "assists_over_high_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))
        for lk, attr in [
            ("assists_over_low_line", "assists_low_line"),
            ("assists_over_mid_line", "assists_mid_line"),
            ("assists_over_high_line", "assists_high_line"),
        ]:
            if lk in extra_models:
                setattr(result, attr, extra_models[lk])

        # ── NHL Period Goals Markets ──────────────────────
        for period in ("p1", "p2", "p3"):
            for tier in ("low", "mid", "high"):
                mk = f"nhl_{period}_goals_over_{tier}"
                p = _cls_prob(mk)
                if p is not None:
                    setattr(result, f"nhl_{period}_goals_over_{tier}_prob", round(p, 4))

        # ── Soccer Total Shots Markets ────────────────────
        for mk, attr in [
            ("shots_total_over_low", "shots_total_over_low_prob"),
            ("shots_total_over_mid", "shots_total_over_mid_prob"),
            ("shots_total_over_high", "shots_total_over_high_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))
        for lk, attr in [
            ("shots_total_over_low_line", "shots_total_low_line"),
            ("shots_total_over_mid_line", "shots_total_mid_line"),
            ("shots_total_over_high_line", "shots_total_high_line"),
        ]:
            if lk in extra_models:
                setattr(result, attr, float(extra_models[lk]))

        # ── UFC Total Rounds Markets ──────────────────────
        for line_str in ("1.5", "2.5", "3.5"):
            mk = f"ufc_rounds_over_{line_str}"
            attr = f"ufc_rounds_over_{line_str.replace('.', '_')}_prob"
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))

        # ── NFL First Quarter Total Markets ───────────────
        for mk, attr in [
            ("q1_total_over_low", "q1_total_over_low_prob"),
            ("q1_total_over_mid", "q1_total_over_mid_prob"),
            ("q1_total_over_high", "q1_total_over_high_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))
        for lk, attr in [
            ("q1_total_over_low_line", "q1_total_low_line"),
            ("q1_total_over_mid_line", "q1_total_mid_line"),
            ("q1_total_over_high_line", "q1_total_high_line"),
        ]:
            if lk in extra_models:
                setattr(result, attr, float(extra_models[lk]))

        # ── Q3/Q4 Period Total Markets ────────────────────
        for period in ("q3", "q4"):
            for tier in ("low", "mid", "high"):
                mk = f"{period}_over_{tier}"
                p = _cls_prob(mk)
                if p is not None:
                    setattr(result, f"{period}_over_{tier}_prob", round(p, 4))
                line_val = extra_models.get(f"{mk}_line")
                if line_val is not None:
                    setattr(result, f"{period}_over_{tier}_line", float(line_val))

        # ── Q1 Winner (Basketball) ────────────────────────
        q1w_sp = _cls_prob("q1_winner")
        if q1w_sp is not None:
            result.q1_home_win_prob = round(q1w_sp, 4)
        for inn in range(1, 10):
            p = _cls_prob(f"nrfi_i{inn}")
            if p is not None:
                setattr(result, f"nrfi_i{inn}_prob", round(p, 4))
        for mk, attr in [
            ("f7_home_win", "f7_home_win_prob"),
            ("f7_over", "f7_over_prob"),
            ("late_inning_over", "late_inning_over_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))
        for mk, attr in [
            ("f7_over_line", "f7_over_line"),
            ("late_inning_line", "late_inning_line"),
        ]:
            v = extra_models.get(mk)
            if v is not None:
                setattr(result, attr, float(v))

        # ── Close Game / Blowout Markets ──────────────────
        for mk, attr in [
            ("close_game", "close_game_prob"),
            ("blowout_win", "blowout_win_prob"),
            ("one_score_game", "one_score_game_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))
        cg_thresh = extra_models.get("close_game_thresh")
        if cg_thresh is not None:
            result.close_game_thresh = float(cg_thresh)

        # ── NFL Second-Half Total Markets ─────────────────
        for mk, attr in [
            ("nfl_2h_over_low", "nfl_2h_over_low_prob"),
            ("nfl_2h_over_mid", "nfl_2h_over_mid_prob"),
            ("nfl_2h_over_high", "nfl_2h_over_high_prob"),
            ("nfl_scoring_surge", "nfl_scoring_surge_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))
        for mk, attr in [
            ("nfl_2h_over_low_line", "nfl_2h_over_low_line"),
            ("nfl_2h_over_mid_line", "nfl_2h_over_mid_line"),
            ("nfl_2h_over_high_line", "nfl_2h_over_high_line"),
        ]:
            v = extra_models.get(mk)
            if v is not None:
                setattr(result, attr, float(v))

        # ── High / Low Scoring Markets ────────────────────
        for mk, attr in [
            ("high_scoring", "high_scoring_prob"),
            ("low_scoring", "low_scoring_prob"),
            ("both_score_high", "both_score_high_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))
        hs_thresh2 = extra_models.get("high_scoring_thresh")
        if hs_thresh2 is not None:
            result.high_scoring_line = float(hs_thresh2)
        ls_thresh2 = extra_models.get("low_scoring_thresh")
        if ls_thresh2 is not None:
            result.low_scoring_line = float(ls_thresh2)

        # ── BTTS + Win Combo Markets ──────────────────────
        for mk, attr in [
            ("btts_home_win", "btts_home_win_prob"),
            ("btts_away_win", "btts_away_win_prob"),
            ("btts_draw", "btts_draw_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))

        # ── eSports Series Markets ────────────────────────
        for mk, attr in [
            ("home_2_0_win", "home_2_0_win_prob"),
            ("away_2_0_win", "away_2_0_win_prob"),
            ("esports_decider_map", "esports_decider_map_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))

        # ── Motorsport Extra Markets ──────────────────────
        for mk, attr in [
            ("motor_podium", "motor_podium_prob"),
            ("motor_points", "motor_points_prob"),
            ("motor_dnf", "motor_dnf_prob"),
            ("motor_fastest_lap", "motor_fastest_lap_prob"),
        ]:
            p = _cls_prob(mk)
            if p is not None:
                setattr(result, attr, round(p, 4))

    def predict_date(self, target_date: str) -> list[PredictionResult]:
        """Generate predictions for all games on *target_date*.

        Uses pre-computed features (batch) when available for speed,
        falling back to per-game feature extraction otherwise.
        """
        target_dt = pd.Timestamp(target_date)
        season = self._date_to_season(target_dt)

        # Try season, season+1, and season-1 to handle naming convention
        # mismatches (e.g., importer uses end-year, predictor uses start-year).
        games_df = pd.DataFrame()
        for s in [season, season + 1, season - 1]:
            try:
                candidate = self.extractor.load_games(s)
            except FileNotFoundError:
                continue
            if candidate.empty or "date" not in candidate.columns:
                continue
            candidate["date"] = pd.to_datetime(candidate["date"])
            day_games = candidate[candidate["date"].dt.date == target_dt.date()]
            if not day_games.empty:
                games_df = candidate
                break

        if games_df.empty:
            logger.info("No games found for %s on %s", self.sport, target_date)
            return []

        games_df["date"] = pd.to_datetime(games_df["date"])
        day_games = games_df[games_df["date"].dt.date == target_dt.date()]

        if day_games.empty:
            logger.info("No games found for %s on %s", self.sport, target_date)
            return []

        # Filter out TBD/placeholder games
        valid_rows = []
        for _, row in day_games.iterrows():
            ht = str(row.get("home_team") or "")
            at = str(row.get("away_team") or "")
            if ht.upper() in ("TBD", "TBA", "") or at.upper() in ("TBD", "TBA", ""):
                continue
            valid_rows.append(row)
        if not valid_rows:
            return []
        day_games = pd.DataFrame(valid_rows)

        # ── Fast path: use pre-computed features (batch) ──
        results = self._predict_date_batch(day_games)
        if results is not None:
            logger.info(
                "Generated %d predictions for %s on %s (batch)",
                len(results), self.sport, target_date,
            )
            return results

        # ── Slow fallback: per-game feature extraction ──
        results = []
        for _, row in day_games.iterrows():
            game = row.to_dict()
            if "game_id" not in game and "id" in game:
                game["game_id"] = game["id"]
            try:
                pred = self.predict_game(game)
                results.append(pred)
            except Exception:
                logger.error(
                    "Prediction failed for game %s",
                    game.get("game_id", game.get("id", "?")),
                    exc_info=True,
                )

        logger.info(
            "Generated %d predictions for %s on %s",
            len(results),
            self.sport,
            target_date,
        )
        return results

    def _predict_date_batch(
        self, day_games: "pd.DataFrame"
    ) -> "list[PredictionResult] | None":
        """Try batch prediction using pre-computed feature parquet.

        If some games are missing from features (e.g. upcoming/scheduled),
        extract their features inline and merge before batch prediction.
        Returns None only if features cannot be obtained at all.
        """
        features_dir = self.data_dir / "features"
        feat_path = features_dir / f"{self.sport}_all.parquet"
        if not feat_path.exists():
            return None
        try:
            precomp = pd.read_parquet(feat_path)
            id_col = "game_id" if "game_id" in precomp.columns else "id"
            precomp.index = precomp[id_col].astype(str)
            gid_col = "game_id" if "game_id" in day_games.columns else "id"
            gids = day_games[gid_col].astype(str).tolist()
            matched = [g for g in gids if g in precomp.index]
            missing = [g for g in gids if g not in precomp.index]

            if missing:
                # Extract features inline for missing games
                inline = self._extract_inline_features(day_games, missing, gid_col)
                if inline is not None and not inline.empty:
                    iid = "game_id" if "game_id" in inline.columns else "id"
                    inline.index = inline[iid].astype(str)
                    precomp = pd.concat([precomp, inline])
                    matched = [g for g in gids if g in precomp.index]
                    missing = [g for g in gids if g not in precomp.index]

            if not matched:
                return None

            # Proceed with whatever games we have features for
            if missing:
                logger.debug(
                    "Batch: %d/%d games have features (%d will use slow path)",
                    len(matched), len(gids), len(missing),
                )
            return self.predict_batch_precomputed(precomp, day_games)
        except Exception:
            logger.debug("Batch prediction failed for %s", self.sport, exc_info=True)
            return None

    def _extract_inline_features(
        self, day_games: "pd.DataFrame", missing_ids: list[str], gid_col: str,
    ) -> "pd.DataFrame | None":
        """Extract features for a handful of games inline (for prediction)."""
        try:
            missing_set = set(missing_ids)
            games_to_extract = day_games[
                day_games[gid_col].astype(str).isin(missing_set)
            ]
            rows = []
            for _, row in games_to_extract.iterrows():
                try:
                    feats = self.extractor.extract_game_features(row.to_dict())
                    rows.append(feats)
                except Exception:
                    logger.debug(
                        "Inline feature extraction failed for game %s",
                        row.get(gid_col, "?"),
                    )
            if rows:
                return pd.DataFrame(rows)
        except Exception:
            logger.debug("Inline feature extraction failed for %s", self.sport)
        return None

    # ── Live prediction (stub) ───────────────────────────

    def predict_live(self, game_id: str) -> PredictionResult:
        """Generate a live in-game prediction.

        .. note::
            Full live-prediction support requires a real-time data feed.
            This method currently loads the latest snapshot from the
            games file and re-runs the model.  A dedicated live pipeline
            with streaming features is planned for a future release.
        """
        if self._bundle is None:
            raise RuntimeError(f"No trained models for {self.sport}")

        # Attempt to find the game across recent seasons
        for season_offset in range(0, 3):
            season = datetime.utcnow().year - season_offset
            try:
                games_df = self.extractor.load_games(season)
            except FileNotFoundError:
                continue
            match = games_df[games_df["game_id"].astype(str) == str(game_id)]
            if not match.empty:
                game = match.iloc[0].to_dict()
                return self.predict_game(game)

        raise ValueError(f"Game {game_id} not found in recent seasons")

    # ── Helpers ──────────────────────────────────────────

    # Sports whose season aligns with the calendar year (start in spring/summer)
    _CALENDAR_YEAR_SPORTS = frozenset({
        "mlb", "mls", "nwsl", "f1", "atp", "wta", "ufc",
        "csgo", "lol", "dota2", "valorant", "golf", "lpga", "indycar",
    })

    # End-year sports: season label = year the season ENDS.
    # NBA/NHL/NCAAB/NCAAW 2025-26 → "2026"; files: games_2026.parquet
    # Season starts in Oct/Nov → before that start month we're in the ending year.
    _END_YEAR_SPORTS: dict[str, int] = {
        "nba": 10, "nhl": 10, "ncaab": 11, "ncaaw": 11,
    }

    # Start-year sports: season label = year the season STARTS.
    # EPL/NFL 2025-26 → "2025"; files: games_2025.parquet
    # These seasons begin in Aug-Sep; before their start month we're still in
    # the prior season.
    _START_YEAR_SPORTS: dict[str, int] = {
        "epl": 8, "laliga": 8, "bundesliga": 8, "seriea": 8, "ligue1": 8, "ucl": 8,
        "nfl": 9, "ncaaf": 8,
        "wnba": 5,  # WNBA starts May, labeled by start year
    }

    def _date_to_season(self, dt: pd.Timestamp) -> int:
        """Map a date to the appropriate season year.

        - Calendar-year sports (MLB, MLS, esports, tennis…): season = calendar year.
        - End-year sports (NBA, NHL, NCAAB, NCAAW): season = year the season ends.
          Oct 2025 → 2026; Mar 2026 → 2026.
        - Start-year sports (EPL, Bundesliga, NFL, NCAAF…): season = year it started.
          Aug 2025 → 2025; Mar 2026 → 2025.
        """
        return self._date_to_season_static(self.sport, dt)

    @staticmethod
    def _date_to_season_static(sport: str, dt: "pd.Timestamp") -> int:
        """Map a date to the appropriate season year without needing an instance."""
        _CAL = GamePredictor._CALENDAR_YEAR_SPORTS
        _END = GamePredictor._END_YEAR_SPORTS
        _START = GamePredictor._START_YEAR_SPORTS
        if sport in _CAL:
            return dt.year
        if sport in _END:
            start_month = _END[sport]
            return dt.year + 1 if dt.month >= start_month else dt.year
        if sport in _START:
            start_month = _START[sport]
            return dt.year if dt.month >= start_month else dt.year - 1
        return dt.year
