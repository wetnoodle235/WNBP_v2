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

        # Load extra-market models (halftime, OT, period, first-score) if present
        extra_path = models_dir / "extra_models.pkl"
        self._extra: dict[str, Any] | None = None
        if extra_path.exists():
            try:
                self._extra = self._unpickle(extra_path)
                n = len(self._extra.get("models", {}))
                logger.info("Loaded %d extra-market models from %s", n, extra_path)
            except Exception:
                logger.warning("Could not load extra_models.pkl", exc_info=True)

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
        if self._extra:
            extra_models = self._extra.get("models", {})
            extra_feature_names = self._extra.get("feature_names", [])

            # Build aligned extra feature matrix once
            X_extra = rows[feat_cols].fillna(0)
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
            if ot_p is not None:
                result.ot_prob = round(ot_p, 4)
            elif draw_p is not None:
                result.ot_prob = result.draw_prob

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

            cs_h = _cls("clean_sheet_home")
            if cs_h is not None:
                result.home_clean_sheet_prob = round(cs_h, 4)
            cs_a = _cls("clean_sheet_away")
            if cs_a is not None:
                result.away_clean_sheet_prob = round(cs_a, 4)

            dec_p = _cls("ufc_decision")
            ko_p = _cls("ufc_ko_tko")
            sub_p = _cls("ufc_submission")
            if any(v is not None for v in (dec_p, ko_p, sub_p)):
                result.decision_prob = round(dec_p, 4) if dec_p is not None else None
                result.ko_tko_prob = round(ko_p, 4) if ko_p is not None else None
                result.submission_prob = round(sub_p, 4) if sub_p is not None else None
                mp: dict[str, float] = {}
                if dec_p is not None:
                    mp["decision"] = round(dec_p, 4)
                if ko_p is not None:
                    mp["ko_tko"] = round(ko_p, 4)
                if sub_p is not None:
                    mp["submission"] = round(sub_p, 4)
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
            over_med_p = _cls("total_over_median")
            if over_med_p is not None:
                result.total_over_median_prob = round(over_med_p, 4)

            sh_p = _cls("second_half_winner")
            if sh_p is not None:
                result.second_half_home_win_prob = round(sh_p, 4)
            sh_t = _reg("second_half_total")
            if sh_t is not None:
                result.second_half_total = round(sh_t, 2)

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
        if ot_p is not None:
            result.ot_prob = round(ot_p, 4)
        elif draw_p is not None:
            # For sports without explicit OT data, OT ≈ draw prob
            result.ot_prob = result.draw_prob

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
        if dec_p is not None or ko_p is not None or sub_p is not None:
            result.decision_prob = round(dec_p, 4) if dec_p is not None else None
            result.ko_tko_prob   = round(ko_p,  4) if ko_p  is not None else None
            result.submission_prob = round(sub_p, 4) if sub_p is not None else None
            # Rebuild method_probs dict for convenience
            mp: dict[str, float] = {}
            if dec_p is not None: mp["decision"] = round(dec_p, 4)
            if ko_p  is not None: mp["ko_tko"]   = round(ko_p,  4)
            if sub_p is not None: mp["submission"] = round(sub_p, 4)
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
        over_med_p = _cls_prob("total_over_median")
        if over_med_p is not None:
            result.total_over_median_prob = round(over_med_p, 4)

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

    def predict_date(self, target_date: str) -> list[PredictionResult]:
        """Generate predictions for all games on *target_date*.

        Loads the games file for the appropriate season, filters to
        the target date, and runs ``predict_game`` for each.
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

        results: list[PredictionResult] = []
        for _, row in day_games.iterrows():
            game = row.to_dict()
            # Skip TBD / placeholder bracket games
            ht = str(game.get("home_team") or "")
            at = str(game.get("away_team") or "")
            if ht.upper() in ("TBD", "TBA", "") or at.upper() in ("TBD", "TBA", ""):
                logger.debug("Skipping TBD game %s", game.get("id", game.get("game_id", "?")))
                continue
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
        "csgo", "lol", "dota2", "valorant", "golf",
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
        if self.sport in self._CALENDAR_YEAR_SPORTS:
            return dt.year
        if self.sport in self._END_YEAR_SPORTS:
            start_month = self._END_YEAR_SPORTS[self.sport]
            return dt.year + 1 if dt.month >= start_month else dt.year
        if self.sport in self._START_YEAR_SPORTS:
            start_month = self._START_YEAR_SPORTS[self.sport]
            return dt.year if dt.month >= start_month else dt.year - 1
        # Unknown sport: fall back to calendar year
        return dt.year
