import Link from "next/link";
import type { Game } from "@/lib/schemas";
import { getSportDef } from "@/lib/sports";
import { formatOdds as fmtOdds, formatGameTime } from "@/lib/formatters";
import { FavoriteButton } from "@/components/ui/FavoriteButton";

interface GameCardProps {
  game: Game;
  odds?: {
    spread_home?: number | null;
    total_line?: number | null;
    h2h_home?: number | null;
    h2h_away?: number | null;
  };
  prediction?: {
    home_win_prob?: number | null;
    away_win_prob?: number | null;
    predicted_total?: number | null;
  };
  className?: string;
}

function formatOddsNullable(val: number | null | undefined): string {
  if (val == null) return "—";
  return fmtOdds(val);
}

function statusLabel(status: string): { text: string; className: string } {
  switch (status) {
    case "in_progress":
      return { text: "LIVE", className: "badge badge-live" };
    case "final":
      return { text: "Final", className: "badge badge-loss" };
    case "postponed":
      return { text: "PPD", className: "badge badge-push" };
    case "cancelled":
      return { text: "CXL", className: "badge badge-push" };
    default:
      return { text: "Scheduled", className: "badge badge-free" };
  }
}

export function GameCard({ game, odds, prediction, className }: GameCardProps) {
  const sportDef = getSportDef(game.sport);
  const { text: statusText, className: statusClass } = statusLabel(game.status);
  const isLive = game.status === "in_progress";
  const isFinal = game.status === "final";

  const homeProb = prediction?.home_win_prob;
  const awayProb = prediction?.away_win_prob;

  return (
    <article
      className={`card game-card${isLive ? " game-card--live" : ""}${className ? ` ${className}` : ""}`}
      aria-label={`${game.away_team} at ${game.home_team}`}
    >
      <div className="card-header" style={{ borderBottomColor: sportDef?.color }}>
        <span className={statusClass}>
          {isLive && <span className="live-dot" aria-hidden="true" />}
          {" "}{statusText}
        </span>
        <div style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}>
          {game.start_time && !isLive && !isFinal && (
            <time
              dateTime={game.start_time}
              style={{ fontSize: "var(--text-xs)", color: "var(--color-text-muted)" }}
            >
              {formatGameTime(game.start_time)}
            </time>
          )}
          <FavoriteButton id={game.id} storageKey="wnbp_fav_games" />
        </div>
      </div>

      <div className="card-body">
        {/* Teams & Scores */}
        <div className="game-card-teams">
          <div className="game-card-team">
            <span className="game-card-team-name">{game.away_team}</span>
            {(isLive || isFinal) && game.away_score != null && (
              <span className="game-card-score" aria-label={`${game.away_team} score ${game.away_score}`}>{game.away_score}</span>
            )}
          </div>
          <div className="game-card-team">
            <span className="game-card-team-name">{game.home_team}</span>
            {(isLive || isFinal) && game.home_score != null && (
              <span className="game-card-score" aria-label={`${game.home_team} score ${game.home_score}`}>{game.home_score}</span>
            )}
          </div>
        </div>

        {/* Odds line */}
        {odds && (
          <div className="game-card-odds" aria-label="Betting odds">
            {odds.spread_home != null && (
              <span className="game-card-odds-item">
                <span className="game-card-odds-label">Spread</span>
                <span>{formatOddsNullable(odds.spread_home)}</span>
              </span>
            )}
            {odds.total_line != null && (
              <span className="game-card-odds-item">
                <span className="game-card-odds-label">O/U</span>
                <span>{odds.total_line}</span>
              </span>
            )}
            {odds.h2h_home != null && (
              <span className="game-card-odds-item">
                <span className="game-card-odds-label">ML</span>
                <span>{formatOddsNullable(odds.h2h_home)}/{formatOddsNullable(odds.h2h_away)}</span>
              </span>
            )}
          </div>
        )}

        {/* Win probability bar */}
        {homeProb != null && awayProb != null && (
          <div className="game-card-prob" role="group" aria-label="Win probability">
            <div className="game-card-prob-bar">
              <div
                className="game-card-prob-fill game-card-prob-fill--away"
                style={{ width: `${(awayProb * 100).toFixed(1)}%` }}
                role="meter"
                aria-valuenow={Math.round(awayProb * 100)}
                aria-valuemin={0}
                aria-valuemax={100}
                aria-label={`${game.away_team} win probability`}
              />
              <div
                className="game-card-prob-fill game-card-prob-fill--home"
                style={{ width: `${(homeProb * 100).toFixed(1)}%` }}
                role="meter"
                aria-valuenow={Math.round(homeProb * 100)}
                aria-valuemin={0}
                aria-valuemax={100}
                aria-label={`${game.home_team} win probability`}
              />
            </div>
            <div className="game-card-prob-labels">
              <span>{(awayProb * 100).toFixed(0)}%</span>
              <span>{(homeProb * 100).toFixed(0)}%</span>
            </div>
          </div>
        )}
      </div>

      <div className="card-footer">
        <Link
          href={`/${game.sport}/games/${game.id}`}
          className="btn btn-ghost btn-sm"
          style={{ color: sportDef?.color }}
        >
          Details →
        </Link>
      </div>
    </article>
  );
}
