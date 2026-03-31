"use client";

interface ScoreboardProps {
  homeTeam: string;
  awayTeam: string;
  homeScore: number | null;
  awayScore: number | null;
  homeLogo?: React.ReactNode;
  awayLogo?: React.ReactNode;
  status?: "scheduled" | "live" | "final";
  gameTime?: string;
  className?: string;
}

export function Scoreboard({
  homeTeam,
  awayTeam,
  homeScore,
  awayScore,
  homeLogo,
  awayLogo,
  status = "scheduled",
  gameTime,
  className,
}: ScoreboardProps) {
  const isLive = status === "live";
  const isFinal = status === "final";
  const homeWin = isFinal && homeScore != null && awayScore != null && homeScore > awayScore;
  const awayWin = isFinal && homeScore != null && awayScore != null && awayScore > homeScore;

  return (
    <div
      className={`scoreboard${isLive ? " scoreboard--live" : ""}${className ? ` ${className}` : ""}`}
      role="region"
      aria-label={`${awayTeam} at ${homeTeam}`}
    >
      <div className="scoreboard-team">
        {awayLogo && <span className="scoreboard-logo">{awayLogo}</span>}
        <span className={`scoreboard-name${awayWin ? " scoreboard-name--winner" : ""}`}>
          {awayTeam}
        </span>
        <span className={`scoreboard-score${awayWin ? " scoreboard-score--winner" : ""}`}>
          {awayScore ?? "–"}
        </span>
      </div>
      <div className="scoreboard-divider">
        {isLive && <span className="scoreboard-live-dot" aria-label="Live" />}
        {gameTime && <span className="scoreboard-time">{gameTime}</span>}
        {isFinal && <span className="scoreboard-final">Final</span>}
      </div>
      <div className="scoreboard-team">
        {homeLogo && <span className="scoreboard-logo">{homeLogo}</span>}
        <span className={`scoreboard-name${homeWin ? " scoreboard-name--winner" : ""}`}>
          {homeTeam}
        </span>
        <span className={`scoreboard-score${homeWin ? " scoreboard-score--winner" : ""}`}>
          {homeScore ?? "–"}
        </span>
      </div>
    </div>
  );
}
