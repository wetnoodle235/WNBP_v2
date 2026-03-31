import type { Prediction, Game } from "@/lib/schemas";
import { getSportDef } from "@/lib/sports";

interface PredictionCardProps {
  prediction: Prediction;
  game?: Game | null;
  className?: string;
}

function confidenceColor(confidence: number | null | undefined): string {
  if (confidence == null) return "var(--color-text-muted)";
  if (confidence >= 0.75) return "var(--color-win)";
  if (confidence >= 0.55) return "var(--color-neutral)";
  return "var(--color-loss)";
}

function confidenceLabel(confidence: number | null | undefined): string {
  if (confidence == null) return "—";
  if (confidence >= 0.75) return "High";
  if (confidence >= 0.55) return "Medium";
  return "Low";
}

export function PredictionCard({ prediction, game, className }: PredictionCardProps) {
  const sportDef = getSportDef(prediction.sport);
  const homeProb = prediction.home_win_prob;
  const awayProb = prediction.away_win_prob;
  const conf = prediction.confidence;

  const homeTeam = game?.home_team ?? "Home";
  const awayTeam = game?.away_team ?? "Away";

  const predictedWinner =
    homeProb != null && awayProb != null
      ? homeProb > awayProb
        ? homeTeam
        : awayTeam
      : null;

  const winProb =
    homeProb != null && awayProb != null
      ? Math.max(homeProb, awayProb)
      : null;

  return (
    <article className={`card prediction-card${className ? ` ${className}` : ""}`}>
      <div className="card-header" style={{ borderBottomColor: sportDef?.color }}>
        <span
          className="badge"
          style={{ background: sportDef?.color, color: "#fff" }}
        >
          {sportDef?.label ?? prediction.sport.toUpperCase()}
        </span>
        <span
          style={{
            fontSize: "var(--text-xs)",
            fontWeight: "var(--fw-semibold)" as unknown as number,
            color: confidenceColor(conf),
          }}
        >
          {confidenceLabel(conf)} Confidence
        </span>
      </div>

      <div className="card-body">
        {/* Matchup */}
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: "var(--space-3)" }}>
          <span style={{ fontWeight: "var(--fw-bold)" as unknown as number }}>{awayTeam}</span>
          <span style={{ color: "var(--color-text-muted)", fontSize: "var(--text-sm)" }}>@</span>
          <span style={{ fontWeight: "var(--fw-bold)" as unknown as number }}>{homeTeam}</span>
        </div>

        {/* Predicted Winner */}
        {predictedWinner && (
          <div style={{ marginTop: "var(--space-3)", textAlign: "center" }}>
            <div style={{ fontSize: "var(--text-xs)", color: "var(--color-text-muted)", textTransform: "uppercase", letterSpacing: "0.06em" }}>
              Predicted Winner
            </div>
            <div style={{ fontWeight: 800, fontSize: "var(--text-lg)", color: "var(--color-win)" }}>
              {predictedWinner}
            </div>
            {winProb != null && (
              <div style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)" }}>
                {(winProb * 100).toFixed(1)}% win probability
              </div>
            )}
          </div>
        )}

        {/* Predicted total */}
        {prediction.predicted_total != null && (
          <div style={{ marginTop: "var(--space-3)", display: "flex", justifyContent: "center", gap: "var(--space-4)" }}>
            <div style={{ textAlign: "center" }}>
              <div style={{ fontSize: "var(--text-xs)", color: "var(--color-text-muted)", textTransform: "uppercase" }}>
                Predicted Total
              </div>
              <div style={{ fontWeight: 700, fontSize: "var(--text-md)" }}>
                {prediction.predicted_total.toFixed(1)}
              </div>
            </div>
            {prediction.predicted_spread != null && (
              <div style={{ textAlign: "center" }}>
                <div style={{ fontSize: "var(--text-xs)", color: "var(--color-text-muted)", textTransform: "uppercase" }}>
                  Spread
                </div>
                <div style={{ fontWeight: 700, fontSize: "var(--text-md)" }}>
                  {prediction.predicted_spread > 0 ? "+" : ""}
                  {prediction.predicted_spread.toFixed(1)}
                </div>
              </div>
            )}
          </div>
        )}

        {/* Model info */}
        <div style={{ marginTop: "var(--space-3)", fontSize: "var(--text-xs)", color: "var(--color-text-muted)", textAlign: "center" }}>
          Model: {prediction.model}
        </div>
      </div>
    </article>
  );
}
