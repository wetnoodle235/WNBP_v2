import Link from "next/link";
import type { SportKey } from "@/lib/sports";
import { SPORTS } from "@/lib/sports";

interface SportCardProps {
  sport: SportKey;
  wins?: number;
  losses?: number;
  nextGame?: {
    opponent: string;
    date: string;
    time?: string;
  };
  className?: string;
}

export function SportCard({ sport, wins, losses, nextGame, className }: SportCardProps) {
  const def = SPORTS[sport];
  if (!def) return null;

  const record = wins != null && losses != null ? `${wins}-${losses}` : null;
  const pct = wins != null && losses != null && (wins + losses) > 0
    ? ((wins / (wins + losses)) * 100).toFixed(1)
    : null;

  return (
    <article className={`card sport-card${className ? ` ${className}` : ""}`}>
      <Link href={def.href} className="sport-card-link">
        <div className="card-header" style={{ borderBottomColor: def.color }}>
          <span
            className="sport-card-badge"
            style={{ background: def.color, color: "#fff" }}
          >
            {def.label}
          </span>
          <span
            className="sport-card-category"
            style={{ color: "var(--color-text-muted)" }}
          >
            {def.category}
          </span>
        </div>
        <div className="card-body">
          {record && (
            <div className="sport-card-record">
              <span className="stat-card-value">{record}</span>
              {pct && (
                <span className="stat-card-sub">{pct}%</span>
              )}
            </div>
          )}
          {nextGame && (
            <div className="sport-card-next">
              <span style={{ fontSize: "var(--text-xs)", color: "var(--color-text-muted)", textTransform: "uppercase", letterSpacing: "0.06em" }}>
                Next
              </span>
              <span style={{ fontWeight: "var(--fw-semibold)" as unknown as number }}>
                vs {nextGame.opponent}
              </span>
              <span style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)" }}>
                {nextGame.date}{nextGame.time ? ` · ${nextGame.time}` : ""}
              </span>
            </div>
          )}
          {!record && !nextGame && (
            <div style={{ color: "var(--color-text-muted)", fontSize: "var(--text-sm)" }}>
              View {def.label} predictions →
            </div>
          )}
        </div>
      </Link>
    </article>
  );
}
