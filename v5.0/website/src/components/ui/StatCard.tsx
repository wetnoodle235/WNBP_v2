import type { ReactNode } from "react";

type Accent = "win" | "loss" | "neutral" | "brand" | "premium" | "blue";

interface StatCardProps {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  accent?: Accent;
  className?: string;
  /** Optional: show a delta badge next to value (e.g. "+2.1%") */
  delta?: { value: string; direction: "up" | "down" | "flat" };
}

export function StatCard({ label, value, sub, accent, className, delta }: StatCardProps) {
  const deltaColor =
    delta?.direction === "up"
      ? "var(--color-win)"
      : delta?.direction === "down"
      ? "var(--color-loss)"
      : "var(--color-text-muted)";

  return (
    <article className={`stat-card${className ? ` ${className}` : ""}`} aria-label={`${label} stat`}>
      <div className="stat-card-label">{label}</div>
      <div
        className={`stat-card-value${accent ? ` text-${accent}` : ""}`}
        style={{ display: "flex", alignItems: "baseline", gap: "var(--space-2)" }}
      >
        {value}
        {delta && (
          <span style={{ fontSize: "var(--text-sm)", fontWeight: "var(--fw-semibold)", color: deltaColor }}>
            {delta.direction === "up" ? "↑" : delta.direction === "down" ? "↓" : "→"}
            {delta.value}
          </span>
        )}
      </div>
      {sub && <div className="stat-card-sub">{sub}</div>}
    </article>
  );
}
