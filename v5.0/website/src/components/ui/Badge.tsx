import type { ReactNode } from "react";

export type BadgeVariant =
  | "live"
  | "win"
  | "loss"
  | "push"
  | "premium"
  | "free"
  | "nba"
  | "mlb"
  | "nfl"
  | "nhl"
  | "ncaab"
  | "ncaaw"
  | "ncaaf"
  | "wnba"
  | "mls"
  | (string & {});  // Allow any string for sports codes

interface BadgeProps {
  variant?: BadgeVariant;
  children: ReactNode;
  className?: string;
}

export function Badge({ variant = "free", children, className }: BadgeProps) {
  return (
    <span className={`badge badge-${variant}${className ? ` ${className}` : ""}`}>
      {children}
    </span>
  );
}

export function LiveBadge() {
  return (
    <Badge variant="live">
      <span className="live-dot" aria-hidden="true" />
      <span className="sr-only">Live</span>
      <span aria-hidden="true"> LIVE</span>
    </Badge>
  );
}

export function SportBadge({ sport }: { sport: string }) {
  return (
    <Badge variant={sport}>{sport.toUpperCase()}</Badge>
  );
}
