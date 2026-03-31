"use client";

type BadgeStatus = "live" | "scheduled" | "final" | "halftime";

interface LiveEventBadgeProps {
  status: BadgeStatus;
  /** Game period or quarter (e.g., "Q3", "3rd") */
  period?: string;
  /** Game clock (e.g., "2:45") */
  time?: string;
  /** Short detail text shown next to status */
  detail?: string;
}

const STATUS_LABELS: Record<BadgeStatus, string> = {
  live: "LIVE",
  scheduled: "UPCOMING",
  final: "FINAL",
  halftime: "HALFTIME",
};

export function LiveEventBadge({
  status,
  period,
  time,
  detail,
}: LiveEventBadgeProps) {
  const label = STATUS_LABELS[status];
  const detailParts = [period, time, detail].filter(Boolean).join(" · ");

  return (
    <span
      className={`live-event-badge live-event-badge--${status}`}
      role="status"
      aria-label={`Game status: ${label}${detailParts ? ` — ${detailParts}` : ""}`}
    >
      {status === "live" && (
        <span className="live-event-badge-dot" aria-hidden="true" />
      )}
      {label}
      {detailParts && (
        <span className="live-event-badge-detail" aria-hidden="true">
          {detailParts}
        </span>
      )}
    </span>
  );
}
