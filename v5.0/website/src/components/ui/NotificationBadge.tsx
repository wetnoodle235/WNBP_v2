"use client";

interface NotificationBadgeProps {
  count: number;
  max?: number;
  className?: string;
  children: React.ReactNode;
}

/**
 * Wraps children with a small count badge indicator (e.g. unread items).
 * Shows nothing when count is 0.
 */
export function NotificationBadge({ count, max = 99, className = "", children }: NotificationBadgeProps) {
  const display = count > max ? `${max}+` : String(count);

  return (
    <span className={`notification-badge-wrapper ${className}`} style={{ position: "relative", display: "inline-flex" }}>
      {children}
      {count > 0 && (
        <span
          className="notification-badge"
          aria-label={`${count} notification${count !== 1 ? "s" : ""}`}
          role="status"
        >
          {display}
        </span>
      )}
    </span>
  );
}
