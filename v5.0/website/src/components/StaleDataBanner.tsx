import type { ReactNode } from "react";

interface StaleDataBannerProps {
  message?: string;
  level?: "warn" | "info" | "error";
  children?: ReactNode;
  className?: string;
}

/**
 * StaleDataBanner — stale data UX fallback state.
 * Renders a non-blocking notice when data is unavailable, delayed, or empty.
 */
export function StaleDataBanner({
  message = "Data temporarily unavailable. Please check back shortly.",
  level = "warn",
  children,
  className,
}: StaleDataBannerProps) {
  const icon = level === "error" ? "✗" : level === "info" ? "ℹ" : "⚠";
  return (
    <div
      className={`stale-banner stale-banner-${level}${className ? ` ${className}` : ""}`}
      role="status"
      aria-live="polite"
    >
      <span className="stale-banner-icon" aria-hidden="true">{icon}</span>
      <span className="stale-banner-text">{message}</span>
      {children}
    </div>
  );
}
