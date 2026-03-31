"use client";

interface StatusDotProps {
  status: "online" | "offline" | "warning" | "idle";
  label?: string;
  size?: number;
  pulse?: boolean;
}

const COLORS: Record<StatusDotProps["status"], string> = {
  online: "var(--color-win, #22c55e)",
  offline: "var(--color-loss, #ef4444)",
  warning: "#eab308",
  idle: "var(--color-text-muted, #6b7280)",
};

/** Small colored dot indicating status — optionally pulsates */
export function StatusDot({ status, label, size = 10, pulse = false }: StatusDotProps) {
  return (
    <span
      role="status"
      aria-label={label ?? status}
      style={{
        display: "inline-block",
        width: size,
        height: size,
        borderRadius: "50%",
        background: COLORS[status],
        animation: pulse ? "pulse-dot 2s ease-in-out infinite" : undefined,
        flexShrink: 0,
      }}
    />
  );
}
