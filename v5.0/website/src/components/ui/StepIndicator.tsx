"use client";

interface Step {
  label: string;
  description?: string;
}

interface StepIndicatorProps {
  steps: Step[];
  current: number;
  /** Orientation */
  direction?: "horizontal" | "vertical";
}

export function StepIndicator({ steps, current, direction = "horizontal" }: StepIndicatorProps) {
  const isHorizontal = direction === "horizontal";

  return (
    <nav
      className={`step-indicator step-indicator--${direction}`}
      aria-label="Progress"
      style={{
        display: "flex",
        flexDirection: isHorizontal ? "row" : "column",
        gap: isHorizontal ? 0 : "var(--space-1)",
        alignItems: isHorizontal ? "center" : "flex-start",
      }}
    >
      {steps.map((step, i) => {
        const status = i < current ? "complete" : i === current ? "current" : "upcoming";
        return (
          <div
            key={i}
            className={`step-indicator-item step-indicator-item--${status}`}
            style={{
              display: "flex",
              alignItems: "center",
              gap: "var(--space-2)",
              flex: isHorizontal ? 1 : undefined,
            }}
          >
            <span
              className="step-indicator-circle"
              aria-current={status === "current" ? "step" : undefined}
              aria-label={`Step ${i + 1}: ${step.label} — ${status}`}
              style={{
                width: 28,
                height: 28,
                borderRadius: "50%",
                display: "inline-flex",
                alignItems: "center",
                justifyContent: "center",
                fontSize: "0.75rem",
                fontWeight: 700,
                flexShrink: 0,
                border: status === "current"
                  ? "2px solid var(--color-brand)"
                  : status === "complete"
                    ? "2px solid var(--color-win)"
                    : "2px solid var(--color-border)",
                background: status === "complete"
                  ? "var(--color-win)"
                  : status === "current"
                    ? "var(--color-brand)"
                    : "transparent",
                color: status === "upcoming" ? "var(--color-text-secondary)" : "#fff",
                transition: "all 0.2s ease",
              }}
            >
              {status === "complete" ? "✓" : i + 1}
            </span>
            <div style={{ minWidth: 0 }}>
              <div
                style={{
                  fontSize: "var(--text-sm)",
                  fontWeight: status === "current" ? 600 : 400,
                  color: status === "upcoming" ? "var(--color-text-secondary)" : "var(--color-text)",
                  whiteSpace: "nowrap",
                }}
              >
                {step.label}
              </div>
              {step.description && (
                <div style={{ fontSize: "var(--text-xs)", color: "var(--color-text-secondary)" }}>
                  {step.description}
                </div>
              )}
            </div>
            {isHorizontal && i < steps.length - 1 && (
              <div
                aria-hidden="true"
                style={{
                  flex: 1,
                  height: 2,
                  minWidth: 16,
                  marginInline: "var(--space-2)",
                  background: i < current
                    ? "var(--color-win)"
                    : "var(--color-border)",
                  borderRadius: 1,
                  transition: "background 0.3s ease",
                }}
              />
            )}
          </div>
        );
      })}
    </nav>
  );
}

export default StepIndicator;
