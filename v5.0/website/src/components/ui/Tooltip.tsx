"use client";

interface TooltipProps {
  text: string;
  children: React.ReactNode;
  position?: "top" | "bottom";
  className?: string;
}

/** Lightweight CSS-only tooltip wrapper */
export function Tooltip({ text, children, position = "top", className }: TooltipProps) {
  const posStyle: React.CSSProperties =
    position === "bottom"
      ? { top: "calc(100% + 6px)", bottom: "auto" }
      : {};

  return (
    <span className={`tooltip-wrapper${className ? ` ${className}` : ""}`}>
      {children}
      <span className="tooltip-text" style={posStyle} role="tooltip">
        {text}
      </span>
    </span>
  );
}
