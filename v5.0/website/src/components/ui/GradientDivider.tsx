"use client";

interface Props {
  className?: string;
  style?: React.CSSProperties;
}

/**
 * Gradient horizontal rule section divider — replaces plain <hr>.
 */
export function GradientDivider({ className = "", style }: Props) {
  return (
    <hr
      className={`gradient-divider ${className}`}
      aria-hidden="true"
      style={style}
    />
  );
}
