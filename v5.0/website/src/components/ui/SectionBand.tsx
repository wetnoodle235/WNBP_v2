import type { ReactNode } from "react";

interface SectionBandProps {
  title: string;
  /** Optional trailing action (e.g. a "View all →" link) */
  action?: ReactNode;
  /** Optional accent override on the bottom border */
  accentColor?: string;
  children: ReactNode;
  id?: string;
  className?: string;
}

export function SectionBand({
  title,
  action,
  accentColor,
  children,
  id,
  className,
}: SectionBandProps) {
  return (
    <section
      className={`section-band${className ? ` ${className}` : ""}`}
      id={id}
      aria-labelledby={id ? `${id}-title` : undefined}
    >
      <div
        className="section-band-header"
        style={accentColor ? { borderBottomColor: accentColor } : undefined}
      >
        <h2 className="section-band-title" id={id ? `${id}-title` : undefined}>
          {title}
        </h2>
        {action && (
          <div className="section-band-action" aria-label={`${title} — see more`}>
            {action}
          </div>
        )}
      </div>
      {children}
    </section>
  );
}
