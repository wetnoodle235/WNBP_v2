"use client";

import { useEffect, useRef, useCallback } from "react";

export interface TabDef {
  id: string;
  label: string;
  count?: number;
}

interface Props {
  tabs: TabDef[];
  active: string;
  onChange: (id: string) => void;
}

/**
 * Shared scrollable tab bar used across player, team, and game pages.
 * - Sticky below nav + sport bar
 * - Touch-friendly 44px min tap targets
 * - Auto-scrolls active tab into view on mobile
 * - Arrow key navigation for accessibility
 */
export default function Tabs({ tabs, active, onChange }: Props) {
  const activeRef = useRef<HTMLButtonElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    activeRef.current?.scrollIntoView({ inline: "nearest", behavior: "smooth", block: "nearest" });
  }, [active]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      const idx = tabs.findIndex((t) => t.id === active);
      let nextIdx = idx;
      if (e.key === "ArrowRight" || e.key === "ArrowDown") {
        e.preventDefault();
        nextIdx = (idx + 1) % tabs.length;
      } else if (e.key === "ArrowLeft" || e.key === "ArrowUp") {
        e.preventDefault();
        nextIdx = (idx - 1 + tabs.length) % tabs.length;
      } else if (e.key === "Home") {
        e.preventDefault();
        nextIdx = 0;
      } else if (e.key === "End") {
        e.preventDefault();
        nextIdx = tabs.length - 1;
      } else {
        return;
      }
      onChange(tabs[nextIdx].id);
      // Focus the new active tab button
      const container = containerRef.current;
      if (container) {
        const buttons = container.querySelectorAll<HTMLButtonElement>("[role=tab]");
        buttons[nextIdx]?.focus();
      }
    },
    [tabs, active, onChange],
  );

  return (
    <div className="tab-bar" role="tablist" ref={containerRef} onKeyDown={handleKeyDown}>
      {tabs.map(tab => {
        const isActive = tab.id === active;
        return (
          <button
            key={tab.id}
            ref={isActive ? activeRef : undefined}
            role="tab"
            aria-selected={isActive}
            tabIndex={isActive ? 0 : -1}
            className={`tab-btn${isActive ? " active" : ""}`}
            onClick={() => onChange(tab.id)}
          >
            {tab.label}
            {tab.count != null && (
              <span style={{
                marginLeft: 4,
                padding: "1px 6px",
                borderRadius: 999,
                fontSize: "10px",
                fontWeight: 700,
                background: isActive ? "rgba(255,255,255,0.25)" : "var(--color-surface-2)",
                color: isActive ? "var(--color-on-brand, #fff)" : "var(--color-text-muted)",
              }}>
                {tab.count}
              </span>
            )}
          </button>
        );
      })}
    </div>
  );
}
