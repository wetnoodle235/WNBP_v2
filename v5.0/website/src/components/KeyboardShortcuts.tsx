"use client";

import { useEffect, useState, useCallback } from "react";
import { useRouter } from "next/navigation";

const SHORTCUTS = [
  { key: "g h", label: "Go to Home", path: "/" },
  { key: "g p", label: "Go to Predictions", path: "/predictions" },
  { key: "g l", label: "Go to Live", path: "/live" },
  { key: "g n", label: "Go to News", path: "/news" },
  { key: "g o", label: "Go to Odds", path: "/odds" },
  { key: "g s", label: "Go to Standings", path: "/standings" },
  { key: "g d", label: "Go to Dashboard", path: "/dashboard" },
  { key: "g f", label: "Go to Favorites", path: "/favorites" },
];

export function KeyboardShortcuts() {
  const router = useRouter();
  const [showHelp, setShowHelp] = useState(false);
  const [pendingPrefix, setPendingPrefix] = useState<string | null>(null);

  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      // Don't fire when typing in inputs
      const target = e.target as HTMLElement;
      if (
        target.tagName === "INPUT" ||
        target.tagName === "TEXTAREA" ||
        target.tagName === "SELECT" ||
        target.isContentEditable
      ) {
        return;
      }

      // Toggle help with ?
      if (e.key === "?" && !e.ctrlKey && !e.metaKey) {
        e.preventDefault();
        setShowHelp((v) => !v);
        return;
      }

      // Escape closes help
      if (e.key === "Escape") {
        setShowHelp(false);
        setPendingPrefix(null);
        return;
      }

      // Two-key combos (g + letter)
      if (pendingPrefix === "g") {
        const match = SHORTCUTS.find((s) => s.key === `g ${e.key}`);
        if (match) {
          e.preventDefault();
          router.push(match.path);
        }
        setPendingPrefix(null);
        return;
      }

      if (e.key === "g" && !e.ctrlKey && !e.metaKey) {
        setPendingPrefix("g");
        // Clear pending after 1.5s
        setTimeout(() => setPendingPrefix((p) => (p === "g" ? null : p)), 1500);
        return;
      }
    },
    [pendingPrefix, router],
  );

  useEffect(() => {
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [handleKeyDown]);

  if (!showHelp) {
    return pendingPrefix ? (
      <div className="kbd-pending" aria-live="polite">
        <kbd>g</kbd> + …
      </div>
    ) : null;
  }

  return (
    <div
      className="kbd-help-backdrop"
      onClick={() => setShowHelp(false)}
      role="dialog"
      aria-modal="true"
      aria-label="Keyboard shortcuts"
    >
      <div className="kbd-help-panel" onClick={(e) => e.stopPropagation()}>
        <div className="kbd-help-header">
          <h2 className="kbd-help-title">Keyboard Shortcuts</h2>
          <button
            className="btn btn-ghost btn-sm"
            onClick={() => setShowHelp(false)}
            aria-label="Close"
          >
            ✕
          </button>
        </div>
        <div className="kbd-help-section">
          <h3 className="kbd-help-section-title">Navigation</h3>
          <ul className="kbd-help-list">
            {SHORTCUTS.map((s) => (
              <li key={s.key} className="kbd-help-item">
                <span className="kbd-help-keys">
                  {s.key.split(" ").map((k, i) => (
                    <span key={i}>
                      {i > 0 && <span className="kbd-help-then"> then </span>}
                      <kbd>{k}</kbd>
                    </span>
                  ))}
                </span>
                <span className="kbd-help-desc">{s.label}</span>
              </li>
            ))}
          </ul>
        </div>
        <div className="kbd-help-section">
          <h3 className="kbd-help-section-title">General</h3>
          <ul className="kbd-help-list">
            <li className="kbd-help-item">
              <span className="kbd-help-keys"><kbd>?</kbd></span>
              <span className="kbd-help-desc">Toggle this help</span>
            </li>
            <li className="kbd-help-item">
              <span className="kbd-help-keys"><kbd>Esc</kbd></span>
              <span className="kbd-help-desc">Close dialog / cancel</span>
            </li>
          </ul>
        </div>
        <p className="kbd-help-footer">
          Press <kbd>?</kbd> to toggle this panel
        </p>
      </div>
    </div>
  );
}
