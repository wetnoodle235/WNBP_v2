"use client";

import { useState, useEffect } from "react";

interface ChangelogEntry {
  id: string;
  message: string;
  link?: string;
  linkText?: string;
}

const LATEST_CHANGELOG: ChangelogEntry = {
  id: "2026-03-31-sports-hub",
  message: "New! Explore now includes Sports Hub with dedicated league pages.",
  link: "/sports",
  linkText: "Open Sports Hub",
};

const DISMISSED_KEY = "wnbp_changelog_dismissed";

export function ChangelogBanner() {
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    try {
      const dismissed = localStorage.getItem(DISMISSED_KEY);
      if (dismissed !== LATEST_CHANGELOG.id) {
        setVisible(true);
      }
    } catch { /* ignore */ }
  }, []);

  const dismiss = () => {
    setVisible(false);
    try { localStorage.setItem(DISMISSED_KEY, LATEST_CHANGELOG.id); } catch { /* ignore */ }
  };

  if (!visible) return null;

  return (
    <div className="changelog-banner" role="status">
      <span className="changelog-banner-badge">NEW</span>
      <span>{LATEST_CHANGELOG.message}</span>
      {LATEST_CHANGELOG.link && (
        <a href={LATEST_CHANGELOG.link} className="changelog-banner-link">
          {LATEST_CHANGELOG.linkText ?? "Details"}
        </a>
      )}
      <button
        className="changelog-banner-close"
        onClick={dismiss}
        aria-label="Dismiss announcement"
      >
        ×
      </button>
    </div>
  );
}
