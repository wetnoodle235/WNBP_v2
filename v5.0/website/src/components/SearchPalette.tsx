"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { useRouter } from "next/navigation";

interface SearchResult {
  label: string;
  href: string;
  category: string;
}

const PAGES: SearchResult[] = [
  { label: "Home", href: "/", category: "Pages" },
  { label: "Predictions", href: "/predictions", category: "Pages" },
  { label: "Live Scores", href: "/live", category: "Pages" },
  { label: "Odds", href: "/odds", category: "Pages" },
  { label: "News", href: "/news", category: "Pages" },
  { label: "Standings", href: "/standings", category: "Pages" },
  { label: "Stats", href: "/stats", category: "Pages" },
  { label: "Players", href: "/players", category: "Pages" },
  { label: "Teams", href: "/teams", category: "Pages" },
  { label: "Prop Opportunities", href: "/opportunities", category: "Pages" },
  { label: "Season Simulator", href: "/season", category: "Pages" },
  { label: "Paper Trading", href: "/paper", category: "Pages" },
  { label: "AutoBets", href: "/autobets", category: "Pages" },
  { label: "Model Health", href: "/model-health", category: "Pages" },
  { label: "API Docs", href: "/api-docs", category: "Pages" },
  { label: "Favorites", href: "/favorites", category: "Pages" },
  { label: "Dashboard", href: "/dashboard", category: "Account" },
  { label: "Account Settings", href: "/dashboard", category: "Account" },
  { label: "Pricing", href: "/pricing", category: "Account" },
  { label: "Login", href: "/login", category: "Account" },
  { label: "NBA Games", href: "/games/nba", category: "Sports" },
  { label: "MLB Games", href: "/games/mlb", category: "Sports" },
  { label: "NFL Games", href: "/games/nfl", category: "Sports" },
  { label: "NHL Games", href: "/games/nhl", category: "Sports" },
  { label: "EPL Games", href: "/games/epl", category: "Sports" },
  { label: "WNBA Games", href: "/games/wnba", category: "Sports" },
  { label: "UFC Events", href: "/games/ufc", category: "Sports" },
  { label: "CS2 Matches", href: "/games/csgo", category: "Sports" },
  { label: "LoL Matches", href: "/games/lol", category: "Sports" },
  { label: "DOTA 2 Matches", href: "/games/dota2", category: "Sports" },
  { label: "Valorant Matches", href: "/games/valorant", category: "Sports" },
  { label: "F1 Races", href: "/games/f1", category: "Sports" },
];

export function SearchPalette() {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [selectedIndex, setSelectedIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);
  const router = useRouter();

  // Open on Cmd+K / Ctrl+K
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setOpen(true);
      }
      if (e.key === "Escape" && open) {
        setOpen(false);
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [open]);

  // Focus input when opened
  useEffect(() => {
    if (open) {
      setQuery("");
      setSelectedIndex(0);
      setTimeout(() => inputRef.current?.focus(), 50);
    }
  }, [open]);

  // Fuzzy match — characters must appear in order but not necessarily contiguous
  const fuzzyMatch = useCallback((text: string, pattern: string) => {
    const lower = text.toLowerCase();
    const pat = pattern.toLowerCase();
    let pi = 0;
    for (let i = 0; i < lower.length && pi < pat.length; i++) {
      if (lower[i] === pat[pi]) pi++;
    }
    return pi === pat.length;
  }, []);

  const filtered = query.trim().length === 0
    ? PAGES
    : PAGES.filter((p) =>
        fuzzyMatch(p.label, query) ||
        fuzzyMatch(p.category, query) ||
        p.href.toLowerCase().includes(query.toLowerCase())
      );

  const navigate = useCallback(
    (href: string) => {
      setOpen(false);
      router.push(href);
    },
    [router],
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setSelectedIndex((i) => Math.min(i + 1, filtered.length - 1));
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        setSelectedIndex((i) => Math.max(i - 1, 0));
      } else if (e.key === "Enter" && filtered[selectedIndex]) {
        e.preventDefault();
        navigate(filtered[selectedIndex].href);
      }
    },
    [filtered, selectedIndex, navigate],
  );

  // Keep selected item in view
  useEffect(() => {
    const container = listRef.current;
    if (!container) return;
    const item = container.children[selectedIndex] as HTMLElement | undefined;
    item?.scrollIntoView({ block: "nearest" });
  }, [selectedIndex]);

  if (!open) return null;

  // Group results by category
  const groups = new Map<string, SearchResult[]>();
  filtered.forEach((r) => {
    const arr = groups.get(r.category) ?? [];
    arr.push(r);
    groups.set(r.category, arr);
  });

  let flatIndex = 0;

  return (
    <div
      className="search-palette-overlay"
      onClick={() => setOpen(false)}
      role="dialog"
      aria-modal="true"
      aria-label="Quick navigation"
    >
      <div className="search-palette" onClick={(e) => e.stopPropagation()}>
        <div className="search-palette-input-wrap">
          <span aria-hidden="true" style={{ fontSize: "1.1rem" }}>🔍</span>
          <input
            ref={inputRef}
            type="text"
            className="search-palette-input"
            placeholder="Search pages…"
            value={query}
            onChange={(e) => { setQuery(e.target.value); setSelectedIndex(0); }}
            onKeyDown={handleKeyDown}
            aria-label="Search pages"
            autoComplete="off"
          />
          <kbd className="search-palette-kbd">ESC</kbd>
        </div>
        <div className="search-palette-results" ref={listRef} role="listbox">
          {filtered.length === 0 ? (
            <div className="search-palette-empty">No results found</div>
          ) : (
            [...groups.entries()].map(([category, items]) => (
              <div key={category}>
                <div className="search-palette-category">{category}</div>
                {items.map((item) => {
                  const idx = flatIndex++;
                  return (
                    <button
                      key={item.href}
                      className={`search-palette-item ${idx === selectedIndex ? "search-palette-item-active" : ""}`}
                      role="option"
                      aria-selected={idx === selectedIndex}
                      onClick={() => navigate(item.href)}
                      onMouseEnter={() => setSelectedIndex(idx)}
                    >
                      {item.label}
                      <span className="search-palette-item-path">{item.href}</span>
                    </button>
                  );
                })}
              </div>
            ))
          )}
        </div>
        <div className="search-palette-footer">
          <span><kbd>↑↓</kbd> Navigate</span>
          <span><kbd>↵</kbd> Open</span>
          <span><kbd>esc</kbd> Close</span>
        </div>
      </div>
    </div>
  );
}
