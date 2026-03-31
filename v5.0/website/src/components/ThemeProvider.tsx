"use client";

import { createContext, useContext, useEffect, useState, useCallback } from "react";

type Theme = "light" | "dark";
type ThemePreference = "light" | "dark" | "system";

interface ThemeContextValue {
  theme: Theme;
  preference: ThemePreference;
  toggle: () => void;
  setTheme: (t: Theme) => void;
  setPreference: (p: ThemePreference) => void;
}

const ThemeContext = createContext<ThemeContextValue>({
  theme: "light",
  preference: "system",
  toggle: () => {},
  setTheme: () => {},
  setPreference: () => {},
});

const THEME_KEY = "wnbp-theme";

function getSystemTheme(): Theme {
  if (typeof window === "undefined") return "light";
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function getInitialPreference(): ThemePreference {
  if (typeof window === "undefined") return "system";
  try {
    const stored = localStorage.getItem(THEME_KEY) as ThemePreference | null;
    if (stored === "light" || stored === "dark" || stored === "system") return stored;
    return "system";
  } catch {
    return "system";
  }
}

function resolveTheme(pref: ThemePreference): Theme {
  if (pref === "system") return getSystemTheme();
  return pref;
}

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  const [preference, setPreferenceState] = useState<ThemePreference>("system");
  const [theme, setThemeState] = useState<Theme>("light");
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    const pref = getInitialPreference();
    const resolved = resolveTheme(pref);
    setPreferenceState(pref);
    setThemeState(resolved);
    document.documentElement.setAttribute("data-theme", resolved);
    setMounted(true);
  }, []);

  // Listen for system theme changes (only matters when preference is "system")
  useEffect(() => {
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const handler = () => {
      setPreferenceState((currentPref) => {
        if (currentPref === "system") {
          const next = mq.matches ? "dark" : "light";
          setThemeState(next);
          document.documentElement.setAttribute("data-theme", next);
        }
        return currentPref;
      });
    };
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, []);

  const applyTheme = useCallback((next: Theme) => {
    setThemeState(next);
    setPreferenceState(next);
    try { localStorage.setItem(THEME_KEY, next); } catch { /* ignore */ }
    document.documentElement.setAttribute("data-theme", next);
  }, []);

  const applyPreference = useCallback((pref: ThemePreference) => {
    setPreferenceState(pref);
    const resolved = resolveTheme(pref);
    setThemeState(resolved);
    try { localStorage.setItem(THEME_KEY, pref); } catch { /* ignore */ }
    document.documentElement.setAttribute("data-theme", resolved);
  }, []);

  const toggle = useCallback(() => {
    setPreferenceState((prevPref) => {
      // Cycle: system → light → dark → system
      const order: ThemePreference[] = ["system", "light", "dark"];
      const idx = order.indexOf(prevPref);
      const nextPref = order[(idx + 1) % 3];
      const resolved = resolveTheme(nextPref);
      setThemeState(resolved);
      try { localStorage.setItem(THEME_KEY, nextPref); } catch { /* ignore */ }
      document.documentElement.setAttribute("data-theme", resolved);
      return nextPref;
    });
  }, []);

  // Prevent flash by hiding content until mounted
  return (
    <ThemeContext.Provider value={{ theme, preference, toggle, setTheme: applyTheme, setPreference: applyPreference }}>
      <div style={mounted ? undefined : { visibility: "hidden" }}>
        {children}
      </div>
    </ThemeContext.Provider>
  );
}

export function useTheme() {
  return useContext(ThemeContext);
}
