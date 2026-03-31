"use client";

import { useEffect, useState, useCallback, useRef } from "react";

/** Reactively tracks browser online/offline status */
export function useOnlineStatus(): boolean {
  const [online, setOnline] = useState(true);

  useEffect(() => {
    setOnline(navigator.onLine);
    const goOnline = () => setOnline(true);
    const goOffline = () => setOnline(false);
    window.addEventListener("online", goOnline);
    window.addEventListener("offline", goOffline);
    return () => {
      window.removeEventListener("online", goOnline);
      window.removeEventListener("offline", goOffline);
    };
  }, []);

  return online;
}

/** Persist state in localStorage with SSR-safe fallback */
export function useLocalStorage<T>(key: string, initialValue: T): [T, (v: T | ((prev: T) => T)) => void] {
  const [storedValue, setStoredValue] = useState<T>(initialValue);

  useEffect(() => {
    try {
      const item = localStorage.getItem(key);
      if (item !== null) setStoredValue(JSON.parse(item));
    } catch { /* ignore */ }
  }, [key]);

  const setValue = useCallback(
    (value: T | ((prev: T) => T)) => {
      setStoredValue((prev) => {
        const next = value instanceof Function ? value(prev) : value;
        try { localStorage.setItem(key, JSON.stringify(next)); } catch { /* quota exceeded */ }
        return next;
      });
    },
    [key],
  );

  return [storedValue, setValue];
}

/** Match a CSS media query reactively */
export function useMediaQuery(query: string): boolean {
  const [matches, setMatches] = useState(false);

  useEffect(() => {
    const mql = window.matchMedia(query);
    setMatches(mql.matches);
    const handler = (e: MediaQueryListEvent) => setMatches(e.matches);
    mql.addEventListener("change", handler);
    return () => mql.removeEventListener("change", handler);
  }, [query]);

  return matches;
}

/** Debounce a value by a specified delay */
export function useDebounce<T>(value: T, delay: number): T {
  const [debounced, setDebounced] = useState(value);

  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delay);
    return () => clearTimeout(timer);
  }, [value, delay]);

  return debounced;
}

/** Track favorite items (games, predictions) with localStorage persistence */
export function useFavorites(storageKey = "wnbp_favorites"): {
  favorites: Set<string>;
  toggle: (id: string) => void;
  isFavorite: (id: string) => boolean;
  count: number;
} {
  const [stored, setStored] = useLocalStorage<string[]>(storageKey, []);
  const favSet = new Set(stored);

  const toggle = useCallback(
    (id: string) => {
      setStored((prev) => {
        const set = new Set(prev);
        if (set.has(id)) set.delete(id);
        else set.add(id);
        return [...set];
      });
    },
    [setStored],
  );

  const isFavorite = useCallback((id: string) => favSet.has(id), [favSet]);

  return { favorites: favSet, toggle, isFavorite, count: favSet.size };
}

/** Observe when an element enters the viewport */
export function useIntersectionObserver(
  options?: IntersectionObserverInit,
): [React.RefCallback<Element>, boolean] {
  const [isVisible, setIsVisible] = useState(false);
  const observerRef = useRef<IntersectionObserver | null>(null);

  const ref = useCallback(
    (node: Element | null) => {
      if (observerRef.current) {
        observerRef.current.disconnect();
      }
      if (!node) return;

      observerRef.current = new IntersectionObserver(([entry]) => {
        if (entry.isIntersecting) {
          setIsVisible(true);
          observerRef.current?.disconnect();
        }
      }, options);

      observerRef.current.observe(node);
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [options?.threshold, options?.rootMargin],
  );

  return [ref, isVisible];
}

/** Copy text to clipboard, returns success boolean */
export function useCopyToClipboard(): [(text: string) => Promise<boolean>, boolean] {
  const [copied, setCopied] = useState(false);

  const copy = useCallback(async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
      return true;
    } catch {
      return false;
    }
  }, []);

  return [copy, copied];
}

/** Track recently viewed pages */
export interface RecentPage {
  path: string;
  title: string;
  timestamp: number;
}

export function useRecentlyViewed(maxItems = 10): {
  recentPages: RecentPage[];
  addPage: (path: string, title: string) => void;
  clearRecent: () => void;
} {
  const [pages, setPages] = useLocalStorage<RecentPage[]>("wnbp_recent", []);

  const addPage = useCallback(
    (path: string, title: string) => {
      setPages((prev) => {
        const filtered = prev.filter((p) => p.path !== path);
        return [{ path, title, timestamp: Date.now() }, ...filtered].slice(0, maxItems);
      });
    },
    [setPages, maxItems],
  );

  const clearRecent = useCallback(() => setPages([]), [setPages]);

  return { recentPages: pages, addPage, clearRecent };
}

/** Track scroll direction for hide-on-scroll patterns */
export function useScrollDirection(threshold = 10) {
  const [direction, setDirection] = useState<"up" | "down">("up");
  const lastY = useRef(0);

  useEffect(() => {
    let ticking = false;
    const onScroll = () => {
      if (ticking) return;
      ticking = true;
      requestAnimationFrame(() => {
        const y = window.scrollY;
        if (Math.abs(y - lastY.current) >= threshold) {
          setDirection(y > lastY.current ? "down" : "up");
          lastY.current = y;
        }
        ticking = false;
      });
    };
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, [threshold]);

  return direction;
}

/** Detect when an element is within a certain distance of the viewport */
export function useNearViewport(ref: React.RefObject<HTMLElement | null>, rootMargin = "200px") {
  const [isNear, setIsNear] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const obs = new IntersectionObserver(
      ([entry]) => { if (entry.isIntersecting) { setIsNear(true); obs.disconnect(); } },
      { rootMargin },
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, [ref, rootMargin]);

  return isNear;
}

/** Detect prefers-reduced-motion user preference */
export function usePrefersReducedMotion(): boolean {
  return useMediaQuery("(prefers-reduced-motion: reduce)");
}

/** Detect high contrast preference */
export function usePrefersContrast(): boolean {
  return useMediaQuery("(prefers-contrast: more)");
}

/** Schedule callback during idle period (defers non-critical work) */
export function useIdleCallback(callback: () => void, deps: unknown[] = []) {
  useEffect(() => {
    if ("requestIdleCallback" in window) {
      const id = (window as unknown as { requestIdleCallback: (cb: () => void) => number }).requestIdleCallback(callback);
      return () => (window as unknown as { cancelIdleCallback: (id: number) => void }).cancelIdleCallback(id);
    }
    const timer = setTimeout(callback, 1);
    return () => clearTimeout(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
}

/** Track the previous value of a variable */
export function usePrevious<T>(value: T): T | undefined {
  const ref = useRef<T | undefined>(undefined);
  useEffect(() => {
    ref.current = value;
  });
  return ref.current;
}

/** Throttle a value to update at most once every `delay` ms */
export function useThrottle<T>(value: T, delay: number): T {
  const [throttled, setThrottled] = useState(value);
  const lastRun = useRef(Date.now());

  useEffect(() => {
    const now = Date.now();
    const elapsed = now - lastRun.current;
    if (elapsed >= delay) {
      setThrottled(value);
      lastRun.current = now;
    } else {
      const timer = setTimeout(() => {
        setThrottled(value);
        lastRun.current = Date.now();
      }, delay - elapsed);
      return () => clearTimeout(timer);
    }
  }, [value, delay]);

  return throttled;
}

/** Register a keyboard shortcut. Ignores events when typing in inputs. */
export function useKeyCombo(
  combo: string,
  callback: () => void,
  enabled = true,
): void {
  const cbRef = useRef(callback);
  cbRef.current = callback;

  useEffect(() => {
    if (!enabled) return;
    const parts = combo.toLowerCase().split("+").map((s) => s.trim());
    const key = parts[parts.length - 1];
    const needsCtrl = parts.includes("ctrl") || parts.includes("meta") || parts.includes("mod");
    const needsShift = parts.includes("shift");
    const needsAlt = parts.includes("alt");

    const handler = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement;
      if (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.tagName === "SELECT" || target.isContentEditable) return;
      if (needsCtrl && !(e.ctrlKey || e.metaKey)) return;
      if (needsShift && !e.shiftKey) return;
      if (needsAlt && !e.altKey) return;
      if (e.key.toLowerCase() !== key) return;
      e.preventDefault();
      cbRef.current();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [combo, enabled]);
}

/** Detect if the user is on a touch device */
export function useIsTouchDevice(): boolean {
  const [isTouch, setIsTouch] = useState(false);
  useEffect(() => {
    setIsTouch("ontouchstart" in window || navigator.maxTouchPoints > 0);
  }, []);
  return isTouch;
}

/** Detect page visibility (tab active/hidden) */
export function usePageVisibility(): boolean {
  const [visible, setVisible] = useState(true);
  useEffect(() => {
    const handler = () => setVisible(document.visibilityState === "visible");
    document.addEventListener("visibilitychange", handler);
    return () => document.removeEventListener("visibilitychange", handler);
  }, []);
  return visible;
}

/** Track window scroll position */
export function useWindowScroll(): { x: number; y: number } {
  const [pos, setPos] = useState({ x: 0, y: 0 });
  useEffect(() => {
    const handler = () => setPos({ x: window.scrollX, y: window.scrollY });
    handler();
    window.addEventListener("scroll", handler, { passive: true });
    return () => window.removeEventListener("scroll", handler);
  }, []);
  return pos;
}
