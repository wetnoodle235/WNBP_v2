"use client";

import {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
  type ReactNode,
} from "react";
import { fetchAPI } from "./api";

/* ── Types ──────────────────────────────────────────────────────── */

export interface AuthUser {
  id: string;
  email: string;
  name: string;
  tier: string;
  api_key?: string;
  referral_code?: string;
}

export interface AuthState {
  user: AuthUser | null;
  token: string | null;
  apiKey: string | null;
  isLoading: boolean;
}

interface AuthContextValue extends AuthState {
  login: (email: string, password: string) => Promise<{ ok: boolean; error?: string }>;
  register: (
    name: string,
    email: string,
    password: string,
    referralCode?: string,
  ) => Promise<{ ok: boolean; error?: string; apiKey?: string }>;
  logout: () => void;
  refreshUser: () => Promise<void>;
  setApiKey: (key: string | null) => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

/* ── localStorage helpers ───────────────────────────────────────── */

const TOKEN_KEY = "wnbp_token";
const API_KEY_KEY = "wnbp_api_key";

function loadToken(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem(TOKEN_KEY);
}

function saveToken(token: string | null) {
  if (typeof window === "undefined") return;
  if (token) {
    localStorage.setItem(TOKEN_KEY, token);
    document.cookie = `wnbp_token=${token}; path=/; max-age=${60 * 60 * 24 * 30}; SameSite=Lax`;
  } else {
    localStorage.removeItem(TOKEN_KEY);
    document.cookie = "wnbp_token=; path=/; max-age=0; SameSite=Lax";
  }
}

function loadApiKey(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem(API_KEY_KEY);
}

function saveApiKey(key: string | null) {
  if (typeof window === "undefined") return;
  if (key) localStorage.setItem(API_KEY_KEY, key);
  else localStorage.removeItem(API_KEY_KEY);
}

/* ── Provider ───────────────────────────────────────────────────── */

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [apiKey, setApiKeyState] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  const fetchMe = useCallback(async (t: string) => {
    const res = await fetchAPI<{ user: AuthUser; api_key?: string }>("/auth/me", {
      headers: { Authorization: `Bearer ${t}` },
    });
    if (res.ok && res.data) {
      // The API may return data in different shapes:
      // { user: {...} } | { data: { user: {...} } } | { data: {...} }
      const d = res.data as unknown as Record<string, unknown>;
      const nested = (d.data as Record<string, unknown>) ?? d;
      const raw: Record<string, unknown> =
        (nested.user as Record<string, unknown>)
        ?? nested;

      const id = (raw.id ?? raw.user_id ?? "") as string;
      if (!id) {
        setUser(null); setToken(null); saveToken(null);
        return;
      }

      setUser({
        id,
        email: (raw.email as string) ?? "",
        name: (raw.name ?? raw.display_name ?? "") as string,
        tier: (raw.tier as string) ?? "free",
        api_key: (raw.api_key as string) ?? undefined,
        referral_code: (raw.referral_code as string) ?? undefined,
      });
      if (raw.api_key) {
        saveApiKey(raw.api_key as string);
        setApiKeyState(raw.api_key as string);
      }
    } else {
      setUser(null);
      setToken(null);
      saveToken(null);
    }
  }, []);

  // Auto-refresh on mount
  useEffect(() => {
    const t = loadToken();
    const k = loadApiKey();
    if (k) setApiKeyState(k);
    if (t) {
      setToken(t);
      fetchMe(t).finally(() => setIsLoading(false));
    } else {
      setIsLoading(false);
    }
  }, [fetchMe]);

  const login = useCallback(
    async (email: string, password: string) => {
      const res = await fetchAPI<Record<string, unknown>>("/auth/login", {
        method: "POST",
        body: JSON.stringify({ email, password }),
      });
      if (!res.ok) {
        return { ok: false, error: res.error === "http_401" ? "Invalid credentials" : "Login failed" };
      }
      const resBody = res.data as Record<string, unknown>;
      const payload = (resBody.data as Record<string, unknown>) ?? resBody;
      const t = (payload.token ?? payload.access_token ?? "") as string;
      if (!t) return { ok: false, error: "No token received" };

      saveToken(t);
      setToken(t);
      await fetchMe(t);
      return { ok: true };
    },
    [fetchMe],
  );

  const register = useCallback(
    async (name: string, email: string, password: string, referralCode?: string) => {
      const reqBody: Record<string, string> = { display_name: name, email, password };
      if (referralCode) reqBody.referral_code = referralCode;
      const res = await fetchAPI<Record<string, unknown>>("/auth/register", {
        method: "POST",
        body: JSON.stringify(reqBody),
      });
      if (!res.ok) {
        const msg =
          res.error === "http_409"
            ? "Account already exists"
            : res.error === "http_422"
              ? "Invalid input"
              : "Registration failed";
        return { ok: false, error: msg };
      }
      const resBody = res.data as Record<string, unknown>;
      const payload = (resBody.data as Record<string, unknown>) ?? resBody;
      const t = (payload.token ?? payload.access_token ?? "") as string;
      const key = (payload.api_key ?? "") as string;

      if (t) {
        saveToken(t);
        setToken(t);
        await fetchMe(t);
      }
      if (key) {
        saveApiKey(key);
        setApiKeyState(key);
      }
      return { ok: true, apiKey: key || undefined };
    },
    [fetchMe],
  );

  const logout = useCallback(() => {
    saveToken(null);
    saveApiKey(null);
    setToken(null);
    setUser(null);
    setApiKeyState(null);
    document.cookie = "wnbp_token=; path=/; max-age=0; SameSite=Lax";
  }, []);

  const refreshUser = useCallback(async () => {
    const t = token ?? loadToken();
    if (t) await fetchMe(t);
  }, [token, fetchMe]);

  const setApiKey = useCallback((key: string | null) => {
    saveApiKey(key);
    setApiKeyState(key);
  }, []);

  return (
    <AuthContext.Provider
      value={{ user, token, apiKey, isLoading, login, register, logout, refreshUser, setApiKey }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within an AuthProvider");
  return ctx;
}
