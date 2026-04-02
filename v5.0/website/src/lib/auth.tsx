"use client";

import {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
  type ReactNode,
} from "react";

type AuthProxyShape = {
  success?: boolean;
  data?: Record<string, unknown>;
  error?: string;
  detail?: string;
};

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

  const callAuthProxy = useCallback(async (
    path: string,
    init?: RequestInit,
  ): Promise<{ ok: boolean; status: number; body: AuthProxyShape | null }> => {
    try {
      const res = await fetch(path, {
        ...init,
        headers: {
          "Content-Type": "application/json",
          ...(init?.headers ?? {}),
        },
      });
      const ct = res.headers.get("content-type") ?? "";
      const body = ct.includes("application/json")
        ? (await res.json() as AuthProxyShape)
        : null;
      return { ok: res.ok, status: res.status, body };
    } catch {
      return { ok: false, status: 502, body: { error: "network_error" } };
    }
  }, []);

  const fetchMe = useCallback(async (t: string) => {
    const res = await callAuthProxy("/api/auth/me", {
      method: "GET",
      headers: { Authorization: `Bearer ${t}` },
      cache: "no-store",
    });
    if (res.ok && res.body) {
      // The API may return data in different shapes:
      // { user: {...} } | { data: { user: {...} } } | { data: {...} }
      const d = res.body as unknown as Record<string, unknown>;
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
  }, [callAuthProxy]);

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
      const res = await callAuthProxy("/api/auth/login", {
        method: "POST",
        body: JSON.stringify({ email, password }),
      });
      if (!res.ok || !res.body) {
        const detail = (res.body?.detail ?? res.body?.error ?? "").toString().toLowerCase();
        if (res.status === 401 || detail.includes("invalid")) {
          return { ok: false, error: "Invalid credentials" };
        }
        return { ok: false, error: "Login failed" };
      }
      const resBody = res.body as Record<string, unknown>;
      const payload = (resBody.data as Record<string, unknown>) ?? resBody;
      const t = (payload.token ?? payload.access_token ?? "") as string;
      if (!t) return { ok: false, error: "No token received" };

      saveToken(t);
      setToken(t);
      await fetchMe(t);
      return { ok: true };
    },
    [callAuthProxy, fetchMe],
  );

  const register = useCallback(
    async (name: string, email: string, password: string, referralCode?: string) => {
      const reqBody: Record<string, string> = { display_name: name, email, password };
      if (referralCode) reqBody.referral_code = referralCode;
      const res = await callAuthProxy("/api/auth/register", {
        method: "POST",
        body: JSON.stringify(reqBody),
      });
      if (!res.ok || !res.body) {
        const detail = (res.body?.detail ?? res.body?.error ?? "").toString().toLowerCase();
        const msg =
          res.status === 409
            ? "Account already exists"
            : res.status === 422 || detail.includes("invalid")
              ? "Invalid input"
              : "Registration failed";
        return { ok: false, error: msg };
      }
      const resBody = res.body as Record<string, unknown>;
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
    [callAuthProxy, fetchMe],
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
