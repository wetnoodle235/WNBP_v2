"use client";

import { useState, useRef, type FormEvent } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useAuth } from "@/lib/auth";

export default function LoginClient() {
  const { login, isLoading: authLoading } = useAuth();
  const router = useRouter();
  const searchParams = useSearchParams();
  const callbackUrl = searchParams.get("callbackUrl") ?? "/";

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [remember, setRemember] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const errorRef = useRef<HTMLDivElement>(null);
  const showDevHint = process.env.NODE_ENV !== "production";
  const devEmail = process.env.NEXT_PUBLIC_DEV_LOGIN_EMAIL ?? "dev@wnbp.com";
  const devPassword = process.env.NEXT_PUBLIC_DEV_LOGIN_PASSWORD ?? "wnbp_dev_2026";

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setError(null);
    setSubmitting(true);

    const result = await login(email, password);
    setSubmitting(false);

    if (result.ok) {
      router.push(callbackUrl);
    } else {
      setError(result.error ?? "Login failed");
      // Focus the error message for screen readers
      requestAnimationFrame(() => errorRef.current?.focus());
    }
  }

  if (authLoading) return null;

  return (
    <div className="auth-card">
      <div className="auth-card-header">
        <h1 className="auth-card-title">Welcome Back</h1>
        <p className="auth-card-subtitle">Sign in to your WNBP account</p>
      </div>

      {error && (
        <div className="auth-error" id="login-form-error" role="alert" ref={errorRef} tabIndex={-1}>
          {error}
        </div>
      )}

      <form onSubmit={handleSubmit} className="auth-form" noValidate>
        {showDevHint && (
          <div className="stale-banner stale-banner-info" style={{ marginBottom: "var(--space-2)" }}>
            <span className="stale-banner-icon" aria-hidden="true">🛠️</span>
            <span className="stale-banner-text">
              Dev login: {devEmail} / {devPassword}
            </span>
          </div>
        )}
        <label className="auth-label" htmlFor="login-email">
          <span>Email</span>
          <input
            id="login-email"
            type="email"
            className="auth-input"
            placeholder="you@example.com"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
            autoComplete="email"
            aria-required="true"
            aria-describedby={error ? "login-form-error" : undefined}
          />
        </label>

        <label className="auth-label" htmlFor="login-password">
          <span>Password</span>
          <div style={{ position: "relative" }}>
            <input
              id="login-password"
              type={showPassword ? "text" : "password"}
              className="auth-input"
              placeholder="••••••••"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              autoComplete="current-password"
              aria-required="true"
              aria-describedby={error ? "login-form-error" : undefined}
              style={{ paddingRight: 48 }}
            />
            <button
              type="button"
              className="password-toggle"
              onClick={() => setShowPassword(!showPassword)}
              aria-label={showPassword ? "Hide password" : "Show password"}
              tabIndex={-1}
            >
              {showPassword ? "🙈" : "👁"}
            </button>
          </div>
        </label>

        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <label className="auth-remember">
            <input
              type="checkbox"
              checked={remember}
              onChange={(e) => setRemember(e.target.checked)}
            />
            <span>Remember me</span>
          </label>
          <Link
            href="/register"
            className="auth-link"
            style={{ fontSize: "var(--text-sm, 0.875rem)" }}
          >
            Create account
          </Link>
        </div>

        <button
          type="submit"
          className="btn btn-primary auth-submit"
          disabled={submitting}
          aria-busy={submitting}
        >
          {submitting ? (
            <span style={{ display: "inline-flex", alignItems: "center", gap: "0.5rem" }}>
              <span className="btn-spinner" aria-hidden="true" />
              Signing in…
            </span>
          ) : "Sign In"}
        </button>
      </form>

      <p className="auth-footer-text">
        Don&apos;t have an account?{" "}
        <Link href="/register" className="auth-link">
          Register
        </Link>
      </p>
    </div>
  );
}
