"use client";

import { useState, useRef, useMemo, type FormEvent } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useAuth } from "@/lib/auth";

function passwordStrength(pw: string): { score: number; label: string; color: string } {
  let score = 0;
  if (pw.length >= 8) score++;
  if (pw.length >= 12) score++;
  if (/[a-z]/.test(pw) && /[A-Z]/.test(pw)) score++;
  if (/\d/.test(pw)) score++;
  if (/[^a-zA-Z0-9]/.test(pw)) score++;

  if (score <= 1) return { score, label: "Weak", color: "var(--color-loss, #ef4444)" };
  if (score <= 2) return { score, label: "Fair", color: "var(--color-warning, #eab308)" };
  if (score <= 3) return { score, label: "Good", color: "var(--color-accent, #f59e0b)" };
  return { score, label: "Strong", color: "var(--color-win, #22c55e)" };
}

export default function RegisterClient() {
  const { register, isLoading: authLoading } = useAuth();
  const router = useRouter();
  const searchParams = useSearchParams();
  const plan = searchParams.get("plan");

  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [referralCode, setReferralCode] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [generatedKey, setGeneratedKey] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const errorRef = useRef<HTMLDivElement>(null);

  const strength = useMemo(() => passwordStrength(password), [password]);
  const passwordsMatch = confirm.length === 0 || password === confirm;

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setError(null);

    if (password !== confirm) {
      setError("Passwords do not match");
      requestAnimationFrame(() => errorRef.current?.focus());
      return;
    }
    if (password.length < 8) {
      setError("Password must be at least 8 characters");
      requestAnimationFrame(() => errorRef.current?.focus());
      return;
    }

    setSubmitting(true);
    const result = await register(name, email, password, referralCode || undefined);
    setSubmitting(false);

    if (result.ok) {
      if (result.apiKey) {
        setGeneratedKey(result.apiKey);
      } else {
        router.push(plan ? `/pricing` : "/pricing");
      }
    } else {
      setError(result.error ?? "Registration failed");
      requestAnimationFrame(() => errorRef.current?.focus());
    }
  }

  function handleCopyKey() {
    if (generatedKey) {
      navigator.clipboard.writeText(generatedKey).catch(() => {
        /* fallback: select text for manual copy */
      });
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  }

  if (authLoading) return null;

  // Show API key after successful registration
  if (generatedKey) {
    return (
      <div className="auth-card">
        <div className="auth-card-header">
          <h1 className="auth-card-title">Account Created! 🎉</h1>
          <p className="auth-card-subtitle">
            Your API key has been generated. Save it now — you won&apos;t see it again.
          </p>
        </div>

        <div className="api-key-display" role="status" aria-live="polite">
          <code className="api-key-value">{generatedKey}</code>
          <button type="button" className="api-key-copy" onClick={handleCopyKey} aria-label="Copy API key">
            {copied ? "✓ Copied" : "Copy"}
          </button>
        </div>

        <button
          type="button"
          className="btn btn-primary auth-submit"
          onClick={() => router.push("/pricing")}
        >
          Choose a Plan →
        </button>

        <p className="auth-footer-text">
          You can also view your key in your{" "}
          <Link href="/account" className="auth-link">
            Account
          </Link>
        </p>
      </div>
    );
  }

  return (
    <div className="auth-card">
      <div className="auth-card-header">
        <h1 className="auth-card-title">Create Account</h1>
        <p className="auth-card-subtitle">Get started with WNBP predictions</p>
      </div>

      {error && (
        <div className="auth-error" role="alert" ref={errorRef} tabIndex={-1}>
          {error}
        </div>
      )}

      <form onSubmit={handleSubmit} className="auth-form" noValidate>
        <label className="auth-label" htmlFor="reg-name">
          <span>Display Name</span>
          <input
            id="reg-name"
            type="text"
            className="auth-input"
            placeholder="Your name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
            autoComplete="name"
            aria-required="true"
          />
        </label>

        <label className="auth-label" htmlFor="reg-email">
          <span>Email</span>
          <input
            id="reg-email"
            type="email"
            className="auth-input"
            placeholder="you@example.com"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
            autoComplete="email"
            aria-required="true"
          />
        </label>

        <label className="auth-label" htmlFor="reg-password">
          <span>Password</span>
          <div style={{ position: "relative" }}>
            <input
              id="reg-password"
              type={showPassword ? "text" : "password"}
              className="auth-input"
              placeholder="At least 8 characters"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              minLength={8}
              autoComplete="new-password"
              aria-required="true"
              aria-describedby="pw-strength"
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
          {/* Password strength meter */}
          {password.length > 0 && (
            <div id="pw-strength" aria-live="polite" aria-atomic="true" style={{ marginTop: 6 }}>
              <div className="pw-strength-bar">
                <div
                  className="pw-strength-fill"
                  style={{
                    width: `${(strength.score / 5) * 100}%`,
                    background: strength.color,
                  }}
                />
              </div>
              <span style={{ fontSize: "var(--text-xs)", color: strength.color, fontWeight: 600 }}>
                Password strength: {strength.label}
              </span>
            </div>
          )}
        </label>

        <label className="auth-label" htmlFor="reg-confirm">
          <span>Confirm Password</span>
          <input
            id="reg-confirm"
            type={showPassword ? "text" : "password"}
            className={`auth-input${!passwordsMatch ? " auth-input--error" : ""}`}
            placeholder="••••••••"
            value={confirm}
            onChange={(e) => setConfirm(e.target.value)}
            required
            minLength={8}
            autoComplete="new-password"
            aria-required="true"
            aria-invalid={!passwordsMatch}
            aria-describedby={!passwordsMatch ? "reg-confirm-err" : undefined}
          />
          {!passwordsMatch && (
            <span id="reg-confirm-err" style={{ fontSize: "var(--text-xs)", color: "var(--color-loss)", marginTop: 4 }}>
              ✗ Passwords don&apos;t match
            </span>
          )}
          {passwordsMatch && confirm.length > 0 && (
            <span style={{ fontSize: "var(--text-xs)", color: "var(--color-win)", marginTop: 4 }}>
              ✓ Passwords match
            </span>
          )}
        </label>

        <label className="auth-label" htmlFor="reg-referral">
          <span>Referral Code <span style={{ fontWeight: "normal", color: "var(--color-text-3)" }}>(optional)</span></span>
          <input
            id="reg-referral"
            type="text"
            className="auth-input"
            placeholder="Enter a referral code"
            value={referralCode}
            onChange={(e) => setReferralCode(e.target.value)}
            autoComplete="off"
          />
        </label>

        <button
          type="submit"
          className="btn btn-primary auth-submit"
          disabled={submitting || !passwordsMatch}
          aria-busy={submitting}
        >
          {submitting ? (
            <span style={{ display: "inline-flex", alignItems: "center", gap: "0.5rem" }}>
              <span className="btn-spinner" aria-hidden="true" />
              Creating account…
            </span>
          ) : "Create Account"}
        </button>
      </form>

      <p className="auth-footer-text">
        Already have an account?{" "}
        <Link href="/login" className="auth-link">
          Sign in
        </Link>
      </p>
    </div>
  );
}
