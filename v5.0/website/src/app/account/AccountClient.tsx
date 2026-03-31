"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useAuth } from "@/lib/auth";
import {
  fetchAuthAPI,
  regenerateApiKey,
  getStripePortal,
  createCheckout,
} from "@/lib/api";

/* ── Types ──────────────────────────────────────────────────────── */

interface ReferralStats {
  referral_code: string;
  total_referrals: number;
  successful_referrals: number;
  days_earned: number;
}

/* ── Confirmation Dialog ────────────────────────────────────────── */

function ConfirmDialog({
  open,
  title,
  message,
  confirmLabel = "Confirm",
  cancelLabel = "Cancel",
  danger = false,
  onConfirm,
  onCancel,
}: {
  open: boolean;
  title: string;
  message: string;
  confirmLabel?: string;
  cancelLabel?: string;
  danger?: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  const confirmRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (open) confirmRef.current?.focus();
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onCancel();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open, onCancel]);

  if (!open) return null;
  return (
    <div
      className="confirm-dialog-backdrop"
      role="dialog"
      aria-modal="true"
      aria-describedby="confirm-dialog-message"
      aria-labelledby="confirm-dialog-title"
      onClick={onCancel}
    >
      <div className="confirm-dialog" onClick={(e) => e.stopPropagation()}>
        <h3 id="confirm-dialog-title" className="confirm-dialog-title">{title}</h3>
        <p id="confirm-dialog-message" className="confirm-dialog-message">{message}</p>
        <div className="confirm-dialog-actions">
          <button className="btn btn-ghost btn-sm" onClick={onCancel}>
            {cancelLabel}
          </button>
          <button
            ref={confirmRef}
            className={`btn btn-sm ${danger ? "btn-danger" : "btn-primary"}`}
            onClick={onConfirm}
          >
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}

/* ── Notification Banner ────────────────────────────────────────── */

function NotificationBanner({ message, type, onDismiss }: { message: string; type: "error" | "info"; onDismiss: () => void }) {
  return (
    <div
      role="alert"
      className={`account-notification account-notification--${type}`}
    >
      <span>{message}</span>
      <button className="btn btn-ghost btn-sm" onClick={onDismiss} aria-label="Dismiss">✕</button>
    </div>
  );
}

/* ── Component ──────────────────────────────────────────────────── */

export default function AccountClient() {
  const { user, apiKey, isLoading, refreshUser, setApiKey, logout } = useAuth();
  const router = useRouter();

  const [keyVisible, setKeyVisible] = useState(false);
  const [keyCopied, setKeyCopied] = useState(false);
  const [codeCopied, setCodeCopied] = useState(false);
  const [regenerating, setRegenerating] = useState(false);
  const [portalLoading, setPortalLoading] = useState(false);
  const [referrals, setReferrals] = useState<ReferralStats | null>(null);
  const [upgradeLoading, setUpgradeLoading] = useState<string | null>(null);
  const [confirmDialog, setConfirmDialog] = useState<{ title: string; message: string; confirmLabel?: string; danger?: boolean; action: () => void } | null>(null);
  const [notification, setNotification] = useState<{ message: string; type: "error" | "info" } | null>(null);

  const displayKey = user?.api_key ?? apiKey;

  const loadReferrals = useCallback(async (signal?: AbortSignal) => {
    const res = await fetchAuthAPI<Record<string, unknown>>("/auth/referrals", { signal });
    if (signal?.aborted) return;
    if (res.ok && res.data) {
      const payload = (res.data as Record<string, unknown>).data ?? res.data;
      setReferrals(payload as unknown as ReferralStats);
    }
  }, []);

  useEffect(() => {
    if (!user) return;
    const controller = new AbortController();
    loadReferrals(controller.signal);
    return () => controller.abort();
  }, [user, loadReferrals]);

  if (isLoading) {
    return (
      <div className="account-page">
        <div className="account-loading">Loading…</div>
      </div>
    );
  }

  if (!user) {
    router.push("/login?callbackUrl=/dashboard");
    return null;
  }

  async function handleRegenerateKey() {
    setConfirmDialog({
      title: "Regenerate API Key?",
      message: "Your old API key will stop working immediately. Any existing integrations using it will break.",
      confirmLabel: "Regenerate",
      danger: true,
      action: async () => {
        setConfirmDialog(null);
        setRegenerating(true);
        const res = await regenerateApiKey();
        setRegenerating(false);
        if (res.ok && res.data) {
          const payload = (res.data as Record<string, unknown>).data ?? res.data;
          const newKey = (payload as Record<string, unknown>).api_key as string;
          if (newKey) {
            setApiKey(newKey);
            await refreshUser();
            setKeyVisible(true);
            setNotification({ message: "API key regenerated successfully.", type: "info" });
          }
        } else {
          setNotification({ message: "Failed to regenerate API key. Please try again.", type: "error" });
        }
      },
    });
  }

  async function handleManageSubscription() {
    setPortalLoading(true);
    const res = await getStripePortal();
    setPortalLoading(false);
    if (res.ok && res.data) {
      const payload = (res.data as Record<string, unknown>).data ?? res.data;
      const url = (payload as Record<string, unknown>).url as string;
      if (url) {
        window.location.href = url;
        return;
      }
    }
    setNotification({ message: "Could not open subscription portal. Please try again.", type: "error" });
  }

  async function handleUpgrade(tier: string) {
    setUpgradeLoading(tier);
    const res = await createCheckout(tier, "monthly");
    setUpgradeLoading(null);
    if (res.ok && res.data) {
      const payload = (res.data as Record<string, unknown>).data ?? res.data;
      const url = (payload as Record<string, unknown>).url ??
        (payload as Record<string, unknown>).checkout_url;
      if (url) {
        window.location.href = url as string;
        return;
      }
    }
    setNotification({ message: "Could not start checkout. Please try again.", type: "error" });
  }

  function copyToClipboard(text: string, setter: (v: boolean) => void) {
    navigator.clipboard.writeText(text).then(
      () => {
        setter(true);
        setTimeout(() => setter(false), 2000);
      },
      () => {
        setNotification({ message: "Failed to copy to clipboard.", type: "error" });
      },
    );
  }

  const referralCode = user.referral_code ?? referrals?.referral_code ?? "";
  const shareUrl = typeof window !== "undefined"
    ? `${window.location.origin}/register?ref=${referralCode}`
    : "";

  const tierOrder = ["free", "starter", "pro", "enterprise"];
  const currentIdx = tierOrder.indexOf(user.tier);

  return (
    <div className="account-page">
      {notification && (
        <NotificationBanner
          message={notification.message}
          type={notification.type}
          onDismiss={() => setNotification(null)}
        />
      )}
      <ConfirmDialog
        open={!!confirmDialog}
        title={confirmDialog?.title ?? ""}
        message={confirmDialog?.message ?? ""}
        confirmLabel={confirmDialog?.confirmLabel}
        danger={confirmDialog?.danger}
        onConfirm={() => confirmDialog?.action()}
        onCancel={() => setConfirmDialog(null)}
      />
      <h1 className="account-title">Account Settings</h1>

      {/* Profile Section */}
      <section className="account-section">
        <h2 className="account-section-title">Profile</h2>
        <div className="account-card">
          <div className="account-profile-row">
            <div className="account-avatar">
              {(user.name || user.email).charAt(0).toUpperCase()}
            </div>
            <div className="account-profile-info">
              <h3 className="account-profile-name">{user.name}</h3>
              <p className="account-profile-email">{user.email}</p>
              <span className={`tier-badge tier-badge-${user.tier}`}>
                {user.tier.charAt(0).toUpperCase() + user.tier.slice(1)}
              </span>
            </div>
          </div>
        </div>
      </section>

      {/* API Key Section */}
      <section className="account-section">
        <h2 className="account-section-title">API Key</h2>
        <div className="account-card">
          {displayKey ? (
            <>
              <div className="account-key-row">
                <code
                  className="account-key-value"
                  aria-label={keyVisible ? "API key displayed" : "API key hidden"}
                  aria-live="polite"
                >
                  {keyVisible ? displayKey : "••••••••••••••••••••••••"}
                </code>
                <div className="account-key-actions">
                  <button
                    className="btn btn-ghost btn-sm"
                    onClick={() => setKeyVisible((v) => !v)}
                  >
                    {keyVisible ? "Hide" : "Reveal"}
                  </button>
                  <button
                    className="btn btn-ghost btn-sm"
                    onClick={() => copyToClipboard(displayKey, setKeyCopied)}
                  >
                    {keyCopied ? "✓ Copied" : "Copy"}
                  </button>
                </div>
              </div>
              <button
                className="btn btn-secondary btn-sm"
                onClick={handleRegenerateKey}
                disabled={regenerating}
                style={{ marginTop: "var(--space-3)" }}
              >
                {regenerating ? "Regenerating…" : "Regenerate Key"}
              </button>
            </>
          ) : (
            <p className="account-muted">
              No API key found. Upgrade to a paid plan to get API access.
            </p>
          )}
        </div>
      </section>

      {/* Subscription Section */}
      <section className="account-section">
        <h2 className="account-section-title">Subscription</h2>
        <div className="account-card">
          <div className="account-sub-row">
            <div>
              <p className="account-sub-tier">
                Current plan:{" "}
                <strong className={`tier-badge tier-badge-${user.tier}`}>
                  {user.tier.charAt(0).toUpperCase() + user.tier.slice(1)}
                </strong>
              </p>
            </div>
            {user.tier !== "free" && (
              <button
                className="btn btn-primary btn-sm"
                onClick={handleManageSubscription}
                disabled={portalLoading}
              >
                {portalLoading ? "Loading…" : "Manage Subscription"}
              </button>
            )}
          </div>

          {/* Upgrade / downgrade options */}
          {currentIdx < tierOrder.length - 1 && (
            <div className="account-upgrade-row">
              {tierOrder.slice(currentIdx + 1).map((t) => (
                <button
                  key={t}
                  className="btn btn-outline btn-sm"
                  onClick={() => handleUpgrade(t)}
                  disabled={upgradeLoading === t}
                >
                  {upgradeLoading === t
                    ? "Loading…"
                    : `Upgrade to ${t.charAt(0).toUpperCase() + t.slice(1)}`}
                </button>
              ))}
            </div>
          )}

          {user.tier === "free" && (
            <p className="account-muted" style={{ marginTop: "var(--space-3)" }}>
              <Link href="/pricing" className="auth-link">
                View pricing plans →
              </Link>
            </p>
          )}
        </div>
      </section>

      {/* Referral Section */}
      <section className="account-section">
        <h2 className="account-section-title">Referrals</h2>
        <div className="account-card">
          {referralCode ? (
            <>
              <div className="account-referral-code-row">
                <div>
                  <span className="account-label">Your referral code</span>
                  <code className="account-referral-code">{referralCode}</code>
                </div>
                <button
                  className="btn btn-ghost btn-sm"
                  onClick={() => copyToClipboard(referralCode, setCodeCopied)}
                >
                  {codeCopied ? "✓ Copied" : "Copy Code"}
                </button>
              </div>

              {shareUrl && (
                <div className="account-share-row">
                  <span className="account-label">Share link</span>
                  <div className="account-share-link">
                    <code className="account-share-url">{shareUrl}</code>
                    <button
                      className="btn btn-ghost btn-sm"
                      onClick={() => copyToClipboard(shareUrl, setCodeCopied)}
                    >
                      Copy
                    </button>
                  </div>
                </div>
              )}

              {referrals && (
                <dl className="account-referral-stats" aria-label="Referral statistics">
                  <div className="account-stat">
                    <dd className="account-stat-value">{referrals.total_referrals}</dd>
                    <dt className="account-stat-label">Total Referrals</dt>
                  </div>
                  <div className="account-stat">
                    <dd className="account-stat-value">{referrals.successful_referrals}</dd>
                    <dt className="account-stat-label">Successful</dt>
                  </div>
                  <div className="account-stat">
                    <dd className="account-stat-value">{referrals.days_earned}</dd>
                    <dt className="account-stat-label">Days Earned</dt>
                  </div>
                </dl>
              )}
            </>
          ) : (
            <p className="account-muted">
              Your referral code will appear here once your account is fully set up.
            </p>
          )}
        </div>
      </section>

      {/* Danger zone */}
      <section className="account-section">
        <button
          className="btn btn-secondary btn-sm"
          onClick={() => {
            logout();
            router.push("/");
          }}
        >
          Sign Out
        </button>
      </section>
    </div>
  );
}
