"use client";

import { useState, useEffect, useCallback } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useAuth } from "@/lib/auth";
import { fetchAPI } from "@/lib/api";
import { useRecentlyViewed } from "@/lib/hooks";
import UpcomingEventsWidget from "@/components/UpcomingEventsWidget";

/* ── Types ──────────────────────────────────────────────────────── */

interface UsageStats {
  requests_today: number;
  request_limit: number;
  period_start?: string;
  period_end?: string;
}

interface SubscriptionInfo {
  tier: string;
  status: string;
  next_billing_date?: string;
  cancel_at?: string;
}

interface TrainedModel {
  sport: string;
  size_bytes: number;
  modified_at: string;
}

interface LeaderboardEntry {
  rank: number;
  sport: string;
  total_evaluated: number;
  correct: number;
  accuracy: number;
}

/* ── Component ──────────────────────────────────────────────────── */

export default function DashboardClient() {
  const { user, token, apiKey, isLoading, logout, setApiKey, refreshUser } = useAuth();
  const router = useRouter();
  const { recentPages } = useRecentlyViewed();

  const [showKey, setShowKey] = useState(false);
  const [copied, setCopied] = useState(false);
  const [usage, setUsage] = useState<UsageStats | null>(null);
  const [subscription, setSubscription] = useState<SubscriptionInfo | null>(null);
  const [creatingKey, setCreatingKey] = useState(false);
  const [revoking, setRevoking] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [referralData, setReferralData] = useState<{
    rewards: Array<{tier_rewarded: string; days_granted: number; expires_at: string}>;
    active_free_days: Record<string, number>;
  } | null>(null);
  const [referralTierPref, setReferralTierPref] = useState<string>("starter");
  const [savingReferralTier, setSavingReferralTier] = useState(false);
  const [referralCopied, setReferralCopied] = useState(false);
  const [trainedModels, setTrainedModels] = useState<TrainedModel[]>([]);
  const [leaderboard, setLeaderboard] = useState<LeaderboardEntry[]>([]);
  const [fetchErrors, setFetchErrors] = useState<{ models?: boolean; leaderboard?: boolean }>({});
  const [confirmDialog, setConfirmDialog] = useState<{
    open: boolean;
    title: string;
    message: string;
    onConfirm: () => void;
  }>({ open: false, title: "", message: "", onConfirm: () => {} });

  const authHeaders: Record<string, string> = token ? { Authorization: `Bearer ${token}` } : {};

  const API_BASE = typeof window === "undefined"
    ? (process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000")
    : "/api/proxy";

  useEffect(() => {
    const ac = new AbortController();
    fetch(`${API_BASE}/v1/predictions/trained-sports`, { cache: "default", signal: ac.signal })
      .then((r) => r.ok ? r.json() : null)
      .then((j) => { if (j?.data && !ac.signal.aborted) setTrainedModels(j.data as TrainedModel[]); })
      .catch((e) => { if (!ac.signal.aborted) { setFetchErrors((p) => ({ ...p, models: true })); } });
    return () => ac.abort();
  }, [API_BASE]);

  useEffect(() => {
    const ac = new AbortController();
    fetch(`${API_BASE}/v1/predictions/leaderboard?min_evaluated=5&limit=5`, { cache: "default", signal: ac.signal })
      .then((r) => r.ok ? r.json() : null)
      .then((j) => { if (j?.data && !ac.signal.aborted) setLeaderboard(j.data as LeaderboardEntry[]); })
      .catch((e) => { if (!ac.signal.aborted) { setFetchErrors((p) => ({ ...p, leaderboard: true })); } });
    return () => ac.abort();
  }, [API_BASE]);

  const fetchUsage = useCallback(async () => {
    if (!token) return;
    const res = await fetchAPI<Record<string, unknown>>("/auth/usage", {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (res.ok && res.data) {
      const d = (res.data as Record<string, unknown>).data ?? res.data;
      setUsage(d as unknown as UsageStats);
    }
  }, [token]);

  const fetchSubscription = useCallback(async () => {
    if (!token) return;
    const res = await fetchAPI<Record<string, unknown>>("/auth/subscription", {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (res.ok && res.data) {
      const d = (res.data as Record<string, unknown>).data ?? res.data;
      setSubscription(d as unknown as SubscriptionInfo);
    }
  }, [token]);

  const fetchReferrals = useCallback(async () => {
    if (!token) return;
    const res = await fetchAPI<Record<string, unknown>>("/auth/referrals", {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (res.ok && res.data) {
      const d = (res.data as Record<string, unknown>).data ?? res.data;
      setReferralData(d as any);
    }
  }, [token]);

  useEffect(() => {
    if (!isLoading && !user) {
      router.push("/login?callbackUrl=/dashboard");
      return;
    }
    // Fetch usage and subscription in parallel for faster load
    Promise.all([fetchUsage(), fetchSubscription(), fetchReferrals()]);
  }, [isLoading, user, router, fetchUsage, fetchSubscription, fetchReferrals]);

  // Initialize referral tier pref from user data
  useEffect(() => {
    if (user && (user as any).referral_reward_tier) {
      setReferralTierPref((user as any).referral_reward_tier);
    }
  }, [user]);

  async function handleCreateKey() {
    setActionError(null);
    setCreatingKey(true);
    const res = await fetchAPI<Record<string, unknown>>("/auth/api-key", {
      method: "POST",
      headers: authHeaders,
    });
    setCreatingKey(false);
    if (res.ok && res.data) {
      const d = (res.data as Record<string, unknown>).data ?? res.data;
      const key = (d as Record<string, unknown>).api_key as string;
      if (key) {
        setApiKey(key);
        setShowKey(true);
      }
    } else {
      setActionError("Failed to create API key");
    }
  }

  async function handleRevokeKey() {
    setConfirmDialog({
      open: true,
      title: "Revoke API Key",
      message: "Revoke your API key? Any integrations using it will stop working.",
      onConfirm: async () => {
        setConfirmDialog((prev) => ({ ...prev, open: false }));
        setActionError(null);
        setRevoking(true);
        const res = await fetchAPI<Record<string, unknown>>("/auth/api-key", {
          method: "DELETE",
          headers: authHeaders,
        });
        setRevoking(false);
        if (res.ok) {
          setApiKey(null);
          setShowKey(false);
        } else {
          setActionError("Failed to revoke API key");
        }
      },
    });
  }

  async function handleCancelSubscription() {
    setConfirmDialog({
      open: true,
      title: "Cancel Subscription",
      message: "Cancel your subscription? You'll keep access until the end of your billing period.",
      onConfirm: async () => {
        setConfirmDialog((prev) => ({ ...prev, open: false }));
        setActionError(null);
        setCancelling(true);
        const res = await fetchAPI<Record<string, unknown>>("/auth/subscription", {
          method: "DELETE",
          headers: authHeaders,
        });
        setCancelling(false);
        if (res.ok) {
          await refreshUser();
          await fetchSubscription();
        } else {
          setActionError("Failed to cancel subscription");
        }
      },
    });
  }

  async function handleSaveReferralTier(newTier: string) {
    setSavingReferralTier(true);
    const res = await fetchAPI<Record<string, unknown>>("/auth/referrals/reward-tier", {
      method: "PUT",
      headers: { ...authHeaders, "Content-Type": "application/json" },
      body: JSON.stringify({ tier: newTier }),
    });
    setSavingReferralTier(false);
    if (res.ok) setReferralTierPref(newTier);
  }

  function handleCopyReferralLink() {
    const link = `${window.location.origin}/register?ref=${user?.referral_code ?? ""}`;
    navigator.clipboard.writeText(link);
    setReferralCopied(true);
    setTimeout(() => setReferralCopied(false), 2000);
  }

  function handleCopyKey() {
    if (apiKey) {
      navigator.clipboard.writeText(apiKey);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  }

  function maskKey(key: string) {
    if (key.length <= 8) return "••••••••";
    return key.slice(0, 4) + "••••••••" + key.slice(-4);
  }

  function handleLogout() {
    logout();
    router.push("/");
  }

  if (isLoading || !user) return null;

  const tier = user.tier ?? "free";
  const usagePercent = usage ? Math.min(100, (usage.requests_today / usage.request_limit) * 100) : 0;

  return (
    <>
    <div className="dashboard-grid">
      {/* Profile card */}
      <section className="card dashboard-profile" aria-labelledby="dash-profile-heading">
        <div className="card-header">
          <h2 id="dash-profile-heading" className="card-heading">Profile</h2>
        </div>
        <div className="card-body">
          <div className="dashboard-avatar">
            {user.name.charAt(0).toUpperCase()}
          </div>
          <div className="dashboard-profile-info">
            <h3 className="dashboard-name">{user.name}</h3>
            <p className="dashboard-email">{user.email}</p>
            <span className={`tier-badge tier-badge-${tier}`}>{tier}</span>
          </div>
          <button className="btn btn-outline btn-sm dashboard-mt-4" onClick={handleLogout}>
            Sign Out
          </button>
        </div>
      </section>

      {/* API Key card */}
      <section className="card dashboard-apikey" aria-labelledby="dash-apikey-heading">
        <div className="card-header">
          <h2 id="dash-apikey-heading" className="card-heading">API Key</h2>
        </div>
        <div className="card-body">
          {apiKey ? (
            <>
              <div className="api-key-display">
                <code className="api-key-value">
                  {showKey ? apiKey : maskKey(apiKey)}
                </code>
                <div className="api-key-actions">
                  <button
                    type="button"
                    className="api-key-toggle"
                    onClick={() => setShowKey(!showKey)}
                  >
                    {showKey ? "Hide" : "Show"}
                  </button>
                  <button type="button" className="api-key-copy" onClick={handleCopyKey}>
                    {copied ? "✓ Copied" : "Copy"}
                  </button>
                </div>
              </div>
              <button
                className="btn btn-outline btn-sm dashboard-mt-3 dashboard-text-loss"
                onClick={handleRevokeKey}
                disabled={revoking}
              >
                {revoking ? "Revoking…" : "Revoke Key"}
              </button>
            </>
          ) : (
            <div className="dashboard-empty-state">
              <p className="dashboard-text-muted-sm">
                No API key generated yet.
              </p>
              <button
                className="btn btn-primary btn-sm"
                onClick={handleCreateKey}
                disabled={creatingKey}
              >
                {creatingKey ? "Creating…" : "Generate API Key"}
              </button>
            </div>
          )}
          {actionError && <p role="alert" className="auth-error dashboard-mt-3">{actionError}</p>}
        </div>
      </section>

      {/* Usage card */}
      <section className="card dashboard-usage" aria-labelledby="dash-usage-heading">
        <div className="card-header">
          <h2 id="dash-usage-heading" className="card-heading">Usage Today</h2>
        </div>
        <div className="card-body">
          {usage ? (
            <>
              <div
                className="dashboard-usage-bar"
                role="progressbar"
                aria-valuenow={usage.requests_today}
                aria-valuemin={0}
                aria-valuemax={usage.request_limit}
                aria-label={`API usage: ${usage.requests_today} of ${usage.request_limit} requests`}
              >
                <div className="dashboard-usage-fill" style={{ width: `${usagePercent}%` }} />
              </div>
              <div className="dashboard-usage-stats">
                <span className="dashboard-usage-count">
                  {usage.requests_today.toLocaleString()} / {usage.request_limit.toLocaleString()}
                </span>
                <span className="dashboard-usage-label">requests</span>
              </div>
            </>
          ) : (
            <p className="dashboard-text-muted-sm">
              Usage data unavailable
            </p>
          )}
        </div>
      </section>

      {/* Subscription card */}
      <section className="card dashboard-subscription" aria-labelledby="dash-sub-heading">
        <div className="card-header">
          <h2 id="dash-sub-heading" className="card-heading">Subscription</h2>
        </div>
        <div className="card-body">
          <div className="dashboard-sub-details">
            <div className="dashboard-sub-row">
              <span className="dashboard-sub-label">Plan</span>
              <span className={`tier-badge tier-badge-${tier}`}>{tier}</span>
            </div>
            {subscription && (
              <>
                <div className="dashboard-sub-row">
                  <span className="dashboard-sub-label">Status</span>
                  <span className={`badge ${subscription.status === "active" ? "badge-win" : ""}`}>
                    {subscription.status}
                  </span>
                </div>
                {subscription.next_billing_date && (
                  <div className="dashboard-sub-row">
                    <span className="dashboard-sub-label">Next Billing</span>
                    <span>{new Date(subscription.next_billing_date).toLocaleDateString()}</span>
                  </div>
                )}
              </>
            )}
          </div>

          <div className="dashboard-sub-actions">
            <Link href="/pricing" className="btn btn-primary btn-sm">
              Upgrade Plan
            </Link>
            {tier !== "free" && (
              <button
                className="btn btn-outline btn-sm dashboard-text-loss"
                onClick={handleCancelSubscription}
                disabled={cancelling}
              >
                {cancelling ? "Cancelling…" : "Cancel Subscription"}
              </button>
            )}
          </div>
          {actionError && <p role="alert" className="auth-error dashboard-mt-3">{actionError}</p>}
        </div>
      </section>
    </div>

      {/* Referrals card */}
      <section className="card dashboard-referral" aria-labelledby="dash-referral-heading">
        <div className="card-header">
          <h2 id="dash-referral-heading" className="card-heading">Referrals</h2>
        </div>
        <div className="card-body">
          {/* Referral link */}
          {user?.referral_code && (
            <div className="dashboard-sub-row" style={{ marginBottom: "var(--space-3)" }}>
              <span className="dashboard-sub-label">Your referral link</span>
              <button
                type="button"
                className="api-key-copy"
                onClick={handleCopyReferralLink}
              >
                {referralCopied ? "✓ Copied!" : "Copy Link"}
              </button>
            </div>
          )}

          {/* Reward tier preference */}
          <div className="dashboard-sub-row" style={{ marginBottom: "var(--space-3)" }}>
            <span className="dashboard-sub-label">Reward tier</span>
            <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
              {(["starter", "pro", "enterprise"] as const).map((t) => (
                <button
                  key={t}
                  type="button"
                  className={`btn btn-sm ${referralTierPref === t ? "btn-primary" : "btn-outline"}`}
                  onClick={() => { setReferralTierPref(t); handleSaveReferralTier(t); }}
                  disabled={savingReferralTier}
                >
                  {t}
                </button>
              ))}
            </div>
          </div>

          {/* Earned days */}
          {referralData?.active_free_days && Object.keys(referralData.active_free_days).length > 0 && (
            <div>
              <p className="dashboard-sub-label" style={{ marginBottom: "0.25rem" }}>Earned free days</p>
              <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem" }}>
                {Object.entries(referralData.active_free_days).map(([tier, days]) => (
                  <span key={tier} className={`tier-badge tier-badge-${tier}`}>
                    {tier}: {days}d
                  </span>
                ))}
              </div>
            </div>
          )}
          {(!referralData?.active_free_days || Object.keys(referralData.active_free_days).length === 0) && (
            <p className="dashboard-text-muted-sm">
              Share your referral link to earn free subscription days when friends subscribe.
            </p>
          )}
        </div>
      </section>

      {/* Prop Models Status */}
      {(trainedModels.length > 0 || fetchErrors.models) && (
        <section className="card dashboard-mt-6" aria-labelledby="dash-models-heading">
          <div className="card-header">
            <h2 id="dash-models-heading" className="card-heading">Prop Models</h2>
          </div>
          {fetchErrors.models ? (
            <div className="card-body dashboard-center-muted">
              <p>Could not load model status.</p>
            </div>
          ) : (
          <div className="card-body dashboard-model-grid">
            {trainedModels.map((m) => (
              <div key={m.sport} className="dash-model-card">
                <span className="dash-model-sport">
                  {m.sport}
                </span>
                <span className="dash-model-badge-active">
                  Active
                </span>
                <span className="dash-model-meta">
                  {(m.size_bytes / 1024).toFixed(0)} KB
                </span>
                <span className="dash-model-meta">
                  Updated {new Date(m.modified_at).toLocaleDateString()}
                </span>
              </div>
            ))}
          </div>
          )}
        </section>
      )}

      {/* Accuracy Leaderboard */}
            {(leaderboard.length > 0 || fetchErrors.leaderboard) && (
              <section className="card dashboard-mt-6" aria-labelledby="dash-leaderboard-heading">
                <div className="card-header dashboard-card-header-flex">
                  <h2 id="dash-leaderboard-heading" className="card-heading">Top Sports by Accuracy</h2>
                  <Link href="/leaderboard" className="dashboard-link-accent">
                    View all →
                  </Link>
                </div>
                <div className="card-body dashboard-pad-compact">
                  <div className="responsive-table-wrap">
                    <table className="responsive-table dashboard-leaderboard-table" aria-label="Top sports by accuracy">
                      <thead>
                        <tr className="dashboard-leaderboard-thead">
                          <th className="dash-th-left">#</th>
                          <th className="dash-th-left">Sport</th>
                          <th className="dash-th-right">Accuracy</th>
                          <th className="dash-th-right">Evaluated</th>
                        </tr>
                      </thead>
                      <tbody>
                        {leaderboard.map((entry) => (
                          <tr key={entry.sport} className="dashboard-leaderboard-row">
                            <td className="dash-td-rank">
                              {entry.rank === 1 ? "🥇" : entry.rank === 2 ? "🥈" : entry.rank === 3 ? "🥉" : entry.rank}
                            </td>
                            <td className="dash-td-sport">
                              {entry.sport}
                            </td>
                            <td className="dash-td-right">
                              <div className="dash-accuracy-cell">
                                <div className="dash-accuracy-track">
                                  <div className="dash-accuracy-fill" style={{
                                    width: `${entry.accuracy}%`,
                                    background: entry.accuracy >= 60 ? "var(--color-win, #16a34a)" : entry.accuracy >= 50 ? "var(--color-draw, #d97706)" : "var(--color-loss, #dc2626)",
                                  }} />
                                </div>
                                <span className="dash-accuracy-value">
                                  {entry.accuracy.toFixed(1)}%
                                </span>
                              </div>
                            </td>
                            <td className="dash-td-eval">
                              {entry.total_evaluated}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </section>
            )}

            {/* Upcoming Racing & Golf Events */}
            <UpcomingEventsWidget sports={["f1", "indycar", "golf", "lpga"]} days={60} maxEvents={8} apiBase={API_BASE} />

            {/* Recently Viewed */}
            {recentPages.length > 0 && (
              <section className="card dashboard-mt-6" aria-labelledby="dash-recent-heading">
                <div className="card-header">
                  <h2 id="dash-recent-heading" className="card-heading">Recently Viewed</h2>
                </div>
                <div className="card-body dashboard-pad-compact">
                  <div className="dashboard-recent-links">
                    {recentPages.slice(0, 8).map((p) => (
                      <Link
                        key={p.path}
                        href={p.path}
                        className="btn btn-ghost btn-sm dashboard-text-sm"
                      >
                        {p.title}
                      </Link>
                    ))}
                  </div>
                </div>
              </section>
            )}

            {/* Confirmation Dialog */}
      {confirmDialog.open && (
        <div
          className="confirm-overlay"
          role="dialog"
          aria-modal="true"
          aria-labelledby="dashboard-confirm-title"
          aria-describedby="dashboard-confirm-message"
          onClick={() => setConfirmDialog((prev) => ({ ...prev, open: false }))}
        >
          <div className="confirm-dialog" onClick={(e) => e.stopPropagation()}>
            <h3 id="dashboard-confirm-title">{confirmDialog.title}</h3>
            <p id="dashboard-confirm-message">{confirmDialog.message}</p>
            <div className="confirm-actions">
              <button
                className="btn btn-outline btn-sm"
                onClick={() => setConfirmDialog((prev) => ({ ...prev, open: false }))}
              >
                Cancel
              </button>
              <button className="btn btn-primary btn-sm" onClick={confirmDialog.onConfirm} autoFocus>
                Confirm
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
