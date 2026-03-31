"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { useAuth } from "@/lib/auth";
import { createCheckout } from "@/lib/api";
import { sanitizeUrl } from "@/lib/formatters";

/* ── Tier data ──────────────────────────────────────────────────── */

interface TierDef {
  id: string;
  name: string;
  monthlyPrice: number;
  yearlyPrice: number;
  description: string;
  features: string[];
  resultsPerQuery: string;
  cta: string;
  popular?: boolean;
}

const TIERS: TierDef[] = [
  {
    id: "free",
    name: "Free",
    monthlyPrice: 0,
    yearlyPrice: 0,
    description: "Get started with the basics",
    features: [
      "Games & Standings",
      "News feed",
    ],
    resultsPerQuery: "10 results per query",
    cta: "Get Started",
  },
  {
    id: "starter",
    name: "Starter",
    monthlyPrice: 5,
    yearlyPrice: 48,
    description: "Unlock predictions & full stats",
    features: [
      "Everything in Free",
      "Full predictions",
      "All player & team stats",
    ],
    resultsPerQuery: "200 results per query",
    cta: "Subscribe",
  },
  {
    id: "pro",
    name: "Pro",
    monthlyPrice: 20,
    yearlyPrice: 192,
    description: "Serious tools for serious analysts",
    popular: true,
    features: [
      "Everything in Starter",
      "Live odds tracking",
      "Advanced analytics",
      "Season simulator access",
    ],
    resultsPerQuery: "1,000 results per query",
    cta: "Go Pro",
  },
  {
    id: "enterprise",
    name: "Enterprise",
    monthlyPrice: 50,
    yearlyPrice: 480,
    description: "Full API access & priority support",
    features: [
      "Everything in Pro",
      "API data export (curl access)",
      "Enterprise API documentation",
      "Unlimited results",
      "Priority support",
    ],
    resultsPerQuery: "Unlimited",
    cta: "Contact Sales",
  },
];

const REFERRAL_REWARDS = [
  { tier: "Starter", days: 7 },
  { tier: "Pro", days: 5 },
  { tier: "Enterprise", days: 3 },
];

const COMPARISON_FEATURES = [
  { feature: "Sports covered", free: "All 20+", starter: "All 20+", pro: "All 20+", enterprise: "All 20+" },
  { feature: "Daily predictions", free: "3", starter: "50", pro: "Unlimited", enterprise: "Unlimited" },
  { feature: "Results per query", free: "10", starter: "100", pro: "1,000", enterprise: "Unlimited" },
  { feature: "Confidence scores", free: "✕", starter: "✓", pro: "✓", enterprise: "✓" },
  { feature: "Live odds tracking", free: "✕", starter: "✕", pro: "✓", enterprise: "✓" },
  { feature: "Player prop analysis", free: "✕", starter: "✕", pro: "✓", enterprise: "✓" },
  { feature: "Season simulator", free: "✕", starter: "✕", pro: "✓", enterprise: "✓" },
  { feature: "API access (curl)", free: "✕", starter: "✕", pro: "✕", enterprise: "✓" },
  { feature: "Priority support", free: "✕", starter: "✕", pro: "✕", enterprise: "✓" },
];

const PRICING_FAQ = [
  {
    q: "Can I switch plans at any time?",
    a: "Yes! You can upgrade or downgrade your plan at any time. When upgrading, you'll get immediate access to the new features. Downgrades take effect at the end of your current billing period.",
  },
  {
    q: "What happens when my free trial ends?",
    a: "You'll continue to have access to the free tier features. No credit card is required for the free plan, and you won't be charged automatically.",
  },
  {
    q: "Do you offer refunds?",
    a: "We offer a 7-day money-back guarantee on all paid plans. If you're not satisfied, contact us within 7 days of your purchase for a full refund.",
  },
  {
    q: "How accurate are the predictions?",
    a: "Our models are trained on historical data across 20+ leagues and continuously calibrated. Check the Model Health page for real-time accuracy metrics, calibration curves, and confidence intervals.",
  },
  {
    q: "What sports do you cover?",
    a: "We cover NBA, MLB, NFL, NHL, WNBA, NCAAB, NCAAF, MLS, EPL, La Liga, Bundesliga, Serie A, Ligue 1, ATP, WTA, UFC, PGA, F1, and CS2 — with more leagues being added regularly.",
  },
  {
    q: "How does the referral program work?",
    a: "Share your unique referral code with friends. When they sign up using your code, both of you receive free premium days based on your plan tier.",
  },
];

/* ── Component ──────────────────────────────────────────────────── */

export default function PricingClient() {
  const { user, isLoading } = useAuth();
  const router = useRouter();
  const currentTier = user?.tier ?? "free";
  const [yearly, setYearly] = useState(false);
  const [loadingTier, setLoadingTier] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function handleSubscribe(tier: TierDef) {
    setError(null);
    if (tier.id === "free") {
      router.push("/register");
      return;
    }
    if (!user) {
      router.push(`/register?plan=${tier.id}`);
      return;
    }

    setLoadingTier(tier.id);
    try {
      const res = await createCheckout(tier.id, yearly ? "yearly" : "monthly");
      if (res.ok && res.data) {
        const payload = (res.data as Record<string, unknown>).data ?? res.data;
        const url = (payload as Record<string, unknown>).url ??
          (payload as Record<string, unknown>).checkout_url;
        if (url) {
          const safeUrl = sanitizeUrl(url as string);
          if (safeUrl !== "#") {
            window.location.href = safeUrl;
            return;
          }
        }
      }
      setError("Could not create checkout session. Please try again.");
    } catch {
      setError("Something went wrong. Please try again.");
    } finally {
      setLoadingTier(null);
    }
  }

  function formatPrice(tier: TierDef) {
    if (tier.monthlyPrice === 0) return { amount: "$0", period: "forever" };
    if (yearly) {
      return {
        amount: `$${tier.yearlyPrice}`,
        period: "/year",
        monthly: `$${(tier.yearlyPrice / 12).toFixed(2)}/mo`,
      };
    }
    return { amount: `$${tier.monthlyPrice}`, period: "/month" };
  }

  function yearlySavingsPercent(tier: TierDef) {
    if (tier.monthlyPrice === 0) return 0;
    return Math.round((1 - tier.yearlyPrice / (tier.monthlyPrice * 12)) * 100);
  }

  return (
    <>
      {/* Hero */}
      <div className="pricing-hero">
        <h1 className="pricing-hero-title">
          Choose Your <span style={{ color: "var(--color-brand)" }}>Edge</span>
        </h1>
        <p className="pricing-hero-subtitle">
          Data-driven predictions across 14+ sports. Pick the plan that matches your game.
        </p>

        {/* Monthly / Yearly toggle */}
        <div className="billing-toggle">
          <span className={`billing-toggle-label${!yearly ? " active" : ""}`}>Monthly</span>
          <button
            className={`billing-toggle-switch${yearly ? " on" : ""}`}
            onClick={() => setYearly((v) => !v)}
            role="switch"
            aria-checked={yearly}
            aria-label="Toggle annual billing"
          >
            <span className="billing-toggle-knob" />
          </button>
          <span className={`billing-toggle-label${yearly ? " active" : ""}`}>
            Yearly <span className="billing-toggle-save">Save ~20%</span>
          </span>
        </div>
      </div>

      {/* Error banner */}
      {error && (
        <div
          role="alert"
          style={{
            maxWidth: 600,
            margin: "0 auto var(--space-4)",
            padding: "var(--space-3) var(--space-4)",
            borderRadius: "var(--radius-md, 8px)",
            backgroundColor: "color-mix(in srgb, var(--color-loss, #dc2626) 10%, transparent)",
            border: "1px solid color-mix(in srgb, var(--color-loss, #dc2626) 30%, transparent)",
            color: "var(--color-loss, #dc2626)",
            fontSize: "var(--text-sm)",
            textAlign: "center",
          }}
        >
          {error}
        </div>
      )}

      {/* Tier cards */}
      <div className="pricing-grid">
        {TIERS.map((tier) => {
          const isCurrent = !isLoading && user && currentTier === tier.id;
          const price = formatPrice(tier);
          const savings = yearlySavingsPercent(tier);
          return (
            <div
              key={tier.id}
              className={`tier-card${tier.popular ? " popular" : ""}${isCurrent ? " current" : ""}`}
            >
              {tier.popular && <div className="tier-popular-badge">MOST POPULAR</div>}
              {isCurrent && <div className="tier-current-badge">Current Plan</div>}

              <div className="tier-card-header">
                <h3 className="tier-card-name" style={{ margin: 0, fontSize: "inherit" }}>
                  <span className={`tier-badge tier-badge-${tier.id}`}>{tier.name}</span>
                </h3>
                <div className="tier-price">
                  <span className="tier-price-amount">{price.amount}</span>
                  <span className="tier-price-period">{price.period}</span>
                </div>
                {yearly && price.monthly && (
                  <p className="tier-price-effective">
                    {price.monthly} · <span className="tier-price-savings">Save {savings}%</span>
                  </p>
                )}
                <p className="tier-description">{tier.description}</p>
              </div>

              <ul className="tier-features">
                {tier.features.map((f) => (
                  <li key={f} className="tier-feature">
                    <span className="tier-feature-check">✓</span>
                    {f}
                  </li>
                ))}
                <li className="tier-feature">
                  <span className="tier-feature-check">✓</span>
                  {tier.resultsPerQuery}
                </li>
              </ul>

              <div className="tier-card-footer">
                {isCurrent ? (
                  <button className="btn btn-outline tier-btn" disabled>
                    Current Plan
                  </button>
                ) : (
                  <button
                    className={`btn ${tier.popular ? "btn-primary" : "btn-outline"} tier-btn`}
                    onClick={() => handleSubscribe(tier)}
                    disabled={loadingTier === tier.id}
                    aria-busy={loadingTier === tier.id}
                  >
                    {loadingTier === tier.id ? "Loading…" : tier.cta}
                  </button>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {/* Feature comparison table */}
      <div className="responsive-table-wrap" style={{ maxWidth: 840, margin: "2rem auto 0", padding: "0 var(--space-4)" }}>
        <h2 style={{ textAlign: "center", fontSize: "1.5rem", fontWeight: 700, marginBottom: "1rem" }}>
          Compare Plans
        </h2>
        <table className="comparison-table responsive-table pricing-comparison-table" role="table">
          <caption className="sr-only">Feature comparison across pricing tiers</caption>
          <thead>
            <tr>
              <th scope="col" style={{ textAlign: "left" }}>Feature</th>
              <th scope="col">Free</th>
              <th scope="col">Starter</th>
              <th scope="col">Pro</th>
              <th scope="col">Enterprise</th>
            </tr>
          </thead>
          <tbody>
            {COMPARISON_FEATURES.map((row) => (
              <tr key={row.feature}>
                <td style={{ fontWeight: 500 }}>{row.feature}</td>
                <td style={{ textAlign: "center" }}>{row.free}</td>
                <td style={{ textAlign: "center" }}>{row.starter}</td>
                <td style={{ textAlign: "center" }}>{row.pro}</td>
                <td style={{ textAlign: "center" }}>{row.enterprise}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Referral section */}
      <div className="pricing-referrals">
        <h2 className="pricing-referrals-title">
          🎁 Share &amp; Earn Free Days
        </h2>
        <p className="pricing-referrals-subtitle">
          Invite friends with your referral code and both of you get free premium time.
        </p>

        <div className="referral-rewards-grid">
          {REFERRAL_REWARDS.map((r) => (
            <div key={r.tier} className="referral-reward-card">
              <span className={`tier-badge tier-badge-${r.tier.toLowerCase()}`}>{r.tier}</span>
              <span className="referral-reward-days">{r.days} days free</span>
              <span className="referral-reward-note">per referral</span>
            </div>
          ))}
        </div>

        {user ? (
          <p className="referral-cta-text">
            Share your code from your{" "}
            <Link href="/account" className="auth-link">Account page</Link>
          </p>
        ) : (
          <p className="referral-cta-text">
            <Link href="/register" className="auth-link">Sign up</Link> to get your referral code
          </p>
        )}
      </div>

      {/* FAQ Section */}
      <div style={{ maxWidth: 720, margin: "3rem auto 0", padding: "0 var(--space-4)" }}>
        <h2 style={{ textAlign: "center", fontSize: "1.5rem", fontWeight: 700, marginBottom: "1.5rem" }}>
          Frequently Asked Questions
        </h2>
        {PRICING_FAQ.map((faq) => (
          <details key={faq.q} className="faq-item" style={{ marginBottom: "0.75rem" }}>
            <summary
              style={{
                fontWeight: 600,
                cursor: "pointer",
                padding: "0.75rem 1rem",
                background: "var(--color-bg-2, #f9fafb)",
                borderRadius: "var(--radius-sm, 4px)",
                border: "1px solid var(--color-border, #e5e7eb)",
                listStyle: "none",
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
              }}
            >
              {faq.q}
              <span aria-hidden="true" style={{ fontSize: "0.8rem", color: "var(--color-text-muted)" }}>▼</span>
            </summary>
            <p
              style={{
                padding: "0.75rem 1rem",
                margin: 0,
                fontSize: "var(--text-sm, 0.875rem)",
                color: "var(--color-text-2, #555)",
                lineHeight: 1.6,
              }}
            >
              {faq.a}
            </p>
          </details>
        ))}
      </div>
    </>
  );
}
