import { buildPageMetadata, buildCollectionJsonLd } from "@/lib/seo";
import type { Metadata } from "next";
import { PremiumTeaser, SectionBand } from "@/components/ui";
import { getMarketSignals } from "@/lib/api";
import type { MarketSignal } from "@/lib/schemas";
import { getDisplayName } from "@/lib/sports-config";
import { getViewerTier, hasPremiumTier } from "@/lib/server-access";
import Link from "next/link";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export const metadata: Metadata = buildPageMetadata({
  title: "Market Intelligence",
  description:
    "Live betting market regime analysis — identify volatile, moving, and stable lines across all sports.",
  path: "/market-intel",
  keywords: ["market signals", "line movement", "betting market", "market regime", "bookmaker", "odds movement"],
});

const SIGNAL_SPORTS = [
  "nba", "nfl", "mlb", "nhl", "wnba",
  "ncaab", "ncaaf", "epl", "mls",
] as const;

const REGIME_ORDER: Record<string, number> = {
  volatile: 0,
  moving: 1,
  stable: 2,
};

const REGIME_COLORS: Record<string, string> = {
  volatile: "var(--color-error, #ef4444)",
  moving: "var(--color-warning, #f59e0b)",
  stable: "var(--color-success, #22c55e)",
};

const PANEL_BG = "var(--color-bg-3)";
const CARD_BG = "var(--color-bg-2)";
const BORDER = "1px solid var(--color-border)";
const TEXT_MUTED = "var(--color-text-muted)";
const TEXT_SECONDARY = "var(--color-text-secondary)";

function RegimeBadge({ regime }: { regime: string | null | undefined }) {
  const label = regime ?? "unknown";
  const color = REGIME_COLORS[label.toLowerCase()] ?? "var(--color-text-muted)";
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 8px",
        borderRadius: 4,
        fontSize: "0.75rem",
        fontWeight: 600,
        textTransform: "capitalize",
        color: "#fff",
        background: color,
      }}
    >
      {label}
    </span>
  );
}

function readSearchParam(value: string | string[] | undefined): string {
  return typeof value === "string" ? value.trim() : "";
}

interface MarketIntelPageProps {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}

export default async function MarketIntelPage({ searchParams }: MarketIntelPageProps) {
  const tier = await getViewerTier();
  const hasPremium = hasPremiumTier(tier);
  const filters = searchParams ? await searchParams : {};
  const selectedSport = readSearchParam(filters.sport).toLowerCase();
  const selectedRegime = readSearchParam(filters.regime).toLowerCase();
  const query = readSearchParam(filters.q).toLowerCase();
  const batches = await Promise.allSettled(
    SIGNAL_SPORTS.map((sport) =>
      getMarketSignals(sport, { limit: "20" }).then((rows) =>
        rows.map((r) => ({ ...r, sport }))
      )
    )
  );

  const allSignals: (MarketSignal & { sport: string })[] = [];
  for (const result of batches) {
    if (result.status === "fulfilled") {
      allSignals.push(...result.value);
    }
  }

  const filteredSignals = allSignals.filter((signal) => {
    if (selectedSport && signal.sport.toLowerCase() !== selectedSport) return false;
    if (selectedRegime && (signal.market_regime ?? "").toLowerCase() !== selectedRegime) return false;
    if (!query) return true;
    const haystack = [
      signal.home_team,
      signal.away_team,
      signal.bookmaker,
      signal.game_id,
      signal.sport,
    ].join(" ").toLowerCase();
    return haystack.includes(query);
  });

  // Sort: volatile first, then moving, then stable; within each group sort by date desc
  filteredSignals.sort((a, b) => {
    const regimeA = REGIME_ORDER[(a.market_regime ?? "").toLowerCase()] ?? 99;
    const regimeB = REGIME_ORDER[(b.market_regime ?? "").toLowerCase()] ?? 99;
    if (regimeA !== regimeB) return regimeA - regimeB;
    const dateA = a.date ?? "";
    const dateB = b.date ?? "";
    return dateB.localeCompare(dateA);
  });

  const volatile = filteredSignals.filter(
    (s) => (s.market_regime ?? "").toLowerCase() === "volatile"
  );
  const moving = filteredSignals.filter(
    (s) => (s.market_regime ?? "").toLowerCase() === "moving"
  );
  const stable = filteredSignals.filter(
    (s) => (s.market_regime ?? "").toLowerCase() === "stable"
  );
  const other = filteredSignals.filter(
    (s) =>
      !["volatile", "moving", "stable"].includes(
        (s.market_regime ?? "").toLowerCase()
      )
  );

  const jsonLd = buildCollectionJsonLd({
    name: "Market Intelligence",
    path: "/market-intel",
    description: "Live betting market regime analysis across all sports.",
  });

  const tableHeaderStyle: React.CSSProperties = {
    textAlign: "left",
    padding: "8px 12px",
    color: TEXT_MUTED,
    fontWeight: 500,
    fontSize: "0.8rem",
    textTransform: "uppercase",
    letterSpacing: "0.05em",
    borderBottom: BORDER,
    whiteSpace: "nowrap",
  };

  const tdStyle: React.CSSProperties = {
    padding: "10px 12px",
    borderBottom: BORDER,
    fontSize: "0.875rem",
    verticalAlign: "middle",
  };

  function SignalTable({ rows }: { rows: (MarketSignal & { sport: string })[] }) {
    if (rows.length === 0) return null;
    return (
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            <th style={tableHeaderStyle}>Sport</th>
            <th style={tableHeaderStyle}>Date</th>
            <th style={tableHeaderStyle}>Matchup</th>
            <th style={tableHeaderStyle}>Regime</th>
            <th style={tableHeaderStyle}>Bookmaker</th>
            <th style={tableHeaderStyle}>Game</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((s, i) => {
            const matchup =
              s.home_team && s.away_team
                ? `${s.away_team} @ ${s.home_team}`
                : s.game_id;
            const dateStr = s.date ? s.date.slice(0, 10) : "—";
            return (
              <tr key={i}>
                <td style={tdStyle}>
                  <span
                    style={{
                      display: "inline-block",
                      padding: "2px 7px",
                      borderRadius: 4,
                      fontSize: "0.75rem",
                      fontWeight: 600,
                      background: PANEL_BG,
                      color: TEXT_SECONDARY,
                    }}
                  >
                    {getDisplayName(s.sport)}
                  </span>
                </td>
                <td style={{ ...tdStyle, color: TEXT_MUTED }}>
                  {dateStr}
                </td>
                <td style={tdStyle}>{matchup}</td>
                <td style={tdStyle}>
                  <RegimeBadge regime={s.market_regime} />
                </td>
                <td style={{ ...tdStyle, color: TEXT_MUTED }}>
                  {s.bookmaker ?? "—"}
                </td>
                <td style={tdStyle}>
                  <Link
                    href={`/games/${s.sport}/${s.game_id}`}
                    style={{ color: "var(--color-brand)", textDecoration: "none", fontSize: "0.8rem", fontWeight: 700 }}
                  >
                    View →
                  </Link>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    );
  }

  const statBarItems = [
    { label: "Volatile", count: volatile.length, color: REGIME_COLORS.volatile },
    { label: "Moving", count: moving.length, color: REGIME_COLORS.moving },
    { label: "Stable", count: stable.length, color: REGIME_COLORS.stable },
  ];

  const previewVolatile = volatile.slice(0, 5);
  const previewMoving = moving.slice(0, 5);
  const previewStable = stable.slice(0, 5);
  const previewOther = other.slice(0, 5);

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: jsonLd }} />

      <SectionBand title="Market Intelligence">
        <p style={{ color: TEXT_SECONDARY, marginTop: 0, marginBottom: "1rem", fontSize: "0.9rem" }}>
          Betting line movement and market regime analysis across all monitored sports.
        </p>
        <form method="get" style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap", marginBottom: "1rem" }}>
          <select name="sport" defaultValue={selectedSport} style={{ padding: "0.65rem 0.8rem", borderRadius: 8, background: CARD_BG, color: "var(--color-text)", border: BORDER, boxShadow: "var(--shadow-sm)" }}>
            <option value="">All sports</option>
            {SIGNAL_SPORTS.map((sport) => (
              <option key={sport} value={sport}>{getDisplayName(sport)}</option>
            ))}
          </select>
          <select name="regime" defaultValue={selectedRegime} style={{ padding: "0.65rem 0.8rem", borderRadius: 8, background: CARD_BG, color: "var(--color-text)", border: BORDER, boxShadow: "var(--shadow-sm)" }}>
            <option value="">All regimes</option>
            <option value="volatile">Volatile</option>
            <option value="moving">Moving</option>
            <option value="stable">Stable</option>
          </select>
          <input name="q" defaultValue={query} placeholder="Search matchup or book" style={{ minWidth: 220, flex: "1 1 220px", padding: "0.65rem 0.8rem", borderRadius: 8, background: CARD_BG, color: "var(--color-text)", border: BORDER, boxShadow: "var(--shadow-sm)" }} />
          <button type="submit" style={{ padding: "0.65rem 0.95rem", borderRadius: 8, background: "var(--color-brand)", color: "#fff", border: 0, fontWeight: 700, boxShadow: "var(--shadow-sm)" }}>Filter</button>
          <Link href="/market-intel" style={{ display: "inline-flex", alignItems: "center", padding: "0.65rem 0.2rem", color: TEXT_SECONDARY, textDecoration: "none", fontWeight: 600 }}>Reset</Link>
        </form>
        {filteredSignals.length === 0 ? (
          <p style={{ color: TEXT_SECONDARY, padding: "1rem 0" }}>
            No market signals match the current filters.
          </p>
        ) : (
          <div
            style={{
              display: "flex",
              gap: "1rem",
              flexWrap: "wrap",
              marginBottom: "1.5rem",
            }}
          >
            {statBarItems.map((item) => (
              <div
                key={item.label}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "0.5rem",
                  padding: "6px 14px",
                  borderRadius: 8,
                  background: PANEL_BG,
                  border: BORDER,
                  fontSize: "0.875rem",
                }}
              >
                <span
                  style={{
                    width: 10,
                    height: 10,
                    borderRadius: "50%",
                    background: item.color,
                    flexShrink: 0,
                  }}
                />
                <span style={{ color: TEXT_SECONDARY, fontWeight: 600 }}>
                  {item.count}
                </span>
                <span style={{ color: TEXT_MUTED }}>{item.label}</span>
              </div>
            ))}
          </div>
        )}
      </SectionBand>

      {!hasPremium && (
        <PremiumTeaser
          icon="◈"
          message="Upgrade to unlock the full market regime board across all sports, including every volatile and moving market."
          ctaHref="/pricing"
        />
      )}

      {(hasPremium ? volatile : previewVolatile).length > 0 && (
        <SectionBand title={`Volatile Markets (${volatile.length})`}>
          <div style={{ overflowX: "auto" }}>
            <SignalTable rows={hasPremium ? volatile : previewVolatile} />
          </div>
        </SectionBand>
      )}

      {(hasPremium ? moving : previewMoving).length > 0 && (
        <SectionBand title={`Moving Markets (${moving.length})`}>
          <div style={{ overflowX: "auto" }}>
            <SignalTable rows={hasPremium ? moving : previewMoving} />
          </div>
        </SectionBand>
      )}

      {(hasPremium ? stable : previewStable).length > 0 && (
        <SectionBand title={`Stable Markets (${stable.length})`}>
          <div style={{ overflowX: "auto" }}>
            <SignalTable rows={hasPremium ? stable : previewStable} />
          </div>
        </SectionBand>
      )}

      {(hasPremium ? other : previewOther).length > 0 && (
        <SectionBand title={`Other Signals (${other.length})`}>
          <div style={{ overflowX: "auto" }}>
            <SignalTable rows={hasPremium ? other : previewOther} />
          </div>
        </SectionBand>
      )}
    </>
  );
}
