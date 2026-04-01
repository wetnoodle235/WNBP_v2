export const dynamic = "force-dynamic";

import { notFound } from "next/navigation";
import type { Metadata } from "next";
import type { Prediction, News, Game, Standing, Injury, Odds } from "@/lib/schemas";
import { getGame, getGames, getStandings, getInjuries, getPredictions, getNews, getOdds } from "@/lib/api";
import { buildPageMetadata, buildSportsEventJsonLd } from "@/lib/seo";
import { getDisplayName } from "@/lib/sports-config";
import {
  formatGameDate,
  formatGameDateTime,
  formatProbability,
  formatLine,
  formatRelativeTime,
} from "@/lib/formatters";
import {
  SectionBand,
  Badge,
  LiveBadge,
  TeamBadge,
  StatCard,
  StoryCard,
} from "@/components/ui";
import { VenueVisual } from "@/components/venue";

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

interface PageProps {
  params: Promise<{ sport: string; id: string }>;
}

/* ------------------------------------------------------------------ */
/*  Status helpers                                                     */
/* ------------------------------------------------------------------ */

function isLive(status: string): boolean {
  const s = status.toLowerCase();
  return s.includes("in progress") || s.includes("live") || s.includes("in_progress");
}

function isFinal(status: string): boolean {
  return status.toLowerCase() === "final";
}

/* ------------------------------------------------------------------ */
/*  Shared styles                                                      */
/* ------------------------------------------------------------------ */

const cardStyle: React.CSSProperties = {
  background: "var(--color-bg-2)",
  border: "1px solid var(--color-border)",
  borderRadius: "var(--radius-md)",
  padding: "var(--space-6)",
};

const metaText: React.CSSProperties = {
  fontSize: "var(--text-sm)",
  color: "var(--color-text-muted)",
};

/* ------------------------------------------------------------------ */
/*  Metadata                                                           */
/* ------------------------------------------------------------------ */

export async function generateMetadata({ params }: PageProps): Promise<Metadata> {
  const { sport, id } = await params;
  const game = await getGame(sport, id);
  const name = getDisplayName(sport);

  if (!game) {
    return buildPageMetadata({
      title: `Game Not Found`,
      description: `The requested ${name} game could not be found.`,
      path: `/games/${sport}/${id}`,
    });
  }

  return buildPageMetadata({
    title: `${game.away_team} @ ${game.home_team}`,
    description: `${name} game details — ${game.away_team} vs ${game.home_team} on ${formatGameDate(game.date)}.`,
    path: `/games/${sport}/${id}`,
    keywords: [`${game.away_team}`, `${game.home_team}`, `${name} predictions`, `${name} scores`],
  });
}

/* ------------------------------------------------------------------ */
/*  Page                                                               */
/* ------------------------------------------------------------------ */

export default async function GameDetailPage({ params }: PageProps) {
  const { sport, id } = await params;

  const [game, predictions, news] = await Promise.all([
    getGame(sport, id),
    getPredictions(sport).catch(() => [] as Prediction[]),
    getNews(sport, 5).catch(() => [] as News[]),
  ]);

  if (!game) notFound();

  /* ----- Fetch H2H, standings, injuries (with safety) ----- */
  let allGames: Game[] = [];
  let standings: Standing[] = [];
  let allInjuries: Injury[] = [];
  let allOdds: Odds[] = [];
  try {
    [allGames, standings, allInjuries, allOdds] = await Promise.all([
      getGames(sport, { season: game.season, limit: "500" }).catch(() => [] as Game[]),
      getStandings(sport, Number(game.season) || undefined).catch(() => [] as Standing[]),
      getInjuries(sport).catch(() => [] as Injury[]),
      getOdds(sport).catch(() => [] as Odds[]),
    ]);
  } catch {
    // Silently continue with empty arrays
  }

  const prediction = predictions.find((p) => p.game_id === id) ?? null;
  const relatedNews = news.slice(0, 5);
  const sportName = getDisplayName(sport);

  /* ----- H2H matchups ----- */
  const h2hGames = allGames
    .filter(
      (g) =>
        g.id !== id &&
        g.status?.toLowerCase() === "final" &&
        ((g.home_team_id === game.home_team_id && g.away_team_id === game.away_team_id) ||
          (g.home_team_id === game.away_team_id && g.away_team_id === game.home_team_id)),
    )
    .sort((a, b) => (b.date || "").localeCompare(a.date || ""))
    .slice(0, 10);

  const h2hRecord = { homeWins: 0, awayWins: 0, draws: 0 };
  h2hGames.forEach((g) => {
    if (g.home_score == null || g.away_score == null) return;
    const homeIsHome = g.home_team_id === game.home_team_id;
    if (g.home_score > g.away_score) homeIsHome ? h2hRecord.homeWins++ : h2hRecord.awayWins++;
    else if (g.away_score > g.home_score) homeIsHome ? h2hRecord.awayWins++ : h2hRecord.homeWins++;
    else h2hRecord.draws++;
  });

  /* ----- Recent form (last 5) ----- */
  const getRecentForm = (teamId: string | null | undefined) => {
    if (!teamId) return [];
    return allGames
      .filter(
        (g) =>
          g.id !== id &&
          g.status?.toLowerCase() === "final" &&
          (g.home_team_id === teamId || g.away_team_id === teamId),
      )
      .sort((a, b) => (b.date || "").localeCompare(a.date || ""))
      .slice(0, 5)
      .map((g) => {
        const isHome = g.home_team_id === teamId;
        const teamScore = isHome ? g.home_score : g.away_score;
        const oppScore = isHome ? g.away_score : g.home_score;
        const oppName = isHome ? g.away_team : g.home_team;
        if (teamScore == null || oppScore == null)
          return { result: "?" as const, opponent: oppName, score: "", date: g.date };
        const result =
          teamScore > oppScore ? ("W" as const) : teamScore < oppScore ? ("L" as const) : ("D" as const);
        return { result, opponent: oppName, score: `${teamScore}-${oppScore}`, date: g.date };
      });
  };
  const homeForm = getRecentForm(game.home_team_id);
  const awayForm = getRecentForm(game.away_team_id);

  /* ----- Standings for each team ----- */
  const homeStanding = standings.find((s) => s.team_id === game.home_team_id);
  const awayStanding = standings.find((s) => s.team_id === game.away_team_id);

  /* ----- Injuries for each team ----- */
  const homeInjuries = allInjuries.filter((i) => i.team_id === game.home_team_id);
  const awayInjuries = allInjuries.filter((i) => i.team_id === game.away_team_id);

  /* ----- Odds for this game ----- */
  const gameOdds = allOdds.find((o) => o.game_id === id) ?? null;

  const live = isLive(game.status);
  const final = isFinal(game.status);
  const hasScores =
    game.home_score != null && game.away_score != null && (live || final);

  /* ----- Win probability helpers ----- */
  const homeProb = prediction?.home_win_prob ?? null;
  const awayProb = prediction?.away_win_prob ?? null;
  const homePct =
    homeProb != null ? (homeProb <= 1 ? homeProb * 100 : homeProb) : null;
  const awayPct =
    awayProb != null ? (awayProb <= 1 ? awayProb * 100 : awayProb) : null;

  /* ----- Determine winner highlight ----- */
  const homeWinning =
    hasScores && game.home_score != null && game.away_score != null
      ? game.home_score > game.away_score
      : null;

  return (
    <main>
      <h1 className="sr-only">{game.away_team} vs {game.home_team} — {sport.toUpperCase()} Game Details</h1>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{
          __html: buildSportsEventJsonLd({
            homeTeam: game.home_team,
            awayTeam: game.away_team,
            sport: getDisplayName(sport),
            startTime: game.start_time ?? undefined,
            status: game.status ?? undefined,
            path: `/games/${sport}/${id}`,
          }),
        }}
      />
      {/* ============================================================ */}
      {/*  SCOREBOARD                                                   */}
      {/* ============================================================ */}
      <SectionBand title="Matchup">
        <div
          style={{
            ...cardStyle,
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            gap: "var(--space-4)",
          }}
        >
          {/* Status badge */}
          <div>
            {live ? (
              <LiveBadge />
            ) : (
              <Badge variant={final ? "win" : sport.toLowerCase()}>
                {game.status}
              </Badge>
            )}
          </div>

          {/* Scoreboard row */}
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              gap: "var(--space-6)",
              flexWrap: "wrap",
              width: "100%",
            }}
          >
            {/* Away team */}
            <div style={{ textAlign: "center", flex: "1 1 0", minWidth: 120 }}>
              <TeamBadge
                teamId={game.away_team_id ?? undefined}
                name={game.away_team}
                sport={sport}
                size="lg"
              />
            </div>

            {/* Scores / vs */}
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: "var(--space-4)",
              }}
              aria-label={hasScores
                ? `Score: ${game.away_team} ${game.away_score}, ${game.home_team} ${game.home_score}${homeWinning === true ? ` — ${game.home_team} leads` : homeWinning === false ? ` — ${game.away_team} leads` : ""}`
                : undefined}
            >
              {hasScores ? (
                <>
                  <span
                    style={{
                      fontSize: "var(--text-3xl)",
                      fontWeight: "var(--fw-black)" as unknown as number,
                      lineHeight: "var(--leading-tight)",
                      color:
                        homeWinning === false
                          ? "var(--color-win)"
                          : "var(--color-text)",
                    }}
                  >
                    {game.away_score}
                  </span>
                  <span style={{ ...metaText, fontSize: "var(--text-lg)" }}>
                    –
                  </span>
                  <span
                    style={{
                      fontSize: "var(--text-3xl)",
                      fontWeight: "var(--fw-black)" as unknown as number,
                      lineHeight: "var(--leading-tight)",
                      color:
                        homeWinning === true
                          ? "var(--color-win)"
                          : "var(--color-text)",
                    }}
                  >
                    {game.home_score}
                  </span>
                </>
              ) : (
                <span
                  style={{
                    fontSize: "var(--text-xl)",
                    fontWeight: "var(--fw-semibold)" as unknown as number,
                    color: "var(--color-text-secondary)",
                  }}
                >
                  {game.start_time
                    ? formatGameDateTime(game.start_time)
                    : "vs"}
                </span>
              )}
            </div>

            {/* Home team */}
            <div style={{ textAlign: "center", flex: "1 1 0", minWidth: 120 }}>
              <TeamBadge
                teamId={game.home_team_id ?? undefined}
                name={game.home_team}
                sport={sport}
                size="lg"
              />
            </div>
          </div>

          {/* Meta line: date · venue · broadcast */}
          <div
            style={{
              ...metaText,
              display: "flex",
              flexWrap: "wrap",
              justifyContent: "center",
              gap: "var(--space-2)",
            }}
          >
            <span>{formatGameDate(game.date)}</span>
            {game.venue && (
              <>
                <span>·</span>
                <span>{game.venue}</span>
              </>
            )}
            {game.broadcast && (
              <>
                <span>·</span>
                <span>{game.broadcast}</span>
              </>
            )}
            {game.period && live && (
              <>
                <span>·</span>
                <span style={{ color: "var(--color-loss)", fontWeight: 600 }}>
                  {game.period}
                </span>
              </>
            )}
          </div>
        </div>
      </SectionBand>

      {/* ============================================================ */}
      {/*  LINESCORE TABLE                                              */}
      {/* ============================================================ */}
      {hasScores && (
        <LinescoreTable game={game} sport={sport} />
      )}

      {/* ============================================================ */}
      {/*  ODDS                                                         */}
      {/* ============================================================ */}
      {gameOdds && (
        <SectionBand title="Odds">
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))",
              gap: "var(--space-4)",
            }}
          >
            {gameOdds.h2h_home != null && (
              <StatCard
                label={`${game.home_team} ML`}
                value={gameOdds.h2h_home > 0 ? `+${gameOdds.h2h_home}` : String(gameOdds.h2h_home)}
                accent={gameOdds.h2h_away != null && Math.abs(gameOdds.h2h_home) < Math.abs(gameOdds.h2h_away) ? "win" : "neutral"}
              />
            )}
            {gameOdds.h2h_away != null && (
              <StatCard
                label={`${game.away_team} ML`}
                value={gameOdds.h2h_away > 0 ? `+${gameOdds.h2h_away}` : String(gameOdds.h2h_away)}
                accent={gameOdds.h2h_home != null && Math.abs(gameOdds.h2h_away) < Math.abs(gameOdds.h2h_home) ? "win" : "neutral"}
              />
            )}
            {gameOdds.h2h_draw != null && (
              <StatCard
                label="Draw"
                value={gameOdds.h2h_draw > 0 ? `+${gameOdds.h2h_draw}` : String(gameOdds.h2h_draw)}
                accent="neutral"
              />
            )}
            {gameOdds.spread_home != null && (
              <StatCard
                label="Spread"
                value={formatLine(gameOdds.spread_home)}
                sub={gameOdds.spread_home_line != null ? `(${gameOdds.spread_home_line > 0 ? "+" : ""}${gameOdds.spread_home_line})` : game.home_team}
              />
            )}
            {gameOdds.total_line != null && (
              <StatCard
                label="Total"
                value={String(gameOdds.total_line)}
                sub={
                  gameOdds.total_over != null && gameOdds.total_under != null
                    ? `O ${gameOdds.total_over > 0 ? "+" : ""}${gameOdds.total_over} / U ${gameOdds.total_under > 0 ? "+" : ""}${gameOdds.total_under}`
                    : "O/U"
                }
              />
            )}
            {gameOdds.bookmaker && (
              <StatCard label="Book" value={gameOdds.bookmaker} accent="neutral" />
            )}
          </div>
        </SectionBand>
      )}

      {/* ============================================================ */}
      {/*  VENUE VISUAL                                                 */}
      {/* ============================================================ */}
      {(() => {
        const sportNorm = sport.toLowerCase().replace(/[^a-z0-9]/g, "");
        const noVisualSports = ["mma", "ufc", "boxing", "esports", "csgo", "lol", "valorant", "dota2"];
        if (noVisualSports.includes(sportNorm)) return null;
        return (
          <SectionBand title="Venue">
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                gap: "var(--space-3)",
              }}
            >
              <div style={{ overflow: "hidden", borderRadius: "var(--radius-md)", maxWidth: "100%" }}>
                <VenueVisual
                  sport={sport}
                  venueName={game.venue ?? ""}
                  homeColor={undefined}
                  awayColor={undefined}
                  animate={sport.toLowerCase() === "f1" || sport.toLowerCase() === "indycar"}
                />
              </div>
              {game.venue && (
                <div style={{ fontSize: "var(--text-sm)", color: "var(--color-text-muted)", textAlign: "center" }}>
                  📍 {game.venue}
                </div>
              )}
            </div>
          </SectionBand>
        );
      })()}

      {/* ============================================================ */}
      {/*  AI PREDICTION                                                */}
      {/* ============================================================ */}
      {prediction && (
        <SectionBand title="AI Prediction">
          {/* Stat cards grid */}
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))",
              gap: "var(--space-4)",
              marginBottom: "var(--space-5)",
            }}
          >
            {homePct != null && (
              <StatCard
                label={`${game.home_team} Win`}
                value={formatProbability(homePct / 100)}
                accent={
                  awayPct != null && homePct > awayPct ? "win" : "neutral"
                }
              />
            )}
            {awayPct != null && (
              <StatCard
                label={`${game.away_team} Win`}
                value={formatProbability(awayPct / 100)}
                accent={
                  homePct != null && awayPct > homePct ? "win" : "neutral"
                }
              />
            )}
            {prediction.predicted_home_score != null &&
              prediction.predicted_away_score != null && (
                <StatCard
                  label="Predicted Score"
                  value={`${Math.round(prediction.predicted_away_score)}–${Math.round(prediction.predicted_home_score)}`}
                  sub={`${game.away_team} @ ${game.home_team}`}
                />
              )}
            {prediction.predicted_spread != null && (
              <StatCard
                label="Spread"
                value={formatLine(prediction.predicted_spread)}
                sub={game.home_team}
              />
            )}
            {prediction.predicted_total != null && (
              <StatCard
                label="Total"
                value={formatLine(prediction.predicted_total)}
              />
            )}
            {prediction.confidence != null && (
              <StatCard
                label="Confidence"
                value={formatProbability(
                  prediction.confidence <= 1
                    ? prediction.confidence
                    : prediction.confidence / 100,
                )}
                accent="brand"
              />
            )}
            {prediction.n_models != null && (
              <StatCard
                label="Models"
                value={String(prediction.n_models)}
                sub="ensemble"
              />
            )}
          </div>

          {/* Win probability bar */}
          {homePct != null && awayPct != null && (
            <div style={{ ...cardStyle, padding: "var(--space-4)" }}>
              {/* Labels */}
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  marginBottom: "var(--space-2)",
                  fontSize: "var(--text-sm)",
                  fontWeight: "var(--fw-semibold)" as unknown as number,
                }}
              >
                <span style={{ color: "var(--color-blue)" }}>
                  {game.away_team} {awayPct.toFixed(1)}%
                </span>
                <span style={{ color: "var(--color-loss)" }}>
                  {homePct.toFixed(1)}% {game.home_team}
                </span>
              </div>
              {/* Bar */}
              <div
                style={{
                  display: "flex",
                  height: 12,
                  borderRadius: "var(--radius-full)",
                  overflow: "hidden",
                  background: "var(--color-bg-3)",
                }}
              >
                <div
                  style={{
                    width: `${awayPct}%`,
                    background: "var(--color-blue)",
                    transition: "width var(--transition-slow)",
                  }}
                />
                <div
                  style={{
                    width: `${homePct}%`,
                    background: "var(--color-loss)",
                    transition: "width var(--transition-slow)",
                  }}
                />
              </div>
              {/* Timestamp */}
              {prediction.timestamp && (
                <div
                  style={{
                    ...metaText,
                    marginTop: "var(--space-2)",
                    textAlign: "right",
                  }}
                >
                  Updated {formatRelativeTime(prediction.timestamp)}
                </div>
              )}
            </div>
          )}
        </SectionBand>
      )}

      {/* ============================================================ */}
      {/*  H2H HISTORY                                                   */}
      {/* ============================================================ */}
      {h2hGames.length > 0 && (
        <SectionBand title="Head-to-Head">
          {/* H2H Record */}
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))",
              gap: "var(--space-4)",
              marginBottom: "var(--space-5)",
            }}
          >
            <StatCard
              label={`${game.home_team} Wins`}
              value={String(h2hRecord.homeWins)}
              accent={h2hRecord.homeWins > h2hRecord.awayWins ? "win" : "neutral"}
            />
            <StatCard
              label={`${game.away_team} Wins`}
              value={String(h2hRecord.awayWins)}
              accent={h2hRecord.awayWins > h2hRecord.homeWins ? "win" : "neutral"}
            />
            {h2hRecord.draws > 0 && (
              <StatCard label="Draws" value={String(h2hRecord.draws)} accent="neutral" />
            )}
          </div>

          {/* Recent H2H games list */}
          <div style={cardStyle}>
            <div
              style={{
                fontSize: "var(--text-xs)",
                color: "var(--color-text-muted)",
                textTransform: "uppercase" as const,
                letterSpacing: "0.05em",
                marginBottom: "var(--space-3)",
              }}
            >
              Recent Matchups
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: "var(--space-2)" }}>
              {h2hGames.map((g) => {
                const homeIsHome = g.home_team_id === game.home_team_id;
                const hScore = homeIsHome ? g.home_score : g.away_score;
                const aScore = homeIsHome ? g.away_score : g.home_score;
                const hWon = hScore != null && aScore != null && hScore > aScore;
                const aWon = hScore != null && aScore != null && aScore > hScore;
                return (
                  <div
                    key={g.id}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                      padding: "var(--space-2) var(--space-3)",
                      borderRadius: "var(--radius-sm)",
                      background: "var(--color-bg-3)",
                      fontSize: "var(--text-sm)",
                    }}
                  >
                    <span style={metaText}>{formatGameDate(g.date)}</span>
                    <span style={{ display: "flex", alignItems: "center", gap: "var(--space-3)" }}>
                      <span
                        style={{
                          fontWeight: hWon ? 700 : 400,
                          color: hWon ? "var(--color-win)" : "var(--color-text)",
                        }}
                      >
                        {game.home_team} {hScore ?? "-"}
                      </span>
                      <span style={metaText}>–</span>
                      <span
                        style={{
                          fontWeight: aWon ? 700 : 400,
                          color: aWon ? "var(--color-win)" : "var(--color-text)",
                        }}
                      >
                        {aScore ?? "-"} {game.away_team}
                      </span>
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        </SectionBand>
      )}

      {/* ============================================================ */}
      {/*  TEAM STATS                                                    */}
      {/* ============================================================ */}
      {(live || final) && (
        <TeamStatsSection game={game} sport={sport} />
      )}

      {/* ============================================================ */}
      {/*  TEAM FORM / RECENT RESULTS                                    */}
      {/* ============================================================ */}
      {(homeForm.length > 0 || awayForm.length > 0) && (
        <SectionBand title="Recent Form">
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "1fr 1fr",
              gap: "var(--space-4)",
            }}
          >
            {/* Home team form */}
            <div style={cardStyle}>
              <div
                style={{
                  fontSize: "var(--text-sm)",
                  fontWeight: 700,
                  marginBottom: "var(--space-3)",
                }}
              >
                {game.home_team}
                {homeForm.length > 0 && (
                  <span style={{ ...metaText, fontWeight: 400, marginLeft: "var(--space-2)" }}>
                    ({homeForm.filter((f) => f.result === "W").length}-
                    {homeForm.filter((f) => f.result === "L").length}
                    {homeForm.some((f) => f.result === "D")
                      ? `-${homeForm.filter((f) => f.result === "D").length}`
                      : ""}
                    )
                  </span>
                )}
              </div>
              {/* W/L dots */}
              <div style={{ display: "flex", gap: "var(--space-2)", marginBottom: "var(--space-3)" }}>
                {homeForm.map((f, i) => (
                  <span
                    key={i}
                    style={{
                      display: "inline-flex",
                      alignItems: "center",
                      justifyContent: "center",
                      width: 28,
                      height: 28,
                      borderRadius: "var(--radius-full)",
                      fontSize: "var(--text-xs)",
                      fontWeight: 700,
                      color: "#fff",
                      background:
                        f.result === "W"
                          ? "var(--color-win)"
                          : f.result === "L"
                            ? "var(--color-loss)"
                            : f.result === "D"
                              ? "#eab308"
                              : "var(--color-text-muted)",
                    }}
                  >
                    {f.result}
                  </span>
                ))}
              </div>
              {/* Game details */}
              <div style={{ display: "flex", flexDirection: "column", gap: "var(--space-1)" }}>
                {homeForm.map((f, i) => (
                  <div key={i} style={{ display: "flex", justifyContent: "space-between", fontSize: "var(--text-xs)" }}>
                    <span style={metaText}>{f.opponent ?? "Unknown"}</span>
                    <span
                      style={{
                        fontWeight: 600,
                        color:
                          f.result === "W"
                            ? "var(--color-win)"
                            : f.result === "L"
                              ? "var(--color-loss)"
                              : "var(--color-text-muted)",
                      }}
                    >
                      {f.score || "–"}
                    </span>
                  </div>
                ))}
                {homeForm.length === 0 && <div style={metaText}>No recent results</div>}
              </div>
            </div>

            {/* Away team form */}
            <div style={cardStyle}>
              <div
                style={{
                  fontSize: "var(--text-sm)",
                  fontWeight: 700,
                  marginBottom: "var(--space-3)",
                }}
              >
                {game.away_team}
                {awayForm.length > 0 && (
                  <span style={{ ...metaText, fontWeight: 400, marginLeft: "var(--space-2)" }}>
                    ({awayForm.filter((f) => f.result === "W").length}-
                    {awayForm.filter((f) => f.result === "L").length}
                    {awayForm.some((f) => f.result === "D")
                      ? `-${awayForm.filter((f) => f.result === "D").length}`
                      : ""}
                    )
                  </span>
                )}
              </div>
              <div style={{ display: "flex", gap: "var(--space-2)", marginBottom: "var(--space-3)" }}>
                {awayForm.map((f, i) => (
                  <span
                    key={i}
                    style={{
                      display: "inline-flex",
                      alignItems: "center",
                      justifyContent: "center",
                      width: 28,
                      height: 28,
                      borderRadius: "var(--radius-full)",
                      fontSize: "var(--text-xs)",
                      fontWeight: 700,
                      color: "#fff",
                      background:
                        f.result === "W"
                          ? "var(--color-win)"
                          : f.result === "L"
                            ? "var(--color-loss)"
                            : f.result === "D"
                              ? "#eab308"
                              : "var(--color-text-muted)",
                    }}
                  >
                    {f.result}
                  </span>
                ))}
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: "var(--space-1)" }}>
                {awayForm.map((f, i) => (
                  <div key={i} style={{ display: "flex", justifyContent: "space-between", fontSize: "var(--text-xs)" }}>
                    <span style={metaText}>{f.opponent ?? "Unknown"}</span>
                    <span
                      style={{
                        fontWeight: 600,
                        color:
                          f.result === "W"
                            ? "var(--color-win)"
                            : f.result === "L"
                              ? "var(--color-loss)"
                              : "var(--color-text-muted)",
                      }}
                    >
                      {f.score || "–"}
                    </span>
                  </div>
                ))}
                {awayForm.length === 0 && <div style={metaText}>No recent results</div>}
              </div>
            </div>
          </div>
        </SectionBand>
      )}

      {/* ============================================================ */}
      {/*  SEASON STANDINGS COMPARISON                                   */}
      {/* ============================================================ */}
      {(homeStanding || awayStanding) && (
        <SectionBand title="Standings Comparison">
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "1fr 1fr",
              gap: "var(--space-4)",
            }}
          >
            {[
              { team: game.home_team, s: homeStanding },
              { team: game.away_team, s: awayStanding },
            ].map(({ team, s }) => (
              <div key={team} style={cardStyle}>
                <div
                  style={{
                    fontSize: "var(--text-sm)",
                    fontWeight: 700,
                    marginBottom: "var(--space-4)",
                  }}
                >
                  {team}
                </div>
                {s ? (
                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "1fr 1fr",
                      gap: "var(--space-3)",
                    }}
                  >
                    <InfoRow label="Record" value={`${s.wins}-${s.losses}${s.ties ? `-${s.ties}` : ""}`} />
                    <InfoRow label="Win %" value={s.pct != null ? `${(s.pct * 100).toFixed(1)}%` : "–"} />
                    {s.conference && <InfoRow label="Conference" value={s.conference} />}
                    {s.division && <InfoRow label="Division" value={s.division} />}
                    {s.overall_rank != null && <InfoRow label="Rank" value={`#${s.overall_rank}`} />}
                    {s.conference_rank != null && !s.overall_rank && (
                      <InfoRow label="Conf. Rank" value={`#${s.conference_rank}`} />
                    )}
                    {s.streak && <InfoRow label="Streak" value={s.streak} />}
                    {s.last_ten && <InfoRow label="Last 10" value={s.last_ten} />}
                    {s.home_record && <InfoRow label="Home" value={s.home_record} />}
                    {s.away_record && <InfoRow label="Away" value={s.away_record} />}
                  </div>
                ) : (
                  <div style={metaText}>No standings data</div>
                )}
              </div>
            ))}
          </div>
        </SectionBand>
      )}

      {/* ============================================================ */}
      {/*  INJURIES                                                      */}
      {/* ============================================================ */}
      {(homeInjuries.length > 0 || awayInjuries.length > 0) && (
        <SectionBand title="Injuries">
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "1fr 1fr",
              gap: "var(--space-4)",
            }}
          >
            {[
              { team: game.home_team, list: homeInjuries },
              { team: game.away_team, list: awayInjuries },
            ].map(({ team, list }) => (
              <div key={team} style={cardStyle}>
                <div
                  style={{
                    fontSize: "var(--text-sm)",
                    fontWeight: 700,
                    marginBottom: "var(--space-3)",
                  }}
                >
                  {team}{" "}
                  <span style={metaText}>({list.length})</span>
                </div>
                {list.length > 0 ? (
                  <div style={{ display: "flex", flexDirection: "column", gap: "var(--space-2)" }}>
                    {list.map((inj) => {
                      const st = inj.status.toLowerCase();
                      const statusColor = st.includes("out")
                        ? "var(--color-loss)"
                        : st.includes("doubtful")
                          ? "#ea580c"
                          : st.includes("questionable")
                            ? "#eab308"
                            : "var(--color-text-muted)";
                      return (
                        <div
                          key={inj.player_id}
                          style={{
                            display: "flex",
                            alignItems: "flex-start",
                            justifyContent: "space-between",
                            gap: "var(--space-2)",
                            padding: "var(--space-2) var(--space-3)",
                            borderRadius: "var(--radius-sm)",
                            background: "var(--color-bg-3)",
                            fontSize: "var(--text-sm)",
                          }}
                        >
                          <div style={{ flex: 1 }}>
                            <div style={{ fontWeight: 600 }}>
                              {inj.player_name ?? "Unknown"}
                            </div>
                            {inj.description && (
                              <div style={{ ...metaText, fontSize: "var(--text-xs)" }}>
                                {inj.body_part ? `${inj.body_part} — ` : ""}
                                {inj.description}
                              </div>
                            )}
                          </div>
                          <Badge
                            variant={
                              st.includes("out")
                                ? "loss"
                                : st.includes("doubtful") || st.includes("questionable")
                                  ? "push"
                                  : "free"
                            }
                          >
                            <span style={{ color: statusColor, fontWeight: 700 }}>
                              {inj.status}
                            </span>
                          </Badge>
                        </div>
                      );
                    })}
                  </div>
                ) : (
                  <div style={metaText}>No injuries reported</div>
                )}
              </div>
            ))}
          </div>
        </SectionBand>
      )}

      {/* ============================================================ */}
      {/*  GAME INFO                                                    */}
      {/* ============================================================ */}
      <SectionBand title="Game Info">
        <div
          style={{
            ...cardStyle,
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))",
            gap: "var(--space-4)",
          }}
        >
          {(() => {
            const tennisGame = game as unknown as Record<string, unknown>;
            const isTennis = ["wta", "atp"].includes(sport.toLowerCase());
            if (!isTennis) return null;
            return (
              <>
                {tennisGame.total_sets != null && (
                  <InfoRow label="Total Sets" value={String(tennisGame.total_sets)} />
                )}
                {tennisGame.home_sets_won != null && (
                  <InfoRow label={`${game.home_team} Sets`} value={String(tennisGame.home_sets_won)} />
                )}
                {tennisGame.away_sets_won != null && (
                  <InfoRow label={`${game.away_team} Sets`} value={String(tennisGame.away_sets_won)} />
                )}
              </>
            );
          })()}
          <InfoRow label="Sport" value={sportName} />
          <InfoRow label="Season" value={game.season} />
          <InfoRow label="Date" value={formatGameDate(game.date)} />
          {game.start_time && (
            <InfoRow
              label="Start Time"
              value={formatGameDateTime(game.start_time)}
            />
          )}
          {game.venue && <InfoRow label="Venue" value={game.venue} />}
          {game.attendance != null && (
            <InfoRow
              label="Attendance"
              value={game.attendance.toLocaleString()}
            />
          )}
          {game.broadcast && (
            <InfoRow label="Broadcast" value={game.broadcast} />
          )}
          {game.weather && <InfoRow label="Weather" value={game.weather} />}
          {game.is_neutral_site && (
            <InfoRow label="Neutral Site" value="Yes" />
          )}
        </div>
      </SectionBand>

      {/* ============================================================ */}
      {/*  RELATED NEWS                                                 */}
      {/* ============================================================ */}
      {relatedNews.length > 0 && (
        <SectionBand title="Related News">
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))",
              gap: "var(--space-4)",
            }}
          >
            {relatedNews.map((article, i) => (
              <StoryCard
                key={article.id ?? i}
                href={article.link ?? "#"}
                title={article.headline}
                excerpt={article.description ?? undefined}
                imageUrl={article.image_url ?? undefined}
                author={article.author ?? undefined}
                publishedAt={article.published ?? undefined}
                sport={sport}
              />
            ))}
          </div>
        </SectionBand>
      )}
    </main>
  );
}

/* ------------------------------------------------------------------ */
/*  Linescore table (sport-specific)                                   */
/* ------------------------------------------------------------------ */

function LinescoreTable({ game, sport }: { game: Game; sport: string }) {
  const s = sport.toLowerCase();

  const cellStyle: React.CSSProperties = {
    padding: "var(--space-2) var(--space-3)",
    textAlign: "center",
    fontSize: "var(--text-sm)",
    minWidth: 36,
  };
  const headerCellStyle: React.CSSProperties = {
    ...cellStyle,
    fontSize: "var(--text-xs)",
    color: "var(--color-text-muted)",
    textTransform: "uppercase",
    letterSpacing: "0.04em",
    fontWeight: 600,
  };
  const teamCellStyle: React.CSSProperties = {
    ...cellStyle,
    textAlign: "left",
    fontWeight: 700,
    minWidth: 100,
    whiteSpace: "nowrap",
  };
  const totalCellStyle: React.CSSProperties = {
    ...cellStyle,
    fontWeight: 700,
    borderLeft: "2px solid var(--color-border)",
  };

  const fmt = (v: number | null | undefined): string =>
    v != null ? String(Math.round(v)) : "-";

  // Determine columns based on sport
  if (["nhl"].includes(s)) {
    // Hockey: P1, P2, P3, OT, T
    const hasOT = game.home_ot != null || game.away_ot != null;
    const periods = [
      { label: "P1", home: game.home_p1, away: game.away_p1 },
      { label: "P2", home: game.home_p2, away: game.away_p2 },
      { label: "P3", home: game.home_p3, away: game.away_p3 },
    ];
    if (hasOT) periods.push({ label: "OT", home: game.home_ot ?? null, away: game.away_ot ?? null });

    const hasData = periods.some((p) => p.home != null || p.away != null);
    if (!hasData) return null;

    return (
      <SectionBand title="Linescore">
        <div style={cardStyle}>
          <div className="responsive-table-wrap">
            <table className="responsive-table linescore-table" style={{ borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid var(--color-border)" }}>
                <th scope="col" style={headerCellStyle}>&nbsp;</th>
                {periods.map((p) => (
                  <th scope="col" key={p.label} style={headerCellStyle}>{p.label}</th>
                ))}
                <th scope="col" style={{ ...headerCellStyle, borderLeft: "2px solid var(--color-border)" }}>T</th>
              </tr>
            </thead>
            <tbody>
              <tr style={{ borderBottom: "1px solid var(--color-border)" }}>
                <th scope="row" style={teamCellStyle}>{game.away_team}</th>
                {periods.map((p) => (
                  <td key={p.label} style={cellStyle}>{fmt(p.away)}</td>
                ))}
                <td style={totalCellStyle}>{fmt(game.away_score)}</td>
              </tr>
              <tr>
                <th scope="row" style={teamCellStyle}>{game.home_team}</th>
                {periods.map((p) => (
                  <td key={p.label} style={cellStyle}>{fmt(p.home)}</td>
                ))}
                <td style={totalCellStyle}>{fmt(game.home_score)}</td>
              </tr>
            </tbody>
            </table>
          </div>
        </div>
      </SectionBand>
    );
  }

  if (["mlb"].includes(s)) {
    // Baseball: Innings 1-9, Extras, R, H
    const innings = [
      { label: "1", home: game.home_i1, away: game.away_i1 },
      { label: "2", home: game.home_i2, away: game.away_i2 },
      { label: "3", home: game.home_i3, away: game.away_i3 },
      { label: "4", home: game.home_i4, away: game.away_i4 },
      { label: "5", home: game.home_i5, away: game.away_i5 },
      { label: "6", home: game.home_i6, away: game.away_i6 },
      { label: "7", home: game.home_i7, away: game.away_i7 },
      { label: "8", home: game.home_i8, away: game.away_i8 },
      { label: "9", home: game.home_i9, away: game.away_i9 },
    ];
    const hasExtras = game.home_extras != null || game.away_extras != null;
    if (hasExtras) innings.push({ label: "X", home: game.home_extras, away: game.away_extras });

    const hasData = innings.some((inn) => inn.home != null || inn.away != null);
    if (!hasData) return null;

    return (
      <SectionBand title="Linescore">
        <div style={cardStyle}>
          <div className="responsive-table-wrap">
            <table className="responsive-table linescore-table" style={{ borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid var(--color-border)" }}>
                <th scope="col" style={headerCellStyle}>&nbsp;</th>
                {innings.map((inn) => (
                  <th scope="col" key={inn.label} style={headerCellStyle}>{inn.label}</th>
                ))}
                <th scope="col" style={{ ...headerCellStyle, borderLeft: "2px solid var(--color-border)" }}>R</th>
                <th scope="col" style={headerCellStyle}>H</th>
              </tr>
            </thead>
            <tbody>
              <tr style={{ borderBottom: "1px solid var(--color-border)" }}>
                <th scope="row" style={teamCellStyle}>{game.away_team}</th>
                {innings.map((inn) => (
                  <td key={inn.label} style={cellStyle}>{fmt(inn.away)}</td>
                ))}
                <td style={totalCellStyle}>{fmt(game.away_score)}</td>
                <td style={cellStyle}>{fmt(game.away_hits)}</td>
              </tr>
              <tr>
                <th scope="row" style={teamCellStyle}>{game.home_team}</th>
                {innings.map((inn) => (
                  <td key={inn.label} style={cellStyle}>{fmt(inn.home)}</td>
                ))}
                <td style={totalCellStyle}>{fmt(game.home_score)}</td>
                <td style={cellStyle}>{fmt(game.home_hits)}</td>
              </tr>
            </tbody>
            </table>
          </div>
        </div>
      </SectionBand>
    );
  }

  if (["ncaab"].includes(s)) {
    // College basketball: H1, H2, OT, T
    const g = game as unknown as Record<string, unknown>;
    const hasOT = game.home_ot != null || game.away_ot != null;
    const halves = [
      { label: "H1", home: g.home_h1_score as number | null, away: g.away_h1_score as number | null },
      { label: "H2", home: g.home_h2_score as number | null, away: g.away_h2_score as number | null },
    ];
    if (hasOT) halves.push({ label: "OT", home: game.home_ot ?? null, away: game.away_ot ?? null });

    const hasData = halves.some((h) => h.home != null || h.away != null);
    if (!hasData) return null;

    return (
      <SectionBand title="Linescore">
        <div style={cardStyle}>
          <div className="responsive-table-wrap">
            <table className="responsive-table linescore-table" style={{ borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid var(--color-border)" }}>
                <th scope="col" style={headerCellStyle}>&nbsp;</th>
                {halves.map((h) => (
                  <th scope="col" key={h.label} style={headerCellStyle}>{h.label}</th>
                ))}
                <th scope="col" style={{ ...headerCellStyle, borderLeft: "2px solid var(--color-border)" }}>T</th>
              </tr>
            </thead>
            <tbody>
              <tr style={{ borderBottom: "1px solid var(--color-border)" }}>
                <th scope="row" style={teamCellStyle}>{game.away_team}</th>
                {halves.map((h) => (
                  <td key={h.label} style={cellStyle}>{fmt(h.away)}</td>
                ))}
                <td style={totalCellStyle}>{fmt(game.away_score)}</td>
              </tr>
              <tr>
                <th scope="row" style={teamCellStyle}>{game.home_team}</th>
                {halves.map((h) => (
                  <td key={h.label} style={cellStyle}>{fmt(h.home)}</td>
                ))}
                <td style={totalCellStyle}>{fmt(game.home_score)}</td>
              </tr>
            </tbody>
            </table>
          </div>
        </div>
      </SectionBand>
    );
  }

  if (["epl", "laliga", "bundesliga", "seriea", "ligue1", "mls", "ucl", "nwsl", "liga_mx"].includes(s)) {
    // Soccer: H1, H2, T
    const g = game as unknown as Record<string, unknown>;
    const halves = [
      { label: "1st Half", home: g.home_h1_score as number | null, away: g.away_h1_score as number | null },
      { label: "2nd Half", home: g.home_h2_score as number | null, away: g.away_h2_score as number | null },
    ];

    const hasData = halves.some((h) => h.home != null || h.away != null);
    if (!hasData) return null;

    return (
      <SectionBand title="Linescore">
        <div style={cardStyle}>
          <div className="responsive-table-wrap">
            <table className="responsive-table linescore-table" style={{ borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid var(--color-border)" }}>
                <th scope="col" style={headerCellStyle}>&nbsp;</th>
                {halves.map((h) => (
                  <th scope="col" key={h.label} style={headerCellStyle}>{h.label}</th>
                ))}
                <th scope="col" style={{ ...headerCellStyle, borderLeft: "2px solid var(--color-border)" }}>FT</th>
              </tr>
            </thead>
            <tbody>
              <tr style={{ borderBottom: "1px solid var(--color-border)" }}>
                <th scope="row" style={teamCellStyle}>{game.away_team}</th>
                {halves.map((h) => (
                  <td key={h.label} style={cellStyle}>{fmt(h.away)}</td>
                ))}
                <td style={totalCellStyle}>{fmt(game.away_score)}</td>
              </tr>
              <tr>
                <th scope="row" style={teamCellStyle}>{game.home_team}</th>
                {halves.map((h) => (
                  <td key={h.label} style={cellStyle}>{fmt(h.home)}</td>
                ))}
                <td style={totalCellStyle}>{fmt(game.home_score)}</td>
              </tr>
            </tbody>
            </table>
          </div>
        </div>
      </SectionBand>
    );
  }

  if (["nba", "ncaaw", "wnba", "nfl", "ncaaf"].includes(s)) {
    // Basketball & Football: Q1, Q2, Q3, Q4, OT, T
    const hasOT = game.home_ot != null || game.away_ot != null;
    const quarters = [
      { label: "Q1", home: game.home_q1, away: game.away_q1 },
      { label: "Q2", home: game.home_q2, away: game.away_q2 },
      { label: "Q3", home: game.home_q3, away: game.away_q3 },
      { label: "Q4", home: game.home_q4, away: game.away_q4 },
    ];
    if (hasOT) quarters.push({ label: "OT", home: game.home_ot ?? null, away: game.away_ot ?? null });

    const hasData = quarters.some((q) => q.home != null || q.away != null);
    if (!hasData) return null;

    return (
      <SectionBand title="Linescore">
        <div style={cardStyle}>
          <div className="responsive-table-wrap">
            <table className="responsive-table linescore-table" style={{ borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid var(--color-border)" }}>
                <th scope="col" style={headerCellStyle}>&nbsp;</th>
                {quarters.map((q) => (
                  <th scope="col" key={q.label} style={headerCellStyle}>{q.label}</th>
                ))}
                <th scope="col" style={{ ...headerCellStyle, borderLeft: "2px solid var(--color-border)" }}>T</th>
              </tr>
            </thead>
            <tbody>
              <tr style={{ borderBottom: "1px solid var(--color-border)" }}>
                <th scope="row" style={teamCellStyle}>{game.away_team}</th>
                {quarters.map((q) => (
                  <td key={q.label} style={cellStyle}>{fmt(q.away)}</td>
                ))}
                <td style={totalCellStyle}>{fmt(game.away_score)}</td>
              </tr>
              <tr>
                <th scope="row" style={teamCellStyle}>{game.home_team}</th>
                {quarters.map((q) => (
                  <td key={q.label} style={cellStyle}>{fmt(q.home)}</td>
                ))}
                <td style={totalCellStyle}>{fmt(game.home_score)}</td>
              </tr>
            </tbody>
            </table>
          </div>
        </div>
      </SectionBand>
    );
  }

  // Soccer / other — no standard linescore
  return null;
}

/* ------------------------------------------------------------------ */
/*  Comparison bar: home value | label | away value                    */
/* ------------------------------------------------------------------ */

interface StatRowDef {
  label: string;
  home: number | string | null | undefined;
  away: number | string | null | undefined;
  /** "higher" = bigger number is better, "lower" = smaller is better, "equal" = no highlight */
  better?: "higher" | "lower" | "equal";
  /** Show as percentage bar (e.g., possession) */
  pctBar?: boolean;
  /** Suffix to append to display values */
  suffix?: string;
}

function ComparisonBar({ label, home, away, better = "higher", pctBar, suffix = "" }: StatRowDef) {
  const hVal = typeof home === "number" ? home : parseFloat(String(home ?? ""));
  const aVal = typeof away === "number" ? away : parseFloat(String(away ?? ""));
  if (isNaN(hVal) && isNaN(aVal)) return null;

  const hNum = isNaN(hVal) ? 0 : hVal;
  const aNum = isNaN(aVal) ? 0 : aVal;
  const total = hNum + aNum || 1;
  const hPct = (hNum / total) * 100;
  const aPct = (aNum / total) * 100;

  const hBetter =
    better === "equal" ? false : better === "higher" ? hNum > aNum : hNum < aNum;
  const aBetter =
    better === "equal" ? false : better === "higher" ? aNum > hNum : aNum < hNum;

  const fmt = (v: number | string | null | undefined): string => {
    if (v == null) return "–";
    const n = typeof v === "number" ? v : parseFloat(String(v));
    if (isNaN(n)) return String(v);
    return Number.isInteger(n) ? String(n) + suffix : n.toFixed(1) + suffix;
  };

  return (
    <div style={{ marginBottom: "var(--space-3)" }}>
      {/* Values + label row */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: "var(--space-1)",
          fontSize: "var(--text-sm)",
        }}
      >
        <span
          style={{
            fontWeight: hBetter ? 700 : 400,
            color: hBetter ? "var(--color-win)" : "var(--color-text)",
            minWidth: 48,
          }}
        >
          {fmt(home)}
        </span>
        <span
          style={{
            flex: 1,
            textAlign: "center",
            fontSize: "var(--text-xs)",
            color: "var(--color-text-muted)",
            textTransform: "uppercase" as const,
            letterSpacing: "0.04em",
            fontWeight: 600,
          }}
        >
          {label}
        </span>
        <span
          style={{
            fontWeight: aBetter ? 700 : 400,
            color: aBetter ? "var(--color-win)" : "var(--color-text)",
            minWidth: 48,
            textAlign: "right",
          }}
        >
          {fmt(away)}
        </span>
      </div>
      {/* Visual bar */}
      <div
        style={{
          display: "flex",
          height: pctBar ? 10 : 6,
          borderRadius: "var(--radius-full)",
          overflow: "hidden",
          background: "var(--color-bg-3)",
        }}
      >
        <div
          style={{
            width: `${hPct}%`,
            background: hBetter ? "var(--color-win)" : "var(--color-text-muted)",
            opacity: hBetter ? 1 : 0.45,
            transition: "width var(--transition-slow)",
            borderRadius: "var(--radius-full) 0 0 var(--radius-full)",
          }}
        />
        <div
          style={{
            width: `${aPct}%`,
            background: aBetter ? "var(--color-win)" : "var(--color-text-muted)",
            opacity: aBetter ? 1 : 0.45,
            transition: "width var(--transition-slow)",
            borderRadius: "0 var(--radius-full) var(--radius-full) 0",
          }}
        />
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Sport-specific team stats section                                  */
/* ------------------------------------------------------------------ */

function getSoccerStats(game: Game): StatRowDef[] {
  const g = game as unknown as Record<string, unknown>;
  return [
    { label: "Possession", home: game.home_possession, away: game.away_possession, pctBar: true, suffix: "%" },
    { label: "xG", home: g.home_xg as number | null, away: g.away_xg as number | null },
    { label: "Total Shots", home: game.home_total_shots, away: game.away_total_shots },
    { label: "Shots on Target", home: game.home_shots_on_target, away: game.away_shots_on_target },
    { label: "Shots on Target %", home: g.home_shots_on_target_pct as number | null, away: g.away_shots_on_target_pct as number | null, suffix: "%" },
    { label: "Shot Accuracy", home: g.home_shot_pct as number | null, away: g.away_shot_pct as number | null, suffix: "%" },
    { label: "Shot Conversion", home: g.home_shot_conversion_rate as number | null, away: g.away_shot_conversion_rate as number | null, suffix: "%" },
    { label: "Blocked Shots", home: g.home_blocked_shots as number | null, away: g.away_blocked_shots as number | null },
    { label: "Passes (Acc/Total)", home: game.home_accurate_passes != null && game.home_total_passes != null ? `${game.home_accurate_passes}/${game.home_total_passes}` : null, away: game.away_accurate_passes != null && game.away_total_passes != null ? `${game.away_accurate_passes}/${game.away_total_passes}` : null, better: "equal" },
    { label: "Pass Accuracy", home: game.home_pass_pct, away: game.away_pass_pct, suffix: "%" },
    { label: "Corners", home: game.home_corners, away: game.away_corners },
    { label: "Crosses (Acc/Total)", home: g.home_accurate_crosses != null && g.home_total_crosses != null ? `${g.home_accurate_crosses}/${g.home_total_crosses}` : null, away: g.away_accurate_crosses != null && g.away_total_crosses != null ? `${g.away_accurate_crosses}/${g.away_total_crosses}` : null, better: "equal" },
    { label: "Tackles", home: game.home_tackles, away: game.away_tackles },
    { label: "Tackle Success", home: g.home_tackle_pct as number | null, away: g.away_tackle_pct as number | null, suffix: "%" },
    { label: "Interceptions", home: game.home_interceptions, away: game.away_interceptions },
    { label: "Clearances", home: game.home_clearances, away: game.away_clearances },
    { label: "Long Balls", home: game.home_long_balls, away: game.away_long_balls },
    { label: "Fouls", home: game.home_fouls, away: game.away_fouls, better: "lower" },
    { label: "Offsides", home: game.home_offsides, away: game.away_offsides, better: "lower" },
    { label: "Yellow Cards", home: game.home_yellow_cards, away: game.away_yellow_cards, better: "lower" },
    { label: "Red Cards", home: game.home_red_cards, away: game.away_red_cards, better: "lower" },
    { label: "Penalty Goals", home: game.home_penalty_goals, away: game.away_penalty_goals },
    { label: "Saves", home: game.home_saves, away: game.away_saves },
  ];
}

function getBasketballStats(game: Game): StatRowDef[] {
  const g = game as unknown as Record<string, unknown>;
  return [
    { label: "FG%", home: game.home_fg_pct, away: game.away_fg_pct, suffix: "%" },
    { label: "eFG%", home: g.home_effective_fg_pct as number | null, away: g.away_effective_fg_pct as number | null, suffix: "%" },
    { label: "TS%", home: g.home_true_shooting_pct as number | null, away: g.away_true_shooting_pct as number | null, suffix: "%" },
    { label: "3PT%", home: game.home_three_pct, away: game.away_three_pct, suffix: "%" },
    { label: "FT%", home: game.home_ft_pct, away: game.away_ft_pct, suffix: "%" },
    { label: "Rebounds", home: game.home_rebounds, away: game.away_rebounds },
    { label: "Off. Rebounds", home: g.home_offensive_rebounds as number | null, away: g.away_offensive_rebounds as number | null },
    { label: "Def. Rebounds", home: g.home_defensive_rebounds as number | null, away: g.away_defensive_rebounds as number | null },
    { label: "Assists", home: game.home_assists, away: game.away_assists },
    { label: "Steals", home: game.home_steals, away: game.away_steals },
    { label: "Blocks", home: game.home_blocks, away: game.away_blocks },
    { label: "Turnovers", home: game.home_turnovers, away: game.away_turnovers, better: "lower" },
    { label: "Possessions", home: g.home_possessions as number | null, away: g.away_possessions as number | null, better: "equal" },
    { label: "Points in Paint", home: g.home_points_in_paint as number | null, away: g.away_points_in_paint as number | null },
    { label: "Fast Break Pts", home: g.home_fast_break_points as number | null, away: g.away_fast_break_points as number | null },
    { label: "Largest Lead", home: g.home_largest_lead as number | null, away: g.away_largest_lead as number | null },
  ];
}

function getFootballStats(game: Game): StatRowDef[] {
  const g = game as unknown as Record<string, unknown>;
  return [
    { label: "Total Yards", home: game.home_total_yards, away: game.away_total_yards },
    { label: "Passing Yards", home: game.home_passing_yards, away: game.away_passing_yards },
    { label: "Comp %", home: g.home_completion_pct as number | null, away: g.away_completion_pct as number | null, suffix: "%" },
    { label: "Rushing Yards", home: game.home_rushing_yards, away: game.away_rushing_yards },
    { label: "First Downs", home: game.home_first_downs, away: game.away_first_downs },
    { label: "3rd Down %", home: g.home_third_down_pct as number | null, away: g.away_third_down_pct as number | null, suffix: "%" },
    { label: "4th Down %", home: g.home_fourth_down_pct as number | null, away: g.away_fourth_down_pct as number | null, suffix: "%" },
    { label: "Red Zone %", home: g.home_red_zone_pct as number | null, away: g.away_red_zone_pct as number | null, suffix: "%" },
    { label: "Pass TDs", home: g.home_passing_touchdowns as number | null, away: g.away_passing_touchdowns as number | null },
    { label: "Rush TDs", home: g.home_rushing_touchdowns as number | null, away: g.away_rushing_touchdowns as number | null },
    { label: "Turnovers", home: game.home_turnovers_football, away: game.away_turnovers_football, better: "lower" },
    { label: "INTs Thrown", home: g.home_interceptions_thrown as number | null, away: g.away_interceptions_thrown as number | null, better: "lower" },
    { label: "Fumbles Lost", home: g.home_fumbles_lost as number | null, away: g.away_fumbles_lost as number | null, better: "lower" },
    { label: "Sacks", home: game.home_sacks, away: game.away_sacks },
    { label: "Tackles", home: g.home_tackles as number | null, away: g.away_tackles as number | null },
    { label: "Total Drives", home: g.home_total_drives as number | null, away: g.away_total_drives as number | null },
    { label: "Def TDs", home: g.home_defensive_tds as number | null, away: g.away_defensive_tds as number | null },
    { label: "Penalties", home: game.home_penalties != null && game.home_penalty_yards != null ? `${game.home_penalties}-${game.home_penalty_yards}` : game.home_penalties, away: game.away_penalties != null && game.away_penalty_yards != null ? `${game.away_penalties}-${game.away_penalty_yards}` : game.away_penalties, better: "lower" },
    { label: "Time of Possession", home: game.home_time_of_possession, away: game.away_time_of_possession, better: "equal" },
  ];
}

function getHockeyStats(game: Game): StatRowDef[] {
  const g = game as unknown as Record<string, unknown>;
  // Prefer backend faceoff_pct, fall back to client-side calculation
  let hFOPct = g.home_faceoff_pct as number | null;
  let aFOPct = g.away_faceoff_pct as number | null;
  if (hFOPct == null || aFOPct == null) {
    const hFO = game.home_faceoffs_won;
    const aFO = game.away_faceoffs_won;
    const foTotal = (hFO ?? 0) + (aFO ?? 0) || null;
    hFOPct = hFO != null && foTotal ? ((hFO / foTotal) * 100) : null;
    aFOPct = aFO != null && foTotal ? ((aFO / foTotal) * 100) : null;
  }

  // Power play: prefer goals/attempts format, fall back to goals alone
  let ppHome: string | number | null = null;
  let ppAway: string | number | null = null;
  if (game.home_power_play_goals != null && game.home_power_play_attempts != null) {
    ppHome = `${game.home_power_play_goals}/${game.home_power_play_attempts}`;
  } else if (game.home_power_play_goals != null) {
    ppHome = game.home_power_play_goals;
  }
  if (game.away_power_play_goals != null && game.away_power_play_attempts != null) {
    ppAway = `${game.away_power_play_goals}/${game.away_power_play_attempts}`;
  } else if (game.away_power_play_goals != null) {
    ppAway = game.away_power_play_goals;
  }

  return [
    { label: "Shots on Goal", home: game.home_shots_on_goal, away: game.away_shots_on_goal },
    { label: "Save %", home: g.home_save_pct as number | null, away: g.away_save_pct as number | null },
    { label: "Hits", home: game.home_hits_nhl, away: game.away_hits_nhl },
    { label: "Blocked Shots", home: game.home_blocked_shots, away: game.away_blocked_shots },
    { label: "Power Play", home: ppHome, away: ppAway, better: "equal" },
    { label: "PP %", home: game.home_power_play_pct, away: game.away_power_play_pct, suffix: "%" },
    { label: "PK %", home: g.home_penalty_kill_pct as number | null, away: g.away_penalty_kill_pct as number | null, suffix: "%" },
    { label: "Faceoff %", home: hFOPct, away: aFOPct, suffix: "%" },
    { label: "Penalty Minutes", home: game.home_penalty_minutes, away: game.away_penalty_minutes, better: "lower" },
    { label: "Takeaways", home: game.home_takeaways, away: game.away_takeaways },
    { label: "Giveaways", home: game.home_giveaways, away: game.away_giveaways, better: "lower" },
    { label: "Shorthanded Goals", home: game.home_shorthanded_goals, away: game.away_shorthanded_goals },
  ];
}

function getBaseballStats(game: Game): StatRowDef[] {
  const g = game as Record<string, unknown>;
  return [
    { label: "Hits", home: g.home_hits as number | null, away: g.away_hits as number | null },
    { label: "Home Runs", home: g.home_home_runs as number | null, away: g.away_home_runs as number | null },
    { label: "RBI", home: g.home_rbi as number | null, away: g.away_rbi as number | null },
    { label: "AVG", home: g.home_batting_avg as number | null, away: g.away_batting_avg as number | null },
    { label: "OBP", home: g.home_obp as number | null, away: g.away_obp as number | null },
    { label: "SLG", home: g.home_slg as number | null, away: g.away_slg as number | null },
    { label: "OPS", home: g.home_ops as number | null, away: g.away_ops as number | null },
    { label: "Walks", home: g.home_walks as number | null, away: g.away_walks as number | null },
    { label: "Strikeouts", home: g.home_strikeouts as number | null, away: g.away_strikeouts as number | null, better: "lower" },
    { label: "Stolen Bases", home: g.home_stolen_bases as number | null, away: g.away_stolen_bases as number | null },
    { label: "Errors", home: g.home_errors as number | null, away: g.away_errors as number | null, better: "lower" },
    { label: "Left on Base", home: g.home_left_on_base as number | null, away: g.away_left_on_base as number | null, better: "lower" },
    { label: "WHIP", home: g.home_whip as number | null, away: g.away_whip as number | null, better: "lower" },
    { label: "Earned Runs", home: g.home_earned_runs as number | null, away: g.away_earned_runs as number | null, better: "lower" },
    { label: "Pitches Thrown", home: g.home_pitches_thrown as number | null, away: g.away_pitches_thrown as number | null },
    { label: "Plate Appearances", home: g.home_plate_appearances as number | null, away: g.away_plate_appearances as number | null },
    { label: "At Bats", home: g.home_at_bats as number | null, away: g.away_at_bats as number | null },
    { label: "Doubles", home: g.home_doubles as number | null, away: g.away_doubles as number | null },
    { label: "Triples", home: g.home_triples as number | null, away: g.away_triples as number | null },
    { label: "Total Bases", home: g.home_total_bases as number | null, away: g.away_total_bases as number | null },
    { label: "ERA", home: g.home_era as number | null, away: g.away_era as number | null, better: "lower" },
    { label: "Innings Pitched", home: g.home_innings_pitched as number | null, away: g.away_innings_pitched as number | null },
    { label: "Pitching K", home: g.home_pitching_strikeouts as number | null, away: g.away_pitching_strikeouts as number | null },
    { label: "Pitching BB", home: g.home_pitching_walks as number | null, away: g.away_pitching_walks as number | null, better: "lower" },
  ];
}

function getTennisStats(game: Game): StatRowDef[] {
  const g = game as unknown as Record<string, unknown>;

  const breakPointsHome =
    g.home_break_points_won != null && g.home_break_points_total != null
      ? `${g.home_break_points_won}/${g.home_break_points_total}`
      : null;
  const breakPointsAway =
    g.away_break_points_won != null && g.away_break_points_total != null
      ? `${g.away_break_points_won}/${g.away_break_points_total}`
      : null;

  return [
    { label: "Sets Won", home: g.home_sets_won as number | null, away: g.away_sets_won as number | null },
    { label: "Aces", home: g.home_aces as number | null, away: g.away_aces as number | null },
    { label: "Double Faults", home: g.home_double_faults as number | null, away: g.away_double_faults as number | null, better: "lower" },
    { label: "Ace/DF Ratio", home: g.home_ace_df_ratio as number | null, away: g.away_ace_df_ratio as number | null },
    { label: "1st Serve %", home: g.home_first_serve_pct as number | null, away: g.away_first_serve_pct as number | null, suffix: "%" },
    { label: "1st Serve Won %", home: g.home_first_serve_won_pct as number | null, away: g.away_first_serve_won_pct as number | null, suffix: "%" },
    { label: "2nd Serve Won %", home: g.home_second_serve_won_pct as number | null, away: g.away_second_serve_won_pct as number | null, suffix: "%" },
    { label: "Break Points Won", home: g.home_break_points_won as number | null, away: g.away_break_points_won as number | null },
    { label: "Break Points", home: breakPointsHome, away: breakPointsAway, better: "equal" },
    { label: "Break Conv %", home: g.home_break_point_conversion_pct as number | null, away: g.away_break_point_conversion_pct as number | null, suffix: "%" },
    { label: "Break Save %", home: g.home_break_point_save_pct as number | null, away: g.away_break_point_save_pct as number | null, suffix: "%" },
    { label: "Return Pts Won", home: g.home_return_points_won as number | null, away: g.away_return_points_won as number | null },
    { label: "Total Points Won", home: g.home_total_points_won as number | null, away: g.away_total_points_won as number | null },
    { label: "Winners", home: g.home_winners as number | null, away: g.away_winners as number | null },
    { label: "Unforced Errors", home: g.home_unforced_errors as number | null, away: g.away_unforced_errors as number | null, better: "lower" },
  ];
}

function getUFCStats(game: Game): StatRowDef[] {
  const g = game as unknown as Record<string, unknown>;
  return [
    { label: "Strikes Landed", home: g.home_strikes_landed as number | null, away: g.away_strikes_landed as number | null },
    { label: "Strikes Attempted", home: g.home_strikes_attempted as number | null, away: g.away_strikes_attempted as number | null },
    { label: "Strike Accuracy", home: g.home_strike_accuracy as number | null, away: g.away_strike_accuracy as number | null, suffix: "%" },
    { label: "Takedowns", home: g.home_takedowns as number | null, away: g.away_takedowns as number | null },
    { label: "Takedown Accuracy", home: g.home_takedown_accuracy as number | null, away: g.away_takedown_accuracy as number | null, suffix: "%" },
    { label: "Knockdowns", home: g.home_knockdowns as number | null, away: g.away_knockdowns as number | null },
    { label: "Clinch Strikes", home: g.home_clinch_strikes as number | null, away: g.away_clinch_strikes as number | null },
    { label: "Ground Strikes", home: g.home_ground_strikes as number | null, away: g.away_ground_strikes as number | null },
    { label: "Submission Att.", home: g.home_submission_attempts as number | null, away: g.away_submission_attempts as number | null },
    { label: "Control Time", home: g.home_control_time as string | null, away: g.away_control_time as string | null, better: "equal" },
  ];
}

function TeamStatsSection({ game, sport }: { game: Game; sport: string }) {
  const s = sport.toLowerCase();
  let stats: StatRowDef[] = [];

  if (["mls", "nwsl", "epl", "bundesliga", "laliga", "ligue1", "seriea", "ucl", "liga_mx"].includes(s)) {
    stats = getSoccerStats(game);
  } else if (["nba", "ncaab", "ncaaw", "wnba"].includes(s)) {
    stats = getBasketballStats(game);
  } else if (["nfl", "ncaaf"].includes(s)) {
    stats = getFootballStats(game);
  } else if (["nhl"].includes(s)) {
    stats = getHockeyStats(game);
  } else if (["mlb"].includes(s)) {
    stats = getBaseballStats(game);
  } else if (["atp", "wta"].includes(s)) {
    stats = getTennisStats(game);
  } else if (["ufc"].includes(s)) {
    stats = getUFCStats(game);
  }

  // Filter to only rows that have at least one non-null value
  const validStats = stats.filter((r) => r.home != null || r.away != null);
  if (validStats.length === 0) return null;

  return (
    <SectionBand title="Team Stats">
      <div
        style={{
          ...cardStyle,
          padding: "var(--space-5) var(--space-6)",
        }}
      >
        {/* Team name headers */}
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            marginBottom: "var(--space-4)",
            paddingBottom: "var(--space-3)",
            borderBottom: "1px solid var(--color-border)",
          }}
        >
          <span style={{ fontWeight: 700, fontSize: "var(--text-sm)" }}>
            {game.home_team}
          </span>
          <span style={{ fontWeight: 700, fontSize: "var(--text-sm)" }}>
            {game.away_team}
          </span>
        </div>

        {/* Stat rows */}
        {validStats.map((stat) => (
          <ComparisonBar key={stat.label} {...stat} />
        ))}
      </div>
    </SectionBand>
  );
}

/* ------------------------------------------------------------------ */
/*  Small helper component for the info grid                           */
/* ------------------------------------------------------------------ */

function InfoRow({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div
        style={{
          fontSize: "var(--text-xs)",
          color: "var(--color-text-muted)",
          textTransform: "uppercase" as const,
          letterSpacing: "0.05em",
          marginBottom: "var(--space-1)",
        }}
      >
        {label}
      </div>
      <div
        style={{
          fontSize: "var(--text-base)",
          color: "var(--color-text)",
          fontWeight: "var(--fw-medium)" as unknown as number,
        }}
      >
        {value}
      </div>
    </div>
  );
}
