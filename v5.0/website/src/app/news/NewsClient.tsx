"use client";

import { useState, useMemo, useCallback } from "react";
import { SectionBand, StoryCard, Pagination } from "@/components/ui";
import { getDisplayName, getSportColor } from "@/lib/sports-config";
import { formatRelativeTime, sanitizeUrl } from "@/lib/formatters";
import { useDebounce } from "@/lib/hooks";

type NewsItem = {
  id?: string;
  source: string;
  sport: string;
  headline: string;
  description?: string | null;
  link?: string | null;
  image_url?: string | null;
  published?: string | null;
  author?: string | null;
};

type Props = {
  news: NewsItem[];
  sports: string[];
};

const PER_PAGE = 20;

function isNew(published: string | null | undefined): boolean {
  if (!published) return false;
  const diff = Date.now() - new Date(published).getTime();
  return diff < 24 * 60 * 60 * 1000;
}

/** Only pass through URLs that will actually load. */
function validImageUrl(url?: string | null): string | undefined {
  if (!url || !url.trim()) return undefined;
  if (url.startsWith("http://") || url.startsWith("https://") || url.startsWith("/"))
    return url;
  return undefined;
}

export function NewsClient({ news, sports }: Props) {
  const [activeSport, setActiveSport] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const debouncedSearch = useDebounce(searchQuery, 250);
  const [page, setPage] = useState(1);

  // Deduplicate articles by headline similarity (keep first occurrence)
  const dedupedNews = useMemo(() => {
    const seen = new Set<string>();
    return news.filter((item) => {
      const key = item.headline
        .toLowerCase()
        .replace(/[^a-z0-9\s]/g, "")
        .replace(/\s+/g, " ")
        .trim();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }, [news]);

  const filtered = useMemo(() => {
    let items = activeSport
      ? dedupedNews.filter((n) => n.sport === activeSport)
      : dedupedNews;
    if (debouncedSearch.trim()) {
      const q = debouncedSearch.toLowerCase();
      items = items.filter(
        (n) =>
          n.headline.toLowerCase().includes(q) ||
          (n.description && n.description.toLowerCase().includes(q)) ||
          (n.author && n.author.toLowerCase().includes(q)),
      );
    }
    return items;
  }, [dedupedNews, activeSport, debouncedSearch]);

  const sportsWithArticles = useMemo(
    () => sports.filter((sport) => dedupedNews.some((item) => item.sport === sport)),
    [sports, dedupedNews],
  );

  const totalPages = Math.max(1, Math.ceil(filtered.length / PER_PAGE));
  const safeP = Math.min(page, totalPages);
  const pageSlice = filtered.slice((safeP - 1) * PER_PAGE, safeP * PER_PAGE);

  const isFirstPage = safeP === 1;
  const [featured, ...rest] = pageSlice;

  const handleSportChange = useCallback((sport: string | null) => {
    setActiveSport(sport);
    setSearchQuery("");
    setPage(1);
  }, []);

  return (
    <main>
      <SectionBand title="News">
        {/* Sport filter tabs */}
        <div
          className="news-filter-tabs"
          role="tablist"
          aria-label="Filter news by sport"
          style={{
            display: "flex",
            gap: "var(--space-2)",
            flexWrap: "wrap",
            marginBottom: "var(--space-6)",
          }}
        >
          <button
            role="tab"
            aria-selected={!activeSport}
            onClick={() => handleSportChange(null)}
            style={{
              padding: "var(--space-2) var(--space-4)",
              borderRadius: "var(--radius-full, 9999px)",
              border: "1px solid var(--border)",
              backgroundColor: !activeSport
                ? "var(--color-accent, #2563eb)"
                : "transparent",
              color: !activeSport ? "#fff" : "inherit",
              cursor: "pointer",
              fontSize: "var(--text-sm)",
              fontWeight: "var(--fw-semibold, 600)",
            }}
          >
            All
          </button>
          {sportsWithArticles.map((sport) => {
            const isActive = activeSport === sport;
            const color = getSportColor(sport);
            return (
              <button
                key={sport}
                role="tab"
                aria-selected={isActive}
                onClick={() => handleSportChange(sport)}
                style={{
                  padding: "var(--space-2) var(--space-4)",
                  borderRadius: "var(--radius-full, 9999px)",
                  border: `1px solid ${color}`,
                  backgroundColor: isActive ? color : "transparent",
                  color: isActive ? "#fff" : "inherit",
                  cursor: "pointer",
                  fontSize: "var(--text-sm)",
                  fontWeight: "var(--fw-semibold, 600)",
                }}
              >
                {getDisplayName(sport)}
              </button>
            );
          })}
        </div>

        {/* Search bar */}
        <div className="news-search-wrap" style={{ marginBottom: "var(--space-4)", position: "relative" }}>
          <input
            className="news-search-input"
            type="search"
            placeholder="Search headlines, descriptions, authors…"
            value={searchQuery}
            onChange={(e) => { setSearchQuery(e.target.value); setPage(1); }}
            aria-label="Search news articles"
            style={{
              width: "100%",
              maxWidth: "400px",
              padding: "var(--space-2) var(--space-4)",
              paddingLeft: "var(--space-8)",
              borderRadius: "var(--radius-full, 9999px)",
              border: "1px solid var(--border)",
              backgroundColor: "var(--surface)",
              color: "var(--text-primary)",
              fontSize: "var(--text-sm)",
            }}
          />
          <span
            style={{
              position: "absolute",
              left: "var(--space-3)",
              top: "50%",
              transform: "translateY(-50%)",
              pointerEvents: "none",
              color: "var(--text-muted)",
              fontSize: "0.875rem",
            }}
            aria-hidden="true"
          >
            🔍
          </span>
        </div>

        {/* Filter count */}
        <div
          aria-live="polite"
          style={{
            fontSize: "0.8125rem",
            color: "var(--text-muted)",
            marginBottom: "var(--space-4)",
          }}
        >
          {filtered.length} article{filtered.length !== 1 ? "s" : ""}
          {activeSport ? ` in ${getDisplayName(activeSport)}` : ""}
          {searchQuery.trim() ? ` matching "${searchQuery.trim()}"` : ""}
        </div>

        {filtered.length === 0 ? (
          <div
            className="card"
            style={{
              padding: "var(--space-8)",
              textAlign: "center",
              color: "var(--text-muted)",
            }}
          >
            No news found
            {activeSport ? ` in ${getDisplayName(activeSport)}` : ""}
            {searchQuery.trim() ? ` matching "${searchQuery.trim()}"` : ""}.
          </div>
        ) : (
          <>
            {/* Featured story (first page only, spans 2 cols) */}
            {isFirstPage && featured && (
              <div style={{ marginBottom: "var(--space-6)" }}>
                <StoryCard
                  href={sanitizeUrl(featured.link)}
                  title={featured.headline}
                  excerpt={featured.description ?? undefined}
                  imageUrl={validImageUrl(featured.image_url)}
                  sport={featured.sport}
                  author={featured.author ?? undefined}
                  publishedAt={
                    featured.published
                      ? formatRelativeTime(featured.published)
                      : undefined
                  }
                  tag={isNew(featured.published) ? "NEW" : undefined}
                  tagColor={isNew(featured.published) ? "var(--color-win)" : undefined}
                  size="featured"
                />
              </div>
            )}

            {/* Story grid */}
            {(isFirstPage ? rest : pageSlice).length > 0 && (
              <div className="news-grid-with-featured">
                {(isFirstPage ? rest : pageSlice).map((item, i) => (
                  <StoryCard
                    key={item.id ?? `${item.sport}-${i}`}
                    href={sanitizeUrl(item.link)}
                    title={item.headline}
                    excerpt={item.description ?? undefined}
                    imageUrl={validImageUrl(item.image_url)}
                    sport={item.sport}
                    author={item.author ?? undefined}
                    publishedAt={
                      item.published
                        ? formatRelativeTime(item.published)
                        : undefined
                    }
                    tag={isNew(item.published) ? "NEW" : undefined}
                    tagColor={isNew(item.published) ? "var(--color-win)" : undefined}
                    hideImageOnFail
                  />
                ))}
              </div>
            )}

            <Pagination
              page={safeP}
              totalPages={totalPages}
              onPageChange={setPage}
            />
          </>
        )}
      </SectionBand>
    </main>
  );
}
