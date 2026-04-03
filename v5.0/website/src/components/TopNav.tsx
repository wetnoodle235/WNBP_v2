"use client";

import { useState, useEffect, useRef, useMemo } from "react";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useSession, signOut } from "next-auth/react";
import { useTheme } from "./ThemeProvider";
import { useAuth } from "@/lib/auth";

type NavLink = {
  href: string;
  label: string;
  icon: string;
  premiumOnly?: boolean;
};

type NavGroup = {
  label: string;
  links: readonly NavLink[];
};

const PRIMARY_LINKS: readonly NavLink[] = [
  { href: "/", label: "Home", icon: "⌂" },
  { href: "/predictions", label: "Predictions", icon: "◬" },
  { href: "/opportunities", label: "Props", icon: "◈" },
  { href: "/odds", label: "Odds", icon: "↕" },
  { href: "/live", label: "Live", icon: "●" },
  { href: "/season", label: "Season", icon: "◍", premiumOnly: true },
] as const;

const NAV_GROUPS: readonly NavGroup[] = [
  {
    label: "Sports Hub",
    links: [
      { href: "/sports", label: "Sports Hub", icon: "◌" },
    ],
  },
  {
    label: "Board",
    links: [
      { href: "/leaderboard", label: "Leaderboard", icon: "☉" },
      { href: "/news", label: "News", icon: "✦" },
      { href: "/standings", label: "Standings", icon: "≣" },
      { href: "/model-health", label: "Model", icon: "◔", premiumOnly: true },
      { href: "/market-intel", label: "Market Intel", icon: "◈", premiumOnly: true },
      { href: "/fatigue", label: "Fatigue", icon: "⚡", premiumOnly: true },
      { href: "/injuries", label: "Injuries", icon: "✚" },
    ],
  },
  {
    label: "Playbook",
    links: [
      { href: "/paper", label: "Paper", icon: "▦" },
      { href: "/ladder", label: "Ladder", icon: "⋮" },
      { href: "/autobets", label: "AutoBets", icon: "⚡", premiumOnly: true },
      { href: "/favorites", label: "Favorites", icon: "♡", premiumOnly: true },
    ],
  },
  {
    label: "Scouting",
    links: [
      { href: "/stats", label: "Stats", icon: "∑", premiumOnly: true },
      { href: "/players", label: "Players", icon: "◉", premiumOnly: true },
      { href: "/teams", label: "Teams", icon: "◫", premiumOnly: true },
      { href: "/dashboard", label: "Dashboard", icon: "□", premiumOnly: true },
      { href: "/pricing", label: "Pricing", icon: "◐" },
    ],
  },
] as const;

const PREMIUM_TIERS = new Set(["trial", "monthly", "yearly", "premium", "dev", "starter", "pro", "enterprise"]);
const ENTERPRISE_DOC_TIERS = new Set(["enterprise", "dev"]);

function normalizeTier(tier?: string): string {
  return (tier ?? "free").trim().toLowerCase();
}

function isActivePath(pathname: string, href: string): boolean {
  return pathname === href || (href !== "/" && pathname.startsWith(href));
}

export function TopNav() {
  const pathname = usePathname();
  const router = useRouter();
  const { data: session, status } = useSession();
  const { theme, preference, toggle } = useTheme();
  const { user: authUser, isLoading: authLoading, logout: authLogout } = useAuth();
  const [menuOpen, setMenuOpen] = useState(false);
  const [commandDeckOpen, setCommandDeckOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);
  const hamburgerRef = useRef<HTMLButtonElement>(null);
  const deckRef = useRef<HTMLDivElement>(null);

  useEffect(() => { setMenuOpen(false); }, [pathname]);
  useEffect(() => { setCommandDeckOpen(false); }, [pathname]);

  useEffect(() => {
    if (!menuOpen && !commandDeckOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setMenuOpen(false);
        setCommandDeckOpen(false);
      }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [menuOpen, commandDeckOpen]);

  useEffect(() => {
    if (!commandDeckOpen) return;
    const onClickOutside = (event: MouseEvent) => {
      if (!deckRef.current) return;
      if (!deckRef.current.contains(event.target as Node)) {
        setCommandDeckOpen(false);
      }
    };
    document.addEventListener("mousedown", onClickOutside);
    return () => document.removeEventListener("mousedown", onClickOutside);
  }, [commandDeckOpen]);

  useEffect(() => {
    if (menuOpen) {
      document.body.style.overflow = "hidden";
      const firstLink = menuRef.current?.querySelector("a, button") as HTMLElement | null;
      firstLink?.focus();
    } else {
      document.body.style.overflow = "";
      hamburgerRef.current?.focus();
    }
    return () => { document.body.style.overflow = ""; };
  }, [menuOpen]);

  const isLoggedIn = !!authUser || !!session;
  const isLoading = authLoading || status === "loading";
  const userName = authUser?.name ?? session?.user?.name ?? "";
  const userEmail = authUser?.email ?? session?.user?.email ?? "";
  const tier = normalizeTier(authUser?.tier ?? ((session?.user as Record<string, unknown> | undefined)?.tier as string | undefined));
  const hasPremium = PREMIUM_TIERS.has(tier);
  const hasEnterpriseDocsAccess = ENTERPRISE_DOC_TIERS.has(tier);
  const showGetPremium = !isLoggedIn || !hasPremium;

  const visiblePrimary = useMemo(
    () => PRIMARY_LINKS.filter((link) => !link.premiumOnly || hasPremium),
    [hasPremium],
  );
  const visibleGroups = useMemo(
    () => NAV_GROUPS
      .map((group) => ({
        ...group,
        links: group.links.filter((link) => !link.premiumOnly || hasPremium),
      }))
      .filter((group) => group.links.length > 0),
    [hasPremium],
  );

  useEffect(() => {
    visiblePrimary.forEach((link) => router.prefetch(link.href));
    visibleGroups.forEach((group) => group.links.forEach((link) => router.prefetch(link.href)));
  }, [router, visiblePrimary, visibleGroups]);

  function handleSignOut() {
    authLogout();
    signOut({ callbackUrl: "/" });
  }

  const mobileGroups = [{ label: "Pulse", links: visiblePrimary }, ...visibleGroups];

  return (
    <>
      <header className="top-nav top-nav-overhaul" role="banner">
        <div className="top-nav-inner top-nav-overhaul-inner">
          <Link href="/" className="nav-logo nav-logo-overhaul" aria-label="WNBP home">
            <span className="nav-logo-mark">WN</span>
            <span className="nav-logo-text-wrap">
              <span className="nav-logo-text">WNBP</span>
              <span className="nav-logo-subtext">Control Desk</span>
            </span>
          </Link>

          <nav className="nav-links nav-links-overhaul" aria-label="Main navigation">
            <div className="nav-dock">
              {visiblePrimary.map(({ href, label, icon }) => {
                const active = isActivePath(pathname, href);
                return (
                  <Link
                    key={href}
                    href={href}
                    className={`nav-link nav-link-overhaul${active ? " active" : ""}`}
                    aria-current={active ? "page" : undefined}
                  >
                    <span className="nav-link-icon" aria-hidden="true">{icon}</span>
                    <span>{label}</span>
                  </Link>
                );
              })}
            </div>

            <div className="nav-more nav-command-deck" ref={deckRef}>
              <button
                type="button"
                className={`nav-link nav-link-overhaul nav-more-trigger${commandDeckOpen ? " active" : ""}`}
                aria-haspopup="menu"
                aria-expanded={commandDeckOpen}
                onClick={() => setCommandDeckOpen((value) => !value)}
              >
                <span className="nav-link-icon" aria-hidden="true">⌘</span>
                <span>Explore</span>
              </button>
              {commandDeckOpen && (
                <div className="nav-more-menu nav-command-menu" role="menu" aria-label="Explore links">
                  {visibleGroups.map((group) => (
                    <div key={group.label} className="nav-command-group">
                      <div className="nav-command-group-title">{group.label}</div>
                      <div className="nav-command-grid">
                        {group.links.map(({ href, label, icon }) => {
                          const active = isActivePath(pathname, href);
                          return (
                            <Link
                              key={href}
                              href={href}
                              className={`nav-more-item nav-command-item${active ? " active" : ""}`}
                              role="menuitem"
                              aria-current={active ? "page" : undefined}
                              onClick={() => setCommandDeckOpen(false)}
                            >
                              <span className="nav-command-item-icon" aria-hidden="true">{icon}</span>
                              <span>{label}</span>
                            </Link>
                          );
                        })}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {tier === "dev" && (
              <Link
                href="/dev"
                className={`nav-link nav-link-overhaul${pathname === "/dev" ? " active" : ""}`}
                aria-current={pathname === "/dev" ? "page" : undefined}
                title="Dev Dashboard"
              >
                <span className="nav-link-icon" aria-hidden="true">⚙</span>
                <span>Dev</span>
              </Link>
            )}
            {hasEnterpriseDocsAccess && (
              <Link
                href="/api-guide"
                className={`nav-link nav-link-overhaul${pathname === "/api-guide" ? " active" : ""}`}
                aria-current={pathname === "/api-guide" ? "page" : undefined}
                title="Enterprise API Guide"
              >
                <span className="nav-link-icon" aria-hidden="true">◎</span>
                <span>API</span>
              </Link>
            )}
            {tier === "dev" && (
              <Link
                href="/api-docs"
                className={`nav-link nav-link-overhaul${pathname === "/api-docs" ? " active" : ""}`}
                aria-current={pathname === "/api-docs" ? "page" : undefined}
                title="Technical API Documentation"
              >
                <span className="nav-link-icon" aria-hidden="true">⋯</span>
                <span>Docs</span>
              </Link>
            )}
          </nav>

          <div className="nav-actions nav-actions-overhaul">
            <button
              className="theme-toggle"
              onClick={toggle}
              aria-label={`Theme: ${preference}. Click to switch.`}
              title={`Theme: ${preference === "system" ? "System" : preference === "dark" ? "Dark" : "Light"}`}
            >
              {preference === "system" ? "🖥" : theme === "dark" ? "☀" : "◐"}
            </button>
            {isLoading ? null : isLoggedIn ? (
              <>
                <Link href="/dashboard" className="btn btn-ghost btn-sm nav-user-btn">
                  <span className="nav-avatar">{userName.charAt(0).toUpperCase()}</span>
                  <span className="nav-user-name">{userName || userEmail}</span>
                  {tier !== "free" && <span className={`tier-badge tier-badge-${tier} tier-badge-sm`}>{tier}</span>}
                </Link>
                {showGetPremium && (
                  <Link href="/pricing" className="btn btn-primary btn-sm">Upgrade</Link>
                )}
                <button onClick={handleSignOut} className="btn btn-secondary btn-sm">Sign out</button>
              </>
            ) : (
              <>
                <Link href="/login" className="btn btn-ghost btn-sm">Sign in</Link>
                <Link href="/register" className="btn btn-secondary btn-sm">Register</Link>
                <Link href="/pricing" className="btn btn-primary btn-sm">Upgrade</Link>
              </>
            )}
          </div>

          <button
            ref={hamburgerRef}
            className={`hamburger${menuOpen ? " open" : ""}`}
            onClick={() => setMenuOpen((v) => !v)}
            aria-label={menuOpen ? "Close navigation" : "Open navigation"}
            aria-expanded={menuOpen}
            aria-controls="mobile-menu"
          >
            <span />
            <span />
            <span />
          </button>
        </div>
      </header>

      <div
        ref={menuRef}
        id="mobile-menu"
        className={`mobile-menu mobile-menu-overhaul${menuOpen ? " open" : ""}`}
        aria-hidden={!menuOpen}
        role="dialog"
        aria-label="Mobile navigation"
      >
        {mobileGroups.map((group) => (
          <div key={group.label} className="mobile-nav-group">
            <div className="mobile-nav-heading">{group.label}</div>
            <div className="mobile-nav-grid">
              {group.links.map(({ href, label, icon }) => {
                const active = isActivePath(pathname, href);
                return (
                  <Link
                    key={`${group.label}-${href}`}
                    href={href}
                    className={`mobile-nav-link${active ? " active" : ""}`}
                    aria-current={active ? "page" : undefined}
                    tabIndex={menuOpen ? 0 : -1}
                  >
                    <span className="mobile-nav-link-icon" aria-hidden="true">{icon}</span>
                    <span>{label}</span>
                  </Link>
                );
              })}
            </div>
          </div>
        ))}
        {tier === "dev" && (
          <Link
            href="/dev"
            className={`mobile-nav-link${pathname === "/dev" ? " active" : ""}`}
            aria-current={pathname === "/dev" ? "page" : undefined}
            tabIndex={menuOpen ? 0 : -1}
          >
            <span className="mobile-nav-link-icon" aria-hidden="true">⚙</span>
            <span>Dev</span>
          </Link>
        )}
        {hasEnterpriseDocsAccess && (
          <Link
            href="/api-guide"
            className={`mobile-nav-link${pathname === "/api-guide" ? " active" : ""}`}
            aria-current={pathname === "/api-guide" ? "page" : undefined}
            tabIndex={menuOpen ? 0 : -1}
          >
            <span className="mobile-nav-link-icon" aria-hidden="true">◎</span>
            <span>API Guide</span>
          </Link>
        )}
        {tier === "dev" && (
          <Link
            href="/api-docs"
            className={`mobile-nav-link${pathname === "/api-docs" ? " active" : ""}`}
            aria-current={pathname === "/api-docs" ? "page" : undefined}
            tabIndex={menuOpen ? 0 : -1}
          >
            <span className="mobile-nav-link-icon" aria-hidden="true">⋯</span>
            <span>Tech Docs</span>
          </Link>
        )}
        <div className="mobile-menu-divider" />
        <div className="mobile-menu-actions">
          {isLoggedIn ? (
            <>
              <Link href="/dashboard" className="btn btn-ghost btn-sm" tabIndex={menuOpen ? 0 : -1}>
                Account {tier !== "free" && <span className={`tier-badge tier-badge-${tier} tier-badge-sm`}>{tier}</span>}
              </Link>
              {showGetPremium && <Link href="/pricing" className="btn btn-primary btn-sm" tabIndex={menuOpen ? 0 : -1}>Upgrade</Link>}
              <button onClick={handleSignOut} className="btn btn-secondary btn-sm" tabIndex={menuOpen ? 0 : -1}>Sign out</button>
            </>
          ) : (
            <>
              <Link href="/login" className="btn btn-ghost btn-sm" tabIndex={menuOpen ? 0 : -1}>Sign in</Link>
              <Link href="/register" className="btn btn-secondary btn-sm" tabIndex={menuOpen ? 0 : -1}>Register</Link>
              <Link href="/pricing" className="btn btn-primary btn-sm" tabIndex={menuOpen ? 0 : -1}>Upgrade</Link>
            </>
          )}
          <button
            className="theme-toggle"
            onClick={toggle}
            aria-label={`Theme: ${preference}. Click to switch.`}
            style={{ marginLeft: "auto" }}
            tabIndex={menuOpen ? 0 : -1}
          >
            {preference === "system" ? "🖥 System" : theme === "dark" ? "☀ Light" : "◐ Dark"}
          </button>
        </div>
      </div>
      <div
        className={`mobile-nav-overlay${menuOpen ? " open" : ""}`}
        onClick={() => setMenuOpen(false)}
        aria-hidden="true"
      />
    </>
  );
}
