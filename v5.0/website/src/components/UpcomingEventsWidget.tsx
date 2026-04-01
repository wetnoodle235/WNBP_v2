"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { getSportIcon, getSportColor } from "@/lib/sports-config";
import { resolveServerApiBase } from "@/lib/api-base";
import { F1Circuit, IndyCarTrack } from "@/components/venue";

interface UpcomingEvent {
  id: string;
  sport: string;
  date: string;
  start_time?: string | null;
  status: string;
  race_name?: string | null;
  circuit_name?: string | null;
  venue?: string | null;
  broadcast?: string | null;
  round_number?: number | null;
  home_team?: string | null;
  day_of_week?: string | null;
}

interface EventsMeta {
  count: number;
  from_date: string;
  to_date: string;
}

const SPORT_LABELS: Record<string, string> = {
  f1: "Formula 1",
  indycar: "IndyCar",
  golf: "PGA Tour",
  lpga: "LPGA",
  pga: "PGA Tour",
  atp: "ATP",
  wta: "WTA",
  ufc: "UFC",
  mma: "MMA",
};

function formatEventDate(dateStr: string, startTime?: string | null): string {
  try {
    if (startTime) {
      const d = new Date(startTime);
      if (!isNaN(d.getTime())) {
        return d.toLocaleDateString("en-US", {
          weekday: "short",
          month: "short",
          day: "numeric",
          hour: "numeric",
          minute: "2-digit",
          timeZoneName: "short",
        });
      }
    }
    const d = new Date(dateStr + "T12:00:00");
    return d.toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" });
  } catch {
    return dateStr;
  }
}

function daysUntil(dateStr: string): number {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const event = new Date(dateStr + "T12:00:00");
  event.setHours(0, 0, 0, 0);
  return Math.round((event.getTime() - today.getTime()) / (1000 * 60 * 60 * 24));
}

function DaysChip({ days }: { days: number }) {
  if (days === 0) return <span className="upcoming-chip upcoming-chip-today">Today</span>;
  if (days === 1) return <span className="upcoming-chip upcoming-chip-soon">Tomorrow</span>;
  if (days <= 7) return <span className="upcoming-chip upcoming-chip-soon">{days}d</span>;
  return <span className="upcoming-chip upcoming-chip-later">{days}d</span>;
}

interface UpcomingEventsWidgetProps {
  sports?: string[];
  days?: number;
  maxEvents?: number;
  apiBase?: string;
}

export default function UpcomingEventsWidget({
  sports = ["f1", "indycar", "golf", "lpga"],
  days = 60,
  maxEvents = 10,
  apiBase,
}: UpcomingEventsWidgetProps) {
  const [events, setEvents] = useState<UpcomingEvent[]>([]);
  const [meta, setMeta] = useState<EventsMeta | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [activeSport, setActiveSport] = useState<string>("all");

  const base =
    apiBase ??
    (typeof window === "undefined"
      ? resolveServerApiBase()
      : "/api/proxy");

  useEffect(() => {
    const ac = new AbortController();
    const sportsParam = sports.join(",");
    fetch(`${base}/v1/events/upcoming?days=${days}&sports=${sportsParam}`, {
      signal: ac.signal,
      cache: "default",
    })
      .then((r) => (r.ok ? r.json() : null))
      .then((j) => {
        if (j?.data && !ac.signal.aborted) {
          setEvents(j.data as UpcomingEvent[]);
          setMeta(j.meta ?? null);
        } else if (!ac.signal.aborted) {
          setError(true);
        }
      })
      .catch(() => {
        if (!ac.signal.aborted) setError(true);
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false);
      });
    return () => ac.abort();
  }, [base, sports.join(","), days]);

  const availableSports = Array.from(new Set(events.map((e) => e.sport)));
  const filtered =
    activeSport === "all" ? events : events.filter((e) => e.sport === activeSport);
  const displayed = filtered.slice(0, maxEvents);

  if (loading) {
    return (
      <section className="card dashboard-mt-6" aria-labelledby="upcoming-events-heading">
        <div className="card-header">
          <h2 id="upcoming-events-heading" className="card-heading">Upcoming Events</h2>
        </div>
        <div className="card-body dashboard-center-muted">
          <p>Loading upcoming events…</p>
        </div>
      </section>
    );
  }

  if (error || events.length === 0) {
    return null; // Don't show widget if no data
  }

  return (
    <section className="card dashboard-mt-6" aria-labelledby="upcoming-events-heading">
      <div className="card-header dashboard-card-header-flex">
        <h2 id="upcoming-events-heading" className="card-heading">Upcoming Events</h2>
        {meta && (
          <span className="dashboard-text-muted-sm">
            Next {days} days · {meta.count} events
          </span>
        )}
      </div>

      {/* Sport filter pills */}
      {availableSports.length > 1 && (
        <div className="card-body" style={{ paddingBottom: 0 }}>
          <div className="upcoming-sport-pills">
            <button
              className={`upcoming-pill${activeSport === "all" ? " upcoming-pill-active" : ""}`}
              onClick={() => setActiveSport("all")}
            >
              All
            </button>
            {availableSports.map((s) => (
              <button
                key={s}
                className={`upcoming-pill${activeSport === s ? " upcoming-pill-active" : ""}`}
                style={activeSport === s ? { borderColor: getSportColor(s), color: getSportColor(s) } : {}}
                onClick={() => setActiveSport(s)}
              >
                {getSportIcon(s)} {SPORT_LABELS[s] ?? s.toUpperCase()}
              </button>
            ))}
          </div>
        </div>
      )}

      <div className="card-body dashboard-pad-compact">
        <div className="upcoming-events-list">
          {displayed.map((ev) => {
            const eventName = ev.race_name || ev.home_team || "Event";
            const d = daysUntil(ev.date);
            const color = getSportColor(ev.sport);
            const icon = getSportIcon(ev.sport);
            return (
              <Link
                key={ev.id || `${ev.sport}-${ev.date}-${eventName}`}
                href={`/sports/${ev.sport}`}
                className="upcoming-event-row"
                style={{ "--sport-color": color } as React.CSSProperties}
              >
                <div className="upcoming-event-sport-badge" style={{ background: `${color}22`, color }}>
                  {ev.sport === "f1" ? (
                    <F1Circuit
                      width={36}
                      height={28}
                      circuitKey={ev.circuit_name ?? ev.venue ?? "bahrain"}
                    />
                  ) : ev.sport === "indycar" ? (
                    <IndyCarTrack
                      width={36}
                      height={28}
                      trackName={ev.venue ?? ev.circuit_name ?? ""}
                    />
                  ) : (
                    icon
                  )}
                </div>
                <div className="upcoming-event-info">
                  <span className="upcoming-event-name">{eventName}</span>
                  <span className="upcoming-event-meta">
                    {SPORT_LABELS[ev.sport] ?? ev.sport.toUpperCase()}
                    {ev.circuit_name && ` · ${ev.circuit_name}`}
                    {!ev.circuit_name && ev.venue && ` · ${ev.venue}`}
                    {ev.broadcast && ` · ${ev.broadcast}`}
                  </span>
                </div>
                <div className="upcoming-event-right">
                  <span className="upcoming-event-date">{formatEventDate(ev.date, ev.start_time)}</span>
                  <DaysChip days={d} />
                </div>
              </Link>
            );
          })}
        </div>
        {filtered.length > maxEvents && (
          <p className="dashboard-text-muted-sm" style={{ marginTop: "var(--space-2)", textAlign: "center" }}>
            +{filtered.length - maxEvents} more events
          </p>
        )}
      </div>
    </section>
  );
}
