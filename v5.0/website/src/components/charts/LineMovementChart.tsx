"use client";

import { useEffect, useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import { ChartPanel } from "@/components/ui";

interface MovementPoint {
  timestamp: string;
  bookmaker?: string;
  h2h_home?: number;
  h2h_away?: number;
  spread_home?: number;
  total_line?: number;
}

type LineKey = "h2h_home" | "h2h_away" | "spread_home" | "total_line";

interface Props {
  sport: string;
  gameId: string;
  /** Which line to chart. Defaults to moneyline home. */
  lineType?: LineKey;
  title?: string;
  height?: number;
}

const LINE_LABELS: Record<LineKey, string> = {
  h2h_home: "ML Home",
  h2h_away: "ML Away",
  spread_home: "Spread",
  total_line: "Total",
};

const LINE_COLORS: Record<LineKey, string> = {
  h2h_home: "#6366f1",
  h2h_away: "#f59e0b",
  spread_home: "#10b981",
  total_line: "#ef4444",
};

export function LineMovementChart({ sport, gameId, lineType = "h2h_home", title, height = 220 }: Props) {
  const [data, setData] = useState<MovementPoint[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(
      `/api/proxy/v1/${sport}/odds_history?game_id=${encodeURIComponent(gameId)}&limit=200`
    )
      .then((r) => r.json())
      .then((d) => {
        const pts: MovementPoint[] = (d.data ?? [])
          .filter((p: MovementPoint) => p[lineType] != null)
          .map((p: MovementPoint) => ({
            ...p,
            timestamp: p.timestamp
              ? new Date(p.timestamp).toLocaleTimeString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })
              : String(p.timestamp),
          }));
        setData(pts);
      })
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, [sport, gameId, lineType]);

  const isEmpty = !loading && data.length < 2;
  const label = LINE_LABELS[lineType];
  const color = LINE_COLORS[lineType];

  return (
    <ChartPanel
      title={title ?? `Line Movement — ${label}`}
      description={isEmpty ? "Insufficient historical odds data for this game." : undefined}
      loading={loading}
      height={isEmpty ? 80 : height}
    >
      {!isEmpty && (
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 8, right: 8, bottom: 8, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border, #374151)" />
            <XAxis
              dataKey="timestamp"
              tick={{ fontSize: 9, fill: "var(--color-text-muted)" }}
              interval="preserveStartEnd"
            />
            <YAxis tick={{ fontSize: 10, fill: "var(--color-text-muted)" }} width={44} />
            <Tooltip
              contentStyle={{
                background: "var(--color-surface)",
                border: "1px solid var(--color-border)",
                borderRadius: "var(--radius)",
                fontSize: 12,
              }}
            />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            <Line
              type="stepAfter"
              dataKey={lineType}
              stroke={color}
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4 }}
              name={label}
            />
          </LineChart>
        </ResponsiveContainer>
      )}
    </ChartPanel>
  );
}
