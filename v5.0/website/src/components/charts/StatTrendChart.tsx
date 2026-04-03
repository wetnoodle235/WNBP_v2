"use client";

import { useEffect, useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import { ChartPanel } from "@/components/ui";

interface StatTrendPoint {
  label: string;
  [stat: string]: string | number;
}

interface Props {
  sport: string;
  stat: string;
  teamId?: string;
  playerId?: string;
  limit?: number;
  title?: string;
  color?: string;
  height?: number;
}

export function StatTrendChart({
  sport,
  stat,
  teamId,
  playerId,
  limit = 20,
  title,
  color = "var(--color-primary, #6366f1)",
  height = 280,
}: Props) {
  const [data, setData] = useState<StatTrendPoint[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const params = new URLSearchParams({ stat, limit: String(limit) });
    if (teamId) params.set("team_id", teamId);
    if (playerId) params.set("player_id", playerId);

    fetch(`/api/proxy/v1/${sport}/charts/stat-trend?${params}`)
      .then((r) => r.json())
      .then((d) => setData(d.data ?? []))
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, [sport, stat, teamId, playerId, limit]);

  return (
    <ChartPanel title={title ?? `${stat} Trend`} loading={loading} height={height}>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border, #374151)" />
          <XAxis dataKey="label" tick={{ fontSize: 11, fill: "var(--color-text-muted)" }} />
          <YAxis tick={{ fontSize: 11, fill: "var(--color-text-muted)" }} width={40} />
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
            type="monotone"
            dataKey={stat}
            stroke={color}
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 4 }}
          />
        </LineChart>
      </ResponsiveContainer>
    </ChartPanel>
  );
}
