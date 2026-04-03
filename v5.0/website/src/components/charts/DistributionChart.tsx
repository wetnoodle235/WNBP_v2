"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Cell,
} from "recharts";
import { ChartPanel } from "@/components/ui";

interface BinEntry {
  bin: string;
  count: number;
}

interface Props {
  sport: string;
  stat: string;
  teamId?: string;
  title?: string;
  color?: string;
  height?: number;
}

export function DistributionChart({ sport, stat, teamId, title, color = "#6366f1", height = 260 }: Props) {
  const [data, setData] = useState<BinEntry[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const params = new URLSearchParams({ stat });
    if (teamId) params.set("team_id", teamId);

    fetch(`/api/proxy/v1/${sport}/charts/distribution?${params}`)
      .then((r) => r.json())
      .then((d) => setData(d.data ?? []))
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, [sport, stat, teamId]);

  return (
    <ChartPanel title={title ?? `${stat} Distribution`} loading={loading} height={height}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 4, right: 8, bottom: 4, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--color-border, #374151)" />
          <XAxis dataKey="bin" tick={{ fontSize: 10, fill: "var(--color-text-muted)" }} />
          <YAxis tick={{ fontSize: 11, fill: "var(--color-text-muted)" }} width={36} />
          <Tooltip
            contentStyle={{
              background: "var(--color-surface)",
              border: "1px solid var(--color-border)",
              borderRadius: "var(--radius)",
              fontSize: 12,
            }}
          />
          <Bar dataKey="count" radius={[3, 3, 0, 0]} name="Count">
            {data.map((_, i) => (
              <Cell key={i} fill={color} fillOpacity={0.7 + (i / data.length) * 0.3} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </ChartPanel>
  );
}
