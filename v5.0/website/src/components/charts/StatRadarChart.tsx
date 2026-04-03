"use client";

import { useEffect, useState } from "react";
import {
  RadarChart, Radar, PolarGrid, PolarAngleAxis,
  PolarRadiusAxis, ResponsiveContainer, Tooltip,
} from "recharts";
import { ChartPanel } from "@/components/ui";

interface RadarPoint {
  subject: string;
  value: number;
  fullMark?: number;
}

interface Props {
  /** Pre-loaded data (avoids extra fetch when parent already has the data) */
  data?: RadarPoint[];
  /** Fetch from backend if data not supplied */
  sport?: string;
  playerId?: string;
  stats?: string[];
  title?: string;
  color?: string;
  height?: number;
}

export function StatRadarChart({
  data: propData,
  sport,
  playerId,
  stats,
  title = "Player Profile",
  color = "#6366f1",
  height = 300,
}: Props) {
  const [data, setData] = useState<RadarPoint[]>(propData ?? []);
  const [loading, setLoading] = useState(!propData);

  useEffect(() => {
    if (propData) {
      setData(propData);
      setLoading(false);
      return;
    }
    if (!sport || !playerId) {
      setLoading(false);
      return;
    }
    const params = new URLSearchParams();
    if (stats?.length) params.set("stats", stats.join(","));

    fetch(`/api/proxy/v1/${sport}/charts/scatter?player_id=${playerId}&${params}`)
      .then((r) => r.json())
      .then((d) => {
        // Transform scatter data to radar format if possible
        const pts: RadarPoint[] = (d.data ?? []).map((p: { x: string; y: number }) => ({
          subject: p.x,
          value: p.y,
        }));
        setData(pts);
      })
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, [propData, sport, playerId, stats]);

  const max = data.length ? Math.max(...data.map((d) => d.value), 1) : 1;

  return (
    <ChartPanel title={title} loading={loading} height={height}>
      <ResponsiveContainer width="100%" height="100%">
        <RadarChart data={data}>
          <PolarGrid stroke="var(--color-border, #374151)" />
          <PolarAngleAxis dataKey="subject" tick={{ fontSize: 11, fill: "var(--color-text-muted)" }} />
          <PolarRadiusAxis
            angle={30}
            domain={[0, max]}
            tick={{ fontSize: 9, fill: "var(--color-text-muted)" }}
          />
          <Radar
            name="Stats"
            dataKey="value"
            stroke={color}
            fill={color}
            fillOpacity={0.25}
          />
          <Tooltip
            contentStyle={{
              background: "var(--color-surface)",
              border: "1px solid var(--color-border)",
              borderRadius: "var(--radius)",
              fontSize: 12,
            }}
          />
        </RadarChart>
      </ResponsiveContainer>
    </ChartPanel>
  );
}
