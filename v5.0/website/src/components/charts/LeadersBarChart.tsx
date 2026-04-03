"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, LabelList, Cell,
} from "recharts";
import { ChartPanel } from "@/components/ui";

interface LeaderEntry {
  name: string;
  value: number;
  team?: string;
}

interface Props {
  sport: string;
  stat: string;
  limit?: number;
  title?: string;
  color?: string;
  /** Flip to horizontal layout for long names */
  horizontal?: boolean;
  height?: number;
}

const PALETTE = [
  "#6366f1","#8b5cf6","#06b6d4","#10b981","#f59e0b",
  "#ef4444","#ec4899","#3b82f6","#14b8a6","#a855f7",
];

export function LeadersBarChart({
  sport,
  stat,
  limit = 10,
  title,
  color,
  horizontal = true,
  height = 320,
}: Props) {
  const [data, setData] = useState<LeaderEntry[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`/api/proxy/v1/${sport}/charts/leaders-bar?stat=${encodeURIComponent(stat)}&limit=${limit}`)
      .then((r) => r.json())
      .then((d) => setData(d.data ?? []))
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, [sport, stat, limit]);

  const tooltipStyle = {
    contentStyle: {
      background: "var(--color-surface)",
      border: "1px solid var(--color-border)",
      borderRadius: "var(--radius)",
      fontSize: 12,
    },
  };

  return (
    <ChartPanel title={title ?? `${stat} Leaders`} loading={loading} height={height}>
      <ResponsiveContainer width="100%" height="100%">
        {horizontal ? (
          <BarChart
            data={data}
            layout="vertical"
            margin={{ top: 4, right: 48, bottom: 4, left: 80 }}
          >
            <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="var(--color-border, #374151)" />
            <XAxis type="number" tick={{ fontSize: 11, fill: "var(--color-text-muted)" }} />
            <YAxis type="category" dataKey="name" tick={{ fontSize: 11, fill: "var(--color-text-muted)" }} width={76} />
            <Tooltip {...tooltipStyle} />
            <Bar dataKey="value" radius={[0, 4, 4, 0]}>
              {data.map((_, i) => (
                <Cell key={i} fill={color ?? PALETTE[i % PALETTE.length]} />
              ))}
              <LabelList dataKey="value" position="right" style={{ fontSize: 11, fill: "var(--color-text-muted)" }} />
            </Bar>
          </BarChart>
        ) : (
          <BarChart data={data} margin={{ top: 4, right: 8, bottom: 24, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--color-border, #374151)" />
            <XAxis dataKey="name" tick={{ fontSize: 10, fill: "var(--color-text-muted)" }} angle={-30} textAnchor="end" />
            <YAxis tick={{ fontSize: 11, fill: "var(--color-text-muted)" }} width={40} />
            <Tooltip {...tooltipStyle} />
            <Bar dataKey="value" radius={[4, 4, 0, 0]}>
              {data.map((_, i) => (
                <Cell key={i} fill={color ?? PALETTE[i % PALETTE.length]} />
              ))}
            </Bar>
          </BarChart>
        )}
      </ResponsiveContainer>
    </ChartPanel>
  );
}
