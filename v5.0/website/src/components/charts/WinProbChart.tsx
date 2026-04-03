"use client";

import { useEffect, useState } from "react";
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, ReferenceLine,
} from "recharts";
import { ChartPanel } from "@/components/ui";

interface WinProbPoint {
  play: number;
  wp: number;
  home_score?: number;
  away_score?: number;
  description?: string;
  quarter?: number | string;
}

interface Props {
  sport: string;
  gameId: string;
  homeTeam: string;
  awayTeam: string;
  height?: number;
}

export function WinProbChart({ sport, gameId, homeTeam, awayTeam, height = 240 }: Props) {
  const [data, setData] = useState<WinProbPoint[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`/api/proxy/v1/${sport}/charts/win-probability?game_id=${encodeURIComponent(gameId)}`)
      .then((r) => r.json())
      .then((d) => setData(d.data ?? []))
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, [sport, gameId]);

  const isEmpty = !loading && data.length === 0;

  return (
    <ChartPanel
      title="Win Probability"
      description={isEmpty ? "Play-by-play data not available for this game." : undefined}
      loading={loading}
      height={isEmpty ? 80 : height}
    >
      {!isEmpty && (
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 8, right: 8, bottom: 8, left: 0 }}>
            <defs>
              <linearGradient id="wpGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#6366f1" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#6366f1" stopOpacity={0.02} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border, #374151)" />
            <XAxis
              dataKey="play"
              tick={{ fontSize: 10, fill: "var(--color-text-muted)" }}
              label={{ value: "Play #", position: "insideBottom", offset: -4, fontSize: 10 }}
            />
            <YAxis
              domain={[0, 1]}
              tickFormatter={(v: number) => `${Math.round(v * 100)}%`}
              tick={{ fontSize: 10, fill: "var(--color-text-muted)" }}
              width={40}
            />
            <Tooltip
              formatter={(v) => [`${typeof v === "number" ? (v * 100).toFixed(1) : "?"}%`, `${homeTeam} WP`]}
              contentStyle={{
                background: "var(--color-surface)",
                border: "1px solid var(--color-border)",
                borderRadius: "var(--radius)",
                fontSize: 12,
              }}
            />
            <ReferenceLine y={0.5} stroke="var(--color-text-muted)" strokeDasharray="4 2" />
            <Area
              type="monotone"
              dataKey="wp"
              stroke="#6366f1"
              strokeWidth={2}
              fill="url(#wpGrad)"
              dot={false}
              activeDot={{ r: 3 }}
              name={`${homeTeam} WP`}
            />
          </AreaChart>
        </ResponsiveContainer>
      )}
    </ChartPanel>
  );
}
