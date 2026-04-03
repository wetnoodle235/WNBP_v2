"use client";

import { useEffect, useState } from "react";
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, ReferenceLine,
} from "recharts";
import { ChartPanel } from "@/components/ui";

interface ForecastPoint {
  ds: string;
  y?: number;
  yhat: number;
  yhat_lower: number;
  yhat_upper: number;
  is_forecast: boolean;
}

interface Props {
  sport: string;
  stat: string;
  periods?: number;
  teamId?: string;
  playerId?: string;
  title?: string;
  height?: number;
}

export function ForecastChart({
  sport,
  stat,
  periods = 30,
  teamId,
  playerId,
  title,
  height = 300,
}: Props) {
  const [data, setData] = useState<ForecastPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [splitIndex, setSplitIndex] = useState<number | null>(null);

  useEffect(() => {
    const params = new URLSearchParams({ stat, periods: String(periods) });
    if (teamId) params.set("team_id", teamId);
    if (playerId) params.set("player_id", playerId);

    fetch(`/api/proxy/v1/${sport}/charts/forecast?${params}`)
      .then((r) => r.json())
      .then((d) => {
        const pts: ForecastPoint[] = d.data ?? [];
        setData(pts);
        const idx = pts.findIndex((p) => p.is_forecast);
        setSplitIndex(idx > 0 ? idx : null);
      })
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, [sport, stat, periods, teamId, playerId]);

  const splitLabel = splitIndex !== null && data[splitIndex]
    ? data[splitIndex].ds
    : null;

  return (
    <ChartPanel title={title ?? `${stat} Forecast`} loading={loading} height={height}>
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
          <defs>
            <linearGradient id="bandGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#6366f1" stopOpacity={0.25} />
              <stop offset="95%" stopColor="#6366f1" stopOpacity={0.04} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border, #374151)" />
          <XAxis dataKey="ds" tick={{ fontSize: 10, fill: "var(--color-text-muted)" }} />
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
          {splitLabel && (
            <ReferenceLine
              x={splitLabel}
              stroke="var(--color-text-muted)"
              strokeDasharray="4 2"
              label={{ value: "Forecast", position: "insideTopRight", fontSize: 10 }}
            />
          )}
          <Area
            type="monotone"
            dataKey="yhat_upper"
            stroke="transparent"
            fill="url(#bandGrad)"
            name="Upper bound"
          />
          <Area
            type="monotone"
            dataKey="yhat_lower"
            stroke="transparent"
            fill="var(--color-surface)"
            name="Lower bound"
          />
          <Area
            type="monotone"
            dataKey="yhat"
            stroke="#6366f1"
            strokeWidth={2}
            fill="none"
            dot={false}
            activeDot={{ r: 4 }}
            name="Forecast"
          />
          <Area
            type="monotone"
            dataKey="y"
            stroke="#10b981"
            strokeWidth={2}
            fill="none"
            dot={false}
            activeDot={{ r: 4 }}
            name="Actual"
          />
        </AreaChart>
      </ResponsiveContainer>
    </ChartPanel>
  );
}
