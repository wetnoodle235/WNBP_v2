"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Cell, ReferenceLine,
} from "recharts";
import { ChartPanel } from "@/components/ui";

interface SHAPEntry {
  feature: string;
  shap_value: number;
}

interface Props {
  sport: string;
  /** Model name as understood by the backend (e.g. "spread", "total") */
  model?: string;
  title?: string;
  height?: number;
}

export function SHAPChart({ sport, model = "spread", title, height = 340 }: Props) {
  const [data, setData] = useState<SHAPEntry[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`/api/proxy/v1/${sport}/charts/shap?model=${encodeURIComponent(model)}`)
      .then((r) => r.json())
      .then((d) => {
        const raw: SHAPEntry[] = d.data ?? [];
        // Sort by absolute value descending, keep top 15
        const sorted = [...raw]
          .sort((a, b) => Math.abs(b.shap_value) - Math.abs(a.shap_value))
          .slice(0, 15);
        setData(sorted);
      })
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, [sport, model]);

  return (
    <ChartPanel title={title ?? "Feature Importance (SHAP)"} loading={loading} height={height}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart
          data={data}
          layout="vertical"
          margin={{ top: 4, right: 48, bottom: 4, left: 140 }}
        >
          <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="var(--color-border, #374151)" />
          <XAxis type="number" tick={{ fontSize: 11, fill: "var(--color-text-muted)" }} />
          <YAxis
            type="category"
            dataKey="feature"
            tick={{ fontSize: 11, fill: "var(--color-text-muted)" }}
            width={136}
          />
          <Tooltip
            formatter={(v) => (typeof v === "number" ? v.toFixed(4) : String(v ?? ""))}
            contentStyle={{
              background: "var(--color-surface)",
              border: "1px solid var(--color-border)",
              borderRadius: "var(--radius)",
              fontSize: 12,
            }}
          />
          <ReferenceLine x={0} stroke="var(--color-text-muted)" />
          <Bar dataKey="shap_value" radius={[0, 4, 4, 0]} name="SHAP value">
            {data.map((entry, i) => (
              <Cell
                key={i}
                fill={entry.shap_value >= 0 ? "#10b981" : "#ef4444"}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </ChartPanel>
  );
}
