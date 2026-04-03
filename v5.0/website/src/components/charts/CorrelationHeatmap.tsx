"use client";

import { useEffect, useState } from "react";
import { ChartPanel } from "@/components/ui";

interface Props {
  sport: string;
  stats?: string[];
  teamId?: string;
  title?: string;
  height?: number;
}

export function CorrelationHeatmap({ sport, stats, teamId, title, height = 360 }: Props) {
  const [src, setSrc] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const params = new URLSearchParams();
    if (stats?.length) params.set("stats", stats.join(","));
    if (teamId) params.set("team_id", teamId);

    fetch(`/api/proxy/v1/${sport}/charts/correlation.png?${params}`)
      .then(async (r) => {
        if (!r.ok) throw new Error("failed");
        const blob = await r.blob();
        setSrc(URL.createObjectURL(blob));
      })
      .catch(() => setSrc(null))
      .finally(() => setLoading(false));
  }, [sport, stats, teamId]);

  return (
    <ChartPanel title={title ?? "Stat Correlation Matrix"} loading={loading} height={height}>
      {src ? (
        <img
          src={src}
          alt="Correlation heatmap"
          style={{ width: "100%", height: "100%", objectFit: "contain" }}
        />
      ) : (
        !loading && (
          <p style={{ color: "var(--color-text-muted)", fontSize: 13, textAlign: "center", paddingTop: 40 }}>
            Not enough data to render heatmap.
          </p>
        )
      )}
    </ChartPanel>
  );
}
