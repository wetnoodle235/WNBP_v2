"use client";

import { useEffect, useState } from "react";
import { ChartPanel } from "@/components/ui";

interface Props {
  sport: string;
  /** Words to build the cloud from (will be joined and sent to the backend) */
  words?: string[];
  /** Raw text to send directly */
  text?: string;
  title?: string;
  height?: number;
}

export function WordCloudImage({ sport, words, text, title, height = 280 }: Props) {
  const [src, setSrc] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const body = text ?? (words ?? []).join(" ");
    if (!body.trim()) {
      setLoading(false);
      return;
    }

    fetch(`/api/proxy/v1/${sport}/charts/wordcloud.png`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text: body }),
    })
      .then(async (r) => {
        if (!r.ok) throw new Error("failed");
        const blob = await r.blob();
        setSrc(URL.createObjectURL(blob));
      })
      .catch(() => setSrc(null))
      .finally(() => setLoading(false));
  }, [sport, words, text]);

  return (
    <ChartPanel title={title ?? "Topic Cloud"} loading={loading} height={height}>
      {src ? (
        <img
          src={src}
          alt="Word cloud"
          style={{ width: "100%", height: "100%", objectFit: "contain" }}
        />
      ) : (
        !loading && (
          <p style={{ color: "var(--color-text-muted)", fontSize: 13, textAlign: "center", paddingTop: 40 }}>
            No data for word cloud.
          </p>
        )
      )}
    </ChartPanel>
  );
}
