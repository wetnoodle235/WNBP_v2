"use client";

interface IndyCarTrackProps {
  className?: string;
  width?: number;
  height?: number;
  /** "oval" = superspeedway oval; "road" = road course */
  variant?: "oval" | "road";
  trackName?: string;
}

// Known IndyCar ovals
const OVAL_TRACKS: Record<string, string> = {
  indianapolis: "M 30 110 A 80 55 0 1 1 190 110 A 80 55 0 1 1 30 110 Z",
  pocono: "M 100 20 L 185 80 Q 195 110 180 140 L 100 170 Q 40 175 20 140 L 15 80 Q 25 40 100 20 Z",
  iowa: "M 35 110 A 75 48 0 1 1 185 110 A 75 48 0 1 1 35 110 Z",
  nashville: "M 40 110 A 72 50 0 1 1 180 110 A 72 50 0 1 1 40 110 Z",
  gateway: "M 38 108 A 70 50 0 1 1 182 108 A 70 50 0 1 1 38 108 Z",
};

// Road courses (simplified)
const ROAD_TRACKS: Record<string, string> = {
  long_beach: "M 100 20 L 160 22 Q 180 24 180 50 L 175 80 Q 165 95 145 98 L 115 100 Q 90 105 85 125 L 80 155 Q 75 170 55 172 L 30 170 Q 15 162 15 142 L 18 100 Q 22 75 45 68 L 75 60 Q 95 52 98 35 L 100 20 Z",
  mid_ohio: "M 95 18 L 140 20 Q 162 22 165 48 L 160 75 Q 155 92 135 96 L 108 99 Q 85 103 80 124 L 76 150 Q 70 168 50 170 L 25 168 Q 10 160 12 140 L 15 105 Q 18 85 40 78 L 68 70 Q 88 63 90 44 L 95 18 Z",
};

function normalizeTrackKey(name: string): { key: string; type: "oval" | "road" } {
  const s = name.toLowerCase();
  if (s.includes("indianapolis") || s.includes("indy 500") || s.includes("ims")) return { key: "indianapolis", type: "oval" };
  if (s.includes("pocono")) return { key: "pocono", type: "oval" };
  if (s.includes("iowa")) return { key: "iowa", type: "oval" };
  if (s.includes("nashville") || s.includes("superspeedway")) return { key: "nashville", type: "oval" };
  if (s.includes("gateway")) return { key: "gateway", type: "oval" };
  if (s.includes("long beach")) return { key: "long_beach", type: "road" };
  if (s.includes("mid-ohio") || s.includes("mid ohio")) return { key: "mid_ohio", type: "road" };
  // Determine by keywords
  if (s.includes("oval") || s.includes("speedway") || s.includes("500")) return { key: "indianapolis", type: "oval" };
  return { key: "long_beach", type: "road" };
}

export default function IndyCarTrack({
  className,
  width = 280,
  height = 220,
  variant,
  trackName = "",
}: IndyCarTrackProps) {
  const detected = normalizeTrackKey(trackName);
  const type = variant ?? detected.type;
  const key = detected.key;

  const trackPath =
    type === "oval"
      ? (OVAL_TRACKS[key] ?? OVAL_TRACKS.indianapolis)
      : (ROAD_TRACKS[key] ?? ROAD_TRACKS.long_beach);

  const isOval = type === "oval";

  return (
    <div className={className} style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 6 }}>
      <svg
        viewBox="0 0 220 180"
        width={width}
        height={height}
        aria-label={`${trackName || "IndyCar track"} diagram`}
        style={{ display: "block", filter: "drop-shadow(0 2px 8px rgba(0,0,0,0.4))" }}
      >
        {/* Background */}
        <rect x={0} y={0} width={220} height={180} fill="#1a1a2e" rx={8} />

        {/* Infield (oval only) */}
        {isOval && (
          <path
            d={trackPath}
            fill="#1f3a1f"
            opacity={0.6}
          />
        )}

        {/* Track outer shadow */}
        <path
          d={trackPath}
          fill="none"
          stroke="rgba(255,255,255,0.06)"
          strokeWidth={isOval ? 28 : 16}
          strokeLinecap="round"
          strokeLinejoin="round"
        />

        {/* Track surface */}
        <path
          d={trackPath}
          fill="none"
          stroke={isOval ? "#3d3d4d" : "#4a4a5a"}
          strokeWidth={isOval ? 22 : 10}
          strokeLinecap="round"
          strokeLinejoin="round"
        />

        {/* Track markings */}
        <path
          d={trackPath}
          fill="none"
          stroke="#fff"
          strokeWidth={0.8}
          strokeLinecap="round"
          strokeLinejoin="round"
          opacity={0.5}
        />

        {/* Start/Finish line (IndyCar uses a "yard of bricks" at Indy) */}
        <path
          d={trackPath}
          fill="none"
          stroke={key === "indianapolis" ? "#c8a87c" : "#e8002d"}
          strokeWidth={3}
          strokeDasharray="4 900"
          strokeLinecap="square"
          opacity={0.9}
        />

        {/* Oval banked turn indicators */}
        {isOval && (
          <>
            <text x={110} y={16} textAnchor="middle" fill="#aaa" fontSize={8}>T1-T2</text>
            <text x={110} y={172} textAnchor="middle" fill="#aaa" fontSize={8}>T3-T4</text>
            <text x={8} y={95} textAnchor="middle" fill="#aaa" fontSize={8} transform="rotate(-90,8,95)">BACK STR</text>
            <text x={213} y={95} textAnchor="middle" fill="#aaa" fontSize={8} transform="rotate(90,213,95)">MAIN STR</text>
          </>
        )}
      </svg>

      <div style={{ textAlign: "center", lineHeight: 1.3 }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: "var(--color-text, #fff)", letterSpacing: "0.5px" }}>
          {trackName || (isOval ? "Oval Speedway" : "Road Course")}
        </div>
        <div style={{ fontSize: 10, color: "var(--color-text-muted, #aaa)" }}>
          {isOval ? "🏁 Oval" : "🔄 Road Course"}
        </div>
      </div>
    </div>
  );
}
