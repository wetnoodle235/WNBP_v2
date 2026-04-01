"use client";

import { useEffect, useRef } from "react";

interface CircuitData {
  name: string;
  path: string;
  viewBox: string;
  country: string;
  sectors: [string, string, string]; // S1 end-point %, S2 end-point %
}

// SVG path data for F1 circuits — simplified but recognizable outlines
// Paths are normalized to fit in ~200×200 viewbox
const CIRCUITS: Record<string, CircuitData> = {
  bahrain: {
    name: "Bahrain International Circuit",
    country: "Bahrain",
    viewBox: "0 0 220 200",
    sectors: ["S1", "S2", "S3"],
    path: "M 110 30 L 160 30 Q 180 30 180 50 L 180 70 Q 180 90 160 90 L 140 90 Q 120 90 120 110 L 120 130 Q 120 150 140 150 L 160 150 Q 180 150 180 130 L 180 110 Q 195 95 195 75 L 195 50 Q 195 15 160 15 L 90 15 Q 60 15 60 45 L 60 80 Q 60 100 80 100 L 90 100 Q 110 100 110 120 L 110 140 Q 110 160 90 160 L 60 160 Q 40 160 40 140 L 40 70 Q 40 50 60 50 L 110 50 Z",
  },
  jeddah: {
    name: "Jeddah Corniche Circuit",
    country: "Saudi Arabia",
    viewBox: "0 0 120 280",
    sectors: ["S1", "S2", "S3"],
    path: "M 60 10 L 80 10 Q 95 10 95 25 L 95 45 Q 95 55 85 55 L 70 55 Q 55 55 55 65 L 55 85 Q 55 95 65 95 L 80 95 Q 95 95 95 110 L 95 130 Q 95 140 85 140 L 70 140 Q 55 140 55 155 L 55 175 Q 55 185 65 185 L 80 185 Q 95 185 95 200 L 95 230 Q 95 250 75 255 L 45 255 Q 25 250 25 230 L 25 60 Q 25 10 60 10 Z",
  },
  monza: {
    name: "Autodromo Nazionale Monza",
    country: "Italy",
    viewBox: "0 0 200 200",
    sectors: ["S1", "S2", "S3"],
    path: "M 100 20 L 140 20 Q 170 20 170 50 L 170 70 Q 170 90 150 90 L 130 90 Q 100 90 100 110 L 100 140 Q 100 160 80 160 L 50 160 Q 30 160 30 140 L 30 110 Q 30 95 45 90 L 70 85 Q 80 80 80 65 L 80 50 Q 80 30 100 20 Z",
  },
  silverstone: {
    name: "Silverstone Circuit",
    country: "Great Britain",
    viewBox: "0 0 230 200",
    sectors: ["S1", "S2", "S3"],
    path: "M 40 90 L 40 60 Q 40 40 60 35 L 100 30 Q 130 25 145 40 L 165 55 Q 180 65 195 55 L 210 40 Q 220 30 215 50 L 205 80 Q 195 100 175 105 L 155 108 Q 140 110 135 125 L 130 145 Q 128 165 110 170 L 80 172 Q 55 170 45 150 L 35 125 Q 25 105 40 90 Z",
  },
  monaco: {
    name: "Circuit de Monaco",
    country: "Monaco",
    viewBox: "0 0 160 220",
    sectors: ["S1", "S2", "S3"],
    path: "M 80 15 L 115 15 Q 135 15 135 35 L 135 60 Q 135 75 120 80 L 100 85 Q 85 90 80 105 L 75 125 Q 70 140 55 145 L 35 148 Q 20 148 18 130 L 18 100 Q 18 75 35 65 L 55 55 Q 70 48 72 35 L 80 15 Z",
  },
  spa: {
    name: "Circuit de Spa-Francorchamps",
    country: "Belgium",
    viewBox: "0 0 240 200",
    sectors: ["S1", "S2", "S3"],
    path: "M 30 90 L 30 60 Q 30 40 50 35 L 80 30 Q 100 28 115 40 L 130 55 Q 145 70 165 65 L 190 55 Q 210 48 215 65 L 210 90 Q 205 110 185 115 L 155 118 Q 130 120 115 140 L 100 165 Q 88 180 68 178 L 45 170 Q 28 160 28 140 L 28 110 Q 25 95 30 90 Z",
  },
  suzuka: {
    name: "Suzuka Circuit",
    country: "Japan",
    viewBox: "0 0 210 240",
    sectors: ["S1", "S2", "S3"],
    path: "M 105 20 L 140 20 Q 165 20 170 45 L 172 75 Q 174 95 155 100 L 135 102 Q 115 105 110 125 L 108 150 Q 106 175 85 180 L 60 180 Q 38 178 35 155 L 35 120 Q 35 100 55 95 L 80 90 Q 100 85 100 65 L 100 45 Q 98 25 105 20 Z M 85 20 L 40 20 Q 20 22 18 45 L 18 70 Q 18 95 40 100 L 68 102 Q 80 103 82 90",
  },
  cota: {
    name: "Circuit of the Americas",
    country: "USA",
    viewBox: "0 0 220 210",
    sectors: ["S1", "S2", "S3"],
    path: "M 50 30 L 50 50 Q 50 65 65 68 L 100 72 Q 125 75 140 95 L 155 120 Q 165 140 185 140 L 200 138 Q 215 135 215 120 L 210 95 Q 205 75 185 70 L 160 65 Q 148 60 145 45 L 142 25 Q 138 10 120 10 L 75 12 Q 55 15 50 30 Z",
  },
  interlagos: {
    name: "Autodromo Jose Carlos Pace",
    country: "Brazil",
    viewBox: "0 0 190 200",
    sectors: ["S1", "S2", "S3"],
    path: "M 95 15 L 130 18 Q 155 22 160 50 L 158 80 Q 155 100 135 105 L 110 108 Q 88 112 80 135 L 75 160 Q 68 178 48 180 L 28 178 Q 12 172 12 152 L 15 120 Q 18 100 38 92 L 65 82 Q 82 75 85 55 L 88 30 Q 88 15 95 15 Z",
  },
  abu_dhabi: {
    name: "Yas Marina Circuit",
    country: "UAE",
    viewBox: "0 0 230 200",
    sectors: ["S1", "S2", "S3"],
    path: "M 30 95 L 30 65 Q 30 45 50 40 L 90 35 Q 115 32 130 50 L 148 70 Q 158 82 175 78 L 200 70 Q 215 65 218 80 L 215 110 Q 210 130 190 135 L 165 138 Q 148 140 140 158 L 135 178 Q 128 192 108 192 L 75 190 Q 52 185 45 162 L 38 135 Q 30 115 30 95 Z",
  },
  melbourne: {
    name: "Albert Park Circuit",
    country: "Australia",
    viewBox: "0 0 200 200",
    sectors: ["S1", "S2", "S3"],
    path: "M 100 20 L 150 22 Q 175 25 175 55 L 172 80 Q 168 95 150 100 L 125 105 Q 108 110 105 130 L 103 155 Q 100 170 82 175 L 55 178 Q 32 175 28 155 L 28 120 Q 28 100 48 95 L 72 88 Q 90 82 93 62 L 95 40 Q 95 20 100 20 Z",
  },
  singapore: {
    name: "Marina Bay Street Circuit",
    country: "Singapore",
    viewBox: "0 0 200 220",
    sectors: ["S1", "S2", "S3"],
    path: "M 100 15 L 140 15 Q 165 15 165 40 L 162 65 Q 158 82 140 88 L 118 92 Q 100 96 95 115 L 90 140 Q 85 160 68 165 L 45 168 Q 25 165 22 145 L 22 100 Q 22 75 42 68 L 68 60 Q 88 52 90 35 L 100 15 Z",
  },
};

// Fallback oval for unknown circuits
const OVAL_PATH = "M 30 100 A 70 50 0 1 1 170 100 A 70 50 0 1 1 30 100 Z";

interface F1CircuitProps {
  className?: string;
  width?: number;
  height?: number;
  /** Circuit name or key (e.g. "bahrain", "Bahrain International Circuit") */
  circuitKey?: string;
  /** Animate a dot going around the track */
  animate?: boolean;
  /** Sector highlight colors */
  sectorColors?: [string, string, string];
}

function normalizeCircuitKey(input: string): string {
  const s = input.toLowerCase();
  if (s.includes("bahrain") || s.includes("sakhir")) return "bahrain";
  if (s.includes("jeddah") || s.includes("saudi")) return "jeddah";
  if (s.includes("monza") || s.includes("italy") || s.includes("italian")) return "monza";
  if (s.includes("silverstone") || s.includes("british") || s.includes("great britain")) return "silverstone";
  if (s.includes("monaco")) return "monaco";
  if (s.includes("spa") || s.includes("belgian") || s.includes("belgium")) return "spa";
  if (s.includes("suzuka") || s.includes("japan")) return "suzuka";
  if (s.includes("americas") || s.includes("cota") || s.includes("austin")) return "cota";
  if (s.includes("interlagos") || s.includes("brazil") || s.includes("sao paulo")) return "interlagos";
  if (s.includes("abu dhabi") || s.includes("yas")) return "abu_dhabi";
  if (s.includes("albert park") || s.includes("australia") || s.includes("melbourne")) return "melbourne";
  if (s.includes("marina bay") || s.includes("singapore")) return "singapore";
  return s.replace(/\s+/g, "_");
}

export default function F1Circuit({
  className,
  width = 300,
  height = 240,
  circuitKey = "bahrain",
  animate = false,
  sectorColors = ["#e8002d", "#ffd700", "#00c6ff"],
}: F1CircuitProps) {
  const pathRef = useRef<SVGPathElement>(null);
  const dotRef = useRef<SVGCircleElement>(null);
  const animRef = useRef<number>(0);

  const key = normalizeCircuitKey(circuitKey);
  const circuit = CIRCUITS[key];
  const trackPath = circuit?.path ?? OVAL_PATH;
  const viewBox = circuit?.viewBox ?? "0 0 200 200";
  const displayName = circuit?.name ?? circuitKey;
  const country = circuit?.country ?? "";

  useEffect(() => {
    if (!animate || !pathRef.current || !dotRef.current) return;
    const path = pathRef.current;
    const dot = dotRef.current;
    const totalLen = path.getTotalLength();
    let progress = 0;
    const speed = 0.001; // fraction per ms

    let lastTime: number | null = null;
    function tick(time: number) {
      if (lastTime !== null) {
        const delta = time - lastTime;
        progress = (progress + speed * delta) % 1;
      }
      lastTime = time;
      const pt = path.getPointAtLength(progress * totalLen);
      dot.setAttribute("cx", String(pt.x));
      dot.setAttribute("cy", String(pt.y));
      animRef.current = requestAnimationFrame(tick);
    }
    animRef.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(animRef.current);
  }, [animate, key]);

  return (
    <div className={className} style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 6 }}>
      <svg
        viewBox={viewBox}
        width={width}
        height={height}
        aria-label={`${displayName} circuit map`}
        style={{ display: "block", filter: "drop-shadow(0 2px 8px rgba(0,0,0,0.4))" }}
      >
        {/* Background */}
        <rect x={0} y={0} width="100%" height="100%" fill="#1a1a2e" rx={8} />

        {/* Track shadow / glow */}
        <path
          d={trackPath}
          fill="none"
          stroke="rgba(255,255,255,0.08)"
          strokeWidth={14}
          strokeLinecap="round"
          strokeLinejoin="round"
        />

        {/* Main track surface */}
        <path
          ref={pathRef}
          d={trackPath}
          fill="none"
          stroke="#4a4a5a"
          strokeWidth={8}
          strokeLinecap="round"
          strokeLinejoin="round"
        />

        {/* Track edges */}
        <path
          d={trackPath}
          fill="none"
          stroke="#fff"
          strokeWidth={0.8}
          strokeLinecap="round"
          strokeLinejoin="round"
          opacity={0.6}
        />

        {/* Start/finish line */}
        <path
          d={trackPath}
          fill="none"
          stroke="#e8002d"
          strokeWidth={3}
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeDasharray={`2 ${/* approx total length */ 800}`}
          opacity={0.9}
        />

        {/* Animated car dot */}
        {animate && (
          <circle
            ref={dotRef}
            cx={0}
            cy={0}
            r={5}
            fill={sectorColors[0]}
            style={{ filter: `drop-shadow(0 0 4px ${sectorColors[0]})` }}
          />
        )}
      </svg>

      {/* Circuit info */}
      <div style={{ textAlign: "center", lineHeight: 1.3 }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: "var(--color-text, #fff)", letterSpacing: "0.5px" }}>
          {displayName}
        </div>
        {country && (
          <div style={{ fontSize: 10, color: "var(--color-text-muted, #aaa)" }}>{country}</div>
        )}
      </div>
    </div>
  );
}

export { CIRCUITS, normalizeCircuitKey };
