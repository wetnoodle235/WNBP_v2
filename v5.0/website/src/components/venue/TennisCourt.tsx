"use client";

interface TennisCourtProps {
  className?: string;
  width?: number;
  height?: number;
  variant?: "hard" | "clay" | "grass";
}

const SURFACE_COLORS = {
  hard: { court: "#4a90d9", service: "#3d7fc4", baseline: "#fff" },
  clay: { court: "#c25b1a", service: "#b0511a", baseline: "#fff" },
  grass: { court: "#3d8b3d", service: "#347534", baseline: "#fff" },
};

export default function TennisCourt({
  className,
  width = 360,
  height = 540,
  variant = "hard",
}: TennisCourtProps) {
  const W = width;
  const H = height;
  const pad = 24;
  const cW = W - pad * 2;
  const cH = H - pad * 2;
  const cX = pad;
  const cY = pad;
  const midX = cX + cW / 2;
  const midY = cY + cH / 2;

  const colors = SURFACE_COLORS[variant];

  // Tennis court: 78ft × 36ft (doubles) / 27ft (singles)
  // Service boxes: 21ft from net
  const serviceLineH = cH * (21 / 78);
  const singlesX = cW * (4.5 / 36); // 4.5ft alley on each side
  const netH = 3; // net thickness

  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      width={width}
      height={height}
      className={className}
      aria-label="Tennis court diagram"
      style={{ display: "block" }}
    >
      {/* Background / out of bounds */}
      <rect x={0} y={0} width={W} height={H} fill={colors.court} opacity={0.4} />

      {/* Court surface */}
      <rect x={cX} y={cY} width={cW} height={cH} fill={colors.court} />

      {/* Service box alternate shading */}
      <rect x={cX + singlesX} y={midY - serviceLineH} width={(cW - singlesX * 2) / 2} height={serviceLineH} fill={colors.service} />
      <rect x={midX} y={cY + serviceLineH} width={(cW - singlesX * 2) / 2} height={serviceLineH} fill={colors.service} />

      {/* Doubles court border */}
      <rect x={cX} y={cY} width={cW} height={cH} fill="none" stroke={colors.baseline} strokeWidth={2} />

      {/* Singles sidelines */}
      <line x1={cX + singlesX} y1={cY} x2={cX + singlesX} y2={cY + cH} stroke={colors.baseline} strokeWidth={1.5} />
      <line x1={cX + cW - singlesX} y1={cY} x2={cX + cW - singlesX} y2={cY + cH} stroke={colors.baseline} strokeWidth={1.5} />

      {/* Net */}
      <line x1={cX - 8} y1={midY} x2={cX + cW + 8} y2={midY} stroke="#ddd" strokeWidth={netH} />
      <line x1={cX - 8} y1={midY - 2} x2={cX + cW + 8} y2={midY - 2} stroke="#888" strokeWidth={1} opacity={0.5} />

      {/* Net posts */}
      <line x1={cX - 8} y1={midY - 12} x2={cX - 8} y2={midY + 4} stroke="#ccc" strokeWidth={3} />
      <line x1={cX + cW + 8} y1={midY - 12} x2={cX + cW + 8} y2={midY + 4} stroke="#ccc" strokeWidth={3} />

      {/* Service lines */}
      <line x1={cX + singlesX} y1={midY - serviceLineH} x2={cX + cW - singlesX} y2={midY - serviceLineH} stroke={colors.baseline} strokeWidth={1.5} />
      <line x1={cX + singlesX} y1={midY + serviceLineH} x2={cX + cW - singlesX} y2={midY + serviceLineH} stroke={colors.baseline} strokeWidth={1.5} />

      {/* Center service line */}
      <line x1={midX} y1={midY - serviceLineH} x2={midX} y2={midY + serviceLineH} stroke={colors.baseline} strokeWidth={1.5} />

      {/* Center marks */}
      <line x1={midX - 4} y1={cY} x2={midX + 4} y2={cY} stroke={colors.baseline} strokeWidth={2} />
      <line x1={midX - 4} y1={cY + cH} x2={midX + 4} y2={cY + cH} stroke={colors.baseline} strokeWidth={2} />

      {/* Alley shading */}
      <rect x={cX} y={cY} width={singlesX} height={cH} fill="rgba(0,0,0,0.08)" />
      <rect x={cX + cW - singlesX} y={cY} width={singlesX} height={cH} fill="rgba(0,0,0,0.08)" />

      {/* Surface label */}
      <text x={midX} y={H - 6} textAnchor="middle" fill="#fff" fontSize={10} opacity={0.6} fontWeight={600}>
        {variant.toUpperCase()} COURT
      </text>

      {/* Service labels */}
      <text x={cX + singlesX + (cW - singlesX * 2) / 4} y={midY - serviceLineH / 2} textAnchor="middle" fill="#fff" fontSize={9} opacity={0.5}>
        Deuce
      </text>
      <text x={cX + singlesX + (cW - singlesX * 2) * 3 / 4} y={midY - serviceLineH / 2} textAnchor="middle" fill="#fff" fontSize={9} opacity={0.5}>
        Ad
      </text>
    </svg>
  );
}
