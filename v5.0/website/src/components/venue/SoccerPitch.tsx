"use client";

interface SoccerPitchProps {
  className?: string;
  width?: number;
  height?: number;
  homeColor?: string;
  awayColor?: string;
}

export default function SoccerPitch({
  className,
  width = 580,
  height = 360,
  homeColor = "#003087",
  awayColor = "#b81c2c",
}: SoccerPitchProps) {
  const W = width;
  const H = height;
  const pad = 16;
  const pW = W - pad * 2;
  const pH = H - pad * 2;
  const pX = pad;
  const pY = pad;
  const midX = pX + pW / 2;
  const midY = pY + pH / 2;

  // Pitch proportions: 105m × 68m
  const scaleX = pW / 105;
  const scaleY = pH / 68;
  const m = (v: number, axis: "x" | "y") => v * (axis === "x" ? scaleX : scaleY);

  // Penalty area: 40.3m wide × 16.5m deep
  const paW = m(40.3, "y");
  const paD = m(16.5, "x");

  // Goal area: 18.3m wide × 5.5m deep
  const gaW = m(18.3, "y");
  const gaD = m(5.5, "x");

  // Penalty spot: 11m from goal line
  const penSpot = m(11, "x");

  // Center circle: 9.15m
  const circleR = m(9.15, "x");

  // Corner arc: 1m
  const cornerR = m(1, "x");

  // Penalty arc radius: 9.15m from penalty spot
  const penArcR = m(9.15, "x");

  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      width={width}
      height={height}
      className={className}
      aria-label="Soccer pitch diagram"
      style={{ display: "block" }}
    >
      {/* Grass */}
      <rect x={0} y={0} width={W} height={H} fill="#2e7d32" />

      {/* Alternating grass stripes */}
      {Array.from({ length: 7 }, (_, i) => (
        <rect
          key={i}
          x={pX + (i * pW) / 7}
          y={pY}
          width={pW / 7}
          height={pH}
          fill={i % 2 === 0 ? "#2e7d32" : "#256529"}
        />
      ))}

      {/* Pitch border */}
      <rect x={pX} y={pY} width={pW} height={pH} fill="none" stroke="#fff" strokeWidth={2} />

      {/* Half-way line */}
      <line x1={midX} y1={pY} x2={midX} y2={pY + pH} stroke="#fff" strokeWidth={1.5} />

      {/* Center circle */}
      <circle cx={midX} cy={midY} r={circleR} fill="none" stroke="#fff" strokeWidth={1.5} />
      <circle cx={midX} cy={midY} r={3} fill="#fff" />

      {/* ── LEFT PENALTY AREA (away) ── */}
      <rect x={pX} y={midY - paW / 2} width={paD} height={paW} fill={`${awayColor}1a`} stroke="#fff" strokeWidth={1.5} />
      {/* Goal area */}
      <rect x={pX} y={midY - gaW / 2} width={gaD} height={gaW} fill="none" stroke="#fff" strokeWidth={1.5} />
      {/* Penalty spot */}
      <circle cx={pX + penSpot} cy={midY} r={2.5} fill="#fff" />
      {/* Penalty arc */}
      <path
        d={`M ${pX + paD} ${midY - m(7.3, "y")} A ${penArcR} ${penArcR} 0 0 1 ${pX + paD} ${midY + m(7.3, "y")}`}
        fill="none" stroke="#fff" strokeWidth={1.5}
      />
      {/* Goal */}
      <rect x={pX - m(2.44, "x")} y={midY - m(3.66, "y")} width={m(2.44, "x")} height={m(7.32, "y")} fill="none" stroke={awayColor} strokeWidth={2} />

      {/* ── RIGHT PENALTY AREA (home) ── */}
      <rect x={pX + pW - paD} y={midY - paW / 2} width={paD} height={paW} fill={`${homeColor}1a`} stroke="#fff" strokeWidth={1.5} />
      <rect x={pX + pW - gaD} y={midY - gaW / 2} width={gaD} height={gaW} fill="none" stroke="#fff" strokeWidth={1.5} />
      <circle cx={pX + pW - penSpot} cy={midY} r={2.5} fill="#fff" />
      <path
        d={`M ${pX + pW - paD} ${midY - m(7.3, "y")} A ${penArcR} ${penArcR} 0 0 0 ${pX + pW - paD} ${midY + m(7.3, "y")}`}
        fill="none" stroke="#fff" strokeWidth={1.5}
      />
      <rect x={pX + pW} y={midY - m(3.66, "y")} width={m(2.44, "x")} height={m(7.32, "y")} fill="none" stroke={homeColor} strokeWidth={2} />

      {/* Corner arcs */}
      <path d={`M ${pX + cornerR} ${pY} A ${cornerR} ${cornerR} 0 0 0 ${pX} ${pY + cornerR}`} fill="none" stroke="#fff" strokeWidth={1.5} />
      <path d={`M ${pX + pW - cornerR} ${pY} A ${cornerR} ${cornerR} 0 0 1 ${pX + pW} ${pY + cornerR}`} fill="none" stroke="#fff" strokeWidth={1.5} />
      <path d={`M ${pX} ${pY + pH - cornerR} A ${cornerR} ${cornerR} 0 0 0 ${pX + cornerR} ${pY + pH}`} fill="none" stroke="#fff" strokeWidth={1.5} />
      <path d={`M ${pX + pW - cornerR} ${pY + pH} A ${cornerR} ${cornerR} 0 0 0 ${pX + pW} ${pY + pH - cornerR}`} fill="none" stroke="#fff" strokeWidth={1.5} />

      {/* Labels */}
      <text x={pX + paD / 2} y={pY - 4} textAnchor="middle" fill="#fff" fontSize={9} opacity={0.7}>AWAY</text>
      <text x={pX + pW - paD / 2} y={pY - 4} textAnchor="middle" fill="#fff" fontSize={9} opacity={0.7}>HOME</text>
    </svg>
  );
}
