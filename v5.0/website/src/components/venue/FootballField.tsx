"use client";

interface FootballFieldProps {
  className?: string;
  width?: number;
  height?: number;
  homeColor?: string;
  awayColor?: string;
  /** college = wider hash marks / different distances */
  variant?: "nfl" | "ncaaf";
}

export default function FootballField({
  className,
  width = 640,
  height = 320,
  homeColor = "#013369",
  awayColor = "#d50a0a",
  variant = "nfl",
}: FootballFieldProps) {
  const W = width;
  const H = height;
  const pad = 14;
  const fW = W - pad * 2; // field area including end zones
  const fH = H - pad * 2;
  const fX = pad;
  const fY = pad;

  // End zones: 10yds each. Field = 120yds total.
  const ezW = fW / 12; // 10/120
  const playW = fW - ezW * 2; // 100 yards
  const yardW = playW / 100; // pixels per yard

  const midY = fY + fH / 2;

  // Hash mark positions (NFL: 18.5ft from center = 37ft spread / 160ft field width)
  const hashOffset = variant === "nfl" ? fH * 0.115 : fH * 0.075;

  // Yard lines every 5 yards
  const yardLines = Array.from({ length: 21 }, (_, i) => i * 5); // 0,5,10,...,100

  // Numbers: 10,20,30,40,50,40,30,20,10
  const numberYards = [10, 20, 30, 40, 50, 60, 70, 80, 90];
  const numberLabels = [10, 20, 30, 40, 50, 40, 30, 20, 10];

  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      width={width}
      height={height}
      className={className}
      aria-label="Football field diagram"
      style={{ display: "block" }}
    >
      {/* Grass */}
      <rect x={0} y={0} width={W} height={H} fill="#2d5a1b" />

      {/* Alternating grass stripes (every 5 yards) */}
      {yardLines.map((y, i) =>
        i % 2 === 0 ? (
          <rect
            key={y}
            x={fX + ezW + y * yardW}
            y={fY}
            width={5 * yardW}
            height={fH}
            fill="#265018"
          />
        ) : null
      )}

      {/* Left end zone */}
      <rect x={fX} y={fY} width={ezW} height={fH} fill={awayColor} opacity={0.6} />
      <text
        x={fX + ezW / 2}
        y={midY}
        textAnchor="middle"
        dominantBaseline="middle"
        fill="#fff"
        fontSize={Math.min(ezW * 0.35, 18)}
        fontWeight={700}
        letterSpacing={2}
        transform={`rotate(-90, ${fX + ezW / 2}, ${midY})`}
      >
        AWAY
      </text>

      {/* Right end zone */}
      <rect x={fX + fW - ezW} y={fY} width={ezW} height={fH} fill={homeColor} opacity={0.6} />
      <text
        x={fX + fW - ezW / 2}
        y={midY}
        textAnchor="middle"
        dominantBaseline="middle"
        fill="#fff"
        fontSize={Math.min(ezW * 0.35, 18)}
        fontWeight={700}
        letterSpacing={2}
        transform={`rotate(90, ${fX + fW - ezW / 2}, ${midY})`}
      >
        HOME
      </text>

      {/* Yard lines */}
      {yardLines.map((y) => (
        <line
          key={y}
          x1={fX + ezW + y * yardW}
          y1={fY}
          x2={fX + ezW + y * yardW}
          y2={fY + fH}
          stroke="#fff"
          strokeWidth={y % 10 === 0 ? 2 : 1}
          opacity={y % 10 === 0 ? 0.9 : 0.5}
        />
      ))}

      {/* Hash marks */}
      {Array.from({ length: 101 }, (_, y) => y).map((y) => (
        <g key={y}>
          <line
            x1={fX + ezW + y * yardW - 0.5}
            y1={midY - hashOffset}
            x2={fX + ezW + y * yardW + 0.5}
            y2={midY - hashOffset - 4}
            stroke="#fff"
            strokeWidth={1}
            opacity={0.7}
          />
          <line
            x1={fX + ezW + y * yardW - 0.5}
            y1={midY + hashOffset}
            x2={fX + ezW + y * yardW + 0.5}
            y2={midY + hashOffset + 4}
            stroke="#fff"
            strokeWidth={1}
            opacity={0.7}
          />
        </g>
      ))}

      {/* Yard numbers */}
      {numberYards.map((y, i) => (
        <g key={y}>
          <text
            x={fX + ezW + y * yardW}
            y={fY + fH * 0.22}
            textAnchor="middle"
            fill="#fff"
            fontSize={Math.max(10, fH * 0.12)}
            fontWeight={600}
            opacity={0.85}
          >
            {numberLabels[i]}
          </text>
          <text
            x={fX + ezW + y * yardW}
            y={fY + fH * 0.82}
            textAnchor="middle"
            fill="#fff"
            fontSize={Math.max(10, fH * 0.12)}
            fontWeight={600}
            opacity={0.85}
          >
            {numberLabels[i]}
          </text>
        </g>
      ))}

      {/* Field border */}
      <rect x={fX} y={fY} width={fW} height={fH} fill="none" stroke="#fff" strokeWidth={2} />

      {/* Goal posts (simplified) */}
      <line x1={fX + ezW} y1={fY} x2={fX + ezW} y2={fY + fH} stroke="#ffd700" strokeWidth={2} opacity={0.9} />
      <line x1={fX + fW - ezW} y1={fY} x2={fX + fW - ezW} y2={fY + fH} stroke="#ffd700" strokeWidth={2} opacity={0.9} />

      {/* 50-yard center indicator */}
      <circle cx={fX + fW / 2} cy={midY} r={3} fill="#fff" opacity={0.7} />
    </svg>
  );
}
