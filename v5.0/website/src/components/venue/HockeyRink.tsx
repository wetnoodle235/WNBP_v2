"use client";

interface HockeyRinkProps {
  className?: string;
  width?: number;
  height?: number;
  homeColor?: string;
  awayColor?: string;
}

export default function HockeyRink({
  className,
  width = 580,
  height = 300,
  homeColor = "#003087",
  awayColor = "#c8102e",
}: HockeyRinkProps) {
  const W = width;
  const H = height;
  const pad = 14;
  const rW = W - pad * 2;
  const rH = H - pad * 2;
  const rX = pad;
  const rY = pad;
  const midX = rX + rW / 2;
  const midY = rY + rH / 2;

  // Rink proportions: 200ft × 85ft
  const scaleX = rW / 200;
  const scaleY = rH / 85;

  const ft = (f: number, axis: "x" | "y") => f * (axis === "x" ? scaleX : scaleY);

  // Blue lines at 25ft from each red line (75ft and 125ft from left)
  const blueL = rX + ft(75, "x");
  const blueR = rX + ft(125, "x");

  // Goal crease: 8ft wide × 6ft deep
  const creaseW = ft(8, "y");
  const creaseD = ft(6, "x");

  // Faceoff circles: 15ft radius, at 20ft and 69ft from center
  const circleR = ft(15, "x");

  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      width={width}
      height={height}
      className={className}
      aria-label="Hockey rink diagram"
      style={{ display: "block" }}
    >
      {/* Ice surface */}
      <rect x={rX} y={rY} width={rW} height={rH} fill="#e8f4fd" rx={ft(28, "x")} />

      {/* Boards */}
      <rect x={rX} y={rY} width={rW} height={rH} fill="none" stroke="#bbb" strokeWidth={3} rx={ft(28, "x")} />

      {/* Center red line */}
      <line x1={midX} y1={rY} x2={midX} y2={rY + rH} stroke="#e8002d" strokeWidth={3} />

      {/* Blue lines */}
      <line x1={blueL} y1={rY} x2={blueL} y2={rY + rH} stroke="#005db7" strokeWidth={3} />
      <line x1={blueR} y1={rY} x2={blueR} y2={rY + rH} stroke="#005db7" strokeWidth={3} />

      {/* Goal lines */}
      <line x1={rX + ft(11, "x")} y1={rY} x2={rX + ft(11, "x")} y2={rY + rH} stroke="#e8002d" strokeWidth={2} opacity={0.8} />
      <line x1={rX + ft(189, "x")} y1={rY} x2={rX + ft(189, "x")} y2={rY + rH} stroke="#e8002d" strokeWidth={2} opacity={0.8} />

      {/* Center faceoff circle */}
      <circle cx={midX} cy={midY} r={circleR} fill="none" stroke="#005db7" strokeWidth={1.5} />
      <circle cx={midX} cy={midY} r={2} fill="#e8002d" />

      {/* Left end zone faceoff circles */}
      <circle cx={rX + ft(31, "x")} cy={midY - ft(22, "y")} r={circleR} fill="none" stroke="#e8002d" strokeWidth={1.5} />
      <circle cx={rX + ft(31, "x")} cy={midY + ft(22, "y")} r={circleR} fill="none" stroke="#e8002d" strokeWidth={1.5} />
      {/* Dot */}
      <circle cx={rX + ft(31, "x")} cy={midY - ft(22, "y")} r={2} fill="#e8002d" />
      <circle cx={rX + ft(31, "x")} cy={midY + ft(22, "y")} r={2} fill="#e8002d" />

      {/* Right end zone faceoff circles */}
      <circle cx={rX + ft(169, "x")} cy={midY - ft(22, "y")} r={circleR} fill="none" stroke="#e8002d" strokeWidth={1.5} />
      <circle cx={rX + ft(169, "x")} cy={midY + ft(22, "y")} r={circleR} fill="none" stroke="#e8002d" strokeWidth={1.5} />
      <circle cx={rX + ft(169, "x")} cy={midY - ft(22, "y")} r={2} fill="#e8002d" />
      <circle cx={rX + ft(169, "x")} cy={midY + ft(22, "y")} r={2} fill="#e8002d" />

      {/* Neutral zone faceoff dots */}
      <circle cx={rX + ft(76, "x")} cy={midY - ft(22, "y")} r={3} fill="#e8002d" />
      <circle cx={rX + ft(76, "x")} cy={midY + ft(22, "y")} r={3} fill="#e8002d" />
      <circle cx={rX + ft(124, "x")} cy={midY - ft(22, "y")} r={3} fill="#e8002d" />
      <circle cx={rX + ft(124, "x")} cy={midY + ft(22, "y")} r={3} fill="#e8002d" />

      {/* Left goal crease */}
      <path
        d={`M ${rX + ft(11, "x")} ${midY - creaseW / 2} A ${creaseD} ${creaseD} 0 0 1 ${rX + ft(11, "x")} ${midY + creaseW / 2}`}
        fill={`${awayColor}33`} stroke={awayColor} strokeWidth={1.5}
        transform={`translate(${creaseD}, 0)`}
      />

      {/* Left goal */}
      <rect
        x={rX + ft(9, "x")}
        y={midY - ft(3, "y")}
        width={ft(2, "x")}
        height={ft(6, "y")}
        fill="none"
        stroke={awayColor}
        strokeWidth={2}
      />

      {/* Right goal crease */}
      <path
        d={`M ${rX + ft(189, "x")} ${midY - creaseW / 2} A ${creaseD} ${creaseD} 0 0 0 ${rX + ft(189, "x")} ${midY + creaseW / 2}`}
        fill={`${homeColor}33`} stroke={homeColor} strokeWidth={1.5}
        transform={`translate(-${creaseD}, 0)`}
      />

      {/* Right goal */}
      <rect
        x={rX + ft(189, "x")}
        y={midY - ft(3, "y")}
        width={ft(2, "x")}
        height={ft(6, "y")}
        fill="none"
        stroke={homeColor}
        strokeWidth={2}
      />

      {/* Labels */}
      <text x={rX + ft(31, "x")} y={rY - 4} textAnchor="middle" fill="#888" fontSize={9}>AWAY END</text>
      <text x={rX + ft(169, "x")} y={rY - 4} textAnchor="middle" fill="#888" fontSize={9}>HOME END</text>
    </svg>
  );
}
