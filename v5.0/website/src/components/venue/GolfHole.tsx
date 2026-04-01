"use client";

interface GolfHoleProps {
  className?: string;
  width?: number;
  height?: number;
  par?: number;
  holeNumber?: number;
  yards?: number;
  name?: string;
}

export default function GolfHole({
  className,
  width = 200,
  height = 340,
  par = 4,
  holeNumber,
  yards,
  name,
}: GolfHoleProps) {
  const W = width;
  const H = height;
  const fairwayW = W * 0.35;
  const midX = W / 2;

  // Hole shapes by par
  const isParFive = par === 5;
  const isParThree = par === 3;

  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      width={width}
      height={height}
      className={className}
      aria-label={`Golf hole ${holeNumber ?? ""} diagram`}
      style={{ display: "block" }}
    >
      {/* Background (rough) */}
      <rect x={0} y={0} width={W} height={H} fill="#1b5e20" />

      {/* Rough texture */}
      {Array.from({ length: 8 }, (_, i) => (
        <rect key={i} x={i * W / 8} y={0} width={W / 8} height={H} fill={i % 2 === 0 ? "#1b5e20" : "#174d1a"} />
      ))}

      {/* Fairway */}
      {isParThree ? (
        // Short straight for par 3
        <rect x={midX - fairwayW / 2} y={H * 0.1} width={fairwayW} height={H * 0.6} fill="#4caf50" rx={8} />
      ) : isParFive ? (
        // Dogleg right par 5
        <path
          d={`M ${midX - fairwayW / 2} ${H - 60} 
              L ${midX - fairwayW / 2} ${H * 0.55} 
              Q ${midX - fairwayW / 2} ${H * 0.45} ${midX + fairwayW * 0.3} ${H * 0.4}
              L ${midX + fairwayW * 0.3 + fairwayW / 2} ${H * 0.4}
              L ${midX + fairwayW * 0.3 + fairwayW / 2} ${H * 0.12}
              Q ${midX + fairwayW * 0.3 + fairwayW / 2} ${H * 0.08} ${midX + fairwayW * 0.3} ${H * 0.08}
              L ${midX + fairwayW * 0.3 - fairwayW / 2} ${H * 0.08}
              Q ${midX + fairwayW * 0.3 - fairwayW * 0.8} ${H * 0.08} ${midX + fairwayW * 0.3 - fairwayW * 0.8} ${H * 0.45}
              Q ${midX + fairwayW * 0.3 - fairwayW * 0.8} ${H * 0.55} ${midX + fairwayW / 2} ${H - 60}
              Z`}
          fill="#4caf50"
        />
      ) : (
        // Slight dogleg left par 4
        <path
          d={`M ${midX - fairwayW / 2} ${H - 60}
              L ${midX - fairwayW / 2 - 15} ${H * 0.4}
              Q ${midX - fairwayW / 2 - 20} ${H * 0.15} ${midX - fairwayW * 0.3} ${H * 0.08}
              L ${midX + fairwayW * 0.3} ${H * 0.08}
              Q ${midX + fairwayW / 2 + 5} ${H * 0.15} ${midX + fairwayW / 2 + 10} ${H * 0.4}
              L ${midX + fairwayW / 2} ${H - 60}
              Z`}
          fill="#4caf50"
        />
      )}

      {/* Green */}
      <ellipse
        cx={isParFive ? midX + fairwayW * 0.3 : midX}
        cy={H * 0.11}
        rx={fairwayW * 0.65}
        ry={fairwayW * 0.5}
        fill="#66bb6a"
      />

      {/* Pin */}
      <circle
        cx={isParFive ? midX + fairwayW * 0.3 : midX}
        cy={H * 0.1}
        r={4}
        fill="#fff"
        stroke="#333"
        strokeWidth={1}
      />
      <line
        x1={isParFive ? midX + fairwayW * 0.3 : midX}
        y1={H * 0.1}
        x2={isParFive ? midX + fairwayW * 0.3 + 12 : midX + 12}
        y2={H * 0.06}
        stroke="#333"
        strokeWidth={1.5}
      />
      {/* Flag */}
      <polygon
        points={`${isParFive ? midX + fairwayW * 0.3 + 12 : midX + 12},${H * 0.06} ${isParFive ? midX + fairwayW * 0.3 + 22 : midX + 22},${H * 0.075} ${isParFive ? midX + fairwayW * 0.3 + 12 : midX + 12},${H * 0.09}`}
        fill="#e8002d"
      />

      {/* Sand bunkers */}
      <ellipse cx={midX - fairwayW * 0.8} cy={H * 0.18} rx={15} ry={10} fill="#f5e6a3" opacity={0.9} />
      <ellipse cx={midX + fairwayW * 0.8} cy={H * 0.22} rx={12} ry={8} fill="#f5e6a3" opacity={0.9} />

      {/* Water hazard (par 5) */}
      {isParFive && (
        <ellipse cx={midX - 20} cy={H * 0.48} rx={22} ry={14} fill="#1565c0" opacity={0.7} />
      )}

      {/* Tee box */}
      <rect x={midX - 18} y={H - 52} width={36} height={22} fill="#81c784" rx={4} stroke="#fff" strokeWidth={1} opacity={0.9} />
      <circle cx={midX} cy={H - 38} r={2} fill="#fff" />

      {/* Info overlay */}
      {holeNumber && (
        <text x={12} y={H - 8} fill="#fff" fontSize={11} fontWeight={700} opacity={0.85}>
          Hole {holeNumber}
        </text>
      )}
      <text x={W - 8} y={H - 22} textAnchor="end" fill="#fff" fontSize={11} fontWeight={700} opacity={0.85}>
        Par {par}
      </text>
      {yards && (
        <text x={W - 8} y={H - 8} textAnchor="end" fill="#fff" fontSize={10} opacity={0.7}>
          {yards} yds
        </text>
      )}
    </svg>
  );
}
