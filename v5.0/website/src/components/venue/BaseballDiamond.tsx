"use client";

interface BaseballDiamondProps {
  className?: string;
  width?: number;
  height?: number;
  homeColor?: string;
  awayColor?: string;
}

export default function BaseballDiamond({
  className,
  width = 440,
  height = 420,
  homeColor = "#002d72",
  awayColor = "#bf0d3e",
}: BaseballDiamondProps) {
  const W = width;
  const H = height;

  // Home plate near bottom-center
  const homeX = W / 2;
  const homeY = H - 40;

  // Bases: 90ft square, perspective-ish bird's eye
  const baseDist = Math.min(W, H) * 0.42;

  const firstBase = { x: homeX + baseDist * 0.7, y: homeY - baseDist * 0.7 };
  const secondBase = { x: homeX, y: homeY - baseDist * 1.4 };
  const thirdBase = { x: homeX - baseDist * 0.7, y: homeY - baseDist * 0.7 };

  // Pitchers mound
  const moundX = homeX;
  const moundY = homeY - baseDist * 0.7;

  // Outfield arc
  const ofRadius = baseDist * 1.75;

  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      width={width}
      height={height}
      className={className}
      aria-label="Baseball field diagram"
      style={{ display: "block" }}
    >
      {/* Sky / background */}
      <rect x={0} y={0} width={W} height={H} fill="#2e7d32" />

      {/* Outfield grass (darker) */}
      <path
        d={`
          M ${homeX} ${homeY}
          L ${firstBase.x} ${firstBase.y}
          A ${ofRadius} ${ofRadius} 0 0 1 ${secondBase.x + ofRadius * 0.82} ${secondBase.y - ofRadius * 0.25}
          A ${ofRadius} ${ofRadius} 0 0 1 ${thirdBase.x} ${thirdBase.y}
          Z
        `}
        fill="#1b5e20"
      />

      {/* Infield dirt */}
      <path
        d={`
          M ${homeX} ${homeY}
          L ${firstBase.x} ${firstBase.y}
          L ${secondBase.x} ${secondBase.y}
          L ${thirdBase.x} ${thirdBase.y}
          Z
        `}
        fill="#c8a97e"
      />

      {/* Infield grass (inside dirt) */}
      <path
        d={`
          M ${homeX} ${homeY - 15}
          L ${firstBase.x - 12} ${firstBase.y + 5}
          L ${secondBase.x} ${secondBase.y + 15}
          L ${thirdBase.x + 12} ${thirdBase.y + 5}
          Z
        `}
        fill="#2e7d32"
      />

      {/* Foul lines */}
      <line x1={homeX} y1={homeY} x2={20} y2={20} stroke="#fff" strokeWidth={1.5} opacity={0.8} />
      <line x1={homeX} y1={homeY} x2={W - 20} y2={20} stroke="#fff" strokeWidth={1.5} opacity={0.8} />

      {/* Outfield warning track arc */}
      <path
        d={`
          M ${30} ${homeY - 10}
          A ${ofRadius * 1.08} ${ofRadius * 1.08} 0 0 1 ${W - 30} ${homeY - 10}
        `}
        fill="none" stroke="#a0714f" strokeWidth={8} opacity={0.6}
      />

      {/* Outfield wall */}
      <path
        d={`
          M ${30} ${homeY - 10}
          A ${ofRadius * 1.08} ${ofRadius * 1.08} 0 0 1 ${W - 30} ${homeY - 10}
        `}
        fill="none" stroke="#4caf50" strokeWidth={3} opacity={0.7}
      />

      {/* Pitcher's mound */}
      <circle cx={moundX} cy={moundY} r={12} fill="#c8a97e" stroke="#fff" strokeWidth={1} opacity={0.9} />
      <circle cx={moundX} cy={moundY} r={3} fill="#fff" opacity={0.8} />

      {/* Bases */}
      {[firstBase, secondBase, thirdBase].map((base, i) => (
        <rect
          key={i}
          x={base.x - 6}
          y={base.y - 6}
          width={12}
          height={12}
          fill="#fff"
          stroke="#888"
          strokeWidth={1}
          transform={`rotate(45, ${base.x}, ${base.y})`}
        />
      ))}

      {/* Home plate (pentagon shape) */}
      <polygon
        points={`${homeX},${homeY - 10} ${homeX + 9},${homeY - 4} ${homeX + 9},${homeY + 6} ${homeX - 9},${homeY + 6} ${homeX - 9},${homeY - 4}`}
        fill="#fff"
        stroke="#888"
        strokeWidth={1}
      />

      {/* Batter's boxes */}
      <rect x={homeX + 10} y={homeY - 16} width={20} height={30} fill="none" stroke="#c8a97e" strokeWidth={1.5} opacity={0.8} />
      <rect x={homeX - 30} y={homeY - 16} width={20} height={30} fill="none" stroke="#c8a97e" strokeWidth={1.5} opacity={0.8} />

      {/* Base paths */}
      <line x1={homeX} y1={homeY} x2={firstBase.x} y2={firstBase.y} stroke="#fff" strokeWidth={1} opacity={0.4} strokeDasharray="4 4" />
      <line x1={firstBase.x} y1={firstBase.y} x2={secondBase.x} y2={secondBase.y} stroke="#fff" strokeWidth={1} opacity={0.4} strokeDasharray="4 4" />
      <line x1={secondBase.x} y1={secondBase.y} x2={thirdBase.x} y2={thirdBase.y} stroke="#fff" strokeWidth={1} opacity={0.4} strokeDasharray="4 4" />
      <line x1={thirdBase.x} y1={thirdBase.y} x2={homeX} y2={homeY} stroke="#fff" strokeWidth={1} opacity={0.4} strokeDasharray="4 4" />

      {/* Distance markers */}
      <text x={W / 2} y={30} textAnchor="middle" fill="#fff" fontSize={11} opacity={0.7}>CF · 400ft</text>
      <text x={W - 28} y={H / 2 - 30} textAnchor="middle" fill="#fff" fontSize={10} opacity={0.6} transform={`rotate(-40,${W - 28},${H / 2 - 30})`}>RF · 330ft</text>
      <text x={28} y={H / 2 - 30} textAnchor="middle" fill="#fff" fontSize={10} opacity={0.6} transform={`rotate(40,${28},${H / 2 - 30})`}>LF · 330ft</text>
    </svg>
  );
}
