"use client";

interface BasketballCourtProps {
  className?: string;
  width?: number;
  height?: number;
  /** 'full' shows entire court; 'half' shows one end */
  view?: "full" | "half";
  /** team colors for the two sides */
  homeColor?: string;
  awayColor?: string;
}

export default function BasketballCourt({
  className,
  width = 600,
  height = 380,
  view = "full",
  homeColor = "#1d428a",
  awayColor = "#ce1141",
}: BasketballCourtProps) {
  // NBA: 94ft × 50ft — we scale to fit the given width/height
  const W = width;
  const H = height;
  const pad = 20; // border padding
  const cW = W - pad * 2; // court width in pixels
  const cH = H - pad * 2; // court height in pixels
  const cX = pad;
  const cY = pad;

  // Key/paint area: 16ft wide × 19ft long (scaled from 94ft court)
  const keyW = (cW * 16) / 94;
  const keyH = (cH * 19) / 50;

  // Three-point arc radius: 23.75ft
  const arcR = (cW * 23.75) / 94;
  const arcCornerY = (cH * 14) / 50; // corner 3 comes down 14ft from baseline

  // Center circle radius: 6ft
  const centerR = (cW * 6) / 94;

  // Free-throw circle: 6ft
  const ftR = (cW * 6) / 94;

  // Half-court line X
  const midX = cX + cW / 2;
  const midY = cY + cH / 2;

  const paint = `rgba(${homeColor.replace("#", "")
    .match(/.{2}/g)
    ?.map((h) => parseInt(h, 16))
    .join(",") ?? "29,66,138"},0.18)`;

  // Left key box
  const leftKeyX = cX;
  const leftKeyY = midY - keyW / 2;
  // Right key box (mirror)
  const rightKeyX = cX + cW - keyH;
  const rightKeyY = midY - keyW / 2;

  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      width={width}
      height={height}
      className={className}
      aria-label="Basketball court diagram"
      style={{ display: "block" }}
    >
      {/* Court surface */}
      <rect x={cX} y={cY} width={cW} height={cH} fill="#c8854a" rx={4} />

      {/* Court border */}
      <rect x={cX} y={cY} width={cW} height={cH} fill="none" stroke="#fff" strokeWidth={2} rx={4} />

      {/* Half-court line */}
      <line x1={midX} y1={cY} x2={midX} y2={cY + cH} stroke="#fff" strokeWidth={1.5} />

      {/* Center circle */}
      <circle cx={midX} cy={midY} r={centerR} fill="none" stroke="#fff" strokeWidth={1.5} />
      <circle cx={midX} cy={midY} r={2} fill="#fff" />

      {/* ── LEFT SIDE (away team) ── */}
      {/* Paint / key */}
      <rect x={leftKeyX} y={leftKeyY} width={keyH} height={keyW} fill={`rgba(206,17,65,0.2)`} stroke="#fff" strokeWidth={1.5} />

      {/* Free-throw circle */}
      <circle cx={cX + keyH} cy={midY} r={ftR} fill="none" stroke="#fff" strokeWidth={1.5} strokeDasharray={view === "full" ? "0" : "4 4"} />

      {/* Basket */}
      <circle cx={cX + 5} cy={midY} r={4} fill="none" stroke="#e87c00" strokeWidth={2} />

      {/* Three-point arc — left */}
      <path
        d={`M ${cX} ${midY - arcCornerY} A ${arcR} ${arcR} 0 0 1 ${cX} ${midY + arcCornerY}`}
        fill="none" stroke="#fff" strokeWidth={1.5}
      />
      {/* Corner 3 lines */}
      <line x1={cX} y1={midY - arcCornerY} x2={cX + (cW * 3) / 94} y2={midY - arcCornerY} stroke="#fff" strokeWidth={1.5} />
      <line x1={cX} y1={midY + arcCornerY} x2={cX + (cW * 3) / 94} y2={midY + arcCornerY} stroke="#fff" strokeWidth={1.5} />

      {/* Restricted area arc (4ft) */}
      <path
        d={`M ${cX + 4} ${midY - (cH * 4) / 50} A ${(cW * 4) / 94} ${(cH * 4) / 50} 0 0 1 ${cX + 4} ${midY + (cH * 4) / 50}`}
        fill="none" stroke="#e87c00" strokeWidth={1} strokeDasharray="3 3"
      />

      {/* ── RIGHT SIDE (home team) ── */}
      <rect x={rightKeyX} y={rightKeyY} width={keyH} height={keyW} fill={`rgba(29,66,138,0.2)`} stroke="#fff" strokeWidth={1.5} />
      <circle cx={cX + cW - keyH} cy={midY} r={ftR} fill="none" stroke="#fff" strokeWidth={1.5} />
      <circle cx={cX + cW - 5} cy={midY} r={4} fill="none" stroke="#e87c00" strokeWidth={2} />

      {/* Three-point arc — right */}
      <path
        d={`M ${cX + cW} ${midY - arcCornerY} A ${arcR} ${arcR} 0 0 0 ${cX + cW} ${midY + arcCornerY}`}
        fill="none" stroke="#fff" strokeWidth={1.5}
      />
      <line x1={cX + cW} y1={midY - arcCornerY} x2={cX + cW - (cW * 3) / 94} y2={midY - arcCornerY} stroke="#fff" strokeWidth={1.5} />
      <line x1={cX + cW} y1={midY + arcCornerY} x2={cX + cW - (cW * 3) / 94} y2={midY + arcCornerY} stroke="#fff" strokeWidth={1.5} />

      <path
        d={`M ${cX + cW - 4} ${midY - (cH * 4) / 50} A ${(cW * 4) / 94} ${(cH * 4) / 50} 0 0 0 ${cX + cW - 4} ${midY + (cH * 4) / 50}`}
        fill="none" stroke="#e87c00" strokeWidth={1} strokeDasharray="3 3"
      />

      {/* Labels */}
      <text x={cX + keyH / 2} y={cY - 6} textAnchor="middle" fill="#fff" fontSize={10} opacity={0.7}>AWAY</text>
      <text x={cX + cW - keyH / 2} y={cY - 6} textAnchor="middle" fill="#fff" fontSize={10} opacity={0.7}>HOME</text>
    </svg>
  );
}
