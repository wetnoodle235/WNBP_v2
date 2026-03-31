"use client";

interface AvatarStackProps {
  /** Array of image URLs */
  images: string[];
  /** Max visible avatars (rest show as +N) */
  max?: number;
  /** Size of each avatar in px */
  size?: number;
  /** Alt text prefix */
  altPrefix?: string;
}

export function AvatarStack({
  images,
  max = 4,
  size = 32,
  altPrefix = "Avatar",
}: AvatarStackProps) {
  const visible = images.slice(0, max);
  const remaining = images.length - max;

  return (
    <div
      className="avatar-stack"
      role="group"
      aria-label={`${images.length} avatars`}
      style={{ display: "inline-flex", alignItems: "center" }}
    >
      {visible.map((src, i) => (
        <img
          key={i}
          src={src}
          alt={`${altPrefix} ${i + 1}`}
          width={size}
          height={size}
          className="avatar-stack-item"
          style={{
            width: size,
            height: size,
            borderRadius: "50%",
            border: "2px solid var(--color-bg, #0f172a)",
            marginLeft: i > 0 ? -(size * 0.3) : 0,
            objectFit: "cover",
            position: "relative",
            zIndex: max - i,
            background: "var(--color-surface-2)",
          }}
        />
      ))}
      {remaining > 0 && (
        <span
          className="avatar-stack-overflow"
          aria-label={`and ${remaining} more`}
          style={{
            width: size,
            height: size,
            borderRadius: "50%",
            border: "2px solid var(--color-bg, #0f172a)",
            marginLeft: -(size * 0.3),
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            background: "var(--color-surface-2)",
            fontSize: size * 0.35,
            fontWeight: 700,
            color: "var(--color-text-secondary)",
            position: "relative",
            zIndex: 0,
          }}
        >
          +{remaining}
        </span>
      )}
    </div>
  );
}

export default AvatarStack;
