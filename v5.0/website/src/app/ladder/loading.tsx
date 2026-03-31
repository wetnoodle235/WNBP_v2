import { Skeleton, SkeletonText } from "@/components/ui";

export default function LadderLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 900, margin: "0 auto" }}>
      <Skeleton style={{ width: 200, height: 32, marginBottom: 24 }} />
      <div style={{ borderRadius: 8, overflow: "hidden", border: "1px solid var(--color-border, #e5e7eb)" }}>
        <Skeleton style={{ height: 44, width: "100%" }} />
        {Array.from({ length: 8 }).map((_, i) => (
          <div key={i} style={{ padding: "12px 16px", borderBottom: "1px solid var(--color-border, #e5e7eb)" }}>
            <SkeletonText lines={1} />
          </div>
        ))}
      </div>
    </main>
  );
}
