import { Skeleton, SkeletonText } from "@/components/ui";

export default function PricingLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1200, margin: "0 auto", textAlign: "center" }}>
      <Skeleton style={{ width: 200, height: 36, margin: "0 auto 8px" }} />
      <Skeleton style={{ width: 320, height: 20, margin: "0 auto 32px" }} />
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))", gap: 24 }}>
        {Array.from({ length: 3 }).map((_, i) => (
          <div key={i} style={{ padding: 24, borderRadius: 12, border: "1px solid var(--color-border, #e5e7eb)" }}>
            <Skeleton style={{ width: 100, height: 24, margin: "0 auto 12px" }} />
            <Skeleton style={{ width: 80, height: 40, margin: "0 auto 16px" }} />
            <SkeletonText lines={4} />
            <Skeleton style={{ width: "100%", height: 44, borderRadius: 8, marginTop: 16 }} />
          </div>
        ))}
      </div>
    </main>
  );
}
