import { Skeleton } from "@/components/ui";

export default function GamesLoading() {
  return (
    <main style={{ padding: "var(--space-6) var(--space-4)", maxWidth: 1000, margin: "0 auto" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
        <Skeleton style={{ width: 200, height: 32 }} />
        <div style={{ display: "flex", gap: 8 }}>
          <Skeleton style={{ width: 100, height: 36, borderRadius: 8 }} />
          <Skeleton style={{ width: 120, height: 36, borderRadius: 8 }} />
          <Skeleton style={{ width: 100, height: 36, borderRadius: 8 }} />
        </div>
      </div>
      {Array.from({ length: 5 }).map((_, i) => (
        <Skeleton key={i} style={{ height: 110, marginBottom: 16, borderRadius: 12 }} />
      ))}
    </main>
  );
}
