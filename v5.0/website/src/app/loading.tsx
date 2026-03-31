import { Skeleton, SkeletonCard } from "@/components/ui";

export default function Loading() {
  return (
    <main aria-busy="true" aria-label="Loading">
      <div style={{ padding: "var(--space-6)" }}>
        <Skeleton width="30%" height={28} style={{ marginBottom: "var(--space-6)" }} />
        <div className="grid-3">
          <SkeletonCard rows={4} />
          <SkeletonCard rows={4} />
          <SkeletonCard rows={4} />
        </div>
      </div>
    </main>
  );
}
