import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import dynamic from "next/dynamic";

// Dynamically import SeasonClient to enable code splitting
// This prevents the 52KB component from loading on other pages
const SeasonClient = dynamic(() => import("./SeasonClient").then(m => ({ default: m.default })), {
  loading: () => <div style={{ padding: "2rem", textAlign: "center" }}>Loading season simulator...</div>,
  ssr: true,
});

export const metadata: Metadata = buildPageMetadata({
  title: "Season Simulator",
  description:
    "Monte Carlo season projections powered by 10,000 simulations — championship odds, playoff probabilities, award predictions, and more.",
  path: "/season",
  keywords: ["season simulation", "Monte Carlo", "playoff odds", "championship probability", "season projections"],
});

export default function SeasonPage() {
  return <SeasonClient />;
}
