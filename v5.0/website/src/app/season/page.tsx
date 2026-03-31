import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import { SeasonClient } from "./SeasonClient";

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
