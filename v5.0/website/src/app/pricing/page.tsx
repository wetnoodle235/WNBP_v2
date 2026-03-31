import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import PricingClient from "./PricingClient";

export const metadata: Metadata = buildPageMetadata({
  title: "Pricing",
  description: "Choose the WNBP plan that fits your needs – free or premium.",
  path: "/pricing",
});

export default function PricingPage() {
  return (
    <div className="pricing-page">
      <PricingClient />
    </div>
  );
}
