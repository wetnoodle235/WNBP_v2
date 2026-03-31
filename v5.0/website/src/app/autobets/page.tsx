import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import AutoBetsClient from "./AutoBetsClient";

export const metadata: Metadata = buildPageMetadata({
  title: "Auto Bets",
  description:
    "Monitor the AutoBet bot — live status, active bets, P/L tracking, and bet history.",
  path: "/autobets",
});

export default function AutoBetsPage() {
  return (
    <main>
      <AutoBetsClient />
    </main>
  );
}
