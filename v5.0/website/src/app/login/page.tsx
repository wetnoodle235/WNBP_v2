import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import { Suspense } from "react";
import LoginClient from "./LoginClient";

export const metadata: Metadata = buildPageMetadata({
  title: "Sign In",
  description: "Sign in to your WNBP account.",
  path: "/login",
});

export default function LoginPage() {
  return (
    <div className="auth-page">
      <Suspense>
        <LoginClient />
      </Suspense>
    </div>
  );
}
