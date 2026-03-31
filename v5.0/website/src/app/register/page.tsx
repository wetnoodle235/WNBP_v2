import { buildPageMetadata } from "@/lib/seo";
import type { Metadata } from "next";
import { Suspense } from "react";
import RegisterClient from "./RegisterClient";

export const metadata: Metadata = buildPageMetadata({
  title: "Register",
  description: "Create a new WNBP account to access predictions and more.",
  path: "/register",
});

export default function RegisterPage() {
  return (
    <div className="auth-page">
      <Suspense>
        <RegisterClient />
      </Suspense>
    </div>
  );
}
