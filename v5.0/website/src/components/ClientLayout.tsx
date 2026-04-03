"use client";

import { Suspense, useEffect, useRef } from "react";
import { usePathname } from "next/navigation";
import { TopNav } from "./TopNav";
import { BackToTop } from "./BackToTop";
import { ScrollProgress } from "./ScrollProgress";
import { OfflineBanner } from "./OfflineBanner";
import { LoadingBar } from "./LoadingBar";
import { PageTracker } from "./PageTracker";
import { ScrollToHash } from "./ScrollToHash";
import { KeyboardShortcuts } from "./KeyboardShortcuts";
import { ChangelogBanner } from "./ChangelogBanner";

/** Move focus to #main-content on route change for screen readers */
function FocusOnRouteChange() {
  const pathname = usePathname();
  const isInitial = useRef(true);

  useEffect(() => {
    // Skip first render (page load already has proper focus)
    if (isInitial.current) {
      isInitial.current = false;
      return;
    }
    const main = document.getElementById("main-content");
    if (main) {
      main.setAttribute("tabindex", "-1");
      main.focus({ preventScroll: true });
    }
  }, [pathname]);

  return null;
}

export function ClientLayout() {
  return (
    <>
      <Suspense fallback={null}>
        <LoadingBar />
      </Suspense>
      <ChangelogBanner />
      <ScrollProgress />
      <TopNav />
      <OfflineBanner />
      <BackToTop />
      <KeyboardShortcuts />
      <PageTracker />
      <ScrollToHash />
      <FocusOnRouteChange />
    </>
  );
}
