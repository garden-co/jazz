"use client";

import type { ReactNode } from "react";
import NextLink from "next/link";
import { AppShell } from "@astryxdesign/core/AppShell";
import { LinkProvider } from "@astryxdesign/core/Link";
import { JazzTheme } from "@/components/design/jazz-theme";
import { SiteTopNav } from "./site-top-nav";

/**
 * The jazz.tools page frame on Astryx: the Jazz theme, Next.js routing for
 * every Astryx link, and an `AppShell` with the shared top nav. Docs pass
 * their sidebar as `sideNav`.
 */
export function SiteShell({ sideNav, children }: { sideNav?: ReactNode; children: ReactNode }) {
  return (
    <JazzTheme>
      <LinkProvider component={NextLink}>
        <AppShell height="auto" variant="section" topNav={<SiteTopNav />} sideNav={sideNav}>
          {children}
        </AppShell>
      </LinkProvider>
    </JazzTheme>
  );
}
