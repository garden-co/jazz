"use client";

import type { ReactNode } from "react";
import NextLink from "next/link";
import { AppShell } from "@astryxdesign/core/AppShell";
import { LinkProvider } from "@astryxdesign/core/Link";
import { JazzTheme } from "@/components/design/jazz-theme";
import { SiteFooter } from "./site-footer";
import { SiteTopNav } from "./site-top-nav";

/**
 * The jazz.tools page frame on Astryx: the Jazz theme, Next.js routing for
 * every Astryx link, and an `AppShell` with the shared top nav. Docs pass
 * their sidebar as `sideNav`. Pages without a sidebar (homepage, blog) keep
 * the site's page background and centred 1400px layout width, and end with
 * the site footer.
 */
export function SiteShell({ sideNav, children }: { sideNav?: ReactNode; children: ReactNode }) {
  const shell = (
    <AppShell height="auto" variant="section" topNav={<SiteTopNav />} sideNav={sideNav}>
      {children}
      {sideNav ? null : <SiteFooter />}
    </AppShell>
  );
  return (
    <JazzTheme>
      <LinkProvider component={NextLink}>
        {sideNav ? shell : <div className="site-page-shell">{shell}</div>}
      </LinkProvider>
    </JazzTheme>
  );
}
