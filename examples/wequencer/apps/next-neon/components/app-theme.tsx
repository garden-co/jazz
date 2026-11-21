"use client";

import type { ReactNode } from "react";
import NextLink from "next/link";
import { Theme } from "@astryxdesign/core";
import { LinkProvider } from "@astryxdesign/core/Link";
import { jazzTheme } from "@garden-co/design/jazz";

/**
 * The shared Jazz design-system theme, following the system colour mode.
 * Astryx links route through Next.js so the Jazz client stays open.
 */
export function AppTheme({ children }: { children: ReactNode }) {
  return (
    <Theme theme={jazzTheme} mode="system">
      <LinkProvider component={NextLink}>{children}</LinkProvider>
    </Theme>
  );
}
