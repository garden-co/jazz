"use client";

import type { ReactNode } from "react";
import NextLink from "next/link";
import { Theme } from "@astryxdesign/core";
import { LinkProvider } from "@astryxdesign/core/Link";
import { jazzTheme } from "@garden-co/design/jazz";

/** The Jazz theme on Astryx, following the system colour mode, with Next routing for links. */
export function AppTheme({ children }: { children: ReactNode }) {
  return (
    <Theme theme={jazzTheme} mode="system">
      <LinkProvider component={NextLink}>{children}</LinkProvider>
    </Theme>
  );
}
