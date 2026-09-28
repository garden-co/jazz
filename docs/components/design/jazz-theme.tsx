"use client";

import { type ReactNode, useEffect, useState } from "react";
import { useTheme } from "next-themes";
import { Theme } from "@astryxdesign/core";
import { jazzTheme } from "@garden-co/design/jazz";
import "@astryxdesign/core/astryx.css";
import "@garden-co/design/jazz/theme.css";

/**
 * Provides the shared Jazz design-system theme (garden-co/design) to Astryx
 * components. The colour mode follows Fumadocs' theme toggle (next-themes),
 * not only the OS preference. The server cannot know the stored choice, so the
 * first render uses "system" on both sides and the stored mode applies after
 * mount, which keeps hydration consistent.
 */
export function JazzTheme({ children }: { children: ReactNode }) {
  const { resolvedTheme } = useTheme();
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  const mode =
    mounted && (resolvedTheme === "dark" || resolvedTheme === "light") ? resolvedTheme : "system";
  return (
    <Theme theme={jazzTheme} mode={mode}>
      {children}
    </Theme>
  );
}
