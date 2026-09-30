"use client";

import type { ReactNode } from "react";
import { Theme } from "@astryxdesign/core";
import { jazzTheme } from "@garden-co/design/jazz";

/** The shared Jazz theme; colour mode follows the operating system. */
export function JazzTheme({ children }: { children: ReactNode }) {
  return (
    <Theme theme={jazzTheme} mode="system">
      {children}
    </Theme>
  );
}
