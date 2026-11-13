"use client";

import { useEffect, useMemo, useState } from "react";
import { useTheme } from "next-themes";
import { StippleCanvas, heroPattern, jazzInks } from "@garden-co/design/stipple";

/**
 * The hero stipple in the brand inks for the page's theme. The canvas paints
 * literal colours, so it waits for the resolved theme before drawing.
 */
export function HeroPattern({ className }: { className?: string }) {
  const { resolvedTheme } = useTheme();
  // The server can't know the theme: render nothing until mounted so hydration matches.
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  const theme = !mounted
    ? null
    : resolvedTheme === "dark"
      ? "dark"
      : resolvedTheme === "light"
        ? "light"
        : null;
  const pattern = useMemo(
    () => (theme ? { ...heroPattern, inks: jazzInks[theme] } : null),
    [theme],
  );
  if (!pattern) return null;
  return <StippleCanvas pattern={pattern} className={className} />;
}
