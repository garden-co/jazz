"use client";

import { useEffect, useMemo, useState } from "react";
import { useTheme } from "next-themes";
import { StippleCanvas, heroPattern } from "@garden-co/design/stipple";

/** The logo blue, and the mint the palette uses per theme (--color-jazz-mint). */
const BLUE = "#146aff";
const MINT = { light: "#0e7550", dark: "#62f5c0" };

/**
 * The hero stipple in the logo blue and the theme's mint. The canvas paints
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
    () => (theme ? { ...heroPattern, inks: { a: MINT[theme], b: BLUE } } : null),
    [theme],
  );
  if (!pattern) return null;
  return <StippleCanvas pattern={pattern} className={className} />;
}
