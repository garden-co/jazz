"use client";

import { ThemeToggle as GardenThemeToggle } from "@garden-co/design-system/src/components/molecules/ThemeToggle";
import { useTheme } from "next-themes";

export function ThemeToggle({ className }: { className?: string }) {
  let useThemeProps = useTheme();

  return <GardenThemeToggle className={className} {...useThemeProps} />;
}
