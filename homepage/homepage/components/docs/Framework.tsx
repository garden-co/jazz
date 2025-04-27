"use client";

import { frameworkNames } from "@/content/framework";
import { useFramework } from "@/lib/use-framework";

export function Framework() {
  const framework = useFramework();

  return <>{frameworkNames[framework].label}</>;
}
