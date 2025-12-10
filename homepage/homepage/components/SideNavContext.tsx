"use client";

import { createContext, useContext, type RefObject } from "react";

// Normalize path by removing framework segment (e.g., /docs/react/... -> /docs/...)
export function normalizePath(path: string): string {
  if (!path.startsWith("/docs/")) {
    return path;
  }
  const parts = path.split("/");
  // Check if second segment is a framework (after /docs/)
  const frameworks = ["react", "svelte", "vue", "react-native", "react-native-expo", "vanilla"];
  if (parts.length >= 3 && frameworks.includes(parts[2])) {
    // Remove the framework segment
    return "/docs/" + parts.slice(3).join("/");
  }
  return path;
}

export interface SideNavContextValue {
  shouldScrollToActive: boolean;
  scrollContainerRef: RefObject<HTMLDivElement | null> | null;
}

export const SideNavContext = createContext<SideNavContextValue>({
  shouldScrollToActive: true,
  scrollContainerRef: null,
});

export function useSideNav() {
  return useContext(SideNavContext);
}
