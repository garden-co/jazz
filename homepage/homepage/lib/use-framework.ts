"use client";
import {
  DEFAULT_FRAMEWORK,
  Framework,
  isValidFramework,
} from "@/content/framework";
import { useParams, usePathname, useRouter } from "next/navigation";
import { useEffect, useState, useCallback } from "react";
import {
  TAB_CHANGE_EVENT,
  isFrameworkChange,
} from "@garden-co/design-system/src/types/tabbed-code-group";

// Keep these module level to avoid all components on the page which depend on useFramework from triggering new redirects
let isRedirecting = false;
let userInitiatedChange = false;
let lastRedirectedFramework: Framework | null = null;

/* 
 * This hook does the following:
 * 1. Lazily initialises the user's preferred framework from localStorage
 * 2. Defines a `setFramework` function which updates the framework in storage 
 *    and dispatches a `TAB_CHANGE_EVENT` 
 * 3. Looks up whether the URL specifies a framework
 * 4. Registers an event listener for the `TAB_CHANGE_EVENT` emitted from a 
 *    TabbedCodeGroup
 * 5. Builds an appropriate URL to redirect to, and completes the redirect.
 */

export const useFramework = () => {
  const pathname = usePathname();
  const { framework } = useParams<{ framework?: string }>();
  const [savedFramework, setSavedFramework] = useState<Framework | null>(() => {
    if (typeof window === "undefined") return null;
    const stored = localStorage.getItem("_tcgpref_framework");
    if (stored && isValidFramework(stored)) {
      return stored;
    }
    return null;
  });
  const router = useRouter();

  useEffect(() => {
    setMounted(true);
    // Check localStorage after mounting
    if (typeof window !== "undefined") {
      const stored = window.localStorage.getItem("_tcgpref_framework");
      if (stored && isValidFramework(stored)) {
        setSavedFramework(stored as Framework);
        // If the currently loaded page is a docs page, make sure that URL matches the selected framework.
        if (!pathname.startsWith('/docs')) return;
        const newPath = pathname.split("/").toSpliced(2, 1, stored).join("/") + window.location.hash;
        window.history.replaceState({}, "", newPath);
      }
    }
  }, []);

  const urlFramework = framework && isValidFramework(framework) ? framework : null;

  useEffect(() => {
    const handleTabChange = (event: CustomEvent) => {
      if (isFrameworkChange(event.detail)) {
        const newFramework = event.detail.value;
        if (isValidFramework(newFramework)) {
          userInitiatedChange = true;
          setSavedFramework(newFramework);
        }
      }
    };
    window.addEventListener(TAB_CHANGE_EVENT, handleTabChange);
    return () =>
      window.removeEventListener(TAB_CHANGE_EVENT, handleTabChange);
  }, []);


  useEffect(() => {
    if (!mounted || !savedFramework || !pathname.startsWith('/docs')) return;
    const parts = pathname.split("/");
    if (parts[2] !== savedFramework) {
      const newPath = parts.toSpliced(2, 1, savedFramework).join("/");
      window.history.replaceState({}, "", newPath);
    }
  }, [mounted, savedFramework, pathname]);


  if (mounted && savedFramework) return savedFramework;
  if (framework && isValidFramework(framework)) return framework;
  return DEFAULT_FRAMEWORK;
};
