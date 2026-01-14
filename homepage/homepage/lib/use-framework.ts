"use client";
import {
  DEFAULT_FRAMEWORK,
  Framework,
  isValidFramework,
} from "@/content/framework";
import { useParams, usePathname, useRouter } from "next/navigation";
import { useEffect, useLayoutEffect, useState, useCallback } from "react";
import {
  TAB_CHANGE_EVENT,
  isFrameworkChange,
} from "@garden-co/design-system/src/types/tabbed-code-group";

// Keep these module level to avoid all components on the page which depend on useFramework from triggering new redirects
let isRedirecting = false;
let userInitiatedChange = false;
let lastRedirectedFramework: Framework | null = null;
let lastWindowScrollY = 0;
let shouldRestoreScroll = false;
let resetScrollRestorationTimeout: ReturnType<typeof setTimeout> | null = null;

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

  const setFramework = useCallback((newFramework: Framework) => {
    if (!isValidFramework(newFramework)) return;
    userInitiatedChange = true;
    localStorage.setItem("_tcgpref_framework", newFramework);
    setSavedFramework(newFramework);
    window.dispatchEvent(
      new CustomEvent(TAB_CHANGE_EVENT, {
        detail: {
          key: "framework",
          value: newFramework,
        },
      }),
    );
  }, []);

  const urlFramework =
    framework && isValidFramework(framework) ? framework : null;

  // Restore window scroll position after framework change
  useLayoutEffect(() => {
    if (
      shouldRestoreScroll &&
      typeof window !== "undefined" &&
      lastWindowScrollY > 0
    ) {
      // Single, precise scroll restoration
      const maxScroll = Math.max(
        0,
        document.documentElement.scrollHeight - window.innerHeight,
      );
      const scrollY = Math.min(lastWindowScrollY, maxScroll);
      window.scrollTo({ top: scrollY, behavior: "instant" });
      shouldRestoreScroll = false;
    }
  }, [pathname, urlFramework]);

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
    return () => window.removeEventListener(TAB_CHANGE_EVENT, handleTabChange);
  }, []);

  useEffect(() => {
    if (!pathname.startsWith("/docs") || isRedirecting) {
      return;
    }

    // Trigger this block only once after the user changes framework to update
    // to match user's choice
    if (userInitiatedChange) {
      userInitiatedChange = false;
      if (!savedFramework) return;

      const parts = pathname.split("/");
      if (parts.length >= 3 && parts[2] === savedFramework) return;

      parts[2] = savedFramework;
      const newPath = parts.join("/");
      lastRedirectedFramework = savedFramework;

      // Capture scroll position before route change
      if (typeof window !== "undefined") {
        lastWindowScrollY = window.scrollY;
        shouldRestoreScroll = true;
        // Set scroll restoration to manual and debounce reset to auto
        try {
          if ("scrollRestoration" in window.history) {
            // Clear any existing reset timeout
            if (resetScrollRestorationTimeout) {
              clearTimeout(resetScrollRestorationTimeout);
              resetScrollRestorationTimeout = null;
            }
            // Set to manual (only if not already manual to avoid unnecessary API calls)
            if (window.history.scrollRestoration !== "manual") {
              window.history.scrollRestoration = "manual";
            }
            // Debounce reset to auto after 3 seconds of inactivity
            resetScrollRestorationTimeout = setTimeout(() => {
              try {
                if ("scrollRestoration" in window.history) {
                  window.history.scrollRestoration = "auto";
                }
              } catch {
                // Ignore security errors
              }
              resetScrollRestorationTimeout = null;
            }, 3000);
          }
        } catch {
          // Ignore security errors when setting scrollRestoration
        }
      }

      isRedirecting = true;
      router.replace(newPath, { scroll: false });

      const timeout = setTimeout(() => {
        isRedirecting = false;
        lastRedirectedFramework = null;
      }, 500); // Increased timeout to prevent rapid successive calls

      return () => {
        clearTimeout(timeout);
        isRedirecting = false;
        lastRedirectedFramework = null;
      };
    }

    if (urlFramework) {
      // Exit early if the redirect has *just* happened
      if (lastRedirectedFramework === urlFramework) {
        lastRedirectedFramework = null;
        return;
      }
      // Otherwise update localStorage and the saved framework (manually, not
      // using the helper otherwise we'll trigger a new loop)
      localStorage.setItem("_tcgpref_framework", urlFramework);
      setSavedFramework(urlFramework);
      return;
    }

    const parts = pathname.split("/");

    // Race condition guard in case effect runs before `useParams` updates
    if (parts.length >= 3 && parts[2] === savedFramework) return;

    // And if not, update the framework and rebuild the URL—if users have no preference and no indication in URL, just use React
    parts[2] = savedFramework || DEFAULT_FRAMEWORK;
    const newPath = parts.join("/");

    // Capture scroll position before route change (only if this is a framework change, not initial load)
    // Only capture if we're already on a docs page (not initial load)
    if (
      typeof window !== "undefined" &&
      pathname.startsWith("/docs/") &&
      window.scrollY > 0
    ) {
      lastWindowScrollY = window.scrollY;
      shouldRestoreScroll = true;
      // Set scroll restoration to manual and debounce reset to auto
      try {
        if ("scrollRestoration" in window.history) {
          // Clear any existing reset timeout
          if (resetScrollRestorationTimeout) {
            clearTimeout(resetScrollRestorationTimeout);
            resetScrollRestorationTimeout = null;
          }
          // Set to manual (only if not already manual to avoid unnecessary API calls)
          if (window.history.scrollRestoration !== "manual") {
            window.history.scrollRestoration = "manual";
          }
          // Debounce reset to auto after 3 seconds of inactivity
          resetScrollRestorationTimeout = setTimeout(() => {
            try {
              if ("scrollRestoration" in window.history) {
                window.history.scrollRestoration = "auto";
              }
            } catch {
              // Ignore security errors
            }
            resetScrollRestorationTimeout = null;
          }, 3000);
        }
      } catch {
        // Ignore security errors when setting scrollRestoration
      }
    }

    // Tell everyone not to trigger cascading updates
    isRedirecting = true;
    // and update the URL
    router.replace(newPath, { scroll: false });

    // Let the change settle, then tell everyone we're free to update the URL
    // again
    const timeout = setTimeout(() => {
      isRedirecting = false;
    }, 500); // Increased timeout to prevent rapid successive calls

    // Clean up
    return () => {
      clearTimeout(timeout);
      isRedirecting = false;
    };
  }, [pathname, urlFramework, savedFramework, router]);

  const currentFramework = urlFramework || savedFramework || DEFAULT_FRAMEWORK;

  return { framework: currentFramework, setFramework };
};
