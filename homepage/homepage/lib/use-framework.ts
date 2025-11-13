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
    if (!pathname.startsWith("/docs") || isRedirecting) return;

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

      isRedirecting = true;
      router.replace(newPath, { scroll: false });

      const timeout = setTimeout(() => {
        isRedirecting = false;
        lastRedirectedFramework = null;
      }, 200);

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

    // Tell everyone not to trigger cascading updates
    isRedirecting = true;
    // and update the URL
    router.replace(newPath, { scroll: false });

    // Let the change settle, then tell everyone we're free to update the URL
    // again
    const timeout = setTimeout(() => {
      isRedirecting = false;
    }, 200);

    // Clean up
    return () => {
      clearTimeout(timeout);
      isRedirecting = false;
    };
  }, [pathname, urlFramework, savedFramework, router]);

  const currentFramework = urlFramework || savedFramework || DEFAULT_FRAMEWORK;

  return { framework: currentFramework, setFramework };
};
