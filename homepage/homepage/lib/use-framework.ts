"use client";
import { DEFAULT_FRAMEWORK, Framework, isValidFramework } from "@/content/framework";
import { useParams, usePathname, useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { TAB_CHANGE_EVENT, isFrameworkChange } from "@garden-co/design-system/src/types/tabbed-code-group";

// Global tracking to prevent multiple simultaneous redirects
// (since useFramework is called by multiple components on the same page)
let isRedirecting = false;
let lastRedirectedTo = "";

export const useFramework = () => {
  const pathname = usePathname();
  const { framework } = useParams<{ framework?: string }>();
  const [savedFramework, setSavedFramework] = useState<Framework | null>(null);
  const [mounted, setMounted] = useState(false);
  const router = useRouter();

  useEffect(() => {
    setMounted(true);
    // Check localStorage after mounting
    if (typeof window !== "undefined") {
      const stored = window.localStorage.getItem("_tcgpref_framework");
      if (stored && isValidFramework(stored)) {
        setSavedFramework(stored as Framework);
      }
    }
  }, []);

  useEffect(() => {
    const handleTabChange = (event: CustomEvent) => {
      if (isFrameworkChange(event.detail)) {
        const newFramework = event.detail.value;
        if (isValidFramework(newFramework)) {
          setSavedFramework(newFramework as Framework);
        }
      }
    };
    window.addEventListener(TAB_CHANGE_EVENT as any, handleTabChange);
    return () => window.removeEventListener(TAB_CHANGE_EVENT as any, handleTabChange);
  }, []);

  useEffect(() => {
    if (!mounted || !savedFramework || !pathname.startsWith('/docs')) return;

    const parts = pathname.split("/");
    const newPath = parts.toSpliced(2, 1, savedFramework).join("/");

    // Don't redirect if already redirecting or if we just redirected to this path
    if (isRedirecting || lastRedirectedTo === newPath) return;

    if (parts[2] !== savedFramework) {
      isRedirecting = true;
      lastRedirectedTo = newPath;
      router.replace(newPath, { scroll: false });
      // Reset the flag after navigation completes
      setTimeout(() => {
        isRedirecting = false;
      }, 200);
    }
  }, [mounted, savedFramework, pathname, router]);


  if (mounted && savedFramework) return savedFramework;
  if (framework && isValidFramework(framework)) return framework;
  return DEFAULT_FRAMEWORK;
};
