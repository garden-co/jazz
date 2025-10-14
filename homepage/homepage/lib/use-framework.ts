"use client";
import { DEFAULT_FRAMEWORK, Framework, isValidFramework } from "@/content/framework";
import { useParams, usePathname, useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { TAB_CHANGE_EVENT, isFrameworkChange } from "@garden-co/design-system/src/types/tabbed-code-group";

export const useFramework = () => {
  const pathname = usePathname();
  const router = useRouter();
  const { framework } = useParams<{ framework?: string }>();
  const [savedFramework, setSavedFramework] = useState<Framework | null>(null);
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    const stored = window.localStorage.getItem("_tcgpref_framework");
    if (stored && isValidFramework(stored)) {
      setSavedFramework(stored as Framework);
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
    if (parts[2] !== savedFramework) {
      const newPath = parts.toSpliced(2, 1, savedFramework).join("/");
      router.replace(newPath, { scroll: false });
    }
  }, [mounted, savedFramework, pathname]);


  if (mounted && savedFramework) return savedFramework;
  if (framework && isValidFramework(framework)) return framework;
  return DEFAULT_FRAMEWORK;
};
