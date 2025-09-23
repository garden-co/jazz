import { DEFAULT_FRAMEWORK, Framework, isValidFramework } from "@/content/framework";
import { useParams } from "next/navigation";
import { useEffect, useState } from "react";
import { TAB_CHANGE_EVENT, isFrameworkChange } from "@garden-co/design-system/src/types/tabbed-code-group";

export const useFramework = () => {
  const { framework } = useParams<{ framework?: string }>();
  const [savedFramework, setSavedFramework] = useState<Framework | null>(null);
  const [mounted, setMounted] = useState(false);

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

    if (typeof window !== "undefined") {
      window.addEventListener(TAB_CHANGE_EVENT as any, handleTabChange);
      return () => {
        window.removeEventListener(TAB_CHANGE_EVENT as any, handleTabChange);
      };
    }
  }, []);

  // Prioritize savedFramework (from events) over URL parameters
  if (mounted && savedFramework) {
    return savedFramework;
  }

  if (framework && isValidFramework(framework)) {
    return framework;
  }

  return DEFAULT_FRAMEWORK;
};
