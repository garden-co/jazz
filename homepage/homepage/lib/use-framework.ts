import { DEFAULT_FRAMEWORK, Framework, isValidFramework } from "@/content/framework";
import { useParams } from "next/navigation";
import { useEffect, useState } from "react";

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

  if (framework && isValidFramework(framework)) {
    return framework;
  }

  // Use saved framework if available and component is mounted
  if (mounted && savedFramework) {
    return savedFramework;
  }

  return DEFAULT_FRAMEWORK;
};
