import { DEFAULT_FRAMEWORK, isValidFramework } from "@/content/framework";
import { useParams } from "next/navigation";

export const useFramework = () => {
  const { framework } = useParams<{ framework?: string }>();

  if (framework && isValidFramework(framework)) {
    return framework;
  }

  if (typeof window !== "undefined") {
  const savedFramework = window.localStorage.getItem("_tcgpref_framework");
    if (savedFramework && isValidFramework(savedFramework)) {
      return savedFramework;
    }
  }

  return DEFAULT_FRAMEWORK;
};
