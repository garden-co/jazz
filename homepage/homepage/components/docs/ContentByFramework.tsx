"use client";

import { useFramework } from "@/lib/use-framework";

export interface ContentByFrameworkProps {
  framework: string;
  children: React.ReactNode;
}

/**
 * Example:
 * <ContentByFramework framework="react">
 *   content visible only if React is the selected framework
 * </ContentByFramework>
 */

export function ContentByFramework(props: {
  framework: string | string[];
  children: React.ReactNode;
}) {
  const { framework } = useFramework();

  if (framework == props.framework) {
    return props.children;
  }

  if (Array.isArray(props.framework) && props.framework.includes(framework)) {
    return props.children;
  }

  return null;
}
