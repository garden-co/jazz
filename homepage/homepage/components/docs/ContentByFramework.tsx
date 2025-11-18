"use client";

import { useFramework } from "@/lib/use-framework";
import { useEffect, useState } from "react";
import { TAB_CHANGE_EVENT } from "@garden-co/design-system/src/types/tabbed-code-group";


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
  const framework = useFramework();

  if (framework == props.framework) {
    return props.children;
  }

  if (Array.isArray(props.framework) && props.framework.includes(framework)) {
    return props.children;
  }

  return null;
}
