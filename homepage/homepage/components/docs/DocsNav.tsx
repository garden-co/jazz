"use client";

import { SideNav, SideNavBody } from "@/components/SideNav";
import { SideNavSection } from "@/components/SideNavSection";
import { FrameworkSelect } from "@/components/docs/FrameworkSelect";
import { docNavigationItems } from "@/content/docs/docNavigationItems";
import { DocNavigationSection } from "@/content/docs/docNavigationItemsTypes";
import { Framework, isValidFramework, DEFAULT_FRAMEWORK } from "@/content/framework";
import { useFramework } from "@/lib/use-framework";
import { usePathname } from "next/navigation";
import React, { useEffect, useState } from "react";

export function DocNav() {
  const [framework, setFramework] = useState<Framework | null>(null);
  const path = usePathname();
  useEffect(() => {
    const pathParts = path.split("/");
    const extractedFramework = pathParts[2];

    // Validate that we have a valid framework value
    const validFramework = isValidFramework(extractedFramework) ? extractedFramework as Framework : DEFAULT_FRAMEWORK;

    setFramework(validFramework);
  }, [path]);
  // Recursive function to process nested items
  const processNavigationItem = (item: any): any => {
    const validFramework = framework ?? DEFAULT_FRAMEWORK;

    // Filter by framework if specified
    if ("framework" in item && item.framework !== framework) {
      return null;
    }

    // Process href and done status
    let processedItem = { ...item };

    if (item.href?.startsWith("/docs")) {
      const frameworkDone = (item.done as any)?.[validFramework] ?? 0;
      const done = typeof item.done === "number" ? item.done : frameworkDone;
      const href = item.href.replace("/docs", `/docs/${validFramework}`);


      processedItem = {
        ...processedItem,
        href,
        done,
      };
    }

    // Recursively process nested items
    if (item.items && item.items.length > 0) {
      const processedItems = item.items
        .map(processNavigationItem)
        .filter(Boolean); // Remove null items (filtered out by framework)

      processedItem = {
        ...processedItem,
        items: processedItems,
      };
    }

    return processedItem;
  };
  const items = (docNavigationItems as DocNavigationSection[]).map(
    (headerItem) => {
      return {
        ...headerItem,
        items: headerItem.items
          .map(processNavigationItem)
          .filter(Boolean), // Remove null items (filtered out by framework)
      };
    },
  );

  return (
    <SideNav>
      <FrameworkSelect />

      <SideNavBody>
        {items.map((item) => (
          <SideNavSection item={item} key={item.name} />
        ))}
      </SideNavBody>
    </SideNav>
  );
}