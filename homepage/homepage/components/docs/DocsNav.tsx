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
  const items = (docNavigationItems as DocNavigationSection[]).map(
    (headerItem) => {
      return {
        ...headerItem,
        items: headerItem.items
          .filter(
            (item) => !("framework" in item) || item.framework === framework,
          )
          .map((item) => {
            if (!item.href?.startsWith("/docs")) return item;
            const validFramework = framework ?? DEFAULT_FRAMEWORK;
            const frameworkDone = (item.done as any)[validFramework] ?? 0;
            let done =
              typeof item.done === "number" ? item.done : frameworkDone;
            let href = item.href.replace("/docs", `/docs/${validFramework}`);

            return {
              ...item,
              href,
              done,
            };
          }),
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