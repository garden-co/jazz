"use client";

import { SideNav, SideNavBody, SideNavHeader } from "@/components/SideNav";
import { SideNavSection } from "@/components/SideNavSection";
import { FrameworkSelect } from "@/components/docs/FrameworkSelect";
import { docNavigationItems } from "@/content/docs/docNavigationItems.js";
import { useFramework } from "@/lib/use-framework";
import React from "react";

export function DocNav() {
  const framework = useFramework();
  const items = docNavigationItems.map((headerItem) => {
    return {
      ...headerItem,
      items: headerItem.items
        .filter(
          (item) => !("framework" in item) || item.framework === framework,
        )
        .map((item) => {
          if (!item.href?.startsWith("/docs")) return item;

          const frameworkDone = (item.done as any)[framework] ?? 0;
          let done = typeof item.done === "number" ? item.done : frameworkDone;
          let href = item.href.replace("/docs", `/docs/${framework}`);

          return {
            ...item,
            href,
            done,
          };
        }),
    };
  });

  return (
    <SideNav>
      <FrameworkSelect />

      <SideNavBody>
        {items.map((item) => (
          <SideNavSection item={item} key={item.name} />
        ))}

        <SideNavHeader href="/api-reference">API Reference</SideNavHeader>
      </SideNavBody>
    </SideNav>
  );
}
