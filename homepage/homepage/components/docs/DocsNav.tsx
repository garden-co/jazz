"use client";

import { SideNav, SideNavBody } from "@/components/SideNav";
import { SideNavSection } from "@/components/SideNavSection";
import { FrameworkSelect } from "@/components/docs/FrameworkSelect";
import { docNavigationItems } from "@/content/docs/docNavigationItems";
import { DocNavigationSection } from "@/content/docs/docNavigationItemsTypes";
import { Framework } from "@/content/framework";
import { useFramework } from "@/lib/use-framework";
import { usePathname } from "next/navigation";
import React, { useEffect, useState } from "react";

export function DocNav() {
  const [framework, setFramework] = useState<Framework | null>(null);
  const path = usePathname();
  useEffect(() => {
    const framework = path.split("/")[2];
    setFramework(framework as Framework);
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

            const frameworkDone = (item.done as any)[framework] ?? 0;
            let done =
              typeof item.done === "number" ? item.done : frameworkDone;
            let href = item.href.replace("/docs", `/docs/${framework}`);

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