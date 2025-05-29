"use client";

import { SideNav, SideNavBody, SideNavHeader } from "@/components/SideNav";
import { SideNavSection } from "@/components/SideNavSection";
import { FrameworkSelect } from "@/components/docs/FrameworkSelect";
import { usePagefindSearch } from "@/components/pagefind";
import { QuickSearch } from "@/components/quick-search";
import { docNavigationItems } from "@/content/docs/docNavigationItems";
import { DocNavigationSection } from "@/content/docs/docNavigationItemsTypes";
import { useFramework } from "@/lib/use-framework";
import React from "react";

export function DocNav() {
  const framework = useFramework();
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
  const { setOpen } = usePagefindSearch();

  return (
    <SideNav>
      <FrameworkSelect />

      <div className="pb-5">
        <QuickSearch onClick={() => setOpen((open) => !open)} />
      </div>

      <SideNavBody>
        {items.map((item) => (
          <SideNavSection item={item} key={item.name} />
        ))}
      </SideNavBody>
    </SideNav>
  );
}
