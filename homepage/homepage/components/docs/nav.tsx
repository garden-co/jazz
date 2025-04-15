"use client";

import { SideNav } from "@/components/SideNav";
import { SideNavHeader } from "@/components/SideNavHeader";
import { FrameworkSelect } from "@/components/docs/FrameworkSelect";
import { docNavigationItems } from "@/content/docs/docNavigationItems.js";
import { useFramework } from "@/lib/use-framework";
import { clsx } from "clsx";

export function DocNav({ className }: { className?: string }) {
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
    <SideNav
      items={items}
      className={clsx(className)}
      footer={
        <SideNavHeader href="/api-reference">API Reference</SideNavHeader>
      }
    >
      <FrameworkSelect />
    </SideNav>
  );
}
