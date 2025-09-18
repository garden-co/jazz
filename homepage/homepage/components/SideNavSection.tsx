"use client";

import {
  SideNavHeader,
  SideNavItem,
  SideNavSectionList,
} from "@/components/SideNav";
import { useFramework } from "@/lib/use-framework";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { usePathname } from "next/navigation";
import React from "react";

export function SideNavSection({
  item: { name, href, collapse, items, prefix, startClosed },
}: { item: SideNavItem }) {
  const path = usePathname();
  const framework = useFramework();
  // If no items, render as a simple header
  if (!items || items.length === 0) {
    return <SideNavHeader href={href}>{name}</SideNavHeader>;
  }

  // If not collapsible, render as an expanded section
  if (!collapse) {
    return (
      <>
        <SideNavHeader href={href}>{name}</SideNavHeader>

        <SideNavSectionList items={items} />
      </>
    );
  }
  // If collapsible, render as a details/summary element
  return (
    <>
      <details
        className="group [&:not(:first-child)]:mt-4"
        open={
          startClosed ? false :
            prefix
              ? path.startsWith(
                prefix.replace("/docs/", "/docs/" + framework + "/"),
              )
              : true
        }
      >
        <summary className="list-none">
          <div className="flex items-center gap-2 justify-between font-medium text-stone-900 py-1 dark:text-white mb-1 [&:not(:first-child)]:mt-4">
            {href ? (
              <a
                href={href}
                className="flex-1 hover:text-stone-700 dark:hover:text-stone-300"
                onClick={(e) => e.stopPropagation()}
              >
                {name}
              </a>
            ) : (
              <span className="flex-1">{name}</span>
            )}
            <Icon
              className="group-open:rotate-180 transition-transform group-hover:text-stone-500 text-stone-400 dark:text-stone-600"
              name="chevronDown"
              size="xs"
            />
          </div>
        </summary>

        <SideNavSectionList items={items} />
      </details>
    </>
  );
}
