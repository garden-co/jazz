"use client";

import {
  SideNavHeader,
  SideNavItem,
  SideNavSectionList,
} from "@/components/SideNav";
import { useFramework } from "@/lib/use-framework";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { usePathname } from "next/navigation";
import React, { useMemo } from "react";

export function SideNavSection({
  item: { name, href, collapse, items, prefix, startClosed },
}: { item: SideNavItem }) {
  const path = usePathname();
  const framework = useFramework();

  const isOpen = useMemo(() => {
    // Check if current path matches this section's href, and open if so
    if (href) {
      if (path === href) return true;
    }

    // If there's a prefix, check if current path starts with the framework-adjusted prefix
    if (prefix) {
      const frameworkPrefix = prefix.replace("/docs/", "/docs/" + framework + "/");
      return path.startsWith(frameworkPrefix);
    }

    // If explicitly set to start closed and no path matches, don't open
    if (startClosed) return false;

    return true;
  }, [path, framework, startClosed, prefix, href, items]);
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
        open={isOpen}
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
