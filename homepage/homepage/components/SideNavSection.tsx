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
  item: { name, href, collapse, items, prefix },
}: { item: SideNavItem }) {
  const path = usePathname();
  const framework = useFramework();
  if (!collapse) {
    return (
      <>
        <SideNavHeader href={href}>{name}</SideNavHeader>

        <SideNavSectionList items={items} />
      </>
    );
  }
  return (
    <>
      <details
        className="group [&:not(:first-child)]:mt-4"
        open={
          prefix
            ? path.startsWith(
                prefix.replace("/docs/", "/docs/" + framework + "/"),
              )
            : true
        }
      >
        <summary className="list-none">
          <SideNavHeader href={href}>
            {name}
            {collapse && (
              <Icon
                className="group-open:rotate-180 transition-transform group-hover:text-stone-500 text-stone-400 dark:text-stone-600"
                name="chevronDown"
                size="xs"
              />
            )}
          </SideNavHeader>
        </summary>

        <SideNavSectionList items={items} />
      </details>
    </>
  );
}
