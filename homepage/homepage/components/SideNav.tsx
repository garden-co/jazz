import { SideNavItem } from "@/components/SideNavItem";
import { Framework } from "@/content/framework";
import { DoneStatus } from "@/content/docs/docNavigationItemsTypes";
import { clsx } from "clsx";
import Link from "next/link";
import React from "react";
import { SideNavSection } from "./SideNavSection";

export interface SideNavItem {
  name: string;
  href?: string;
  done?: DoneStatus;
  items?: SideNavItem[];
  collapse?: boolean;
  prefix?: string;
  excludeFromNavigation?: boolean;
  startClosed?: boolean;
}
export function SideNav({
  children,
  className,
}: {
  className?: string;
  children?: React.ReactNode;
}) {
  return (
    <div className={clsx(className, "text-sm h-full flex flex-col gap-4 px-2")}>
      {children}
    </div>
  );
}

export function SideNavSectionList({ items }: { items?: SideNavItem[] }) {
  return (
    !!items?.length && (
      <ul>
        {items.map((item) => {
          const { name, href, items: childItems, done, excludeFromNavigation } = item;

          // Skip items that are excluded from navigation
          if (excludeFromNavigation) {
            return null;
          }

          // If item has child items, render as a section with potential nesting
          if (childItems && childItems.length > 0) {
            return (
              <li key={name}>
                <div className="ms-2">
                  <SideNavSection item={item} />
                </div>
              </li>
            );
          }

          // If item has no child items, render as a simple link
          return (
            <li key={name}>
              <SideNavItem href={href}>
                <span className={done === 0 ? "text-muted" : ""}>
                  {name}
                  {done === 0 && <span className="text-stone-800 text-[0.5rem]"> (docs coming soon)</span>}
                </span>
              </SideNavItem>
            </li>
          );
        })}
      </ul>
    )
  );
}

export function SideNavBody({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex-1 relative overflow-y-auto px-2 -mx-2">
      {children}

      <div
        aria-hidden
        className={clsx(
          "h-12 right-0 sticky bottom-0 left-0",
          "bg-gradient-to-t from-white  to-transparent",
          "dark:from-stone-950",
          "hidden md:block",
        )}
      />
    </div>
  );
}

export function SideNavHeader({
  href,
  children,
  className,
}: {
  href?: string;
  children: React.ReactNode;
  className?: string;
}) {
  const classes = clsx(
    className,
    "flex items-center gap-2 justify-between font-medium text-stone-900 py-1 dark:text-white mb-1 [&:not(:first-child)]:mt-4",
  );
  if (href) {
    return (
      <Link className={classes} href={href}>
        {children}
      </Link>
    );
  }

  return <p className={classes}>{children}</p>;
}
