"use client";
import { SideNavItem } from "@/components/SideNavItem";
import { DoneStatus } from "@/content/docs/docNavigationItemsTypes";
import { clsx } from "clsx";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { SideNavSection } from "./SideNavSection";
import { normalizePath, SideNavContext } from "./SideNavContext";

// Module-level variables to persist scroll state across re-renders
let lastScrollTop = 0;
let lastNormalizedPath: string | null = null;
let lastRawPath: string | null = null;

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
    <div
      className={clsx(className, "text-sm h-full flex flex-col gap-4 px-2 overflow-y-auto")}
    >
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
  const ref = useRef<HTMLDivElement>(null);
  const [extraPadding, setExtraPadding] = useState(false);
  const pathname = usePathname();
  const currentNormalizedPath = normalizePath(pathname);

  // Determine if this is a framework change (same normalized path, different raw path)
  const isFrameworkChange = lastNormalizedPath !== null &&
    currentNormalizedPath === lastNormalizedPath &&
    lastRawPath !== null &&
    pathname !== lastRawPath; // Raw paths differ but normalized paths match

  // Restore scroll position on framework change
  useLayoutEffect(() => {
    const div = ref.current;
    if (!div) return;

    if (isFrameworkChange && lastScrollTop > 0) {
      // Restore scroll position instantly (no animation)
      div.scrollTop = lastScrollTop;
    }
  }, [isFrameworkChange, pathname]);

  // Track scroll position
  useEffect(() => {
    const div = ref.current;
    if (!div) return;

    const handleScroll = () => {
      lastScrollTop = div.scrollTop;
    };

    div.addEventListener("scroll", handleScroll, { passive: true });
    return () => div.removeEventListener("scroll", handleScroll);
  }, []);

  // Update tracking variables after determining framework change
  useEffect(() => {
    lastNormalizedPath = currentNormalizedPath;
    lastRawPath = pathname;
  }, [currentNormalizedPath, pathname]);

  useEffect(() => {
    const div = ref.current;
    if (!div) return;

    const updatePadding = () => {
      const scrollbarWidth = div.offsetWidth - div.clientWidth;
      // If we're dealing with overlay scroll bars, then we'll set extra padding
      setExtraPadding(scrollbarWidth === 0)
    };

    updatePadding();
    window.addEventListener("resize", updatePadding);
    return () => window.removeEventListener("resize", updatePadding);
  }, []);

  // Determine if we should scroll to active item (true for navigation, false for framework change)
  const shouldScrollToActive = !isFrameworkChange;

  return (
    <SideNavContext.Provider value={{ shouldScrollToActive, scrollContainerRef: ref }}>
      <div ref={ref}
        className={clsx("flex-1 relative overflow-y-auto -mx-2", extraPadding ? 'px-2 pr-4' : 'px-2')}>
        {children}
        <div
          aria-hidden
          className={clsx(
            "h-12 right-0 sticky bottom-0 left-0",
            "bg-linear-to-t from-white  to-transparent",
            "dark:from-stone-950",
            "hidden md:block",
          )}
        />
      </div>
    </SideNavContext.Provider>
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
    "flex items-center gap-2 justify-between font-medium text-stone-900 py-1 dark:text-white mb-1 not-first:mt-4",
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
