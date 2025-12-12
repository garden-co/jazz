"use client";

import {
  SideNavHeader,
  SideNavItem,
  SideNavSectionList,
} from "@/components/SideNav";
import { useFramework } from "@/lib/use-framework";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { usePathname } from "next/navigation";
import React, { useEffect, useMemo, useRef } from "react";

const openedSections = new Set<string>();

// Helper function to recursively check if any item in the tree matches the current path
function hasMatchingPath(
  item: SideNavItem,
  currentPath: string,
  framework: string,
): boolean {
  // Check if this item's href matches the current path
  // Note: hrefs are already framework-adjusted by DocsNav.tsx, so compare directly
  if (item.href) {
    if (currentPath === item.href) {
      return true;
    }
  }

  // Check if current path starts with this item's prefix
  // Prefixes may need framework adjustment if they start with /docs
  if (item.prefix) {
    let prefixToCheck = item.prefix;
    // If prefix starts with /docs but doesn't already have a framework segment, add it
    if (prefixToCheck.startsWith("/docs/") && !prefixToCheck.match(/^\/docs\/(react|svelte|vue|react-native|react-native-expo|vanilla)\//)) {
      prefixToCheck = prefixToCheck.replace("/docs/", "/docs/" + framework + "/");
    }
    if (currentPath.startsWith(prefixToCheck)) {
      return true;
    }
  }

  // Recursively check child items
  if (item.items && item.items.length > 0) {
    return item.items.some((childItem) =>
      hasMatchingPath(childItem, currentPath, framework),
    );
  }

  return false;
}

export function SideNavSection({
  item: { name, href, collapse, items, prefix, startClosed },
}: { item: SideNavItem }) {
  const path = usePathname();
  const { framework } = useFramework();

  const sectionId = useMemo(() => {
    return `${name}-${prefix || href || ''}`;
  }, [name, prefix, href]);

  const pathMatches = useMemo(() => {
    // Note: hrefs are already framework-adjusted by DocsNav.tsx
    if (href) {
      if (path === href) return true;
    }

    // If there's a prefix, check if current path starts with the framework-adjusted prefix
    if (prefix) {
      let prefixToCheck = prefix;
      // If prefix starts with /docs but doesn't already have a framework segment, add it
      if (prefixToCheck.startsWith("/docs/") && !prefixToCheck.match(/^\/docs\/(react|svelte|vue|react-native|react-native-expo|vanilla)\//)) {
        prefixToCheck = prefixToCheck.replace("/docs/", "/docs/" + framework + "/");
      }
      if (path.startsWith(prefixToCheck)) return true;
    }

    // Recursively check if any child item matches the current path
    if (items && items.length > 0) {
      if (items.some((childItem) => hasMatchingPath(childItem, path, framework))) {
        return true;
      }
    }

    return false;
  }, [path, framework, prefix, href, items]);

  // Determine if section should be open: path matches OR section was previously opened
  const isOpen = useMemo(() => {
    if (pathMatches) return true;

    // If section was previously opened (by user or path match), keep it open
    if (openedSections.has(sectionId)) return true;

    // If explicitly set to start closed and no path matches, don't open
    if (startClosed) return false;

    // Default: open (for sections that don't have startClosed)
    return true;
  }, [pathMatches, sectionId, startClosed]);

  // Track when section is opened by path match
  useEffect(() => {
    if (pathMatches) {
      openedSections.add(sectionId);
    }
  }, [pathMatches, sectionId]);

  // Track manual toggles to update opened sections set
  const detailsRef = useRef<HTMLDetailsElement>(null);

  const handleToggle = (e: React.SyntheticEvent<HTMLDetailsElement>) => {
    const details = e.currentTarget;

    if (details.open) {
      // Section was opened - add to opened set
      openedSections.add(sectionId);
    } else {
      // Section was closed - only remove from set if path doesn't match
      // (if path matches, we want it to reopen automatically)
      if (!pathMatches) {
        openedSections.delete(sectionId);
      }
    }
  };
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
        ref={detailsRef}
        className="group not-first:mt-4"
        open={isOpen}
        onToggle={handleToggle}
      >
        <summary className="list-none">
          <div className="flex items-center gap-2 justify-between font-medium text-stone-900 py-1 dark:text-white mb-1 not-first:mt-4">
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
