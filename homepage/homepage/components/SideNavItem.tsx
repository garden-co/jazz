"use client";

import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { clsx } from "clsx";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { ReactNode, useEffect, useRef } from "react";
import { useSideNav } from "./SideNavContext";

export function SideNavItem({
  href,
  children,
  className = "",
}: {
  href?: string;
  children: ReactNode;
  className?: string;
}) {
  const classes = clsx(
    className,
    "py-1 px-2 group rounded-md flex items-center transition-colors relative",
  );
  const path = usePathname();
  const itemRef = useRef<HTMLElement>(null);
  const isActive = href && path === href;
  const { shouldScrollToActive, scrollContainerRef } = useSideNav();

  // Scroll sidebar container to show active item (only on real navigation, not framework change)
  useEffect(() => {
    if (
      isActive &&
      shouldScrollToActive &&
      itemRef.current &&
      scrollContainerRef?.current
    ) {
      // Wait for parent details elements to expand and DOM to update
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          const item = itemRef.current;
          const container = scrollContainerRef.current;
          if (!item || !container) return;

          // Calculate position relative to container
          const containerRect = container.getBoundingClientRect();
          const itemRect = item.getBoundingClientRect();

          // Calculate the item's position relative to the container's scroll position
          const itemTopRelativeToContainer =
            itemRect.top - containerRect.top + container.scrollTop;

          // Calculate desired scroll position to center the item
          const containerHeight = container.clientHeight;
          const itemHeight = itemRect.height;
          const desiredScrollTop =
            itemTopRelativeToContainer - containerHeight / 2 + itemHeight / 2;

          // Scroll the container smoothly (not the window)
          container.scrollTo({
            top: desiredScrollTop,
            behavior: "smooth",
          });
        });
      });
    }
  }, [isActive, shouldScrollToActive, scrollContainerRef]);

  if (href) {
    return (
      <Link
        ref={itemRef as React.RefObject<HTMLAnchorElement>}
        href={href}
        className={clsx(
          classes,
          isActive
            ? "text-stone-900 font-medium  bg-stone-200/50 dark:text-white dark:bg-stone-800/50"
            : "hover:text-stone-900 dark:hover:text-stone-200",
        )}
      >
        {children}

        {!href.startsWith("/docs") && (
          <Icon
            name="arrowRight"
            size="2xs"
            className="ml-2 text-stone-500 invisible group-hover:visible"
          ></Icon>
        )}
      </Link>
    );
  }

  return (
    <p
      ref={itemRef as React.RefObject<HTMLParagraphElement>}
      className={classes}
    >
      {children}
    </p>
  );
}
