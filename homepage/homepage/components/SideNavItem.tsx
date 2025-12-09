"use client";

import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { clsx } from "clsx";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { ReactNode, useEffect, useRef } from "react";

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

  // Scroll into view when this item becomes active
  useEffect(() => {
    if (isActive && itemRef.current) {
      // Wait for parent details elements to expand and DOM to update
      // Use requestAnimationFrame twice to ensure layout is complete
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          itemRef.current?.scrollIntoView({
            behavior: "smooth",
            block: "center",
          });
        });
      });
    }
  }, [isActive, path]);

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

  return <p ref={itemRef as React.RefObject<HTMLParagraphElement>} className={classes}>{children}</p>;
}
