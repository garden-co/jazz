import { SideNavHeader } from "@/components/SideNavHeader";
import { SideNavItem } from "@/components/SideNavItem";
import { Framework } from "@/content/framework";
import { useFramework } from "@/lib/use-framework";
import { clsx } from "clsx";
import { Icon } from "gcmp-design-system/src/app/components/atoms/Icon";
import { usePathname } from "next/navigation";
import React from "react";

interface SideNavItem {
  name: string;
  href?: string;
  done?:
    | number
    | {
        [key in Framework]: number;
      };
  items?: SideNavItem[];
  collapse?: boolean;
  prefix?: string;
}
export function SideNav({
  items,
  children,
  footer,
  className,
}: {
  items: SideNavItem[];
  className?: string;
  children?: React.ReactNode;
  footer?: React.ReactNode;
}) {
  return (
    <div
      className={clsx(
        className,
        "text-sm h-full pt-3 md:pt-8 flex flex-col gap-4 px-2",
      )}
    >
      {children}

      <div className="flex-1 relative overflow-y-auto px-2 -mx-2">
        {items.map((item) => (
          <SideNavSection item={item} key={item.name} />
        ))}

        {footer}

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
    </div>
  );
}

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

export function SideNavSectionList({ items }: { items?: SideNavItem[] }) {
  return (
    !!items?.length && (
      <ul>
        {items.map(({ name, href, items, done }) => (
          <li key={name}>
            <SideNavItem href={href}>
              <span
                className={
                  done === 0 ? "text-stone-400 dark:text-stone-600" : ""
                }
              >
                {name}
              </span>
            </SideNavItem>
          </li>
        ))}
      </ul>
    )
  );
}
