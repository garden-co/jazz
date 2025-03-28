"use client";

import { TableOfContents } from "@/components/docs/TableOfContents";
import { JazzNav } from "@/components/nav";
import { useTocItems } from "@/lib/TocContext";
import { clsx } from "clsx";
import type { IconName } from "gcmp-design-system/src/app/components/atoms/Icon";
import type { NavSection } from "gcmp-design-system/src/app/components/organisms/Nav";

export default function DocsLayout({
  children,
  nav,
  navName,
  navIcon,
}: {
  children: React.ReactNode;
  nav?: React.ReactNode;
  navName?: string;
  navIcon?: IconName;
}) {
  const { tocItems } = useTocItems();

  const navSections: NavSection[] = [
    {
      name: navName || "Docs",
      content: nav,
      icon: navIcon || "docs",
    },
    {
      name: "Outline",
      content: tocItems?.length && (
        <TableOfContents className="text-sm" items={tocItems} />
      ),
      icon: "tableOfContents",
    },
  ];

  return (
    <div className="flex-1 w-full">
      <JazzNav sections={navSections} />
      <main>
        <div className="container relative md:grid md:grid-cols-12 md:gap-12">
          <div
            className={clsx(
              "pr-3 md:col-span-4 lg:col-span-3",
              "sticky align-start top-[61px] h-[calc(100vh-61px)] overflow-y-auto",
              "hidden md:block",
            )}
          >
            {nav}
          </div>
          <div className={clsx("md:col-span-8 lg:col-span-9 flex gap-12")}>
            {children}
            {tocItems?.length && (
              <>
                <TableOfContents
                  className="pl-3 py-6 shrink-0 text-sm sticky align-start top-[61px] w-[16rem] h-[calc(100vh-61px)] overflow-y-auto hidden lg:block"
                  items={tocItems}
                />
              </>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}
