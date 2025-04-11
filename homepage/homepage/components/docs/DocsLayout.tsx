"use client";

import { ThemeToggle } from "@/components/ThemeToggle";
import { TableOfContents } from "@/components/docs/TableOfContents";
import { JazzLogo } from "@/components/forMdx";
import { JazzNav } from "@/components/nav";
import { navigationItems } from "@/lib/navigation-items";
import { TocEntry } from "@stefanprobst/rehype-extract-toc";
import { clsx } from "clsx";
import type { IconName } from "gcmp-design-system/src/app/components/atoms/Icon";
import {
  MobileNav,
  NavSection,
} from "gcmp-design-system/src/app/components/organisms/Nav";

export default function DocsLayout({
  children,
  nav,
  navName,
  navIcon,
  tocItems,
}: {
  children: React.ReactNode;
  nav?: React.ReactNode;
  navName?: string;
  navIcon?: IconName;
  tocItems?: TocEntry[];
}) {
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
    <>
      {children}
      {!!tocItems?.length && (
        <>
          <TableOfContents
            className="pl-3 py-6 shrink-0 text-sm sticky align-start top-[61px] w-[16rem] h-[calc(100vh-61px)] overflow-y-auto hidden lg:block"
            items={tocItems}
          />
        </>
      )}

      <MobileNav
        sections={navSections}
        themeToggle={ThemeToggle}
        mainLogo={<JazzLogo className="w-20" />}
        items={navigationItems}
      />
    </>
  );
}
