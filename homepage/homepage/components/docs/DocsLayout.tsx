"use client";

import { TableOfContents } from "@/components/docs/TableOfContents";
import { JazzMobileNav } from "@/components/nav";
import type { IconName } from "@garden-co/design-system/src/components/atoms/Icon";
import { NavSection } from "@garden-co/design-system/src/components/organisms/Nav";
import { TocEntry } from "@stefanprobst/rehype-extract-toc";

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
  const tableOfContentsItems =  tocItems ?? [];

  const navSections: NavSection[] = [
    {
      name: navName || "Docs",
      content: nav,
      icon: navIcon || "docs",
    },
    {
      name: "Outline",
      content: tocItems?.length && (
        <TableOfContents className="text-sm" items={tableOfContentsItems} />
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
            className="pl-3 py-8 shrink-0 text-sm sticky align-start top-[61px] w-[16rem] h-[calc(100vh-61px)] overflow-y-auto hidden lg:block"
            items={tableOfContentsItems}
          />
        </>
      )}

      <JazzMobileNav sections={navSections} />
    </>
  );
}
