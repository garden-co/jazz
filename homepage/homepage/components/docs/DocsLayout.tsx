"use client";

import { HelpLinks } from "@/components/docs/HelpLinks";
import { TableOfContents } from "@/components/docs/TableOfContents";
import { JazzMobileNav } from "@/components/nav";
import { useFramework } from "@/lib/use-framework";
import type { IconName } from "@garden-co/design-system/src/components/atoms/Icon";
import { Separator } from "@garden-co/design-system/src/components/atoms/Separator";
import { NavSection } from "@garden-co/design-system/src/components/organisms/Nav";
import { TocEntry } from "@stefanprobst/rehype-extract-toc";

export default function DocsLayout({
  children,
  nav,
  navName,
  navIcon,
  tocItems,
  pagefindIgnore,
  pagefindLowPriority,
}: {
  children: React.ReactNode;
  nav?: React.ReactNode;
  navName?: string;
  navIcon?: IconName;
  tocItems?: TocEntry[];
  pagefindIgnore?: boolean;
  pagefindLowPriority?: boolean;
}) {
  const tableOfContentsItems = tocItems ?? [];

  const itemsWithoutH1 =
    tableOfContentsItems.length > 0 ? tableOfContentsItems[0].children : [];

  const navSections: NavSection[] = [
    {
      name: navName || "Docs",
      content: nav,
      icon: navIcon || "docs",
    },
  ];

  if (itemsWithoutH1?.length) {
    navSections.push({
      name: "Outline",
      content: (
        <TableOfContents className="text-sm" items={tableOfContentsItems} />
      ),
      icon: "tableOfContents",
    });
  }

  const framework = useFramework();

  let pagefindProps: {
    [key: `data-pagefind-${string}`]: string | boolean | number;
  } = pagefindIgnore
    ? { "data-pagefind-ignore": true }
    : {
        "data-pagefind-body": true,
        "data-pagefind-meta": `framework: ${framework}`,
      };

  if (pagefindLowPriority) {
    pagefindProps["data-pagefind-weight"] = 0.5;
  }

  return (
    <>
      <div className="w-full" {...pagefindProps}>
        {children}
      </div>

      <div className="pl-3 py-8 shrink-0 text-sm sticky align-start top-[61px] w-[16rem] h-[calc(100vh-61px)] overflow-y-auto hidden lg:block">
        {itemsWithoutH1?.length ? (
          <>
            <TableOfContents items={tableOfContentsItems} />
            <Separator className="my-5" />
          </>
        ) : null}

        <HelpLinks />
      </div>

      <JazzMobileNav sections={navSections} />
    </>
  );
}
