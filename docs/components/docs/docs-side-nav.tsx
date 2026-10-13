"use client";

import { usePathname } from "next/navigation";
import { SideNav, SideNavItem, SideNavSection } from "@astryxdesign/core/SideNav";

export type NavEntry = { label: string; url?: string; children?: NavEntry[] };
export type NavSection = { title: string; entries: NavEntry[] };

function contains(entry: NavEntry, pathname: string): boolean {
  return entry.url === pathname || (entry.children ?? []).some((c) => contains(c, pathname));
}

function Entry({ entry, pathname }: { entry: NavEntry; pathname: string }) {
  const children = entry.children ?? [];
  if (children.length === 0) {
    return <SideNavItem label={entry.label} href={entry.url} isSelected={entry.url === pathname} />;
  }
  return (
    <SideNavItem
      label={entry.label}
      href={entry.url}
      isSelected={entry.url === pathname}
      collapsible={{ defaultIsCollapsed: !contains(entry, pathname) }}
    >
      {children.map((child) => (
        <Entry key={child.url ?? child.label} entry={child} pathname={pathname} />
      ))}
    </SideNavItem>
  );
}

/** The docs page tree as an Astryx `SideNav`; separators become sections. */
export function DocsSideNav({ sections }: { sections: NavSection[] }) {
  const pathname = usePathname();
  return (
    <SideNav>
      {sections.map((section, index) => (
        <SideNavSection
          key={section.title}
          title={section.title}
          isHeaderHidden={index === 0 && section.title === "Overview"}
        >
          {section.entries.map((entry) => (
            <Entry key={entry.url ?? entry.label} entry={entry} pathname={pathname} />
          ))}
        </SideNavSection>
      ))}
    </SideNav>
  );
}
