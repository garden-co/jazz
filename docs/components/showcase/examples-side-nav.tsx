"use client";

import { useEffect, useState } from "react";
import { SideNav, SideNavItem, SideNavSection } from "@astryxdesign/core/SideNav";
import { heroExamples, moreBenchmarkSections } from "@/lib/showcase/catalogue";

const sections = [
  { title: "Examples", items: heroExamples.map((e) => ({ id: e.id, label: e.title })) },
  {
    title: "More benchmarks",
    items: moreBenchmarkSections.map((section) => ({ id: section.id, label: section.title })),
  },
];
const ids = sections.flatMap((s) => s.items.map((i) => i.id));

/** The section nearest the top of the viewport, below the sticky top nav. */
function useCurrentSection() {
  const [current, setCurrent] = useState<string | null>(null);
  useEffect(() => {
    const update = () => {
      let found: string | null = null;
      for (const id of ids) {
        const top = document.getElementById(id)?.getBoundingClientRect().top;
        if (top !== undefined && top <= 120) found = id;
      }
      setCurrent(found);
    };
    update();
    addEventListener("scroll", update, { passive: true });
    addEventListener("hashchange", update);
    return () => {
      removeEventListener("scroll", update);
      removeEventListener("hashchange", update);
    };
  }, []);
  return current;
}

/** Jumps between the sections of the one continuously scrolling examples page. */
export function ExamplesSideNav() {
  const current = useCurrentSection();
  return (
    <SideNav>
      {sections.map((section) => (
        <SideNavSection key={section.title} title={section.title}>
          {section.items.map((item) => (
            <SideNavItem
              key={item.id}
              label={item.label}
              href={`#${item.id}`}
              isSelected={current === item.id}
            />
          ))}
        </SideNavSection>
      ))}
    </SideNav>
  );
}
