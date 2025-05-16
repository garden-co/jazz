"use client";

import { docNavigationItems } from "@/content/docs/docNavigationItems";
import { useFramework } from "@/lib/use-framework";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import Link from "next/link";
import { usePathname } from "next/navigation";

export function PreviousNextLinks() {
  const path = usePathname();
  const framework = useFramework();

  // Find the current navigation item
  const currentItem = docNavigationItems
    .flatMap((section) => section.items)
    .find((item) => {
      const itemPath = item.href.replace("/docs", `/docs/${framework}`);
      return path === itemPath;
    });

  if (!currentItem?.next && !currentItem?.back) {
    return null;
  }

  return (
    <div className="flex justify-between gap-4 not-prose">
      {currentItem.back && (
        <Link
          href={currentItem.back.href.replace("/docs", `/docs/${framework}`)}
          className="group py-5 pr-12"
        >
          <span className="text-sm block mb-1">Previous</span>
          <div className="text-highlight font-medium inline-flex gap-2 items-center text-lg group-hover:text-blue">
            {currentItem.back.name}
          </div>
        </Link>
      )}
      {currentItem.next && (
        <Link
          href={currentItem.next.href.replace("/docs", `/docs/${framework}`)}
          className="group text-right ml-auto py-5 pl-12"
        >
          <span className="text-sm block mb-1">Next</span>
          <div className="text-highlight font-medium inline-flex gap-2 items-center text-lg group-hover:text-blue">
            {currentItem.next.name}
          </div>
        </Link>
      )}
    </div>
  );
}
