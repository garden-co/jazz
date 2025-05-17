import {
  docNavigationItems,
  flatItemsWithNavLinks,
} from "@/content/docs/docNavigationItems";
import Link from "next/link";

interface PreviousNextLinksProps {
  slug?: string[];
  framework: string;
}

export function PreviousNextLinks({ slug, framework }: PreviousNextLinksProps) {
  const currentItem = flatItemsWithNavLinks.find((item) => {
    const itemPath = item.href.replace("/docs", `/docs/${framework}`);
    const currentPath = slug
      ? `/docs/${framework}/${slug.join("/")}`
      : `/docs/${framework}`;
    return currentPath === itemPath;
  });

  if (!currentItem?.next && !currentItem?.previous) {
    return null;
  }

  return (
    <div className="flex justify-evenly gap-3 not-prose">
      {currentItem.previous && (
        <Link
          href={currentItem.previous.href.replace(
            "/docs",
            `/docs/${framework}`,
          )}
          className="group py-5 xl:pr-12"
        >
          <span className="text-xs block md:text-sm md:mb-1">Previous</span>
          <div className="text-highlight font-medium inline-flex gap-2 items-center group-hover:text-blue">
            {currentItem.previous.name}
          </div>
        </Link>
      )}
      {currentItem.next && (
        <Link
          href={currentItem.next.href.replace("/docs", `/docs/${framework}`)}
          className="group text-right ml-auto py-5 xl:pl-12"
        >
          <span className="text-xs block md:text-sm md:mb-1">Next</span>
          <div className="text-highlight font-medium inline-flex gap-2 items-center group-hover:text-blue">
            {currentItem.next.name}
          </div>
        </Link>
      )}
    </div>
  );
}
