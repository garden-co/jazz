import {
  flatItemsWithNavLinks,
} from "@/content/docs/docNavigationItems";
import { DocNavigationItem } from "@/content/docs/docNavigationItemsTypes";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import Link from "next/link";
interface PreviousNextLinksProps {
  slug?: string[];
  framework: string;
}

export function PreviousNextLinks({ slug, framework }: PreviousNextLinksProps) {
  const currentItem = flatItemsWithNavLinks.find((item: DocNavigationItem) => {
    if (!item.href) return false;
    const itemPath = item.href.replace("/docs", `/docs/${framework}`);
    const currentPath = slug && slug.length > 0
      ? `/docs/${framework}/${slug.join("/")}`
      : `/docs/${framework}`;
    return currentPath === itemPath;
  }) as DocNavigationItem;

  if (
    currentItem?.excludeFromNavigation ||
    (!currentItem?.next && !currentItem?.previous)
  ) {
    return null;
  }

  return (
    <>
      <div className="flex justify-evenly gap-3 not-prose">
        {currentItem.previous && currentItem.previous.href && (<Link
          href={currentItem.previous.href.replace(
            "/docs",
            `/docs/${framework}`,
          )}
          className="group py-5 xl:pr-12"
        >
          <span className="hidden text-sm mb-1 sm:block">Previous</span>
          <div className="text-sm text-highlight font-medium inline-flex gap-1 items-center group-hover:text-blue sm:text-base">
            <Icon name="previous" size="xs" className="sm:hidden" />
            {currentItem.previous.name}
          </div>
        </Link>
        )}
        {currentItem.next && currentItem.next.href && (<Link
          href={currentItem.next.href.replace("/docs", `/docs/${framework}`)}
          className="group text-right ml-auto py-5 xl:pl-12"
        >
          <span className="hidden text-sm mb-1 sm:block">Next</span>
          <div className="text-sm text-highlight font-medium inline-flex gap-1 items-center group-hover:text-blue sm:text-base">
            {currentItem.next.name}
            <Icon name="next" size="xs" className="sm:hidden" />
          </div>
        </Link>
        )}
      </div>
    </>
  );
}
