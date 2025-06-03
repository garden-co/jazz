"use client";

import { usePagefindSearch } from "@/components/pagefind";
import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";

export function QuickSearch() {
  const isMac = navigator.userAgent.includes("Mac");
  const { setOpen } = usePagefindSearch();

  return (
    <Button
      className="group xl:min-w-48  text-stone-600 mr-5"
      variant="secondary"
      onClick={() => setOpen((open) => !open)}
    >
      <Icon name="search" size="xs" className=" text-stone-600" />
      <span className="sr-only font-normal flex-1 text-left text-sm text-stone-600 group-hover:text-blue xl:not-sr-only">
        Search docs
      </span>
      <kbd className="inline-flex gap-0.5 xl:text-sm text-stone-600">
        <kbd className="font-sans">{isMac ? "⌘" : "Ctrl"}</kbd>
        <kbd className="font-sans">K</kbd>
      </kbd>
    </Button>
  );
}
