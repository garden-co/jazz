"use client";

import { usePagefindSearch } from "@/components/pagefind";
import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { useEffect, useState } from "react";

export function QuickSearch() {
  const [isMac, setIsMac] = useState(true);
  const { setOpen } = usePagefindSearch();

  useEffect(() => {
    setIsMac(navigator.userAgent.includes("Mac"));
  }, []);

  return (
    <Button
      className="group xl:min-w-48  text-stone-600 md:mr-5"
      variant="secondary"
      onClick={() => setOpen((open) => !open)}
    >
      <Icon name="search" size="xs" className=" text-stone-600" />
      <span className="font-normal flex-1 text-left text-sm text-stone-600 group-hover:text-blue xl:not-sr-only">
        Search docs
      </span>
      <kbd className="hidden gap-0.5 xl:text-sm text-stone-600 lg:inline-flex">
        <kbd className="font-sans">{isMac ? "⌘" : "Ctrl"}</kbd>
        <kbd className="font-sans">K</kbd>
      </kbd>
    </Button>
  );
}
