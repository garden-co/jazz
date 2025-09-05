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
      className="group xl:min-w-48 "
      intent="muted"
      variant="outline"
      onClick={() => setOpen((open) => !open)}
    >
      <Icon name="search" size="xs" intent="default" />
      <span className="font-normal flex-1 text-left text-sm xl:not-sr-only">
        Search<span className="md:hidden xl:inline"> docs</span>
      </span>
      <kbd className="hidden gap-0.5 xl:text-sm lg:inline-flex">
        <kbd className="font-sans">{isMac ? "⌘" : "Ctrl"}</kbd>
        <kbd className="font-sans">K</kbd>
      </kbd>
    </Button>
  );
}
