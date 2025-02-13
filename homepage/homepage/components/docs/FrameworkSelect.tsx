"use client";

import { Framework } from "@/lib/framework";
import { useFramework } from "@/lib/use-framework";
import { Button } from "gcmp-design-system/src/app/components/atoms/Button";
import {
  Dropdown,
  DropdownButton,
  DropdownItem,
  DropdownMenu,
} from "gcmp-design-system/src/app/components/organisms/Dropdown";
import { usePathname, useRouter } from "next/navigation";
import { useState } from "react";

const frameworks: Record<
  Framework,
  {
    label: string;
    experimental: boolean;
  }
> = {
  [Framework.React]: {
    label: "React",
    experimental: false,
  },
  [Framework.ReactNative]: {
    label: "React Native",
    experimental: false,
  },
  [Framework.Svelte]: {
    label: "Svelte",
    experimental: true,
  },
  [Framework.Vue]: {
    label: "Vue",
    experimental: true,
  },
};

export function FrameworkSelect({ className }: { className?: string }) {
  const router = useRouter();
  const defaultFramework = useFramework();
  const [selectedFramework, setSelectedFramework] =
    useState<Framework>(defaultFramework);

  const path = usePathname();

  const selectFramework = (newFramework: Framework) => {
    setSelectedFramework(newFramework);
    router.push(path.replace(defaultFramework, newFramework));
  };

  return (
    <Dropdown>
      <DropdownButton
        icon="chevronDown"
        className="flex-row-reverse w-full justify-between"
        as={Button}
        variant="secondary"
      >
        {frameworks[selectedFramework].label}
      </DropdownButton>
      <DropdownMenu anchor="bottom start" className="z-50">
        {Object.entries(frameworks).map(([key, framework]) => (
          <DropdownItem
            className="items-baseline"
            key={key}
            onClick={() => selectFramework(key as Framework)}
          >
            {framework.label}
            {framework.experimental && (
              <span className="ml-1 text-xs text-stone-500">
                (experimental)
              </span>
            )}
          </DropdownItem>
        ))}
      </DropdownMenu>
    </Dropdown>
  );
}
