"use client";

import { Framework } from "@/lib/framework";
import { useFramework } from "@/lib/use-framework";
import { Button } from "gcmp-design-system/src/app/components/atoms/Button";
import { Icon } from "gcmp-design-system/src/app/components/atoms/Icon";
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
  [Framework.ReactNativeExpo]: {
    label: "React Native Expo",
    experimental: false,
  },
  [Framework.Vanilla]: {
    label: "VanillaJS",
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

export function FrameworkSelect() {
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
        className="w-full justify-between"
        as={Button}
        variant="secondary"
      >
        {frameworks[selectedFramework].label}
        <Icon
          name="chevronDown"
          size="sm"
          className="text-stone-400 dark:text-stone-600"
        />
      </DropdownButton>
      <DropdownMenu className="w-[--button-width] z-50" anchor="bottom start">
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
