"use client";

import { Framework, frameworkNames } from "@/content/framework";
import { useFramework } from "@/lib/use-framework";
import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import {
  Dropdown,
  DropdownButton,
  DropdownItem,
  DropdownMenu,
} from "@garden-co/design-system/src/components/organisms/Dropdown";
import { usePathname, useRouter } from "next/navigation";
import { useState } from "react";

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
        {frameworkNames[selectedFramework].label}
        <Icon name="chevronDown" size="sm" className="text-muted" />
      </DropdownButton>
      <DropdownMenu className="w-[--button-width] z-50" anchor="bottom start">
        {Object.entries(frameworkNames).map(([key, framework]) => (
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
