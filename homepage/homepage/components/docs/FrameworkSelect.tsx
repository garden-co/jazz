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
import clsx from "clsx";
import { usePathname, useRouter } from "next/navigation";
import { useState } from "react";

export function FrameworkSelect({
  onSelect,
  size = "md",
  routerPush = true,
  className,
}: {
  onSelect?: (framework: Framework) => void;
  size?: "sm" | "md" | "lg";
  routerPush?: boolean;
  className?: string;
}) {
  const router = useRouter();
  const defaultFramework = useFramework();
  const [selectedFramework, setSelectedFramework] =
    useState<Framework>(defaultFramework);

  const path = usePathname();

  const selectFramework = (newFramework: Framework) => {
    setSelectedFramework(newFramework);
    onSelect && onSelect(newFramework);
    routerPush && router.push(path.replace(defaultFramework, newFramework));
  };

  return (
    <Dropdown>
      <DropdownButton
        className={clsx("w-full justify-between overflow-hidden text-nowrap", size === "sm" && "text-sm", className)}
        size={size}
      >
        <span className="text-nowrap max-w-full overflow-hidden text-ellipsis">{frameworkNames[selectedFramework].label}</span>
      </DropdownButton>
      <DropdownMenu className="w-[--button-width] z-50">
        {Object.entries(frameworkNames)
          .map(([key, framework]) => (
            <DropdownItem
            className={clsx(
              "items-baseline", 
              size === "sm" && "text-xs text-nowrap"
            )}
              key={key}
              selected={selectedFramework === key}
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
