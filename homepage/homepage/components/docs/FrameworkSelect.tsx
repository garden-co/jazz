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
import { useEffect, useState } from "react";

import { TAB_CHANGE_EVENT } from "@garden-co/design-system/src/components/molecules/TabbedCodeGroup";

export function FrameworkSelect({
  onSelect,
  size = "md",
  routerPush = true,
  className,
}: {
  onSelect?: (framework: Framework) => void;
  size?: "sm" | "md";
  routerPush?: boolean;
  className?: string;
}) {
  const router = useRouter();
  const defaultFramework = useFramework();
  const [selectedFramework, setSelectedFramework] =
    useState<Framework>(defaultFramework);

  const path = usePathname();

  const handleFrameworkChange = (event: CustomEvent) => {
      if (event.detail.key === 'framework') {
        const newTab = event.detail.value;
        selectFramework(event.detail.value);
      }
    };
  

  useEffect(() => {
      window.addEventListener(
        TAB_CHANGE_EVENT as any, // Yes, ugly type hack, because otherwise I need to create a new definition for window
        handleFrameworkChange,
      );
      return () => {
        window.removeEventListener(TAB_CHANGE_EVENT as any, handleFrameworkChange);
      };
    }, []);

  useEffect(() => {
    const timer = setTimeout(() => {
      window.dispatchEvent(
        new CustomEvent(TAB_CHANGE_EVENT, {
          detail: {
            key: 'framework',
            value: defaultFramework,
          },
        }),
      );
    }, 0);    
    return () => clearTimeout(timer);
  }, [defaultFramework]);

  const selectFramework = (newFramework: Framework) => {
    setSelectedFramework(newFramework);
    onSelect && onSelect(newFramework);
    localStorage.setItem("_tcgpref_framework", newFramework);
    routerPush && router.push(path.replace(defaultFramework, newFramework));
  };

  return (
    <Dropdown>
      <DropdownButton
        className={clsx("w-full justify-between overflow-hidden text-nowrap", size === "sm" && "text-sm", className)}
        as={Button}
        variant="outline"
        intent="default"
      >
        <span className="text-nowrap max-w-full overflow-hidden text-ellipsis">{frameworkNames[selectedFramework].label}</span>
        <Icon name="chevronDown" size="sm" />
      </DropdownButton>
      <DropdownMenu className="w-[--button-width] z-50" anchor="bottom start">
        {Object.entries(frameworkNames)
          .map(([key, framework]) => (
            <DropdownItem
            className={clsx("items-baseline", size === "sm" && "text-xs text-nowrap", selectedFramework === key && "text-primary dark:text-primary")}
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
