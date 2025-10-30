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
import { useEffect, useState, useCallback } from "react";

import {
  TAB_CHANGE_EVENT,
  isFrameworkChange,
  type TabChangeEventDetail,
} from "@garden-co/design-system/src/types/tabbed-code-group";

export function FrameworkSelect({
  onSelect,
  size = "md",
  className,
}: {
  onSelect?: (framework: Framework) => void;
  size?: "sm" | "md";
  routerPush?: boolean;
  className?: string;
}) {
  const defaultFramework = useFramework();
  const [selectedFramework, setSelectedFramework] =
    useState<Framework>(defaultFramework);
  const [initialized, setInitialized] = useState(false);

  const selectFramework = useCallback((newFramework: Framework, shouldNavigate = true) => {
    setSelectedFramework(newFramework);
    onSelect && onSelect(newFramework);
    localStorage.setItem("_tcgpref_framework", newFramework);

    // Dispatch event to notify other components (including useFramework)
    // The useFramework hook will handle the actual navigation
    window.dispatchEvent(
      new CustomEvent(TAB_CHANGE_EVENT, {
        detail: {
          key: "framework",
          value: newFramework,
        },
      }),
    );
  }, [onSelect]);

  const handleTabChange = useCallback((event: CustomEvent<TabChangeEventDetail>) => {
    if (isFrameworkChange(event.detail)) {
      selectFramework(event.detail.value as Framework, false);
    }
  }, [selectFramework]);

  useEffect(() => {
    window.addEventListener(TAB_CHANGE_EVENT, handleTabChange as EventListener);
    return () => {
      window.removeEventListener(TAB_CHANGE_EVENT, handleTabChange as EventListener);
    };
  }, [handleTabChange]);

  useEffect(() => {
    if (!initialized) {
      setSelectedFramework(defaultFramework);
      setInitialized(true);
    }
  }, [defaultFramework, initialized]);

  return (
    <Dropdown>
      <DropdownButton
        className={clsx(
          "w-full justify-between overflow-hidden text-nowrap",
          size === "sm" && "text-sm",
          className,
        )}
        as={Button}
        variant="outline"
        intent="default"
      >
        <span className="max-w-full overflow-hidden text-ellipsis text-nowrap">
          {frameworkNames[selectedFramework].label}
        </span>
        <Icon name="chevronDown" size="sm" />
      </DropdownButton>
      <DropdownMenu className="z-50 w-[--button-width]" anchor="bottom start">
        {Object.entries(frameworkNames).map(([key, framework]) => (
          <DropdownItem
            className={clsx(
              "items-baseline",
              size === "sm" && "text-nowrap text-xs",
              selectedFramework === key && "text-primary dark:text-primary",
            )}
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
