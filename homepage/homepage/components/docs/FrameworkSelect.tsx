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
import { useRef, useEffect } from "react";

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
  const { framework, setFramework } = useFramework();
  const onSelectRef = useRef(onSelect);

  useEffect(() => {
    onSelectRef.current = onSelect;
  }, [onSelect]);

  const handleSelect = (newFramework: Framework) => {
    setFramework(newFramework);
    onSelectRef.current?.(newFramework);
  };

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
        <span className="w-full overflow-hidden text-ellipsis text-nowrap text-left">
          {frameworkNames[framework].label}
        </span>
        <Icon name="chevronDown" size="sm" />
      </DropdownButton>
      <DropdownMenu className="w-(--button-width) z-50" anchor="bottom start">
        {Object.entries(frameworkNames).map(([key, frameworkInfo]) => (
          <DropdownItem
            className={clsx(
              "items-baseline",
              size === "sm" && "text-nowrap text-xs",
              framework === key && "text-primary dark:text-primary",
            )}
            key={key}
            onClick={() => handleSelect(key as Framework)}
          >
            {frameworkInfo.label}
            {frameworkInfo.experimental && (
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
