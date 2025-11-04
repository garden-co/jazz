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
    pathRef.current = path;
  }, [path]);

  useEffect(() => {
    if (!initialized) {
      setSelectedFramework(defaultFramework);
      setInitialized(true);
    }
  }, [defaultFramework, initialized]);


  useEffect(() => {
    window.addEventListener(
      TAB_CHANGE_EVENT,
      handleTabChange,
    );
    return () => {
      window.removeEventListener(TAB_CHANGE_EVENT, handleTabChange);
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

  const selectFramework = (newFramework: Framework, shouldNavigate = true) => {
    setSelectedFramework(newFramework);
    onSelect && onSelect(newFramework);
    localStorage.setItem("_tcgpref_framework", newFramework);
    if (!shouldNavigate) return;
    const newPath = path.split("/").toSpliced(2, 1, newFramework).join("/") + window.location.hash;
    routerPush && router.replace(newPath, { scroll: false });
  };

  const handleTabChange = (event: CustomEvent<TabChangeEventDetail>) => {
    if (isFrameworkChange(event.detail)) {
      selectFramework(event.detail.value as Framework, false);
    }
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
