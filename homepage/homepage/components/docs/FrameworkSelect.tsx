"use client";
import { Framework, frameworkNames } from "@/content/framework";
import { useFramework } from "@/lib/use-framework";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import clsx from "clsx";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useState, useRef } from "react";

import {
  TAB_CHANGE_EVENT,
  isFrameworkChange,
  type TabChangeEventDetail,
} from "@garden-co/design-system/src/types/tabbed-code-group";

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
  const [initialized, setInitialized] = useState(false);

  const path = usePathname();
  const pathRef = useRef(path);

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
    window.addEventListener(TAB_CHANGE_EVENT, handleTabChange);
    return () => {
      window.removeEventListener(TAB_CHANGE_EVENT, handleTabChange);
    };
  }, []);

  useEffect(() => {
    const timer = setTimeout(() => {
      window.dispatchEvent(
        new CustomEvent(TAB_CHANGE_EVENT, {
          detail: {
            key: "framework",
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
    const newPath =
      path.split("/").toSpliced(2, 1, newFramework).join("/") +
      window.location.hash;
    routerPush && router.replace(newPath, { scroll: true });
  };

  const handleTabChange = (event: CustomEvent<TabChangeEventDetail>) => {
    if (isFrameworkChange(event.detail)) {
      selectFramework(event.detail.value as Framework, false);
    }
  };

  return (
    <Select
      value={selectedFramework}
      onValueChange={(value) => selectFramework(value as Framework)}
    >
      <SelectTrigger
        className={clsx("w-full", size === "sm" && "h-9 text-sm", className)}
      >
        <SelectValue>{frameworkNames[selectedFramework].label}</SelectValue>
      </SelectTrigger>
      <SelectContent>
        {Object.entries(frameworkNames).map(([key, framework]) => (
          <SelectItem
            key={key}
            value={key}
            className={clsx(
              size === "sm" && "text-xs",
              selectedFramework === key && "text-primary dark:text-primary",
            )}
          >
            {framework.label}
            {framework.experimental && (
              <span className="ml-1 text-xs text-stone-500">
                (experimental)
              </span>
            )}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
