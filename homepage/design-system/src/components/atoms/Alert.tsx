import clsx from "clsx";
import type { ReactNode } from "react";
import { Icon } from "./Icon";

interface Props {
  children: ReactNode;
  variant?: "warning" | "info";
  title: string;
  className?: string;
}

export function Alert({
  children,
  variant = "warning",
  title,
  className,
}: Props) {
  return (
    <div
      className={clsx(
        "border-l-4 p-4 pl-6 dark:bg-red-200/5 overflow-hidden relative rounded",
        {
          "border-yellow-400 bg-yellow-50 dark:border-yellow-500 dark:bg-yellow-200/5":
            variant === "warning",
          "border-blue-400 bg-blue-50 dark:border-blue-500 dark:bg-blue-200/5":
            variant === "info",
        },
        className,
      )}
    >
      <span
        className={clsx(
          "text-sm font-bold flex items-center gap-1",
          variant === "warning" && "text-yellow-700 dark:text-yellow-400",
          variant === "info" && "text-blue-700 dark:text-blue-400",
        )}
      >
        <Icon
          name={variant}
          size="7xl"
          className="absolute -z-10 right-0 opacity-5 top-0 rotate-12 pointer-events-none"
        />
        <Icon name={variant} size="xs" />
        {title}
      </span>
      <span className={clsx("text-sm")}>{children}</span>
    </div>
  );
}
