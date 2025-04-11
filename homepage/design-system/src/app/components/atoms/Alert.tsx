import clsx from "clsx";
import type { ReactNode } from "react";
import { Icon } from "./Icon";

interface Props {
  children: ReactNode;
  variant?: "warning" | "info" | "danger";
  title: string;
  className?: string;
}

export function Alert({ children, variant = "info", title, className }: Props) {
  return (
    <div
      className={clsx(
        "border-l-4 p-4 overflow-hidden relative flex gap-3 rounded ",
        {
          "border-yellow-500 bg-yellow-50 dark:bg-stone-925":
            variant === "warning",
          "border-blue-500 bg-blue-50 dark:bg-stone-925": variant === "info",
          "border-red-500 bg-red-50 dark:bg-stone-925": variant === "danger",
        },
        className,
      )}
    >
      <Icon
        name={variant}
        size="7xl"
        className="absolute z-0 right-0 opacity-5 top-0 rotate-12 pointer-events-none"
      />

      <Icon
        name={variant}
        size="sm"
        className={clsx("shrink-0", {
          "text-red-500": variant === "danger",
          "text-yellow-500": variant === "warning",
          "text-blue-500": variant === "info",
        })}
      />

      <div>
        <p
          className={clsx(
            "not-prose text-sm font-semibold text-stone-900 dark:text-white flex items-center gap-1 mb-2",
          )}
        >
          {title}
        </p>
        <div
          className={clsx(
            "prose prose-sm dark:prose-invert dark:prose-code:bg-stone-900",
            {
              "prose-code:bg-red-100": variant === "danger",
              "prose-code:bg-yellow-100": variant === "warning",
              "prose-code:bg-blue-100": variant === "info",
            },
          )}
        >
          {children}
        </div>
      </div>
    </div>
  );
}
