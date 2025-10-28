"use client";

import { clsx } from "clsx";
import { useId } from "react";
import { Icon } from "../atoms/Icon";

export function Select(
  props: React.SelectHTMLAttributes<HTMLSelectElement> & { label: string },
) {
  const { label, id: customId, className, size } = props;
  const generatedId = useId();
  const id = customId || generatedId;

  const containerClassName = clsx("grid gap-1", className);

  const selectClassName = clsx(
    "w-full rounded-md border shadow-sm px-2 py-1.5 text-sm",
    "font-medium text-stone-900",
    "dark:text-white dark:bg-stone-925",
    "appearance-none",
  );

  return (
    <div className={containerClassName}>
      <label htmlFor={id} className="text-stone-600 dark:text-stone-300">
        {label}
      </label>

      <div className="relative flex items-center">
        <select {...props} id={id} className={selectClassName}>
          {props.children}
        </select>

        <Icon
          name="chevronDown"
          className="absolute right-[0.5em] text-muted"
          size="xs"
        />
      </div>
    </div>
  );
}
