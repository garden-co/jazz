import { clsx } from "clsx";
import { useId } from "react";
import { Icon } from "./icon.js";

export function Select(
  props: React.SelectHTMLAttributes<HTMLSelectElement> & { label: string },
) {
  const { label, id: customId, className } = props;
  const generatedId = useId();
  const id = customId || generatedId;

  const containerClassName = clsx("grid gap-1", className);

  const selectClassName = clsx(
    "w-full rounded-md border pl-3.5 py-2 pr-8 shadow-sm",
    "font-medium text-stone-900",
    "dark:text-white dark:bg-stone-925",
    "appearance-none",
    "truncate",
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
          className="absolute right-[0.5em] text-stone-400 dark:text-stone-600"
          size="sm"
        />
      </div>
    </div>
  );
}
