import clsx from "clsx";
import { Switch as RadixSwitch } from "radix-ui";

export function Switch({
  id,
  size = "sm",
  checked,
  onChange,
  label,
}: {
  id: string;
  size?: "sm" | "md";
  checked: boolean;
  onChange: (checked: boolean) => void;
  label?: string;
}) {
  return (
    <div className="flex items-center gap-2 w-content">
      <label
        className={clsx("text-xs text-gray-500", labelSizeClass[size])}
        htmlFor={id}
      >
        {label}
      </label>
      <RadixSwitch.Root
        id={id}
        className={clsx(
          "min-w-10 h-6 rounded-full relative",
          size === "sm" && "min-w-6 h-3.5",
          checked ? "bg-primary" : "bg-stone-200",
        )}
        checked={checked}
        onCheckedChange={onChange}
      >
        <RadixSwitch.Thumb
          className={clsx(
            "block w-4 h-4 bg-white rounded-full transition-transform duration-300 translate-x-0 ml-[0.06rem]",
            size === "sm" && "w-3 h-3",
            checked && "translate-x-[0.6rem]",
          )}
        />
      </RadixSwitch.Root>
    </div>
  );
}

const labelSizeClass = {
  sm: "text-xs",
  md: "text-sm",
};
