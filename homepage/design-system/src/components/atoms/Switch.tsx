import clsx from "clsx";
import { Switch as RadixSwitch } from "radix-ui";
import { Style, styleToBgMap } from "../../utils/tailwindClassesMap";

export function Switch({
  id,
  size = "md",
  intent = "primary",
  checked,
  onChange,
  label,
}: {
  id: string;
  size?: "sm" | "md";
  intent?: Style;
  checked: boolean;
  onChange: (checked: boolean) => void;
  label?: string;
}) {
  return (
    <div className="flex items-center w-content">
      <label
        className={clsx("text-gray-500 mr-2", labelSizeClass[size])}
        htmlFor={id}
      >
        {label}
      </label>
      <RadixSwitch.Root
        id={id}
        className={clsx(
          "rounded-full relative",
          size === "sm" ? "min-w-6 h-4" : "min-w-10 h-6",
          checked ? styleToBgMap[intent] : "bg-stone-200",
        )}
        checked={checked}
        onCheckedChange={onChange}
      >
        <RadixSwitch.Thumb
          className={clsx(
            "block bg-white rounded-full transition-transform duration-300 translate-x-0 ml-[0.1em]",
            size === "sm" ? "w-3 h-3" : "w-5 h-5",
            checked &&
              (size === "sm" ? "translate-x-2" : "translate-x-[1.01rem]"),
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
