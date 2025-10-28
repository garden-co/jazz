import { ComponentProps } from "react";
import { VariantProps, tv } from "tailwind-variants";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { cn } from "../lib/utils";

type LabelVariants = VariantProps<typeof label>;

interface LabelProps extends ComponentProps<"label">, LabelVariants {}

export function Label({ size, isHiddenVisually, ...props }: LabelProps) {
  return (
    <label
      className={cn(
        label({
          isHiddenVisually: isHiddenVisually,
          size: size,
        }),
        props.className,
      )}
      {...props}
    />
  );
}

const label = tv({
  base: "block text-sm font-medium text-stone-900 dark:text-white flex items-center",
  variants: {
    isHiddenVisually: {
      true: "sr-only",
    },
    size: {
      sm: "text-sm",
      md: "text-base",
      lg: "text-lg",
    },
  },
  defaultVariants: {
    size: "md",
    isHiddenVisually: false,
  },
});
