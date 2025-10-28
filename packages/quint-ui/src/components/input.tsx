import { Input as BaseUiInput } from "@base-ui-components/react/input";
import * as React from "react";
import { ComponentProps } from "react";
import { VariantProps, tv } from "tailwind-variants";

type InputVariants = VariantProps<typeof input>;

interface InputProps extends ComponentProps<"input">, InputVariants {}

export function Input({ sizeStyle, intent, ...props }: InputProps) {
  return <BaseUiInput className={input({ sizeStyle, intent })} {...props} />;
}

const input = tv({
  base: "w-full rounded-md border pl-3.5 text-base text-gray-900",
  variants: {
    base: "w-full rounded-md border px-2.5 py-1 shadow-sm h-[36px] font-medium text-stone-900 dark:text-white dark:bg-stone-925",
    intent: {
      default: "border-stone-500/50 focus:ring-stone-800/50",
      primary: "border-primary focus:ring-blue/50",
      success: "border-success focus:ring-green/50",
      warning: "border-warning focus:ring-yellow/50",
      danger: "border-danger focus:ring-red/50",
      info: "border-info focus:ring-blue/50",
      tip: "border-tip focus:ring-cyan/50",
      muted: "border-muted focus:ring-gray/50",
      strong: "border-strong focus:ring-stone-900/50",
    },
    sizeStyle: {
      sm: "text-sm py-1 px-2 [&>svg]:size-4 h-7",
      md: "py-1.5 px-3 h-[36px] [&>svg]:size-5 h-9",
      lg: "py-2 px-5 md:px-6 md:py-2.5 [&>svg]:size-6 h-10",
    },
  },
  defaultVariants: {
    sizeStyle: "md",
    intent: "default",
  },
});
