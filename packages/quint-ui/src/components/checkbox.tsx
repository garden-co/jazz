import { ComponentProps } from "react";
import { Checkbox as BaseUiCheckbox } from "@base-ui-components/react/checkbox";
import { tv, VariantProps } from "tailwind-variants";
import { CheckIcon } from "lucide-react";

interface CheckboxProps
  extends ComponentProps<typeof BaseUiCheckbox.Root>,
    VariantProps<typeof checkbox> {}

export function Checkbox({
  sizeStyle,
  intent,
  variant,
  value,
  ...props
}: CheckboxProps) {
  return (
    <BaseUiCheckbox.Root
      {...props}
      className={checkbox({ sizeStyle, intent, variant })}
    >
      <BaseUiCheckbox.Indicator>
        <CheckIcon />
      </BaseUiCheckbox.Indicator>
    </BaseUiCheckbox.Root>
  );
}

const checkbox = tv({
  base: "text-white rounded-sm ",
  variants: {
    sizeStyle: {
      xs: "w-3 h-3 [&>span>svg]:size-3",
      sm: "w-4 h-4 [&>span>svg]:size-4",
      md: "w-5 h-5 [&>span>svg]:size-5",
      lg: "w-6 h-6 [&>span>svg]:size-6",
    },
    intent: {
      default: "",
      primary: "",
      success: "",
      warning: "",
      danger: "",
      info: "",
      tip: "",
      muted: "",
      strong: "",
    },
    variant: {
      default: "bg-gray-500",
      outline:
        "border bg-transparent box-border [&>span]:relative [&>span]:left-[-1px] [&>span]:top-[-1px]",
      inverted: "shadow-sm",
    },
    disabled: {
      true: "opacity-50",
    },
  },
  compoundVariants: [
    // Default Variants
    {
      intent: "default",
      variant: "default",
      className: "bg-gray-500",
    },
    {
      intent: "primary",
      variant: "default",
      className: "bg-blue",
    },
    {
      intent: "success",
      variant: "default",
      className: "bg-green",
    },
    {
      intent: "warning",
      variant: "default",
      className: "bg-yellow",
    },
    {
      intent: "danger",
      variant: "default",
      className: "bg-red",
    },
    {
      intent: "info",
      variant: "default",
      className: "bg-blue",
    },
    {
      intent: "tip",
      variant: "default",
      className: "bg-purple",
    },
    {
      intent: "muted",
      variant: "default",
      className: "bg-muted",
    },
    {
      intent: "strong",
      variant: "default",
      className: "bg-strong",
    },
    // Outline Variants
    {
      intent: "default",
      variant: "outline",
      className: "border border-gray-500 text-gray-500",
    },
    {
      intent: "primary",
      variant: "outline",
      className: "border border-blue text-blue",
    },
    {
      intent: "success",
      variant: "outline",
      className: "border border-green text-green",
    },
    {
      intent: "danger",
      variant: "outline",
      className: "border border-red text-red",
    },
    {
      intent: "warning",
      variant: "outline",
      className: "border border-yellow bg-transparent text-yellow",
    },
    {
      intent: "info",
      variant: "outline",
      className: "border border-blue text-blue",
    },
    {
      intent: "tip",
      variant: "outline",
      className: "border border-purple text-purple",
    },
    {
      intent: "muted",
      variant: "outline",
      className: "border border-muted text-muted",
    },
    {
      intent: "strong",
      variant: "outline",
      className: "border border-strong text-strong",
    },
    // Inverted Variants
    {
      intent: "default",
      variant: "inverted",
      className: "bg-transparent text-gray-500",
    },
    {
      intent: "primary",
      variant: "inverted",
      className: "bg-blue/50 text-blue",
    },
    {
      intent: "success",
      variant: "inverted",
      className: "bg-green/50 text-green",
    },
    {
      intent: "warning",
      variant: "inverted",
      className: "bg-yellow/50 text-yellow",
    },

    {
      intent: "danger",
      variant: "inverted",
      className: "bg-red/50 text-red",
    },

    {
      intent: "info",
      variant: "inverted",
      className: "bg-cyan/50 text-cyan",
    },
    {
      intent: "tip",
      variant: "inverted",
      className: "bg-purple/50 text-purple",
    },
    {
      intent: "muted",
      variant: "inverted",
      className: "bg-gray-500/50 text-gray-500",
    },
    {
      intent: "strong",
      variant: "inverted",
      className: "bg-strong/50 text-strong",
    },
  ],
  defaultVariants: {
    sizeStyle: "md",
    intent: "default",
    variant: "default",
  },
});
