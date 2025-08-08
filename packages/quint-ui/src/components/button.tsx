import { mergeProps } from "@base-ui-components/react/merge-props";
import { useRender } from "@base-ui-components/react/use-render";
import { tv, type VariantProps } from "tailwind-variants";

const button = tv({
  base: "inline-flex items-center justify-center gap-2 rounded-sm text-center transition-colors w-fit text-nowrap disabled:pointer-events-none disabled:opacity-70 disabled:cursor-not-allowed disabled:pointer-events-none font-medium",
  variants: {
    variant: {
      default:
        "focus:outline-none focus-visible:ring focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-opacity-10",
      ghost: "",
      outline: "shadow-sm border",
      link: "",
      inverted: "",
    },
    intent: {
      default: "border-secondary",
      primary: "border-primary",
      success: "border-success",
      danger: "border-danger",
    },
    size: {
      sm: "text-sm py-1 px-2 [&>svg]:size-4",
      md: "py-1.5 px-3 h-[36px] [&>svg]:size-4",
      lg: "py-2 px-5 md:px-6 md:py-2.5 [&>svg]:size-5",
      icon: "p-2 [&>svg]:size-4",
    },
  },
  compoundVariants: [
    // Default Variants
    {
      variant: "default",
      intent: ["primary", "success", "danger", "default"],
      className: "bg-gradient-to-t from-7% via-50% to-95%",
    },
    {
      variant: "default",
      intent: "primary",
      className: [
        "text-primary-foreground",
        "from-primary-dark via-primary to-primary-light",
        "hover:from-primary-brightLight hover:to-primary-light",
        "active:from-primary-brightDark active:to-primary-light active:border-primary-transparent ",
        "focus:ring-primary focus:border-primary-transparent",
      ],
    },
    {
      variant: "default",
      intent: "success",
      className: [
        "text-success-foreground",
        "from-success-dark via-success to-success-light",
        "hover:from-success-brightLight hover:to-success-light",
        "active:from-success-brightDark active:to-success-light",
        "focus:ring-success",
      ],
    },
    {
      variant: "default",
      intent: "danger",
      className: [
        "text-danger-foreground",
        "from-danger-dark via-danger to-danger-light",
        "hover:from-danger-brightLight hover:to-danger-light",
        "active:from-danger-brightDark active:to-danger-light",
        "focus:ring-danger",
      ],
    },
    // Outline Variants
    {
      variant: "outline",
      intent: "primary",
      className: [
        "text-primary",
        "active:bg-primary/30",
        "hover:text-primary-light hover:border-primary-light",
      ],
    },
    {
      variant: "outline",
      intent: "danger",
      className: [
        "text-danger",
        "active:bg-danger/30",
        "hover:text-danger-light hover:border-danger-light",
      ],
    },
    {
      variant: "outline",
      intent: "success",
      className: [
        "text-success-dark",
        "active:bg-success/30",
        "hover:text-success-light hover:border-success-light",
      ],
    },
    // Ghost Variants
    {
      variant: "ghost",
      intent: "primary",
      className: [
        "text-primary",
        "hover:bg-primary/10",
        "active:bg-primary/30",
      ],
    },
    {
      variant: "ghost",
      intent: "primary",
      className: [
        "text-primary",
        "hover:bg-primary/10",
        "active:bg-primary/30",
      ],
    },
    {
      variant: "ghost",
      intent: "danger",
      className: ["text-danger", "hover:bg-danger/10", "active:bg-danger/30"],
    },
    {
      variant: "ghost",
      intent: "success",
      className: [
        "text-success",
        "hover:bg-success/10",
        "active:bg-success/30",
      ],
    },
  ],
  defaultVariants: {
    size: "md",
    variant: "default",
    intent: "default",
  },
});

type ButtonVariants = VariantProps<typeof button>;

interface ButtonProps
  extends Omit<useRender.ComponentProps<"button">, keyof ButtonVariants>,
    ButtonVariants {}

export function Button({ render = <button />, ...props }: ButtonProps) {
  const element = useRender({
    render,
    props: mergeProps<"button">(
      {
        className: button({
          size: props.size,
          variant: props.variant,
          intent: props.intent,
        }),
      },
      props,
    ),
  });

  return element;
}
