import { mergeProps } from "@base-ui-components/react/merge-props";
import { useRender } from "@base-ui-components/react/use-render";
import { type VariantProps, tv } from "tailwind-variants";

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

const button = tv({
  base: "inline-flex items-center justify-center gap-2 rounded-lg text-center transition-colors w-fit text-nowrap disabled:pointer-events-none disabled:opacity-70 cursor-pointer font-medium",
  variants: {
    variant: {
      default:
        "relative backdrop-blur-sm overflow-hidden transition-all duration-400 ease-in-out border-0 bg-gradient-to-br shadow-[0_6px_6px_rgba(0,0,0,0.2),0_0_20px_rgba(0,0,0,0.1)]",
      secondary:
        "shadow-sm shadow-stone-400/20 focus:outline-none focus-visible:ring focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-opacity-10 bg-gradient-to-t from-7% via-50% to-95%",
      destructive:
        "shadow-sm shadow-stone-400/20 focus:outline-none focus-visible:ring focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-opacity-10 bg-gradient-to-t from-7% via-50% to-95%",
      ghost: "bg-transparent",
      outline: "border bg-transparent shadow-[5px_0px]",
      link: "bg-transparent underline underline-offset-2 p-0 hover:bg-transparent",
      inverted: "shadow-sm",
    },
    intent: {
      default: "",
      primary: "",
      tip: "",
      info: "",
      success: "",
      warning: "",
      alert: "",
      danger: "",
      muted: "",
      strong: "",
    },
    size: {
      sm: "text-sm py-1 px-2",
      md: "py-1.5 px-3 h-[36px]",
      lg: "py-2 px-5 md:px-6 md:py-2.5",
    },
  },
  compoundVariants: [
    // Default Variant
    {
      variant: "default",
      intent: "default",
      className:
        "text-white from-10% to-90% from-stone-400 to-stone-500 hover:from-stone-500 hover:to-zinc-400 active:from-zinc-400 active:to-stone-500 shadow-[inset_0_0_0_0.4px_rgba(120,113,108,0.2),inset_0_0.4px_0_rgba(245,245,244,0.4),inset_0_-0.4px_0_rgba(68,64,60,0.3)]",
    },
    {
      variant: "default",
      intent: "primary",
      className:
        "text-white from-10% to-90% from-blue-500 to-blue-600 hover:from-blue-600 hover:to-sky-500 active:from-sky-500 active:to-blue-600 shadow-[inset_0_0_0_0.4px_rgba(59,130,246,0.2),inset_0_0.4px_0_rgba(147,197,253,0.4),inset_0_-0.4px_0_rgba(30,64,175,0.3)]",
    },
    {
      variant: "default",
      intent: "tip",
      className:
        "text-white from-10% to-90% from-cyan-400 to-cyan-500 hover:from-cyan-500 hover:to-sky-400 active:from-sky-400 active:to-cyan-500 shadow-[inset_0_0_0_0.4px_rgba(6,182,212,0.2),inset_0_0.4px_0_rgba(207,250,254,0.4),inset_0_-0.4px_0_rgba(21,94,117,0.3)]",
    },
    {
      variant: "default",
      intent: "info",
      className:
        "text-white from-10% to-90% from-purple-500 to-purple-600 hover:from-purple-600 hover:to-violet-500 active:from-violet-500 active:to-purple-600 shadow-[inset_0_0_0_0.4px_rgba(147,51,234,0.2),inset_0_0.4px_0_rgba(221,214,254,0.4),inset_0_-0.4px_0_rgba(88,28,135,0.3)]",
    },
    {
      variant: "default",
      intent: "success",
      className:
        "text-white from-10% to-90% from-green-500 to-green-600 hover:from-green-600 hover:to-emerald-500 active:from-emerald-500 active:to-green-600 shadow-[inset_0_0_0_0.4px_rgba(34,197,94,0.2),inset_0_0.4px_0_rgba(187,247,208,0.4),inset_0_-0.4px_0_rgba(22,101,52,0.3)]",
    },
    {
      variant: "default",
      intent: "warning",
      className:
        "text-white from-10% to-90% from-orange-500 to-orange-600 hover:from-orange-600 hover:to-amber-500 active:from-amber-500 active:to-orange-600 shadow-[inset_0_0_0_0.4px_rgba(249,115,22,0.2),inset_0_0.4px_0_rgba(254,240,138,0.4),inset_0_-0.4px_0_rgba(154,52,18,0.3)]",
    },
    {
      variant: "default",
      intent: "alert",
      className:
        "text-white from-10% to-90% from-amber-400 to-amber-500 hover:from-amber-500 hover:to-yellow-400 active:from-yellow-400 active:to-amber-500 shadow-[inset_0_0_0_0.4px_rgba(234,179,8,0.2),inset_0_0.4px_0_rgba(254,249,195,0.4),inset_0_-0.4px_0_rgba(133,77,14,0.3)]",
    },
    {
      variant: "default",
      intent: "danger",
      className:
        "text-white from-10% to-90% from-red-500 to-red-600 hover:from-red-600 hover:to-rose-500 active:from-rose-500 active:to-red-600 shadow-[inset_0_0_0_0.4px_rgba(239,68,68,0.2),inset_0_0.4px_0_rgba(254,202,202,0.4),inset_0_-0.4px_0_rgba(153,27,27,0.3)]",
    },
    {
      variant: "default",
      intent: "muted",
      className:
        "text-stone-700 dark:text-white from-10% to-90% from-stone-300 to-stone-400 hover:from-stone-400 hover:to-zinc-300 active:from-zinc-300 active:to-stone-400 shadow-[inset_0_0_0_0.4px_rgba(255,255,255,0.2),inset_0_0.4px_0_rgba(255,255,255,0.4),inset_0_-0.4px_0_rgba(156,163,175,0.3)]",
    },
    {
      variant: "default",
      intent: "strong",
      className:
        "text-stone-100 dark:text-stone-900 from-10% to-90% from-stone-700 to-stone-800 hover:from-stone-800 hover:to-zinc-600 active:from-zinc-600 active:to-stone-800 shadow-[inset_0_0_0_0.4px_rgba(68,64,60,0.2),inset_0_0.4px_0_rgba(168,162,158,0.4),inset_0_-0.4px_0_rgba(41,37,36,0.3)]",
    },
    // Secondary Variant (alias for default-muted)
    {
      variant: "secondary",
      className:
        "text-stone-100 dark:text-stone-900 from-stone-200 via-stone-300 to-stone-400 dark:from-stone-900 dark:via-stone-900 dark:to-stone-800 hover:from-stone-200 hover:to-stone-300 dark:hover:from-stone-900 dark:hover:to-stone-700/70 active:from-stone-300 active:to-stone-300 dark:active:from-stone-900 dark:active:to-stone-800 focus:ring-stone-200 dark:focus:ring-stone-900",
    },
    // Destructive Variant (alias for default-danger)
    {
      variant: "destructive",
      className:
        "text-white from-danger-dark via-danger to-danger-light hover:from-danger-brightLight hover:to-danger-light active:from-danger-brightDark active:to-danger-light focus:ring-danger",
    },
    // Outline Variant
    {
      variant: "outline",
      intent: "default",
      className:
        "border-stone-600 dark:border-stone-200 text-stone-700 dark:text-stone-100 hover:text-stone-600 dark:hover:text-stone-200 active:bg-stone-600/20 dark:active:bg-stone-100/20 shadow-[inset_0_0_0_0.4px_rgba(255,255,255,0.2),inset_0_0.4px_0_rgba(255,255,255,0.4),inset_0_-0.4px_0_rgba(156,163,175,0.3)]",
    },
    {
      variant: "outline",
      intent: "primary",
      className:
        "border-primary text-primary hover:text-primary-light active:bg-primary-20 shadow-[inset_0_0_0_0.4px_rgba(59,130,246,0.2),inset_0_0.4px_0_rgba(147,197,253,0.4),inset_0_-0.4px_0_rgba(30,64,175,0.3)]",
    },
    {
      variant: "outline",
      intent: "tip",
      className:
        "border-tip text-tip hover:text-tip-light active:bg-tip-20 shadow-[inset_0_0_0_0.4px_rgba(6,182,212,0.2),inset_0_0.4px_0_rgba(207,250,254,0.4),inset_0_-0.4px_0_rgba(21,94,117,0.3)]",
    },
    {
      variant: "outline",
      intent: "info",
      className:
        "border-info text-info hover:text-info-light active:bg-info-20 shadow-[inset_0_0_0_0.4px_rgba(147,51,234,0.2),inset_0_0.4px_0_rgba(221,214,254,0.4),inset_0_-0.4px_0_rgba(88,28,135,0.3)]",
    },
    {
      variant: "outline",
      intent: "success",
      className:
        "border-success text-success hover:text-success-light active:bg-success-20 shadow-[inset_0_0_0_0.4px_rgba(34,197,94,0.2),inset_0_0.4px_0_rgba(187,247,208,0.4),inset_0_-0.4px_0_rgba(22,101,52,0.3)]",
    },
    {
      variant: "outline",
      intent: "warning",
      className:
        "border-warning text-warning hover:text-warning-light active:bg-warning-20 shadow-[inset_0_0_0_0.4px_rgba(249,115,22,0.2),inset_0_0.4px_0_rgba(254,240,138,0.4),inset_0_-0.4px_0_rgba(154,52,18,0.3)]",
    },
    {
      variant: "outline",
      intent: "alert",
      className:
        "border-alert text-alert hover:text-alert-light active:bg-alert-20 shadow-[inset_0_0_0_0.4px_rgba(234,179,8,0.2),inset_0_0.4px_0_rgba(254,249,195,0.4),inset_0_-0.4px_0_rgba(133,77,14,0.3)]",
    },
    {
      variant: "outline",
      intent: "danger",
      className:
        "border-danger text-danger hover:text-danger-light active:bg-danger-20 shadow-[inset_0_0_0_0.4px_rgba(239,68,68,0.2),inset_0_0.4px_0_rgba(254,202,202,0.4),inset_0_-0.4px_0_rgba(153,27,27,0.3)]",
    },
    {
      variant: "outline",
      intent: "muted",
      className:
        "border-stone-200 dark:border-stone-700 text-stone-500 dark:text-stone-400 hover:text-stone-400 dark:hover:text-stone-500 active:bg-stone-400/20 shadow-[inset_0_0_0_0.4px_rgba(120,113,108,0.2),inset_0_0.4px_0_rgba(245,245,244,0.4),inset_0_-0.4px_0_rgba(68,64,60,0.3)]",
    },
    {
      variant: "outline",
      intent: "strong",
      className:
        "border-stone-900 dark:border-stone-100 text-stone-900 dark:text-white hover:text-stone-700 dark:hover:text-stone-300 active:bg-stone-900/20 shadow-[inset_0_0_0_0.4px_rgba(68,64,60,0.2),inset_0_0.4px_0_rgba(168,162,158,0.4),inset_0_-0.4px_0_rgba(41,37,36,0.3)]",
    },
    // Inverted Variant
    {
      variant: "inverted",
      intent: "default",
      className:
        "text-stone-700 dark:text-stone-100 hover:bg-stone-600/30 dark:hover:bg-white/30 bg-stone-600/20 dark:bg-white/20 active:bg-stone-900/40 dark:active:bg-white/50",
    },
    {
      variant: "inverted",
      intent: "primary",
      className:
        "text-primary hover:bg-primary-30 bg-primary-20 active:bg-primary-50",
    },
    {
      variant: "inverted",
      intent: "tip",
      className: "text-tip hover:bg-tip-30 bg-tip-20 active:bg-tip-50",
    },
    {
      variant: "inverted",
      intent: "info",
      className: "text-info hover:bg-info-30 bg-info-20 active:bg-info-50",
    },
    {
      variant: "inverted",
      intent: "success",
      className:
        "text-success hover:bg-success-30 bg-success-20 active:bg-success-50",
    },
    {
      variant: "inverted",
      intent: "warning",
      className:
        "text-warning hover:bg-warning-30 bg-warning-20 active:bg-warning-50",
    },
    {
      variant: "inverted",
      intent: "alert",
      className: "text-alert hover:bg-alert-30 bg-alert-20 active:bg-alert-50",
    },
    {
      variant: "inverted",
      intent: "danger",
      className:
        "text-danger hover:bg-danger-30 bg-danger-20 active:bg-danger-50",
    },
    {
      variant: "inverted",
      intent: "muted",
      className:
        "text-stone-500 dark:text-stone-400 hover:bg-stone-200/30 dark:hover:bg-stone-900/30 bg-stone-200/20 dark:bg-stone-900/50 active:bg-stone-100/50 dark:active:bg-stone-900/50",
    },
    {
      variant: "inverted",
      intent: "strong",
      className:
        "text-stone-900 dark:text-white hover:bg-stone-900/30 dark:hover:bg-stone-100/30 bg-stone-900/20 dark:bg-stone-100/50 active:bg-stone-800/40 dark:active:bg-stone-200/40",
    },
    // Ghost Variant
    {
      variant: "ghost",
      intent: "default",
      className:
        "text-stone-700 dark:text-stone-100 hover:bg-stone-600/10 dark:hover:bg-white/10 active:bg-black/25 dark:active:bg-white/25",
    },
    {
      variant: "ghost",
      intent: "primary",
      className: "text-primary hover:bg-primary-10 active:bg-primary-25",
    },
    {
      variant: "ghost",
      intent: "tip",
      className: "text-tip hover:bg-tip-10 active:bg-tip-25",
    },
    {
      variant: "ghost",
      intent: "info",
      className: "text-info hover:bg-info-10 active:bg-info-25",
    },
    {
      variant: "ghost",
      intent: "success",
      className: "text-success hover:bg-success-10 active:bg-success-25",
    },
    {
      variant: "ghost",
      intent: "warning",
      className: "text-warning hover:bg-warning-10 active:bg-warning-25",
    },
    {
      variant: "ghost",
      intent: "alert",
      className: "text-alert hover:bg-alert-10 active:bg-alert-25",
    },
    {
      variant: "ghost",
      intent: "danger",
      className: "text-danger hover:bg-danger-10 active:bg-danger-25",
    },
    {
      variant: "ghost",
      intent: "muted",
      className:
        "text-stone-500 dark:text-stone-400 hover:bg-stone-200/30 dark:hover:bg-stone-800/30 active:bg-stone-100/25 dark:active:bg-stone-900/25",
    },
    {
      variant: "ghost",
      intent: "strong",
      className:
        "text-stone-900 dark:text-white hover:bg-stone-900/10 dark:hover:bg-stone-100/10 active:bg-stone-900/25 dark:active:bg-stone-100/25",
    },
    // Link Variant
    {
      variant: "link",
      intent: "default",
      className:
        "text-stone-700 dark:text-stone-100 hover:text-stone-600 dark:hover:text-stone-200 active:text-stone-800 dark:active:text-stone-400 active:underline-stone-500",
    },
    {
      variant: "link",
      intent: "primary",
      className:
        "text-primary hover:text-primary-light active:text-primary-dark",
    },
    {
      variant: "link",
      intent: "tip",
      className: "text-tip hover:text-tip-light active:text-tip-dark",
    },
    {
      variant: "link",
      intent: "info",
      className: "text-info hover:text-info-light active:text-info-dark",
    },
    {
      variant: "link",
      intent: "success",
      className:
        "text-success hover:text-success-light active:text-success-dark",
    },
    {
      variant: "link",
      intent: "warning",
      className:
        "text-warning hover:text-warning-light active:text-warning-dark",
    },
    {
      variant: "link",
      intent: "alert",
      className: "text-alert hover:text-alert-light active:text-alert-dark",
    },
    {
      variant: "link",
      intent: "danger",
      className: "text-danger hover:text-danger-light active:text-danger-dark",
    },
    {
      variant: "link",
      intent: "muted",
      className:
        "text-stone-500 dark:text-stone-400 hover:text-stone-400 dark:hover:text-stone-500 active:text-stone-400 dark:active:text-stone-500",
    },
    {
      variant: "link",
      intent: "strong",
      className:
        "text-stone-900 dark:text-white hover:text-stone-700 dark:hover:text-stone-300 active:text-stone-700 dark:active:text-stone-300",
    },
  ],
  defaultVariants: {
    size: "md",
    variant: "default",
    intent: "default",
  },
});
