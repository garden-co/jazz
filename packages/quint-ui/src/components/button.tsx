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
        "text-white relative backdrop-blur-sm overflow-hidden transition-all duration-400 ease-in-out border-0 bg-gradient-to-t from-1% via-50% to-99% shadow-[0_6px_6px_rgba(0,0,0,0.2),0_0_20px_rgba(0,0,0,0.1)]",
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
      sm: "text-sm p-1 px-2 [&>svg]:size-4 h-7",
      md: "py-1.5 px-3 [&>svg]:size-4.5 h-9",
      lg: "py-2 px-5 md:px-6 md:py-2.5 [&>svg]:size-5 h-10",
    },
  },
  compoundVariants: [
    // Default Variants
    {
      variant: "default",
      intent: "default",
      className:
        "from-stone-400 to-stone-500 hover:from-stone-500 hover:via-stone-500/90 hover:to-zinc-400 active:from-zinc-400 active:to-stone-500 shadow-[inset_0_0_0_0.4px_rgba(120,113,108,0.2),inset_0_0.4px_0_rgba(245,245,244,0.4),inset_0_-0.4px_0_rgba(68,64,60,0.3)]",
    },
    {
      variant: "default",
      intent: "primary",
      className:
        "from-blue-400 to-blue-500 hover:from-blue-500 hover:via-blue-500/90 hover:to-sky-500 active:from-sky-500 active:to-blue-500 shadow-[inset_0_0_0_0.4px_rgba(59,130,246,0.2),inset_0_0.4px_0_rgba(147,197,253,0.4),inset_0_-0.4px_0_rgba(30,64,175,0.3)]",
    },
    {
      variant: "default",
      intent: "tip",
      className:
        "from-cyan-400 to-cyan-500 hover:from-cyan-400 hover:via-cyan-400 hover:to-sky-400 active:from-sky-400 active:to-cyan-500 shadow-[inset_0_0_0_0.4px_rgba(6,182,212,0.2),inset_0_0.4px_0_rgba(207,250,254,0.4),inset_0_-0.4px_0_rgba(21,94,117,0.3)]",
    },
    {
      variant: "default",
      intent: "info",
      className:
        "from-purple-400 to-purple-500 hover:from-purple-500 hover:via-purple-500/90 hover:to-violet-500 active:from-violet-500 active:to-purple-500 shadow-[inset_0_0_0_0.4px_rgba(147,51,234,0.2),inset_0_0.4px_0_rgba(221,214,254,0.4),inset_0_-0.4px_0_rgba(88,28,135,0.3)]",
    },
    {
      variant: "default",
      intent: "success",
      className:
        "from-green-400 to-green-500 hover:from-green-500 hover:via-green-500/90 hover:to-emerald-500 active:from-emerald-500 active:to-green-500 shadow-[inset_0_0_0_0.4px_rgba(34,197,94,0.2),inset_0_0.4px_0_rgba(187,247,208,0.4),inset_0_-0.4px_0_rgba(22,101,52,0.3)]",
    },
    {
      variant: "default",
      intent: "warning",
      className:
        "from-orange-400 to-orange-500 hover:from-orange-500 hover:via-orange-500/90 hover:to-amber-500 active:from-amber-500 active:to-orange-500 shadow-[inset_0_0_0_0.4px_rgba(249,115,22,0.2),inset_0_0.4px_0_rgba(254,240,138,0.4),inset_0_-0.4px_0_rgba(154,52,18,0.3)]",
    },
    {
      variant: "default",
      intent: "alert",
      className:
        "from-yellow-400 to-yellow-500 hover:from-amber-400 hover:to-yellow-400 active:from-yellow-400 active:to-amber-400 shadow-[inset_0_0_0_0.4px_rgba(234,179,8,0.2),inset_0_0.4px_0_rgba(254,249,195,0.4),inset_0_-0.4px_0_rgba(133,77,14,0.3)]",
    },
    {
      variant: "default",
      intent: "danger",
      className:
        "from-red-400 to-red-500 hover:from-red-500 hover:to-rose-500 active:from-rose-500 active:to-red-500 shadow-[inset_0_0_0_0.4px_rgba(239,68,68,0.2),inset_0_0.4px_0_rgba(254,202,202,0.4),inset_0_-0.4px_0_rgba(153,27,27,0.3)]",
    },
    {
      variant: "default",
      intent: "muted",
      className:
        "from-stone-300 to-stone-400 hover:from-stone-400 hover:via-stone-400/90 hover:to-zinc-300 active:from-zinc-300 active:to-stone-400 shadow-[inset_0_0_0_0.4px_rgba(255,255,255,0.2),inset_0_0.4px_0_rgba(255,255,255,0.4),inset_0_-0.4px_0_rgba(156,163,175,0.3)]",
    },
    {
      variant: "default",
      intent: "strong",
      className:
        "text-stone-100 dark:text-stone-900 from-stone-700 to-stone-800 hover:from-stone-800 hover:to-zinc-600 active:from-zinc-600 active:to-stone-800 shadow-[inset_0_0_0_0.4px_rgba(68,64,60,0.2),inset_0_0.4px_0_rgba(168,162,158,0.4),inset_0_-0.4px_0_rgba(41,37,36,0.3)]",
    },
    // Tailwind Classes
    {
      variant: "secondary",
      className:
        "text-white from-stone-300 to-stone-400 hover:from-stone-400 hover:to-zinc-300 active:from-zinc-300 active:to-stone-400 shadow-[inset_0_0_0_0.4px_rgba(255,255,255,0.2),inset_0_0.4px_0_rgba(255,255,255,0.4),inset_0_-0.4px_0_rgba(156,163,175,0.3)]",
    },
    {
      variant: "destructive",
      className:
        "text-white from-red-400 to-red-500 hover:from-red-500 hover:to-rose-500 active:from-rose-500 active:to-red-500 shadow-[inset_0_0_0_0.4px_rgba(239,68,68,0.2),inset_0_0.4px_0_rgba(254,202,202,0.4),inset_0_-0.4px_0_rgba(153,27,27,0.3)]",
    },
    // Outline Variants
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
        "border-primary text-primary hover:text-primary-light active:bg-blue/20 shadow-[inset_0_0_0_0.4px_rgba(59,130,246,0.2),inset_0_0.4px_0_rgba(147,197,253,0.4),inset_0_-0.4px_0_rgba(30,64,175,0.3)]",
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
    // Inverted Variants
    {
      variant: "inverted",
      intent: "default",
      className:
        "text-stone-700 dark:text-stone-100 hover:bg-stone-600/30 dark:hover:bg-white/30 bg-stone-600/20 dark:bg-white/20 active:bg-stone-900/40 dark:active:bg-white/50",
    },
    {
      variant: "inverted",
      intent: "primary",
      className: "text-primary hover:bg-blue/30 bg-blue/20 active:bg-blue/50",
    },
    {
      variant: "inverted",
      intent: "tip",
      className: "text-tip hover:bg-cyan/30 bg-cyan/20 active:bg-cyan/50",
    },
    {
      variant: "inverted",
      intent: "info",
      className:
        "text-info hover:bg-purple/30 bg-purple/20 active:bg-purple/50",
    },
    {
      variant: "inverted",
      intent: "success",
      className:
        "text-success hover:bg-green/30 bg-green/20 active:bg-green/50",
    },
    {
      variant: "inverted",
      intent: "warning",
      className:
        "text-warning hover:bg-yellow/30 bg-yellow/20 active:bg-yellow/50",
    },
    {
      variant: "inverted",
      intent: "alert",
      className:
        "text-alert hover:bg-orange/30 bg-orange/20 active:bg-orange/50",
    },
    {
      variant: "inverted",
      intent: "danger",
      className: "text-danger hover:bg-red/30 bg-red/20 active:bg-red/50",
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
    // Ghost Variants
    {
      variant: "ghost",
      intent: "default",
      className:
        "text-stone-700 dark:text-stone-100 hover:bg-stone-600/10 dark:hover:bg-white/10 active:bg-black/25 dark:active:bg-white/25",
    },
    {
      variant: "ghost",
      intent: "primary",
      className: "text-primary hover:bg-blue/10 active:bg-blue/25",
    },
    {
      variant: "ghost",
      intent: "tip",
      className: "text-tip hover:bg-cyan/10 active:bg-cyan/25",
    },
    {
      variant: "ghost",
      intent: "info",
      className: "text-info hover:bg-purple/10 active:bg-purple/25",
    },
    {
      variant: "ghost",
      intent: "success",
      className: "text-success hover:bg-green/10 active:bg-green/25",
    },
    {
      variant: "ghost",
      intent: "warning",
      className: "text-warning hover:bg-yellow/10 active:bg-yellow/25",
    },
    {
      variant: "ghost",
      intent: "alert",
      className: "text-alert hover:bg-orange/10 active:bg-orange/25",
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
    // Link Variants
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
