import React from "react";
import { type VariantProps, tv } from "tailwind-variants";

type IconVariants = VariantProps<typeof icon>;

interface IconProps
  extends Omit<React.HTMLAttributes<HTMLDivElement>, keyof IconVariants>,
    IconVariants {
  children?: React.ReactNode;
  className?: string;
}

export function Icon({
  children,
  size = "md",
  intent = "default",
  hasBackground = false,
  className,
  hasHover = false,
  ...divProps
}: IconProps) {
  const iconSize = iconSizes[size as keyof typeof iconSizes];
  const strokeWidth = strokeWidths[size as keyof typeof strokeWidths];

  let iconToRender: React.ReactNode;

  if (React.isValidElement(children)) {
    const childProps = {
      size: iconSize,
      strokeWidth: strokeWidth,
      strokeLinecap: "round" as const,
      ...(children.props as Record<string, any>),
    };
    iconToRender = React.cloneElement(children, childProps);
  } else {
    iconToRender = children;
  }

  return (
    <div
      className={icon({ intent, hasBackground, hasHover, size, className })}
      {...divProps}
    >
      {iconToRender}
    </div>
  );
}

const icon = tv({
  variants: {
    intent: {
      default: "text-stone-700 dark:text-stone-100",
      primary: "text-primary",
      tip: "text-tip",
      info: "text-info",
      success: "text-success",
      warning: "text-warning",
      alert: "text-alert",
      danger: "text-danger",
      muted: "text-stone-500 dark:text-stone-400",
      strong: "text-stone-900 dark:text-white",
      white: "text-white",
    },
    hasBackground: {
      true: "",
      false: "",
    },
    hasHover: {
      true: "",
      false: "",
    },
    size: {
      "2xs": "rounded-xs",
      xs: "rounded-xs",
      sm: "rounded-sm",
      md: "rounded-md",
      lg: "rounded-lg",
      xl: "rounded-xl",
    },
  },
  compoundVariants: [
    {
      intent: "default",
      hasHover: true,
      className: "hover:text-stone-600 dark:hover:text-stone-200",
    },
    {
      intent: "primary",
      hasHover: true,
      className: "hover:text-primary-light",
    },
    {
      intent: "tip",
      hasHover: true,
      className: "hover:text-tip-light",
    },
    {
      intent: "info",
      hasHover: true,
      className: "hover:text-info-light",
    },
    {
      intent: "success",
      hasHover: true,
      className: "hover:text-success-light",
    },
    {
      intent: "warning",
      hasHover: true,
      className: "hover:text-warning-light",
    },
    {
      intent: "alert",
      hasHover: true,
      className: "hover:text-alert-light",
    },
    {
      intent: "danger",
      hasHover: true,
      className: "hover:text-danger-light",
    },
    {
      intent: "muted",
      hasHover: true,
      className: "hover:text-stone-400 dark:hover:text-stone-500",
    },
    {
      intent: "strong",
      hasHover: true,
      className: "hover:text-stone-700 dark:hover:text-stone-300",
    },
    {
      intent: "white",
      hasHover: true,
      className: "hover:text-white/90",
    },
    {
      intent: "default",
      hasBackground: true,
      className: "bg-stone-200/30 dark:bg-stone-900/30",
    },
    {
      intent: "primary",
      hasBackground: true,
      className: "bg-primary-transparent",
    },
    {
      intent: "tip",
      hasBackground: true,
      className: "bg-tip-transparent",
    },
    {
      intent: "info",
      hasBackground: true,
      className: "bg-info-transparent",
    },
    {
      intent: "success",
      hasBackground: true,
      className: "bg-success-transparent",
    },
    {
      intent: "warning",
      hasBackground: true,
      className: "bg-warning-transparent",
    },
    {
      intent: "alert",
      hasBackground: true,
      className: "bg-alert-transparent",
    },
    {
      intent: "danger",
      hasBackground: true,
      className: "bg-danger-transparent",
    },
    {
      intent: "muted",
      hasBackground: true,
      className: "bg-stone-300/30 dark:bg-stone-700/30",
    },
    {
      intent: "strong",
      hasBackground: true,
      className: "bg-stone-900/30 dark:bg-stone-100/30",
    },
  ],
  defaultVariants: {
    size: "md",
    intent: "default",
  },
});

const iconSizes = {
  "2xs": 14,
  xs: 16,
  sm: 20,
  md: 22,
  lg: 26,
  xl: 28,
  "2xl": 32,
  "3xl": 36,
  "4xl": 40,
  "5xl": 48,
  "6xl": 60,
  "7xl": 72,
  "8xl": 96,
  "9xl": 128,
};

const strokeWidths = {
  "2xs": 2.5,
  xs: 2,
  sm: 2,
  md: 1.5,
  lg: 1.5,
  xl: 1.5,
  "2xl": 1.25,
  "3xl": 1.25,
  "4xl": 1.25,
  "5xl": 1,
  "6xl": 1,
  "7xl": 1,
  "8xl": 1,
  "9xl": 1,
};
