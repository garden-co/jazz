import { forwardRef } from "react";
import { classNames } from "../utils.js";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "link" | "plain";
  children?: React.ReactNode;
  className?: string;
  disabled?: boolean;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      className,
      children,
      variant = "primary",
      disabled,
      type = "button",
      ...buttonProps
    },
    ref,
  ) => {
    const variantClasses = {
      primary:
        "py-1.5 px-3 bg-blue border-blue text-white font-medium bg-blue hover:bg-blue-800 hover:border-blue-800",
      secondary:
        "py-1.5 px-3 text-stone-900 border font-medium hover:border-stone-300 hover:dark:border-stone-700 dark:text-white",
      link: "text-blue dark:text-blue-400 hover:underline",
    };

    const classes =
      variant === "plain"
        ? className
        : classNames(
            className,
            "inline-flex items-center justify-center gap-2 rounded-lg text-center transition-colors",
            "disabled:pointer-events-none disabled:opacity-70",
            variantClasses[variant],
            disabled && "opacity-50 cursor-not-allowed pointer-events-none",
          );

    return (
      <button
        ref={ref}
        {...buttonProps}
        disabled={disabled}
        className={classes}
        type={type}
      >
        {children}
      </button>
    );
  },
);
