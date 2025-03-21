import { clsx } from "clsx";
import { forwardRef } from "react";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "tertiary" | "destructive" | "plain";
  size?: "sm" | "md" | "lg";
  children?: React.ReactNode;
  className?: string;
  disabled?: boolean;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      className,
      children,
      size = "md",
      variant = "primary",
      disabled,
      type = "button",
      ...buttonProps
    },
    ref,
  ) => {
    const sizeClasses = {
      sm: "text-sm py-1 px-2",
      md: "py-1.5 px-3",
      lg: "md:text-lg  py-2 px-3 md:px-8 md:py-3",
    };

    const variantClasses = {
      primary:
        "bg-blue border-blue text-white font-medium bg-blue hover:bg-blue-800 hover:border-blue-800",
      secondary:
        "text-stone-900 border font-medium hover:border-stone-300 hover:dark:border-stone-700 dark:text-white",
      tertiary: "text-blue underline underline-offset-4",
      destructive:
        "bg-red-600 border-red-600 text-white font-medium hover:bg-red-700 hover:border-red-700",
    };

    const classNames =
      variant === "plain"
        ? className
        : clsx(
            className,
            "inline-flex items-center justify-center gap-2 rounded-lg text-center transition-colors",
            "disabled:pointer-events-none disabled:opacity-70",
            sizeClasses[size],
            variantClasses[variant],
            disabled && "opacity-50 cursor-not-allowed pointer-events-none",
          );

    return (
      <button
        ref={ref}
        {...buttonProps}
        disabled={disabled}
        className={classNames}
        type={type}
      >
        {children}
      </button>
    );
  },
);
