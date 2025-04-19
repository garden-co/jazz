import { clsx } from "clsx";
import Link from "next/link";
import { forwardRef } from "react";
import { Icon } from "./Icon";
import type { IconName } from "./Icon";
import { Spinner } from "./Spinner";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "tertiary" | "destructive" | "plain";
  size?: "sm" | "md" | "lg";
  href?: string;
  newTab?: boolean;
  icon?: IconName;
  loading?: boolean;
  loadingText?: string;
  children?: React.ReactNode;
  className?: string;
  disabled?: boolean;
}

function ButtonIcon({ icon, loading }: ButtonProps) {
  if (!Icon) return null;

  const className = "size-5";

  if (loading) return <Spinner className={className} />;

  if (icon) {
    return <Icon name={icon} className={className} />;
  }
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      className,
      children,
      size = "md",
      variant = "primary",
      href,
      disabled,
      newTab,
      loading,
      loadingText,
      icon,
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
        "bg-primary border border-primary text-white font-medium hover:bg-highlight hover:border-primary hover:text-primary dark:hover:bg-highlight dark:hover:text-primary",
      secondary:
        "text-stone-900 border font-medium hover:border-primary hover:text-primary hover:bg-highlight hover:dark:border-primary dark:text-white dark:hover:text-primary",
      tertiary: "text-primary underline underline-offset-4",
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

    if (href) {
      return (
        <Link
          href={href}
          target={newTab ? "_blank" : undefined}
          className={classNames}
        >
          <ButtonIcon icon={icon} loading={loading} />
          {children}
          {newTab ? (
            <span className="inline-block text-muted relative -top-0.5 -left-2 -mr-2">
              ⌝
            </span>
          ) : (
            ""
          )}
        </Link>
      );
    }

    return (
      <button
        ref={ref}
        {...buttonProps}
        disabled={disabled || loading}
        className={classNames}
        type={type}
      >
        <ButtonIcon icon={icon} loading={loading} />

        {loading && loadingText ? loadingText : children}
      </button>
    );
  },
);
