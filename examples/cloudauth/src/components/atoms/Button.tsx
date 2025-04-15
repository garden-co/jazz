import { clsx } from "clsx";
import { forwardRef } from "react";
import { Icon } from "./Icon";
import { Spinner } from "./Spinner";

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "tertiary" | "destructive" | "plain";
  size?: "sm" | "md" | "lg";
  href?: string;
  newTab?: boolean;
  icon?: string;
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
      sm: "text-sm gap-1 py-1.5 px-2.5",
      md: "gap-2 py-2 px-3",
      lg: "md:text-lg gap-2  py-2 px-3 md:px-8 md:py-3",
    };

    const variantClasses = {
      primary:
        "bg-blue-600 border-blue text-white font-medium hover:bg-blue-800 hover:border-blue-800",
      secondary:
        "text-stone-900 border font-medium hover:border-stone-300 hover:dark:border-stone-700 dark:text-white",
      tertiary: "text-blue-600 underline underline-offset-4",
      destructive:
        "bg-red-600 border-red-600 text-white font-medium hover:bg-red-700 hover:border-red-700",
    };

    const classNames =
      variant === "plain"
        ? className
        : clsx(
            className,
            "inline-flex items-center justify-center rounded-lg text-center transition-colors",
            "disabled:pointer-events-none disabled:opacity-70",
            sizeClasses[size],
            variantClasses[variant],
            disabled && "opacity-50 cursor-not-allowed pointer-events-none",
          );

    if (href) {
      return (
        <a
          href={href}
          target={newTab ? "_blank" : undefined}
          className={classNames}
        >
          <ButtonIcon icon={icon} loading={loading} />
          {children}
          {newTab ? (
            <span className="inline-block text-stone-300 dark:text-stone-700 relative -top-0.5 -left-2 -mr-2">
              ⌝
            </span>
          ) : (
            ""
          )}
        </a>
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

/**
 * Expand the hit area to at least 44×44px on touch devices
 */
export function TouchTarget({ children }: { children: React.ReactNode }) {
  return (
    <>
      <span
        className="absolute left-1/2 top-1/2 size-[max(100%,2.75rem)] -translate-x-1/2 -translate-y-1/2 [@media(pointer:fine)]:hidden"
        aria-hidden="true"
      />
      {children}
    </>
  );
}
