import { clsx } from "clsx";
import Link from "next/link";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  children: React.ReactNode;
  variant?: "primary" | "secondary" | "tertiary";
  size?: "sm" | "md" | "lg";
  href?: string;
  newTab?: boolean;
}

export function Button(props: ButtonProps) {
  const {
    className,
    children,
    size = "md",
    variant = "primary",
    href,
    disabled,
    newTab,
  } = props;

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
  };

  const classNames = clsx(
    className,
    "inline-flex items-center justify-center gap-2 rounded-lg text-center transition-colors",
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
        {children}
      </Link>
    );
  }

  return (
    <button {...props} className={classNames}>
      {children}
    </button>
  );
}
