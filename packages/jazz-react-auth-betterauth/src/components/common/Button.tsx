import { clsx } from "clsx";
import { forwardRef } from "react";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { useAuth } from "../../contexts/Auth";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "danger";
  children?: React.ReactNode;
  className?: string;
  src?: InstanceType<typeof Image>["src"];
  alt?: InstanceType<typeof Image>["alt"];
  imageClassName?: InstanceType<typeof Image>["className"];
  href?: string;
  newTab?: boolean;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      className,
      children,
      variant = "primary",
      src,
      alt,
      imageClassName,
      href,
      newTab = false,
      ...buttonProps
    },
    ref,
  ) => {
    const { Image, Link } = useAuth();
    const primary =
      "rounded-full border border-solid border-transparent transition-colors flex items-center justify-center bg-foreground text-background gap-2 hover:bg-[#383838] dark:hover:bg-[#ccc] font-medium text-sm sm:text-base h-10 sm:h-12 px-4 sm:px-5 sm:w-auto";
    const secondary =
      "rounded-full border border-solid border-black/[.08] dark:border-white/[.145] transition-colors flex items-center justify-center gap-2 hover:bg-[#f2f2f2] dark:hover:bg-[#1a1a1a] hover:border-transparent font-medium text-sm sm:text-base h-10 sm:h-12 px-4 sm:px-5 w-full sm:w-auto";
    const danger =
      "rounded-full border border-solid border-black/[.08] dark:border-white/[.145] transition-colors flex items-center justify-center bg-red-400 dark:bg-red-600 gap-2 hover:bg-red-300 dark:hover:bg-red-700 hover:border-transparent font-medium text-sm sm:text-base h-10 sm:h-12 px-4 sm:px-5 w-full sm:w-auto";
    const classes = clsx(
      variant === "primary"
        ? primary
        : variant === "secondary"
          ? secondary
          : danger,
      className,
    );
    const [width, height] = [
      variant === "primary" ? 20 : 16,
      variant === "primary" ? 20 : 16,
    ];
    if (href) {
      return (
        <Link
          href={href}
          target={newTab ? "_blank" : undefined}
          className={classes}
          rel="noopener noreferrer"
        >
          {src && alt && (
            <Image
              className={imageClassName}
              src={src}
              alt={alt}
              width={width}
              height={height}
            />
          )}
          {children}
        </Link>
      );
    } else {
      return (
        <button
          ref={ref}
          {...buttonProps}
          className={classes}
          rel="noopener noreferrer"
        >
          {src && alt && (
            <Image
              className={imageClassName}
              src={src}
              alt={alt}
              width={width}
              height={height}
            />
          )}
          {children}
        </button>
      );
    }
  },
);
