import { clsx } from "clsx";
import Link from "next/link";
import { forwardRef } from "react";
import {
  colorClasses,
  colorToBgHoverMap10,
  colorToBgHoverMap30,
  colorToBgMap,
  sizeClasses,
  variantToBgMap,
  variantToBgTransparentHoverMap,
  variantToBorderMap,
  variantToColorMap,
  variantToTextHoverMap,
  variantToTextMap,
} from "../../utils/tailwindClassesMap";
import { Variant } from "../../utils/variants";
import { Icon } from "./Icon";
import type { IconName } from "./Icon";
import { Spinner } from "./Spinner";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant;
  color?: "light" | "dark" | "white" | "black" | "default";
  styleVariant?: "outline" | "inverted" | "ghost" | "text" | "default";
  state?: "hover" | "active" | "focus" | "disabled";
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

  return loading ? (
    <Spinner className={className} />
  ) : icon ? (
    <Icon name={icon} className={className} />
  ) : null;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      className,
      children,
      size = "md",
      variant = "primary",
      color,
      styleVariant,
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
    const styleClass =
      styleClasses(variant)[styleVariant as keyof typeof styleClasses] || "";

    const getClasses = ({
      styleVariant,
    }: { styleVariant: string | undefined }) => {
      return {
        [sizeClasses[size as keyof typeof sizeClasses]]: size,
        [variantClass(variant)]: !styleVariant && !color,
        [colorClasses[color as keyof typeof colorClasses]]:
          color && !styleVariant,
        [styleClass]: styleVariant,
      };
    };

    const classNames = clsx(
      className,
      "inline-flex items-center justify-center gap-2 rounded-full text-center transition-colors",
      getClasses({ styleVariant }),
      "disabled:pointer-events-none disabled:opacity-70",
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
            <span
              className={`inline-block relative -top-0.5 -left-2 -mr-2 ${variantToTextMap[variant as keyof typeof variantToTextMap]}`}
            >
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

const variantClass = (variant: keyof typeof variantToBgMap) =>
  `${variantToBgMap[variant]} ${variantToBgTransparentHoverMap[variant]} text-white`;

const styleClasses = (variant: keyof typeof variantToBgMap) => {
  return {
    outline: `border ${variantToBorderMap[variant]} bg-transparent hover:bg-transparent my-[0.06rem] hover:m-0 ${variantToTextMap[variant]} hover:border-2 dark:border-${variant}`,
    inverted: `${variantToTextMap[variant]} ${colorToBgHoverMap30[variantToColorMap[variant] as keyof typeof colorToBgHoverMap30]} ${colorToBgMap[variantToColorMap[variant] as keyof typeof colorToBgMap]}`,
    ghost: `bg-transparent ${variantToTextMap[variant]} ${colorToBgHoverMap10[variantToColorMap[variant] as keyof typeof colorToBgHoverMap10]}`,
    text: `bg-transparent ${variantToTextMap[variant]} underline underline-offset-2 hover:bg-transparent ${variantToTextHoverMap[variant]}`,
  };
};
