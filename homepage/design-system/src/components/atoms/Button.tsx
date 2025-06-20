import { clsx } from "clsx";
import Link from "next/link";
import { forwardRef } from "react";
import {
  VariantColor,
  colorToBgActiveMap25,
  colorToBgActiveMap50,
  colorToBgHoverMap10,
  colorToBgHoverMap30,
  colorToBgMap,
  shadowClassesBase,
  sizeClasses,
  variantToBgGradientColorMap,
  variantToBgGradientHoverMap,
  variantToBgTransparentActiveMap,
  variantToBorderMap,
  variantToButtonStateMap,
  variantToColorMap,
  variantToHoverShadowMap,
  variantToTextActiveMap,
  variantToTextHoverMap,
  variantToTextMap,
} from "../../utils/tailwindClassesMap";
import { Variant } from "../../utils/variants";
import { Icon } from "./Icon";
import type { IconName } from "./Icon";
import { Spinner } from "./Spinner";

export type StyleVariant =
  | "outline"
  | "inverted"
  | "ghost"
  | "text"
  | "default";
export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant;
  styleVariant?: StyleVariant;
  state?: "hover" | "active" | "focus" | "disabled";
  size?: "sm" | "md" | "lg";
  href?: string;
  newTab?: boolean;
  icon?: IconName;
  iconPosition?: "left" | "right" | "center";
  loading?: boolean;
  loadingText?: string;
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
      styleVariant,
      href,
      disabled,
      newTab,
      loading,
      loadingText,
      icon,
      iconPosition = "left",
      type = "button",
      ...buttonProps
    },
    ref,
  ) => {
    const styleClass =
      styleClasses(variant, styleVariant)[
        styleVariant as keyof typeof styleClasses
      ] || "";

    const getClasses = ({
      styleVariant,
    }: { styleVariant: string | undefined }) => {
      return {
        [sizeClasses[size as keyof typeof sizeClasses]]: size,
        [variantClass(variant, styleVariant as StyleVariant | undefined)]:
          !styleVariant,
        [styleClass]: styleVariant,
      };
    };

    const classNames = clsx(
      "inline-flex items-center justify-center gap-2 rounded-lg text-center transition-colors w-fit text-nowrap",
      getClasses({ styleVariant }),
      "disabled:pointer-events-none disabled:opacity-70",
      disabled && "opacity-50 cursor-not-allowed pointer-events-none",
      className,
    );

    if (href) {
      return (
        <Link
          href={href}
          target={newTab ? "_blank" : undefined}
          className={classNames}
        >
          {icon && (
            <Icon
              name={icon}
              className={`size-5 ${iconPosition === "left" ? "mr-2" : iconPosition === "right" ? "ml-2" : ""}, ${iconVariant(variant, styleVariant)}`}
            />
          )}
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
        {loading ? (
          <Spinner className="size-5" />
        ) : (
          icon &&
          iconPosition === "left" && (
            <Icon name={icon} variant={iconVariant(variant, styleVariant)} />
          )
        )}
        {loading && loadingText ? loadingText : children}
        {icon && iconPosition === "right" && (
          <Icon name={icon} variant={iconVariant(variant, styleVariant)} />
        )}
      </button>
    );
  },
);

const iconVariant = (
  variant: Variant,
  styleVariant: StyleVariant | undefined,
) => {
  return styleVariant ? variant : "white";
};

const textColorVariant = (variant: Variant) => {
  return variant === "default" ? "text-black dark:text-white" : "text-white";
};

const variantClass = (
  variant: Variant,
  styleVariant: StyleVariant | undefined,
) =>
  `bg-gradient-to-tr ${variantToBgGradientColorMap[variant]} ${variantToBgGradientHoverMap[variant]} ${textColorVariant(variant)} ${variantToButtonStateMap[variant]} ${shadowClassesBase} shadow-stone-400/20`;

const styleClasses = (
  variant: Variant,
  styleVariant: StyleVariant | undefined,
) => {
  return {
    outline: `border ${variantToBorderMap[variant]} ${variantToTextMap[variant]} ${variantToHoverShadowMap[variant]} ${variantToBgTransparentActiveMap[variant]} shadow-[5px_0px]`,
    inverted: `${variantToTextMap[variant]} ${colorToBgHoverMap30[variantToColorMap[variant] as VariantColor]} ${colorToBgMap[variantToColorMap[variant] as VariantColor]} ${colorToBgActiveMap50[variantToColorMap[variant] as VariantColor]} ${shadowClassesBase}`,
    ghost: `bg-transparent ${variantToTextMap[variant]} ${colorToBgHoverMap10[variantToColorMap[variant] as VariantColor]} ${colorToBgActiveMap25[variantToColorMap[variant] as VariantColor]}`,
    text: `bg-transparent ${variantToTextMap[variant]} underline underline-offset-2 p-0 hover:bg-transparent ${variantToTextHoverMap[variant]} ${variantToTextActiveMap[variant]} active:underline-stone-500`,
  };
};
