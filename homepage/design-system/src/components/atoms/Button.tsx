import { clsx } from "clsx";
import Link from "next/link";
import { forwardRef } from "react";
import {
  Style,
  Variant,
  VariantColor,
  colorToBgActiveMap25,
  colorToBgActiveMap50,
  colorToBgHoverMap10,
  colorToBgHoverMap30,
  colorToBgMap,
  shadowClassesBase,
  sizeClasses,
  styleToBgGradientColorMap,
  styleToBgGradientHoverMap,
  styleToBgTransparentActiveMap,
  styleToBorderMap,
  styleToButtonStateMap,
  styleToColorMap,
  styleToHoverShadowMap,
  styleToTextActiveMap,
  styleToTextHoverMap,
  styleToTextMap,
} from "../../utils/tailwindClassesMap";
import { Icon } from "./Icon";
import type { IconName } from "./Icon";
import { Spinner } from "./Spinner";

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  intent?: Style;
  variant?: Variant;
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
  onClick?: React.MouseEventHandler;
  ariaLabel?: string;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      className,
      children,
      size = "md",
      intent = "default",
      variant,
      href,
      disabled,
      newTab,
      loading,
      loadingText,
      icon,
      iconPosition = "left",
      type = "button",
      onClick,
      ariaLabel,
      ...buttonProps
    },
    ref,
  ) => {
    const styleClass =
      styleClasses(intent, variant)[variant as keyof typeof styleClasses] || "";

    const getClasses = ({ variant }: { variant: string | undefined }) => {
      return {
        [sizeClasses[size as keyof typeof sizeClasses]]: size,
        [variantClass(intent)]: !variant,
        [styleClass]: variant,
      };
    };

    const classNames = clsx(
      "inline-flex items-center justify-center gap-2 rounded-lg text-center transition-colors w-fit text-nowrap",
      getClasses({ variant }),
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
          onClick={onClick}
          aria-label={ariaLabel}
        >
          {icon && (
            <Icon
              name={icon}
              className={`size-5 ${iconPosition === "left" ? "mr-2" : iconPosition === "right" ? "ml-2" : ""}, ${iconVariant(intent, variant)}`}
            />
          )}
          {children}
          {newTab ? (
            <span className="inline-block relative -top-0.5 -left-2 -mr-2">
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
        onClick={onClick}
        aria-label={ariaLabel}
      >
        {loading ? (
          <Spinner className="size-5" />
        ) : (
          icon &&
          iconPosition === "left" && (
            <Icon name={icon} intent={iconVariant(intent, variant)} />
          )
        )}
        {loading && loadingText ? loadingText : children}
        {icon && iconPosition === "right" && (
          <Icon
            name={icon}
            intent={iconVariant(intent, variant)}
            hasHover={true}
          />
        )}
      </button>
    );
  },
);

const iconVariant = (intent: Style, variant: Variant | undefined) => {
  return variant ? intent : intent === "default" ? "default" : "white";
};
const textColorVariant = (style: Style) => {
  return style === "default"
    ? "text-stone-700 dark:text-white hover:text-stone-800 active:text-stone-700 dark:hover:text-stone-100 dark:active:text-stone-200"
    : style === "strong"
      ? "text-stone-100 dark:text-stone-900"
      : "text-white";
};

const variantClass = (intent: Style) =>
  `${styleToBgGradientColorMap[intent]} ${styleToBgGradientHoverMap[intent]} ${textColorVariant(intent)} ${styleToButtonStateMap[intent]} ${shadowClassesBase} shadow-stone-400/20`;

const styleClasses = (intent: Style, variant: Variant | undefined) => {
  return {
    outline: `border ${styleToBorderMap[intent]} ${styleToTextMap[intent]} ${styleToTextHoverMap[intent]} ${styleToHoverShadowMap[intent]} ${styleToBgTransparentActiveMap[intent]} shadow-[5px_0px]`,
    inverted: `${styleToTextMap[intent]} ${colorToBgHoverMap30[styleToColorMap[intent] as VariantColor]} ${colorToBgMap[styleToColorMap[intent] as VariantColor]} ${colorToBgActiveMap50[styleToColorMap[intent] as VariantColor]} ${shadowClassesBase}`,
    ghost: `bg-transparent ${styleToTextMap[intent]} ${colorToBgHoverMap10[styleToColorMap[intent] as VariantColor]} ${colorToBgActiveMap25[styleToColorMap[intent] as VariantColor]}`,
    link: `bg-transparent ${styleToTextMap[intent]} underline underline-offset-2 p-0 hover:bg-transparent ${styleToTextHoverMap[intent]} ${styleToTextActiveMap[intent]} active:underline-stone-500`,
    secondary: variantClass("muted"),
    destructive: variantClass("danger"),
    default: `${styleToBgGradientColorMap["default"]} ${styleToBgGradientHoverMap["default"]} ${textColorVariant("default")} ${styleToButtonStateMap["default"]} ${shadowClassesBase} shadow-stone-400/20`,
  };
};
