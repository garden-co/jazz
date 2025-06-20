import { clsx } from "clsx";
import { forwardRef, useId } from "react";
import { Variant } from "../..//utils/variants";
import { variantToActiveBorderMap } from "../../utils/tailwindClassesMap";
import { Button, ButtonProps } from "../atoms/Button";
import { Icon, icons } from "../atoms/Icon";
import { Label } from "../atoms/Label";

export interface InputProps
  extends React.InputHTMLAttributes<HTMLInputElement> {
  label: string;
  className?: string;
  id?: string;
  placeholder?: string;
  icon?: keyof typeof icons;
  iconPosition?: "left" | "right";
  labelHidden?: boolean;
  labelPosition?: "column" | "row";
  button?: ButtonProps;
  variant?: Variant;
}

export const Input = forwardRef<HTMLInputElement, InputProps>(
  (
    {
      label,
      className,
      id: customId,
      placeholder,
      icon,
      iconPosition,
      labelHidden,
      labelPosition,
      button,
      variant = "primary",
      ...inputProps
    },
    ref,
  ) => {
    const generatedId = useId();
    const id = customId || generatedId;

    const inputClassName = clsx(
      "w-full rounded-md border px-3.5 py-2 shadow-sm",
      "font-medium text-stone-900",
      "dark:text-white dark:bg-stone-925",
    );

    return (
      <div
        className={clsx(
          "relative w-full",
          labelPosition === "row" ? "flex flex-row items-center" : "",
        )}
      >
        <Label
          label={label}
          htmlFor={id}
          className={clsx(
            labelPosition === "row" ? "mr-2" : "w-full",
            labelHidden ? "sr-only" : "",
          )}
        />
        <div className={clsx("flex gap-2 w-full items-center")}>
          <input
            ref={ref}
            {...inputProps}
            id={id}
            className={clsx(
              inputClassName,
              iconPosition === "left"
                ? "pl-9"
                : iconPosition === "right"
                  ? "pr-9"
                  : "",
              className,
              "px-2",
              variantToActiveBorderMap[variant],
            )}
            placeholder={placeholder}
          />
          {icon && (
            <Icon
              name={icon}
              className={clsx(
                "absolute",
                iconPosition === "left"
                  ? "left-2"
                  : iconPosition === "right"
                    ? "right-2"
                    : "",
              )}
            />
          )}
          {button && <Button {...button} />}
        </div>
      </div>
    );
  },
);
