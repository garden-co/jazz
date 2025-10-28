import { clsx } from "clsx";
import { forwardRef, useId } from "react";
import { Style, styleToActiveBorderMap } from "../../utils/tailwindClassesMap";
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
  intent?: Style;
}

export const Input = forwardRef<HTMLInputElement, InputProps>(
  (
    {
      label,
      className,
      id: customId,
      placeholder,
      icon,
      iconPosition = "left",
      labelHidden,
      labelPosition,
      button,
      intent = "default",
      ...inputProps
    },
    ref,
  ) => {
    const generatedId = useId();
    const id = customId || generatedId;
    const inputIconClassName =
      icon && iconPosition === "left"
        ? "pl-9"
        : icon && iconPosition === "right";

    const inputClassName = clsx(
      "w-full rounded-md border px-2.5 py-1 shadow-sm h-[36px]",
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
              inputIconClassName,
              className,
              "px-2",
              styleToActiveBorderMap[
                intent as keyof typeof styleToActiveBorderMap
              ],
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
              intent={intent}
            />
          )}
          {button && <Button {...button} />}
        </div>
      </div>
    );
  },
);
