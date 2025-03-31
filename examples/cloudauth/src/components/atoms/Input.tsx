import { clsx } from "clsx";
import { forwardRef, useId } from "react";

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  // label can be hidden with a "label:sr-only" className
  label: string;
  className?: string;
  id?: string;
}
export const Input = forwardRef<HTMLInputElement, InputProps>(
  ({ label, className, id: customId, ...inputProps }, ref) => {
    const generatedId = useId();
    const id = customId || generatedId;

    const inputClassName = clsx(
      "w-full rounded-md border px-3.5 py-2 shadow-sm",
      "font-medium text-stone-900",
      "dark:text-white dark:bg-stone-925",
    );

    const containerClassName = clsx("grid gap-1", className);

    return (
      <div className={containerClassName}>
        <label htmlFor={id} className="mb-1 text-stone-600 dark:text-stone-300">
          {label}
        </label>

        <input ref={ref} {...inputProps} id={id} className={inputClassName} />
      </div>
    );
  },
);
