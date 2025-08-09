import clsx from "clsx";
import { tv } from "tailwind-variants";

type LabelProps = React.HTMLAttributes<HTMLLabelElement> & {
  isHidden?: boolean;
  size?: "sm" | "md" | "lg";
  htmlFor?: HTMLLabelElement["htmlFor"];
};

export default function Label({ ...props }: LabelProps) {
  return (
    <label
      className={clsx(
        label({ isHidden: props.isHidden || false, size: props.size || "md" }),
        props.className,
      )}
      {...props}
    />
  );
}

const label = tv({
  base: "block text-sm font-medium text-stone-900 dark:text-white flex items-center",
  variants: {
    isHidden: {
      true: "sr-only",
    },
    size: {
      sm: "text-sm",
      md: "text-base",
      lg: "text-lg",
    },
  },
});
