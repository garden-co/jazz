import clsx from "clsx";
import { forwardRef } from "react";

export const Separator = forwardRef<
  HTMLDivElement,
  React.ComponentPropsWithoutRef<"hr">
>(({ className, ...props }, ref) => {
  return (
    <div
      ref={ref}
      role="none"
      className={clsx("border-t", className)}
      {...props}
    />
  );
});
