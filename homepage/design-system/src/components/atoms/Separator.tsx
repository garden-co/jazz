import clsx from "clsx";
import { forwardRef } from "react";

export const Separator = forwardRef<
  HTMLHRElement,
  React.ComponentPropsWithoutRef<"hr">
>(({ className, ...props }, ref) => {
  return <hr ref={ref} className={clsx("border-t", className)} {...props} />;
});
