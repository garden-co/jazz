import type * as React from "react";
import { cn } from "@/lib/utils";

export function ButtonGroup({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      className={cn(
        "inline-flex items-center rounded-md border shadow-xs",
        "[&>button]:rounded-none [&>button]:border-0 [&>button]:shadow-none",
        "first:[&>button]:rounded-l-md last:[&>button]:rounded-r-md",
        "[&>button+button]:border-l",
        className,
      )}
      {...props}
    />
  );
}
