import type * as React from "react";
import { cn } from "@/lib/utils";

// The chat-react Item/ItemContent presentation, without unused slot/variant machinery.
export function Item({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="item"
      className={cn(
        "group/item flex items-center border border-border text-sm rounded-md transition-colors flex-wrap p-4 gap-4",
        className,
      )}
      {...props}
    />
  );
}
export function ItemContent({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="item-content"
      className={cn("flex flex-1 flex-col gap-1", className)}
      {...props}
    />
  );
}
