"use client";

import * as TooltipPrimitive from "@radix-ui/react-tooltip";
import * as React from "react";

import { cn } from "@/lib/utils";

const TooltipProvider = TooltipPrimitive.Provider;

const Tooltip = TooltipPrimitive.Root;

const TooltipTrigger = TooltipPrimitive.Trigger;

const TooltipContent = React.forwardRef<
  React.ElementRef<typeof TooltipPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof TooltipPrimitive.Content>
>(({ className, sideOffset = 4, children, ...props }, ref) => (
  <TooltipPrimitive.Portal>
    <TooltipPrimitive.Content
      ref={ref}
      sideOffset={sideOffset}
      className={cn(
        "z-50 overflow-hidden rounded-md bg-stone-925 px-3 py-1.5 text-xs text-stone-200",
        className,
      )}
      {...props}
    >
      <TooltipPrimitive.Arrow className="w-3 h-3 fill-stone-925" />
      {children}
    </TooltipPrimitive.Content>
  </TooltipPrimitive.Portal>
));

export { Tooltip, TooltipTrigger, TooltipContent, TooltipProvider };
