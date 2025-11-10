import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { AlertCircle, AlertTriangle, Info } from "lucide-react";

import { cn } from "@/lib/utils";

const alertVariants = cva(
  "relative w-full grid gap-2 rounded border-l-4 pr-4 pl-12 py-3 text-sm",
  {
    variants: {
      variant: {
        info: "border-l-blue-500 bg-blue-50 text-stone-900 dark:bg-blue-200/5 dark:text-white",
        destructive:
          "border-l-red-500 bg-red-500/5 text-red-900 dark:text-red-400",
        warning:
          "border-l-yellow-500 bg-yellow-50 text-stone-900 dark:bg-yellow-200/5 dark:text-white",
      },
    },
    defaultVariants: {
      variant: "info",
    },
  },
);

const variantIcons = {
  destructive: AlertCircle,
  info: Info,
  warning: AlertTriangle,
  default: undefined,
} as const;

const iconColors = {
  destructive: "text-red-500",
  info: "text-blue-500",
  warning: "text-yellow-500",
  default: "text-foreground",
} as const;

const Alert = React.forwardRef<
  HTMLDivElement,
  React.HTMLAttributes<HTMLDivElement> & VariantProps<typeof alertVariants>
>(({ className, variant, children, ...props }, ref) => {
  const normalizedVariant = variant ?? undefined;
  // Use "info" as default when variant is undefined (matches defaultVariants in CVA)
  const effectiveVariant = normalizedVariant || "info";
  const Icon = variantIcons[effectiveVariant];
  const iconColor = iconColors[effectiveVariant];

  return (
    <div
      ref={ref}
      role="alert"
      className={cn(alertVariants({ variant: normalizedVariant }), className)}
      {...props}
    >
      {Icon && (
        <Icon className={cn("absolute top-3 left-4 size-5", iconColor)} />
      )}
      {children}
    </div>
  );
});
Alert.displayName = "Alert";

const AlertTitle = React.forwardRef<
  HTMLParagraphElement,
  React.HTMLAttributes<HTMLHeadingElement>
>(({ className, ...props }, ref) => (
  <h5
    ref={ref}
    className={cn(
      "text-md leading-none font-semibold tracking-tight",
      className,
    )}
    {...props}
  />
));
AlertTitle.displayName = "AlertTitle";

const AlertDescription = React.forwardRef<
  HTMLParagraphElement,
  React.HTMLAttributes<HTMLParagraphElement>
>(({ className, ...props }, ref) => (
  <div
    ref={ref}
    className={cn("text-sm [&_p]:leading-relaxed", className)}
    {...props}
  />
));
AlertDescription.displayName = "AlertDescription";

export { Alert, AlertTitle, AlertDescription };
