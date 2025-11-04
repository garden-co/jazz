import { Loader2Icon } from "lucide-react";

import { cn } from "@/lib/utils";

const sizeClasses = {
  sm: "size-4",
  md: "size-6",
  lg: "size-8",
} as const;

type SpinnerProps = Omit<React.ComponentProps<"svg">, "size"> & {
  size?: keyof typeof sizeClasses;
};

function Spinner({ className, size = "md", ...props }: SpinnerProps) {
  return (
    <Loader2Icon
      role="status"
      aria-label="Loading"
      className={cn(sizeClasses[size], "animate-spin", className)}
      {...props}
    />
  );
}

export { Spinner };
