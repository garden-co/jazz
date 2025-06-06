import { Label as LabelRadix } from "radix-ui";
import * as React from "react";

import { clsx } from "clsx";

export function Label({
  className,
  ...props
}: React.ComponentProps<typeof LabelRadix.Root>) {
  return (
    <LabelRadix.Root
      data-slot="label"
      className={clsx(
        "flex items-center gap-2 text-sm leading-none font-medium select-none group-data-[disabled=true]:pointer-events-none group-data-[disabled=true]:opacity-50 peer-disabled:cursor-not-allowed peer-disabled:opacity-50",
        className,
      )}
      {...props}
    />
  );
}
