import type { AppComputed } from "./jazz.generated.js";

// Writes to its input row.
export const computed = {
  rows: {
    invalid: (value: Readonly<Record<string, unknown>>) => {
      (value as Record<string, unknown>).score = 0;
    },
  },
} as unknown as AppComputed;
