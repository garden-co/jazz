import type { AppComputed } from "./jazz.generated.js";

// Reads its own field.
export const computed = {
  rows: { invalid: (value: Readonly<Record<string, unknown>>) => value.invalid },
} as unknown as AppComputed;
