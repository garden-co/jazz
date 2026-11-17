import type { AppComputed } from "./jazz.generated.js";

// Reads a column `from` doesn't name.
export const computed = {
  rows: { invalid: (value: Readonly<Record<string, unknown>>) => value.missing },
} as unknown as AppComputed;
