import type { AppComputed } from "./jazz.generated.js";

// Returns a promise.
export const computed = {
  rows: { invalid: () => Promise.resolve(1) },
} as unknown as AppComputed;
