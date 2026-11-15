import type { AppComputed } from "./jazz.generated.js";

export const computed = {
  rows: { computedValue: (row) => row.constructor.toUpperCase() },
} satisfies AppComputed;
