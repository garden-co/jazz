import type { AppComputed } from "./jazz.generated.js";

export const computed = {
  rows: { computedValue: (row) => row.valueOf.toUpperCase() },
} satisfies AppComputed;
