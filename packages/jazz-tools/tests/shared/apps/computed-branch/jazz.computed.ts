import type { AppComputed } from "./jazz.generated.js";

export const computed = {
  rows: { value: (row) => row.score ?? -1 },
} satisfies AppComputed;
