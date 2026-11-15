import type { AppComputed } from "./jazz.generated.js";

export const computed = {
  rows: { value: (row) => (row.useLeft ? row.left : row.right) },
} satisfies AppComputed;
